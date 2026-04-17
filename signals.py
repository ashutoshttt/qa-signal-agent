"""
Company signals enrichment — no external APIs required.

For each company, gathers:
  1. Recent funding     — Google News search for funding mentions
  2. Tech stack         — scrape company website for known tools/frameworks
  3. Leadership hiring  — query our own jobs.db for past 180 days
  4. Headcount          — from Apollo enrichment (already collected)
"""

import logging
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "jobs.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Tech stack detection patterns ─────────────────────────────────────────────

TECH_PATTERNS = {
    # Test frameworks
    "Selenium":      [r"selenium"],
    "Cypress":       [r"cypress"],
    "Playwright":    [r"playwright"],
    "Appium":        [r"appium"],
    "Jest":          [r"jest"],
    "PyTest":        [r"pytest"],
    "TestNG":        [r"testng"],
    "JUnit":         [r"junit"],
    # CI/CD
    "GitHub Actions":[r"github\.com/[^/]+/[^/]+/actions", r"github actions"],
    "Jenkins":       [r"jenkins"],
    "CircleCI":      [r"circleci"],
    "GitLab CI":     [r"gitlab-ci", r"\.gitlab-ci"],
    # Cloud
    "AWS":           [r"amazonaws\.com", r"aws\.amazon"],
    "GCP":           [r"googleapis\.com", r"cloud\.google"],
    "Azure":         [r"azure\.com", r"azurewebsites"],
    # Frontend
    "React":         [r"react(?:\.min)?\.js", r"reactjs"],
    "Angular":       [r"angular(?:\.min)?\.js"],
    "Vue":           [r"vue(?:\.min)?\.js"],
    # Backend
    "Node.js":       [r"node(?:js)?"],
    "Python":        [r"python"],
    "Java":          [r"\bjava\b"],
    "Go":            [r"\bgolang\b"],
    # Monitoring
    "Datadog":       [r"datadog"],
    "Sentry":        [r"sentry\.io"],
    "New Relic":     [r"newrelic"],
}

LEADERSHIP_TITLES = [
    "vp engineering", "vp of engineering", "vice president engineering",
    "cto", "chief technology officer",
    "head of engineering", "director of engineering",
    "engineering director", "head of product", "vp product",
    "chief product officer", "cpo",
]


# ── Signal 1: Recent Funding ───────────────────────────────────────────────────

def get_funding_signal(company_name: str) -> Optional[str]:
    """
    Search Google News RSS for recent funding/IPO mentions.
    Returns a headline string (with source + date) or None.
    """
    FUNDING_KEYWORDS = [
        "funding", "raises", "raised", "series a", "series b", "series c",
        "series d", "series e", "ipo", "investment", "crore", "million",
    ]
    try:
        query = f'"{company_name}" funding OR raised OR Series OR IPO'
        url = (
            "https://news.google.com/rss/search"
            f"?q={requests.utils.quote(query)}"
            "&hl=en-IN&gl=IN&ceid=IN:en"
        )
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return None

        root = ET.fromstring(resp.text)
        items = root.findall(".//item")

        # Look for articles in the last 12 months
        cutoff = datetime.now(timezone.utc) - timedelta(days=365)

        for item in items[:10]:
            title   = item.findtext("title", "").strip()
            pub_raw = item.findtext("pubDate", "")

            # Strip source suffix (e.g. " - TechCrunch" or " - Groww") BEFORE matching
            # so we don't falsely match company name that appears only as a news source
            headline_only = re.sub(r"\s*[-–]\s*[^-–]+$", "", title).strip()
            title_lower = headline_only.lower()

            # Must mention company and a funding keyword
            if company_name.lower() not in title_lower:
                continue
            if not any(kw in title_lower for kw in FUNDING_KEYWORDS):
                continue

            # Try to parse and check date
            try:
                pub_dt = datetime.strptime(pub_raw[:25], "%a, %d %b %Y %H:%M")
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue
                date_str = pub_dt.strftime("%b %Y")
            except Exception:
                date_str = pub_raw[:11] if pub_raw else ""

            return f"{headline_only[:100]} ({date_str})" if date_str else headline_only[:110]

        return None
    except Exception as e:
        logger.debug("Funding signal failed for '%s': %s", company_name, e)
        return None


# ── Signal 2: Product / Growth News ───────────────────────────────────────────

def get_product_signal(company_name: str) -> Optional[str]:
    """
    Search Google News RSS for product launches, growth, or expansion news.
    Returns a headline string or None.
    """
    PRODUCT_KEYWORDS = [
        "launch", "launches", "launched", "release", "releases",
        "expands", "expansion", "growth", "new product", "new feature",
        "partnership", "announces", "unveiled",
    ]
    try:
        query = f'"{company_name}" launch OR release OR expansion OR growth OR announces'
        url = (
            "https://news.google.com/rss/search"
            f"?q={requests.utils.quote(query)}"
            "&hl=en-IN&gl=IN&ceid=IN:en"
        )
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return None

        root = ET.fromstring(resp.text)
        items = root.findall(".//item")
        cutoff = datetime.now(timezone.utc) - timedelta(days=180)

        for item in items[:10]:
            title   = item.findtext("title", "").strip()
            pub_raw = item.findtext("pubDate", "")

            # Strip source suffix before matching
            headline_only = re.sub(r"\s*[-–]\s*[^-–]+$", "", title).strip()
            title_lower = headline_only.lower()

            if company_name.lower() not in title_lower:
                continue
            if not any(kw in title_lower for kw in PRODUCT_KEYWORDS):
                continue

            try:
                pub_dt = datetime.strptime(pub_raw[:25], "%a, %d %b %Y %H:%M")
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue
                date_str = pub_dt.strftime("%b %Y")
            except Exception:
                date_str = ""

            return f"{headline_only[:100]} ({date_str})" if date_str else headline_only[:110]

        return None
    except Exception as e:
        logger.debug("Product signal failed for '%s': %s", company_name, e)
        return None


# ── Signal 3: Tech Stack ───────────────────────────────────────────────────────

def get_tech_stack_from_description(description: str) -> list[str]:
    """
    Parse a job description text for known tech/tool mentions.
    More reliable than website scraping — job postings explicitly list required tools.
    """
    if not description:
        return []
    detected = []
    content = description.lower()
    for tech, patterns in TECH_PATTERNS.items():
        if any(re.search(p, content, re.IGNORECASE) for p in patterns):
            detected.append(tech)
    return detected


def get_tech_stack(website_url: str) -> list[str]:
    """
    Scrape a company's website and detect known technologies.
    Returns list of detected tech names.
    """
    if not website_url:
        return []

    # Normalise URL
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    try:
        resp = requests.get(website_url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return []

        content = resp.text.lower()
        detected = []

        for tech, patterns in TECH_PATTERNS.items():
            if any(re.search(p, content, re.IGNORECASE) for p in patterns):
                detected.append(tech)

        return detected

    except Exception as e:
        logger.debug("Tech stack detection failed for '%s': %s", website_url, e)
        return []


# ── Signal 3: Leadership Hiring (from our DB) ──────────────────────────────────

def get_leadership_signal(company_name: str) -> Optional[str]:
    """
    Query jobs.db for leadership-level roles at this company in last 180 days.
    Returns a summary string or None.
    """
    if not DB_PATH.exists():
        return None

    cutoff = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()

    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT title, first_seen FROM jobs
                WHERE  company = ?
                AND    first_seen >= ?
                ORDER  BY first_seen DESC
                """,
                (company_name, cutoff),
            ).fetchall()

        if not rows:
            return None

        leadership_roles = []
        for title, seen in rows:
            title_lower = title.lower()
            if any(lt in title_lower for lt in LEADERSHIP_TITLES):
                # Format date nicely
                try:
                    dt = datetime.fromisoformat(seen.replace("Z", "+00:00"))
                    months_ago = (datetime.now(timezone.utc) - dt).days // 30
                    when = f"{months_ago}mo ago" if months_ago > 0 else "this month"
                except Exception:
                    when = "recently"
                leadership_roles.append(f"{title} ({when})")

        if leadership_roles:
            return ", ".join(leadership_roles[:2])

        return None

    except Exception as e:
        logger.debug("Leadership signal failed for '%s': %s", company_name, e)
        return None


# ── Signal 4: Repeat Hiring (hiring QA repeatedly = scaling) ──────────────────

def get_repeat_hiring_signal(company_name: str) -> Optional[str]:
    """
    Check if this company has appeared in our DB before (repeat hiring = scaling team).
    Returns a signal string or None.
    """
    if not DB_PATH.exists():
        return None

    cutoff_180 = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()
    cutoff_7   = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Total historical QA roles at this company
            total = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE company = ?",
                (company_name,),
            ).fetchone()[0]

            # Roles in last 180 days excluding last 7 (i.e. previously seen)
            prev = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE company = ? AND first_seen >= ? AND first_seen < ?",
                (company_name, cutoff_180, cutoff_7),
            ).fetchone()[0]

        if total > 10:
            return f"Recurring hirer — {total} QA roles seen in DB"
        elif prev > 0:
            return f"Hired QA {prev} time(s) in last 6 months"
        return None

    except Exception as e:
        logger.debug("Repeat hiring signal failed for '%s': %s", company_name, e)
        return None


# ── Main enrichment function ───────────────────────────────────────────────────

def enrich_signals(company_groups: dict[str, dict]) -> dict[str, dict]:
    """
    Add signals to each company's enrichment dict.

    Input:  {company_name: enrichment_dict}
      enrichment_dict may include:
        - apollo_url:       company website (from Apollo)
        - job_descriptions: list[str] of raw description texts for that company's jobs
    Output: same dict with added keys:
      - funding:          str or None   (Google News RSS)
      - product:          str or None   (Google News RSS — launches/growth)
      - tech_stack:       list[str]     (from job descriptions + website)
      - leadership:       str or None   (from jobs.db)
      - repeat_hiring:    str or None   (from jobs.db)
    """
    results = {}

    for i, (company_name, enrichment) in enumerate(company_groups.items(), 1):
        logger.info("Signals %d/%d: %s", i, len(company_groups), company_name)
        website      = enrichment.get("apollo_url", "")
        descriptions = enrichment.get("job_descriptions", [])

        # Tech stack: parse job descriptions first (most reliable), fallback to website
        tech_from_desc = []
        for desc in descriptions:
            for t in get_tech_stack_from_description(desc):
                if t not in tech_from_desc:
                    tech_from_desc.append(t)

        tech_from_web = get_tech_stack(website) if website else []

        # Merge — description findings take priority, then add any extras from website
        tech_stack = tech_from_desc + [t for t in tech_from_web if t not in tech_from_desc]

        # Other signals
        funding       = get_funding_signal(company_name)
        product       = get_product_signal(company_name)
        leadership    = get_leadership_signal(company_name)
        repeat_hiring = get_repeat_hiring_signal(company_name)

        enrichment["funding"]       = funding
        enrichment["product"]       = product
        enrichment["tech_stack"]    = tech_stack
        enrichment["leadership"]    = leadership
        enrichment["repeat_hiring"] = repeat_hiring

        logger.info(
            "  funding=%s | product=%s | stack=%d | leadership=%s | repeat=%s",
            "✓" if funding else "-",
            "✓" if product else "-",
            len(tech_stack),
            "✓" if leadership else "-",
            "✓" if repeat_hiring else "-",
        )

        results[company_name] = enrichment
        time.sleep(1.5)   # polite delay between companies

    return results
