"""
Naukri.com scraper — uses Google site: search to discover Naukri job URLs,
then fetches each listing page for structured data.
"""

import time
import logging
import json
from typing import Optional
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from .constants import KEYWORDS, is_india_location

logger = logging.getLogger(__name__)

GOOGLE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

NAUKRI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.naukri.com/",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=15))
def _google_search(keyword: str, session: requests.Session) -> list[str]:
    """Return Naukri job URLs found via Google site: search (last 24h)."""
    query = f'site:naukri.com/job-listings "{keyword}" india'
    url = (
        "https://www.google.com/search"
        f"?q={requests.utils.quote(query)}"
        "&tbs=qdr:d"
        "&num=20"
        "&hl=en"
    )
    resp = session.get(url, headers=GOOGLE_HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    job_urls = []

    for a in soup.select("a[href]"):
        href = a["href"]
        if "/url?q=" in href:
            inner = href.split("/url?q=")[1].split("&")[0]
            href = requests.utils.unquote(inner)
        if "naukri.com/job-listings" in href:
            clean = href.split("?")[0]
            if clean not in job_urls:
                job_urls.append(clean)

    return job_urls


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=8))
def _fetch_naukri_job(url: str, session: requests.Session) -> Optional[dict]:
    """Fetch a single Naukri job listing page and extract structured data."""
    resp = session.get(url, headers=NAUKRI_HEADERS, timeout=15)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if data.get("@type") == "JobPosting":
                company = ""
                if isinstance(data.get("hiringOrganization"), dict):
                    company = data["hiringOrganization"].get("name", "")
                elif isinstance(data.get("hiringOrganization"), str):
                    company = data["hiringOrganization"]

                location = ""
                if isinstance(data.get("jobLocation"), dict):
                    location = (
                        data["jobLocation"].get("address", {}).get("addressLocality", "")
                        or data["jobLocation"].get("address", {}).get("addressRegion", "")
                    )
                elif isinstance(data.get("jobLocation"), list) and data["jobLocation"]:
                    loc = data["jobLocation"][0]
                    if isinstance(loc, dict):
                        location = loc.get("address", {}).get("addressLocality", "")

                return {
                    "title": data.get("title", ""),
                    "company": company,
                    "location": location or "India",
                }
        except (json.JSONDecodeError, AttributeError):
            continue

    # HTML fallback
    title_el = soup.select_one("h1.jd-header-title, h1[class*='title'], .jd-header h1")
    company_el = soup.select_one("a.jd-header-comp-name, .jd-header-comp-name, [class*='comp-name']")
    location_el = soup.select_one("li[class*='location'], span[class*='location'], .loc")

    title = title_el.get_text(strip=True) if title_el else ""
    company = company_el.get_text(strip=True) if company_el else ""
    location = location_el.get_text(strip=True) if location_el else "India"

    if not title:
        return None

    return {"title": title, "company": company, "location": location}


def scrape_naukri() -> list[dict]:
    """Scrape Naukri via Google site: search + individual page fetch. India only."""
    all_jobs = []
    session = requests.Session()

    for keyword in KEYWORDS:
        try:
            urls = _google_search(keyword, session)
            logger.info("Naukri [%s]: found %d URLs via Google", keyword, len(urls))
        except Exception as e:
            logger.warning("Google site: search failed for Naukri [%s]: %s", keyword, e)
            continue

        for url in urls[:15]:
            try:
                job_data = _fetch_naukri_job(url, session)
                if not job_data or not job_data.get("title"):
                    continue
                if not is_india_location(job_data.get("location", "")):
                    logger.debug("Skipping non-India Naukri job: %s (%s)", job_data["title"], job_data.get("location"))
                    continue
                all_jobs.append(
                    {
                        **job_data,
                        "link": url,
                        "source": "Naukri",
                        "keyword": keyword,
                    }
                )
            except Exception as e:
                logger.debug("Failed to fetch Naukri job %s: %s", url, e)
            time.sleep(0.5)

        time.sleep(4)  # longer delay — 34 keywords hits Google rate limits fast

    seen = set()
    unique = []
    for job in all_jobs:
        if job["link"] not in seen:
            seen.add(job["link"])
            unique.append(job)

    logger.info("Naukri total unique India jobs: %d", len(unique))
    return unique
