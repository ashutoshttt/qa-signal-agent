"""
Indeed India scraper — uses Google site: search (in.indeed.com/viewjob) to discover
job URLs, since Indeed blocks direct scraping with 403/captcha pages.
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
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

INDEED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://in.indeed.com/",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=15))
def _google_search_indeed(keyword: str, session: requests.Session) -> list[str]:
    """Return Indeed India job URLs via Google site: search (last 24h)."""
    query = f'site:in.indeed.com/viewjob "{keyword}"'
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
        if "in.indeed.com/viewjob" in href or "in.indeed.com/rc/clk" in href:
            clean = href.split("&")[0] if "?" in href else href
            if clean not in job_urls:
                job_urls.append(clean)

    return job_urls


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=8))
def _fetch_indeed_job(url: str, session: requests.Session) -> Optional[dict]:
    """Fetch one Indeed job page and extract title/company/location."""
    resp = session.get(url, headers=INDEED_HEADERS, timeout=15)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if data.get("@type") == "JobPosting":
                company = ""
                org = data.get("hiringOrganization", {})
                if isinstance(org, dict):
                    company = org.get("name", "")
                location = ""
                loc = data.get("jobLocation", {})
                if isinstance(loc, dict):
                    location = loc.get("address", {}).get("addressLocality", "")
                elif isinstance(loc, list) and loc:
                    location = loc[0].get("address", {}).get("addressLocality", "")
                return {
                    "title": data.get("title", ""),
                    "company": company,
                    "location": location or "India",
                }
        except (json.JSONDecodeError, AttributeError):
            continue

    # HTML fallback
    title_el = soup.select_one(
        "h1.jobsearch-JobInfoHeader-title, h1[class*='jobTitle'], "
        "[data-testid='jobsearch-JobInfoHeader-title']"
    )
    company_el = soup.select_one(
        "[data-testid='inlineHeader-companyName'], "
        "div[class*='companyName'], span.companyName"
    )
    location_el = soup.select_one(
        "[data-testid='job-location'], div[class*='companyLocation']"
    )

    title = title_el.get_text(strip=True) if title_el else ""
    company = company_el.get_text(strip=True) if company_el else ""
    location = location_el.get_text(strip=True) if location_el else "India"

    if not title:
        return None
    return {"title": title, "company": company, "location": location}


def scrape_indeed() -> list[dict]:
    """Scrape Indeed India via Google site: search + job page fetch. India only."""
    all_jobs = []
    session = requests.Session()

    for keyword in KEYWORDS:
        try:
            urls = _google_search_indeed(keyword, session)
            logger.info("Indeed [%s]: found %d URLs via Google", keyword, len(urls))
        except Exception as e:
            logger.warning("Google site: search failed for Indeed [%s]: %s", keyword, e)
            continue

        for url in urls[:15]:
            try:
                job_data = _fetch_indeed_job(url, session)
                if not job_data or not job_data.get("title"):
                    continue
                if not is_india_location(job_data.get("location", "")):
                    logger.debug("Skipping non-India Indeed job: %s (%s)", job_data["title"], job_data.get("location"))
                    continue
                all_jobs.append(
                    {
                        **job_data,
                        "link": url,
                        "source": "Indeed",
                        "keyword": keyword,
                    }
                )
            except Exception as e:
                logger.debug("Failed to fetch Indeed job %s: %s", url, e)
            time.sleep(0.5)

        time.sleep(4)  # longer delay — 34 keywords hits Google rate limits fast

    seen = set()
    unique = []
    for job in all_jobs:
        if job["link"] not in seen:
            seen.add(job["link"])
            unique.append(job)

    logger.info("Indeed total unique India jobs: %d", len(unique))
    return unique
