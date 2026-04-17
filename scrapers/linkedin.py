"""
LinkedIn Jobs scraper for QA roles in India.
Uses public LinkedIn job search (no auth required).
"""

import time
import logging
from typing import Optional
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from .constants import KEYWORDS, is_india_location

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_page(url: str, session: requests.Session) -> Optional[str]:
    resp = session.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def _parse_jobs(html: str, keyword: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    jobs = []

    cards = soup.select("div.base-card, li.jobs-search__results-list > div")
    if not cards:
        cards = soup.select("li.result-card, div[data-entity-urn]")

    for card in cards:
        try:
            title_el = card.select_one(
                "h3.base-search-card__title, h3.result-card__title, span.sr-only"
            )
            company_el = card.select_one(
                "h4.base-search-card__subtitle, h4.result-card__subtitle, a.result-card__subtitle-link"
            )
            location_el = card.select_one(
                "span.job-search-card__location, span.result-card__location"
            )
            link_el = card.select_one("a.base-card__full-link, a.result-card__full-card-link")

            title = title_el.get_text(strip=True) if title_el else ""
            company = company_el.get_text(strip=True) if company_el else ""
            location = location_el.get_text(strip=True) if location_el else ""
            link = link_el["href"].split("?")[0] if link_el and link_el.get("href") else ""

            if not title or not company or not link:
                continue

            if not is_india_location(location):
                logger.debug("Skipping non-India job: %s @ %s (%s)", title, company, location)
                continue

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "link": link,
                    "source": "LinkedIn",
                    "keyword": keyword,
                }
            )
        except Exception as e:
            logger.debug("Error parsing LinkedIn card: %s", e)
            continue

    return jobs


def fetch_job_description(link: str, session: Optional[requests.Session] = None) -> str:
    """
    Fetch the full description text for a single LinkedIn job page.
    Returns plain text or empty string on failure.
    Used to extract tech stack keywords for high-priority companies.
    """
    try:
        s = session or requests.Session()
        resp = s.get(link, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "lxml")
        desc_el = soup.select_one(
            "div.description__text, div.show-more-less-html__markup, "
            "section.description div"
        )
        return desc_el.get_text(separator=" ", strip=True) if desc_el else ""
    except Exception as e:
        logger.debug("Failed to fetch job description for %s: %s", link, e)
        return ""


def scrape_linkedin() -> list[dict]:
    """Scrape LinkedIn Jobs for QA roles in India. Returns list of job dicts."""
    all_jobs = []
    session = requests.Session()

    for keyword in KEYWORDS:
        url = (
            "https://www.linkedin.com/jobs/search/"
            f"?keywords={requests.utils.quote(keyword)}"
            "&location=India"
            "&f_TPR=r86400"   # posted in last 24h
            "&start=0"
        )
        try:
            html = _fetch_page(url, session)
            jobs = _parse_jobs(html, keyword)
            logger.info("LinkedIn [%s]: %d India jobs", keyword, len(jobs))
            all_jobs.extend(jobs)
            time.sleep(2)
        except Exception as e:
            logger.warning("LinkedIn scrape failed for '%s': %s", keyword, e)

    seen = set()
    unique = []
    for job in all_jobs:
        if job["link"] not in seen:
            seen.add(job["link"])
            unique.append(job)

    logger.info("LinkedIn total unique India jobs: %d", len(unique))
    return unique
