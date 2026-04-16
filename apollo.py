"""
Apollo.io enrichment module.

For each company:
  1. Search Apollo for the company → get employee count
  2. If employee count > 5000 → skip (too large)
  3. If ≤ 5000 → search for QA decision-maker contacts at that company
"""

import logging
import os
from typing import Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

APOLLO_BASE = "https://api.apollo.io/v1"

# Titles to search for in Apollo (in priority order)
QA_TITLES = [
    "Head of QA",
    "Head of Quality",
    "Head of Testing",
    "Director of QA",
    "Director of Quality Assurance",
    "VP of Quality",
    "QA Manager",
    "Test Manager",
    "Engineering Manager QA",
    "QA Lead",
    "Test Lead",
]

LARGE_COMPANY_THRESHOLD = 5000


def _headers(api_key: str) -> dict:
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _search_company(name: str, api_key: str) -> Optional[dict]:
    """
    Search Apollo for a company by name.
    Returns the best-match company dict or None.
    """
    resp = requests.post(
        f"{APOLLO_BASE}/mixed_companies/search",
        headers=_headers(api_key),
        json={
            "q_organization_name": name,
            "page": 1,
            "per_page": 1,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        logger.debug("Apollo company search failed for '%s': %s", name, resp.status_code)
        return None

    data = resp.json()
    orgs = data.get("organizations") or data.get("accounts") or []
    return orgs[0] if orgs else None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _search_contacts(company_name: str, apollo_org_id: str, api_key: str) -> list[dict]:
    """
    Search Apollo for QA decision-maker contacts at a given company.
    Returns list of contact dicts.
    """
    resp = requests.post(
        f"{APOLLO_BASE}/mixed_people/search",
        headers=_headers(api_key),
        json={
            "q_organization_name": company_name,
            "organization_ids": [apollo_org_id] if apollo_org_id else [],
            "person_titles": QA_TITLES,
            "page": 1,
            "per_page": 5,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        logger.debug("Apollo contact search failed for '%s': %s", company_name, resp.status_code)
        return []

    data = resp.json()
    return data.get("people") or []


def _extract_contact(person: dict) -> dict:
    """Flatten an Apollo person record into a clean contact dict."""
    email = ""
    # Apollo returns email in different fields depending on plan/reveal status
    if person.get("email"):
        email = person["email"]
    elif person.get("email_status") == "verified" and person.get("email"):
        email = person["email"]

    return {
        "name": person.get("name", ""),
        "title": person.get("title", ""),
        "email": email,
        "linkedin_url": person.get("linkedin_url", ""),
        "city": person.get("city", ""),
    }


def enrich_companies(company_groups: dict[str, list[dict]]) -> dict[str, dict]:
    """
    Enrich a dict of {company_name: [job, ...]} with Apollo data.

    Returns dict of {company_name: enrichment_dict} where enrichment_dict has:
      - employee_count: int or None
      - too_large: bool
      - contacts: list of contact dicts (empty if too_large or not found)
      - apollo_url: str (Apollo company profile URL)
    """
    api_key = os.environ.get("APOLLO_API_KEY", "")
    if not api_key:
        logger.warning("APOLLO_API_KEY not set — skipping enrichment")
        return {name: {"employee_count": None, "too_large": False, "contacts": [], "apollo_url": ""}
                for name in company_groups}

    results = {}

    for company_name in company_groups:
        logger.info("Apollo: enriching '%s'", company_name)
        enrichment = {
            "employee_count": None,
            "too_large": False,
            "contacts": [],
            "apollo_url": "",
        }

        try:
            org = _search_company(company_name, api_key)

            if not org:
                logger.info("Apollo: no match found for '%s'", company_name)
                results[company_name] = enrichment
                continue

            employee_count = (
                org.get("estimated_num_employees")
                or org.get("num_employees")
                or org.get("employee_count")
            )
            enrichment["employee_count"] = employee_count
            enrichment["apollo_url"] = org.get("website_url", "")

            if employee_count and employee_count > LARGE_COMPANY_THRESHOLD:
                logger.info(
                    "Apollo: '%s' has %d employees — skipping contact enrichment",
                    company_name, employee_count,
                )
                enrichment["too_large"] = True
                results[company_name] = enrichment
                continue

            # Fetch QA contacts
            org_id = org.get("id", "")
            contacts_raw = _search_contacts(company_name, org_id, api_key)
            enrichment["contacts"] = [_extract_contact(p) for p in contacts_raw]
            logger.info(
                "Apollo: '%s' (%s employees) → %d contacts found",
                company_name,
                employee_count or "unknown",
                len(enrichment["contacts"]),
            )

        except Exception as e:
            logger.error("Apollo enrichment failed for '%s': %s", company_name, e)

        results[company_name] = enrichment

    return results
