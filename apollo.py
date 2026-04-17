"""
Contact enrichment module.

Flow per company:
  1. Apollo  — company search → get employee count + website domain
  2. If > 5000 employees → skip
  3. Hunter  — domain search → find QA contacts (name + title + email)
  4. Apollo  — people/match → enrich each contact with LinkedIn URL + phone

Requires:
  APOLLO_API_KEY  — Apollo.io API key (Basic plan)
  HUNTER_API_KEY  — Hunter.io API key (Free/Starter plan)
"""

import logging
import os
import time
from typing import Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

APOLLO_BASE = "https://api.apollo.io/v1"
HUNTER_BASE = "https://api.hunter.io/v2"

LARGE_COMPANY_THRESHOLD = 5000

# QA-related title keywords to match against Hunter results
QA_TITLE_KEYWORDS = [
    "qa", "quality", "test", "testing", "sdet", "automation",
    "engineer in test", "quality assurance", "quality engineer",
]


def _apollo_headers(api_key: str) -> dict:
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }


# ── Step 1: Apollo company search ─────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _apollo_search_company(name: str, api_key: str) -> Optional[dict]:
    """Find a company in Apollo. Returns org dict with domain + employee count."""
    resp = requests.post(
        f"{APOLLO_BASE}/organizations/search",
        headers=_apollo_headers(api_key),
        json={"q_organization_name": name, "page": 1, "per_page": 1},
        timeout=15,
    )
    if resp.status_code == 200:
        orgs = resp.json().get("organizations", [])
        if orgs:
            return orgs[0]

    # Fallback
    resp2 = requests.post(
        f"{APOLLO_BASE}/mixed_companies/search",
        headers=_apollo_headers(api_key),
        json={"q_organization_name": name, "page": 1, "per_page": 1},
        timeout=15,
    )
    if resp2.status_code == 200:
        orgs2 = resp2.json().get("organizations") or resp2.json().get("accounts") or []
        if orgs2:
            return orgs2[0]

    logger.debug("Apollo: company not found for '%s'", name)
    return None


# ── Step 2: Hunter domain search ──────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _hunter_domain_search(domain: str, api_key: str) -> list[dict]:
    """
    Search Hunter.io for all emails at a domain.
    Returns list of person dicts filtered to QA-related titles.
    """
    resp = requests.get(
        f"{HUNTER_BASE}/domain-search",
        params={
            "domain": domain,
            "api_key": api_key,
            "limit": 20,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        logger.debug("Hunter domain search failed for '%s': %s", domain, resp.status_code)
        return []

    emails = resp.json().get("data", {}).get("emails", [])

    # Priority 1: QA-specific title keywords
    qa_contacts = [
        e for e in emails
        if any(kw in (e.get("position") or "").lower() for kw in QA_TITLE_KEYWORDS)
    ]

    # Priority 2: Engineering/IT/Product/Technology department
    if not qa_contacts:
        qa_contacts = [
            e for e in emails
            if (e.get("department") or "").lower() in ("engineering", "it", "product", "technology")
        ]

    # Priority 3: Senior/executive/director/manager by seniority field
    if not qa_contacts:
        qa_contacts = [
            e for e in emails
            if (e.get("seniority") or "").lower() in ("senior", "executive", "director", "manager")
        ]

    # Fallback: return top 3 — better to have someone than no one
    if not qa_contacts:
        qa_contacts = emails[:3]

    logger.debug(
        "Hunter '%s': %d total emails, %d QA-relevant",
        domain, len(emails), len(qa_contacts),
    )
    return qa_contacts[:5]


# ── Step 3: Apollo people/match for LinkedIn + phone ─────────────────────────

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=8))
def _apollo_match_person(
    first_name: str,
    last_name: str,
    email: str,
    company_domain: str,
    api_key: str,
) -> dict:
    """
    Enrich a person via Apollo people/match using their email.
    Returns dict with linkedin_url and phone (when available).
    Uses export credits for phone/email reveal.
    """
    resp = requests.post(
        f"{APOLLO_BASE}/people/match",
        headers=_apollo_headers(api_key),
        json={
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "organization_name": company_domain,
            "reveal_personal_emails": False,
            "reveal_phone_number": True,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        return {}

    person = resp.json().get("person", {})
    return {
        "linkedin_url": person.get("linkedin_url", ""),
        "phone": _extract_phone(person),
    }


def _extract_phone(person: dict) -> str:
    """Extract best available phone from Apollo person record."""
    # Try sanitized phone numbers first
    for ph in person.get("phone_numbers", []):
        if ph.get("sanitized_number"):
            return ph["sanitized_number"]
    return person.get("phone_numbers", [{}])[0].get("raw_number", "") if person.get("phone_numbers") else ""


# ── Main enrichment function ───────────────────────────────────────────────────

def enrich_companies(company_groups: dict[str, list[dict]]) -> dict[str, dict]:
    """
    Enrich companies with contact data.

    For each company:
      - Apollo: get employee count + domain
      - Skip if > 5000 employees
      - Hunter: find QA contacts by domain (name + title + email)
      - Apollo match: add LinkedIn URL + phone per contact

    Returns {company_name: enrichment_dict}
    """
    apollo_key = os.environ.get("APOLLO_API_KEY", "")
    hunter_key = os.environ.get("HUNTER_API_KEY", "")

    if not apollo_key and not hunter_key:
        logger.warning("No APOLLO_API_KEY or HUNTER_API_KEY — skipping enrichment")
        return {
            name: {
                "employee_count": None, "too_large": False, "contacts": [],
                "apollo_url": "", "industry": "", "founded_year": None, "funding_stage": "",
            }
            for name in company_groups
        }

    results = {}

    for company_name in company_groups:
        logger.info("Enriching '%s'", company_name)
        enrichment = {
            "employee_count": None,
            "too_large": False,
            "contacts": [],
            "apollo_url": "",
            "industry": "",
            "founded_year": None,
            "funding_stage": "",
        }

        try:
            # ── Step 1: Apollo company lookup ──────────────────────────────
            domain = None
            if apollo_key:
                org = _apollo_search_company(company_name, apollo_key)
                if org:
                    employee_count = (
                        org.get("estimated_num_employees")
                        or org.get("num_employees")
                        or org.get("employee_count")
                    )
                    enrichment["employee_count"]  = employee_count
                    enrichment["apollo_url"]      = org.get("website_url", "") or ""
                    enrichment["industry"]        = org.get("industry", "") or ""
                    enrichment["founded_year"]    = org.get("founded_year") or org.get("year_founded")
                    enrichment["funding_stage"]   = org.get("latest_funding_stage") or org.get("funding_stage") or ""
                    domain = (
                        org.get("primary_domain")
                        or org.get("website_url", "").replace("https://", "").replace("http://", "").split("/")[0]
                    )

                    if employee_count and employee_count > LARGE_COMPANY_THRESHOLD:
                        logger.info("'%s' has %d employees — skipping", company_name, employee_count)
                        enrichment["too_large"] = True
                        results[company_name] = enrichment
                        continue
                else:
                    logger.info("Apollo: no match for '%s'", company_name)

            # ── Step 2: Hunter domain search ───────────────────────────────
            if not hunter_key or not domain:
                results[company_name] = enrichment
                continue

            hunter_contacts = _hunter_domain_search(domain, hunter_key)
            logger.info(
                "'%s' (%s) → %d Hunter contacts",
                company_name, domain, len(hunter_contacts),
            )

            if not hunter_contacts:
                results[company_name] = enrichment
                continue

            # ── Step 3: Apollo match for LinkedIn + phone ──────────────────
            contacts = []
            for hc in hunter_contacts[:3]:
                first = hc.get("first_name", "")
                last  = hc.get("last_name", "")
                email = hc.get("value", "")
                title = hc.get("position", "")

                contact = {
                    "name":         f"{first} {last}".strip(),
                    "title":        title,
                    "email":        email,
                    "linkedin_url": hc.get("linkedin", ""),   # Hunter provides this directly
                    "phone":        "",
                    "confidence":   hc.get("confidence", 0),
                }

                # Apollo match to get phone (LinkedIn already from Hunter)
                if apollo_key and (first or last) and email:
                    try:
                        apollo_data = _apollo_match_person(
                            first, last, email, domain, apollo_key
                        )
                        # Only override LinkedIn if Hunter didn't provide it
                        if not contact["linkedin_url"]:
                            contact["linkedin_url"] = apollo_data.get("linkedin_url", "")
                        contact["phone"] = apollo_data.get("phone", "")
                        time.sleep(0.3)
                    except Exception as e:
                        logger.debug("Apollo match failed for %s: %s", email, e)

                contacts.append(contact)
                logger.info(
                    "  Contact: %s | %s | email: %s | linkedin: %s | phone: %s",
                    contact["name"], contact["title"],
                    contact["email"] or "-",
                    "✓" if contact["linkedin_url"] else "-",
                    contact["phone"] or "-",
                )

            enrichment["contacts"] = contacts

        except Exception as e:
            logger.error("Enrichment failed for '%s': %s", company_name, e)

        results[company_name] = enrichment
        time.sleep(0.5)

    return results
