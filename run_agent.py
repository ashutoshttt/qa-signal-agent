#!/usr/bin/env python3
"""
QA Hiring Signal Agent — main entry point.

Pipeline:
  1. Scrape   — LinkedIn (Naukri/Indeed disabled for now)
  2. Dedup    — filter already-seen jobs via SQLite
  3. Score    — company-level Claude scoring (all roles per company together)
  4. Enrich   — Apollo.io: company size check + QA contacts (score >= 5 only)
  5. Signals  — funding news, tech stack, leadership hiring, repeat hiring (no APIs)
  6. Email    — HTML digest with highlights table + full job list

Usage:
  python run_agent.py              # full run
  python run_agent.py --dry-run    # scrape + score + enrich, print only, no email
  python run_agent.py --email-only # skip scraping, email whatever is pending in DB
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)

from scrapers import scrape_linkedin, fetch_job_description
# from scrapers import scrape_naukri, scrape_indeed  # disabled — re-enable when ready
from processor import score_jobs, group_by_company
from deduplicator import init_db, filter_new, save_jobs, get_pending_digest, mark_emailed
from apollo import enrich_companies
from signals import enrich_signals
from emailer import send_digest

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "agent.log"),
    ],
)
logger = logging.getLogger("run_agent")

ENRICH_SCORE_THRESHOLD = 5   # enrich medium + high priority companies (score >= 5)


# ── Pipeline steps ─────────────────────────────────────────────────────────────

def step_scrape() -> list[dict]:
    logger.info("── Step 1/5: Scraping ──────────────────────────────")
    all_jobs: list[dict] = []
    for name, fn in [("LinkedIn", scrape_linkedin)]:
        # ("Naukri", scrape_naukri),   # disabled
        # ("Indeed", scrape_indeed),   # disabled
        logger.info("Scraping %s …", name)
        try:
            jobs = fn()
            logger.info("  ✓ %s: %d India jobs", name, len(jobs))
            all_jobs.extend(jobs)
        except Exception as e:
            logger.error("  ✗ %s scraper failed: %s", name, e)
    logger.info("Total raw jobs: %d", len(all_jobs))
    return all_jobs


def step_deduplicate(raw_jobs: list[dict]) -> list[dict]:
    logger.info("── Step 2/5: Deduplication ─────────────────────────")
    new_jobs = filter_new(raw_jobs)
    logger.info("New (unseen) jobs: %d", len(new_jobs))
    return new_jobs


def step_score(new_jobs: list[dict]) -> list[dict]:
    logger.info("── Step 3/5: Scoring with Claude ───────────────────")
    if not new_jobs:
        logger.info("Nothing to score.")
        return []
    scored = score_jobs(new_jobs)
    save_jobs(scored)
    return scored


def step_enrich(scored_jobs: list[dict]) -> list[dict]:
    logger.info("── Step 4/6: Apollo enrichment ─────────────────────")

    # Only enrich companies at or above threshold
    groups = group_by_company(scored_jobs)
    companies_to_enrich = {
        name: jobs
        for name, jobs in groups.items()
        if max(j.get("score", 0) for j in jobs) >= ENRICH_SCORE_THRESHOLD
    }

    skipped = len(groups) - len(companies_to_enrich)
    logger.info(
        "Enriching %d companies (score >= %d) · skipping %d low-score companies",
        len(companies_to_enrich), ENRICH_SCORE_THRESHOLD, skipped,
    )

    if not companies_to_enrich:
        logger.info("No companies meet enrichment threshold.")
        return scored_jobs

    enrichment = enrich_companies(companies_to_enrich)

    # Attach Apollo data to every job for that company
    for job in scored_jobs:
        company = job["company"]
        if company in enrichment:
            e = enrichment[company]
            job["employee_count"] = e.get("employee_count")
            job["too_large"]      = e.get("too_large", False)
            job["contacts"]       = e.get("contacts", [])
            job["apollo_url"]     = e.get("apollo_url", "")
            job["industry"]       = e.get("industry", "")
            job["founded_year"]   = e.get("founded_year")
            job["funding_stage"]  = e.get("funding_stage", "")
        else:
            job.setdefault("employee_count", None)
            job.setdefault("too_large", False)
            job.setdefault("contacts", [])
            job.setdefault("apollo_url", "")
            job.setdefault("industry", "")
            job.setdefault("founded_year", None)
            job.setdefault("funding_stage", "")

    return scored_jobs


def step_signals(enriched_jobs: list[dict]) -> list[dict]:
    logger.info("── Step 5/6: Company signals ────────────────────────")

    groups = group_by_company(enriched_jobs)
    companies_to_signal = {
        name: jobs
        for name, jobs in groups.items()
        if max(j.get("score", 0) for j in jobs) >= ENRICH_SCORE_THRESHOLD
    }

    if not companies_to_signal:
        logger.info("No companies meet signal threshold.")
        return enriched_jobs

    # Fetch job descriptions for high-score companies (to extract tech stack)
    logger.info("Fetching job descriptions for tech stack extraction …")
    company_descriptions: dict[str, list[str]] = {}
    for name, jobs in companies_to_signal.items():
        descs = []
        for job in jobs[:3]:   # max 3 descriptions per company
            desc = fetch_job_description(job["link"])
            if desc:
                descs.append(desc)
            time.sleep(1)
        company_descriptions[name] = descs
        logger.info("  %s: %d description(s) fetched", name, len(descs))

    # Build enrichment stubs from existing job data
    enrichment_stubs = {}
    for name, jobs in companies_to_signal.items():
        job0 = jobs[0]
        enrichment_stubs[name] = {
            "apollo_url":       job0.get("apollo_url", ""),
            "employee_count":   job0.get("employee_count"),
            "job_descriptions": company_descriptions.get(name, []),
        }

    logger.info("Running signals for %d companies …", len(enrichment_stubs))
    signalled = enrich_signals(enrichment_stubs)

    # Attach signal data to every job for that company
    for job in enriched_jobs:
        company = job["company"]
        if company in signalled:
            s = signalled[company]
            job["funding"]       = s.get("funding")
            job["product"]       = s.get("product")
            job["tech_stack"]    = s.get("tech_stack", [])
            job["leadership"]    = s.get("leadership")
            job["repeat_hiring"] = s.get("repeat_hiring")
            job["ai_mentions"]   = s.get("ai_mentions", [])
        else:
            job.setdefault("funding", None)
            job.setdefault("product", None)
            job.setdefault("tech_stack", [])
            job.setdefault("leadership", None)
            job.setdefault("repeat_hiring", None)
            job.setdefault("ai_mentions", [])

    return enriched_jobs


def step_email(jobs: list[dict], dry_run: bool = False) -> None:
    logger.info("── Step 6/6: Email digest ──────────────────────────")

    # Always pull from DB for the email (includes any previously pending jobs)
    pending = get_pending_digest()

    if not pending:
        logger.info("No pending jobs to email.")
        return

    # Merge Apollo + signals enrichment from in-memory jobs into pending DB jobs
    enrichment_map = {j["link"]: j for j in jobs}
    for p in pending:
        if p["link"] in enrichment_map:
            src = enrichment_map[p["link"]]
            p["employee_count"] = src.get("employee_count")
            p["too_large"]      = src.get("too_large", False)
            p["contacts"]       = src.get("contacts", [])
            p["apollo_url"]     = src.get("apollo_url", "")
            p["industry"]       = src.get("industry", "")
            p["founded_year"]   = src.get("founded_year")
            p["funding_stage"]  = src.get("funding_stage", "")
            p["funding"]        = src.get("funding")
            p["product"]        = src.get("product")
            p["tech_stack"]     = src.get("tech_stack", [])
            p["leadership"]     = src.get("leadership")
            p["repeat_hiring"]  = src.get("repeat_hiring")
            p["ai_mentions"]    = src.get("ai_mentions", [])
        else:
            p.setdefault("employee_count", None)
            p.setdefault("too_large", False)
            p.setdefault("contacts", [])
            p.setdefault("apollo_url", "")
            p.setdefault("industry", "")
            p.setdefault("founded_year", None)
            p.setdefault("funding_stage", "")
            p.setdefault("funding", None)
            p.setdefault("product", None)
            p.setdefault("tech_stack", [])
            p.setdefault("leadership", None)
            p.setdefault("repeat_hiring", None)
            p.setdefault("ai_mentions", [])

    if dry_run:
        logger.info("DRY RUN — would send %d jobs:", len(pending))
        from processor import group_by_company as _grp
        for company, cjobs in _grp(pending).items():
            score = max(j.get("score", 0) for j in cjobs)
            contacts = cjobs[0].get("contacts", [])
            contact_str = contacts[0]["name"] if contacts else "no contact"
            print(f"  [{score:2d}/10] {company:<40} {len(cjobs)} role(s) · {contact_str}")
        return

    sent = send_digest(pending)
    if sent:
        mark_emailed([j["id"] for j in pending])
        logger.info("Digest sent. %d jobs marked as emailed.", len(pending))


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="QA Hiring Signal Agent")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Scrape, score, enrich — print results, no email")
    parser.add_argument("--email-only", action="store_true",
                        help="Skip scraping; email whatever is pending in the DB")
    args = parser.parse_args()

    init_db()

    if args.email_only:
        pending = get_pending_digest()
        for p in pending:
            p.setdefault("employee_count", None)
            p.setdefault("too_large", False)
            p.setdefault("contacts", [])
        if pending:
            sent = send_digest(pending)
            if sent:
                mark_emailed([j["id"] for j in pending])
        else:
            logger.info("No pending jobs in DB.")
        return

    raw_jobs    = step_scrape()
    new_jobs    = step_deduplicate(raw_jobs)
    scored_jobs = step_score(new_jobs)
    enriched    = step_enrich(scored_jobs)
    signalled   = step_signals(enriched)
    step_email(signalled, dry_run=args.dry_run)

    logger.info("── Done ────────────────────────────────────────────")


if __name__ == "__main__":
    main()
