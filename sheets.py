"""
Google Sheets logger via Apps Script Web App.

After each daily run, sends one row per company to a master Google Sheet.
No Google Cloud setup needed — just an Apps Script Web App URL.

Setup:
  1. Open your Google Sheet
  2. Extensions → Apps Script → paste the Apps Script code
  3. Deploy as Web App (Anyone can access)
  4. Copy the Web App URL → add to .env as GOOGLE_SHEET_URL
  5. Add GOOGLE_SHEET_URL to GitHub Secrets
"""

import logging
import os
import json
import requests
from datetime import date
from collections import defaultdict

logger = logging.getLogger(__name__)


def _group_by_company(jobs: list[dict]) -> list[dict]:
    """Group jobs by company and build one summary row per company."""
    groups: dict[str, dict] = defaultdict(lambda: {
        "company": "",
        "score": 0,
        "roles": [],
        "location": "",
        "industry": "",
        "employee_count": None,
        "funding_stage": "",
        "founded_year": None,
        "funding": None,
        "product": None,
        "tech_stack": [],
        "ai_mentions": [],
        "leadership": None,
        "repeat_hiring": None,
        "contacts": [],
    })

    for job in jobs:
        company = job["company"]
        g = groups[company]
        g["company"] = company
        g["score"] = max(g["score"], job.get("score", 0))
        g["roles"].append(job.get("title", ""))
        g["location"] = g["location"] or job.get("location", "")
        g["industry"] = g["industry"] or job.get("industry", "")
        g["employee_count"] = g["employee_count"] or job.get("employee_count")
        g["funding_stage"] = g["funding_stage"] or job.get("funding_stage", "")
        g["founded_year"] = g["founded_year"] or job.get("founded_year")
        g["funding"] = g["funding"] or job.get("funding")
        g["product"] = g["product"] or job.get("product")
        g["leadership"] = g["leadership"] or job.get("leadership")
        g["repeat_hiring"] = g["repeat_hiring"] or job.get("repeat_hiring")

        if job.get("tech_stack"):
            existing = set(g["tech_stack"])
            g["tech_stack"] += [t for t in job["tech_stack"] if t not in existing]

        if job.get("ai_mentions"):
            existing = set(g["ai_mentions"])
            g["ai_mentions"] += [m for m in job["ai_mentions"] if m not in existing]

        if job.get("contacts"):
            g["contacts"] = job["contacts"]

    return sorted(groups.values(), key=lambda x: (len(x["roles"]), x["score"]), reverse=True)


def _build_rows(jobs: list[dict]) -> list[dict]:
    """Build one flat dict per company ready to send to Sheets."""
    today = date.today().strftime("%Y-%m-%d")
    company_groups = _group_by_company(jobs)
    rows = []

    for g in company_groups:
        # Contacts: first 2, formatted as "Name | Title | Email"
        contact_strs = []
        for c in g["contacts"][:2]:
            parts = [p for p in [c.get("name"), c.get("title"), c.get("email")] if p]
            contact_strs.append(" | ".join(parts))

        # AI mention — first sentence only
        ai_signal = g["ai_mentions"][0][:120] if g["ai_mentions"] else ""

        rows.append({
            "date":           today,
            "company":        g["company"],
            "score":          g["score"],
            "num_roles":      len(g["roles"]),
            "positions":      ", ".join(g["roles"][:5]),
            "location":       g["location"],
            "industry":       g["industry"],
            "employees":      g["employee_count"] or "",
            "funding_stage":  g["funding_stage"],
            "founded_year":   g["founded_year"] or "",
            "funding_news":   g["funding"] or "",
            "product_news":   g["product"] or "",
            "tech_stack":     ", ".join(g["tech_stack"]),
            "ai_signal":      ai_signal,
            "leadership":     g["leadership"] or "",
            "repeat_hiring":  g["repeat_hiring"] or "",
            "contacts":       " | ".join(contact_strs),
        })

    return rows


def log_to_sheets(jobs: list[dict]) -> bool:
    """
    Send today's company summaries to Google Sheets via Apps Script Web App.
    Returns True on success, False on failure.
    """
    url = os.environ.get("GOOGLE_SHEET_URL", "")
    if not url:
        logger.info("GOOGLE_SHEET_URL not set — skipping Sheets logging")
        return False

    if not jobs:
        logger.info("No jobs to log to Sheets")
        return False

    rows = _build_rows(jobs)
    logger.info("Logging %d company rows to Google Sheets …", len(rows))

    try:
        resp = requests.post(
            url,
            json={"rows": rows},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 200:
            result = resp.json() if resp.text else {}
            logger.info("Sheets: %s rows written", result.get("written", len(rows)))
            return True
        else:
            logger.error("Sheets POST failed: %s %s", resp.status_code, resp.text[:200])
            return False

    except Exception as e:
        logger.error("Sheets logging failed: %s", e)
        return False
