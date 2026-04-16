#!/usr/bin/env python3
"""
Weekly QA Hiring Signal Digest.
Queries the last 7 days from jobs.db and sends a curated summary email.
Highlights: top companies by score, companies with multiple roles, new entrants.
"""

import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)

from deduplicator import init_db, DB_PATH
from emailer import _priority_label, _group_jobs
from emailer import send_digest as _send_daily_style

import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weekly_digest")


def get_last_7_days_jobs() -> list[dict]:
    """Fetch all jobs seen in the last 7 days from the DB."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, title, company, location, link, source, score, rationale, first_seen
            FROM   jobs
            WHERE  first_seen >= ?
            ORDER  BY score DESC, first_seen DESC
            """,
            (cutoff,),
        ).fetchall()
    jobs = [dict(r) for r in rows]
    for j in jobs:
        j.setdefault("contacts", [])
        j.setdefault("employee_count", None)
        j.setdefault("too_large", False)
    return jobs


def _build_weekly_html(jobs: list[dict]) -> str:
    today = date.today().strftime("%B %d, %Y")
    week_start = (date.today() - timedelta(days=7)).strftime("%B %d")

    company_groups = _group_jobs(jobs)

    # Top companies (score 7+, sorted by roles then score)
    top = [g for g in company_groups if g["score"] >= 7]
    multi_role = [g for g in company_groups if len(g["roles"]) >= 2]

    high   = sum(1 for g in company_groups if g["score"] >= 9)
    med_hi = sum(1 for g in company_groups if 7 <= g["score"] < 9)

    # Top companies table
    top_rows = ""
    for g in company_groups[:20]:   # top 20 by score+roles
        label, color = _priority_label(g["score"])
        roles_str = ", ".join(g["roles"][:3])
        if len(g["roles"]) > 3:
            roles_str += f" +{len(g['roles'])-3} more"
        top_rows += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;">
            <strong>{g['company']}</strong><br>
            <small style="color:#888;">{g['location']}</small>
          </td>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;text-align:center;">
            <strong style="color:{color};font-size:18px;">{g['score']}</strong><small>/10</small>
          </td>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;text-align:center;">
            <strong style="font-size:18px;">{len(g['roles'])}</strong>
          </td>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;font-size:13px;">
            {roles_str}
          </td>
        </tr>"""

    # Multi-role companies callout
    multi_html = ""
    if multi_role:
        multi_items = "".join(
            f'<li><strong>{g["company"]}</strong> — {len(g["roles"])} roles: {", ".join(g["roles"])}</li>'
            for g in multi_role[:10]
        )
        multi_html = f"""
        <div style="background:#fef9e7;border-left:4px solid #f39c12;padding:16px 20px;margin:24px 0;border-radius:0 6px 6px 0;">
          <strong style="color:#e67e22;">🏗 Companies building QA teams this week ({len(multi_role)})</strong>
          <ul style="margin:8px 0 0;padding-left:20px;font-size:13px;">
            {multi_items}
          </ul>
        </div>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
             background:#f5f6fa;margin:0;padding:20px;">
  <div style="max-width:900px;margin:auto;background:#fff;
              border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">

    <!-- Header -->
    <div style="background:#1a252f;color:#fff;padding:24px 32px;">
      <h1 style="margin:0;font-size:22px;">Weekly QA Hiring Signal Report</h1>
      <p style="margin:6px 0 0;opacity:.7;">{week_start} – {today}</p>
    </div>

    <!-- Stats -->
    <div style="padding:16px 32px;background:#ecf0f1;display:flex;gap:32px;">
      <div><strong style="color:#c0392b;font-size:24px;">{high}</strong><br><small>High Priority Companies</small></div>
      <div><strong style="color:#e67e22;font-size:24px;">{med_hi}</strong><br><small>Medium-High</small></div>
      <div><strong style="color:#e67e22;font-size:24px;">{len(multi_role)}</strong><br><small>Building Teams (2+ roles)</small></div>
      <div style="margin-left:auto;"><strong style="color:#2c3e50;font-size:24px;">{len(company_groups)}</strong><br><small>Companies Total</small></div>
      <div><strong style="color:#2c3e50;font-size:24px;">{len(jobs)}</strong><br><small>Total Roles</small></div>
    </div>

    <div style="padding:24px 32px;">

      {multi_html}

      <!-- Top companies table -->
      <h2 style="font-size:16px;color:#2c3e50;margin:0 0 12px;">Top Companies This Week</h2>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead>
          <tr style="background:#f8f9fa;text-align:left;">
            <th style="padding:10px 8px;border-bottom:2px solid #ddd;width:30%;">Company</th>
            <th style="padding:10px 8px;border-bottom:2px solid #ddd;width:10%;text-align:center;">Score</th>
            <th style="padding:10px 8px;border-bottom:2px solid #ddd;width:10%;text-align:center;">Roles</th>
            <th style="padding:10px 8px;border-bottom:2px solid #ddd;">Positions</th>
          </tr>
        </thead>
        <tbody>{top_rows}</tbody>
      </table>
    </div>

    <div style="padding:16px 32px;background:#f8f9fa;color:#888;font-size:12px;">
      Weekly digest · {week_start} – {today} · QA Signal Agent
    </div>
  </div>
</body>
</html>"""


def send_weekly_digest(jobs: list[dict]) -> None:
    from_addr = os.environ.get("EMAIL_FROM", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    to_raw    = os.environ.get("EMAIL_TO", "")

    if not from_addr or not password or not to_raw:
        raise EnvironmentError("Set EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO in .env")

    to_addrs  = [a.strip() for a in to_raw.split(",") if a.strip()]
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    today      = date.today().strftime("%B %d, %Y")
    week_start = (date.today() - timedelta(days=7)).strftime("%b %d")
    subject    = f"Weekly QA Hiring Signals — {week_start} to {today} — {len(jobs)} roles"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addrs)
    msg.attach(MIMEText(_build_weekly_html(jobs), "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(from_addr, password)
        server.sendmail(from_addr, to_addrs, msg.as_string())

    logger.info("Weekly digest sent to %s (%d jobs)", to_addrs, len(jobs))


def main() -> None:
    init_db()
    jobs = get_last_7_days_jobs()
    logger.info("Found %d jobs from the last 7 days", len(jobs))

    if not jobs:
        logger.info("No jobs in last 7 days — skipping weekly digest.")
        return

    send_weekly_digest(jobs)


if __name__ == "__main__":
    main()
