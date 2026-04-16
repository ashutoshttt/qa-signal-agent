"""
Gmail SMTP digest emailer.
Sends a daily HTML email with:
  1. Company Highlights table (grouped, sorted by # roles + score)
  2. Full job-by-job detail table
"""

import logging
import os
import smtplib
from collections import defaultdict
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _priority_label(score: int) -> tuple[str, str]:
    if score >= 9:
        return ("🔥 High", "#c0392b")
    elif score >= 7:
        return ("⚡ Medium-High", "#e67e22")
    elif score >= 5:
        return ("👀 Medium", "#2980b9")
    elif score >= 3:
        return ("↓ Low", "#7f8c8d")
    else:
        return ("— Noise", "#bdc3c7")


def _group_jobs(jobs: list[dict]) -> list[dict]:
    """
    Group jobs by company and return sorted list of company summaries.
    Sort: number of roles DESC, then score DESC.
    """
    groups: dict[str, dict] = defaultdict(lambda: {
        "company": "",
        "roles": [],
        "score": 0,
        "rationale": "",
        "location": "",
        "contacts": [],
        "employee_count": None,
        "too_large": False,
    })

    for job in jobs:
        company = job["company"]
        g = groups[company]
        g["company"] = company
        g["roles"].append(job["title"])
        g["score"] = max(g["score"], job.get("score", 0))
        g["rationale"] = job.get("rationale", "")
        g["location"] = job.get("location", "")
        # Apollo enrichment fields (set on job by run_agent)
        if job.get("contacts") is not None:
            g["contacts"] = job["contacts"]
        if job.get("employee_count") is not None:
            g["employee_count"] = job["employee_count"]
        if job.get("too_large"):
            g["too_large"] = True

    return sorted(
        groups.values(),
        key=lambda g: (len(g["roles"]), g["score"]),
        reverse=True,
    )


def _highlights_html(company_groups: list[dict]) -> str:
    rows = ""
    for g in company_groups:
        label, color = _priority_label(g["score"])
        role_count = len(g["roles"])

        # Truncate role list if too long
        if role_count <= 3:
            roles_str = ", ".join(g["roles"])
        else:
            roles_str = ", ".join(g["roles"][:3]) + f" +{role_count - 3} more"

        # Employee count badge
        if g["employee_count"]:
            emp = f"{g['employee_count']:,}"
            emp_badge = f'<span style="background:#ecf0f1;padding:2px 6px;border-radius:3px;font-size:11px;">{emp} employees</span>'
        elif g["too_large"]:
            emp_badge = '<span style="background:#fadbd8;padding:2px 6px;border-radius:3px;font-size:11px;">Large (skipped)</span>'
        else:
            emp_badge = '<span style="color:#bdc3c7;font-size:11px;">size unknown</span>'

        # Contacts
        if g["too_large"]:
            contacts_html = '<span style="color:#bdc3c7;font-size:12px;">Skipped — large company</span>'
        elif g["contacts"]:
            contact_lines = []
            for c in g["contacts"][:3]:
                name_part = f"<strong>{c['name']}</strong>" if c["name"] else "Unknown"
                title_part = f" · {c['title']}" if c["title"] else ""
                email_part = f' · <a href="mailto:{c["email"]}" style="color:#2980b9;">{c["email"]}</a>' if c.get("email") else ""
                li_part = f' · <a href="{c["linkedin_url"]}" style="color:#2980b9;">LinkedIn</a>' if c.get("linkedin_url") else ""
                contact_lines.append(f"{name_part}{title_part}{email_part}{li_part}")
            contacts_html = "<br>".join(contact_lines)
        else:
            contacts_html = '<span style="color:#bdc3c7;font-size:12px;">No contacts found</span>'

        rows += f"""
        <tr>
          <td style="padding:12px 10px;border-bottom:1px solid #eee;vertical-align:top;">
            <strong style="font-size:15px;">{g['company']}</strong><br>
            <span style="color:#888;font-size:12px;">{g['location']}</span><br>
            {emp_badge}
          </td>
          <td style="padding:12px 10px;border-bottom:1px solid #eee;vertical-align:top;text-align:center;">
            <span style="font-size:22px;font-weight:700;color:{color};">{role_count}</span>
          </td>
          <td style="padding:12px 10px;border-bottom:1px solid #eee;vertical-align:top;font-size:13px;">
            {roles_str}
          </td>
          <td style="padding:12px 10px;border-bottom:1px solid #eee;vertical-align:top;">
            <strong style="color:{color};">{label}</strong> ({g['score']}/10)<br>
            <small style="color:#888;">{g['rationale']}</small>
          </td>
          <td style="padding:12px 10px;border-bottom:1px solid #eee;vertical-align:top;font-size:13px;">
            {contacts_html}
          </td>
        </tr>"""

    return f"""
    <h2 style="font-size:16px;color:#2c3e50;margin:0 0 12px;">Company Highlights</h2>
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <thead>
        <tr style="background:#f8f9fa;text-align:left;">
          <th style="padding:10px;border-bottom:2px solid #ddd;width:20%;">Company</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:7%;text-align:center;"># Roles</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:25%;">Positions</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:18%;">Signal</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;">QA Contacts (Apollo)</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


def _detail_html(jobs: list[dict]) -> str:
    rows = ""
    for job in jobs:
        label, color = _priority_label(job["score"])
        rows += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;">
            <strong style="color:{color}">{label}</strong><br>
            <span style="font-size:14px;font-weight:600;">{job['title']}</span>
          </td>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;">{job['company']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;">{job['location']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;">
            <a href="{job['link']}" style="color:#2c3e50;">View →</a>
          </td>
        </tr>"""

    return f"""
    <h2 style="font-size:16px;color:#2c3e50;margin:24px 0 12px;">All Jobs</h2>
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <thead>
        <tr style="background:#f8f9fa;text-align:left;">
          <th style="padding:10px 8px;border-bottom:2px solid #ddd;width:28%;">Signal / Role</th>
          <th style="padding:10px 8px;border-bottom:2px solid #ddd;width:24%;">Company</th>
          <th style="padding:10px 8px;border-bottom:2px solid #ddd;width:24%;">Location</th>
          <th style="padding:10px 8px;border-bottom:2px solid #ddd;">Link</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


def _build_html(jobs: list[dict]) -> str:
    today = date.today().strftime("%B %d, %Y")
    company_groups = _group_jobs(jobs)

    high    = sum(1 for g in company_groups if g["score"] >= 9)
    med_hi  = sum(1 for g in company_groups if 7 <= g["score"] < 9)
    medium  = sum(1 for g in company_groups if 5 <= g["score"] < 7)
    total_companies = len(company_groups)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
             background:#f5f6fa;margin:0;padding:20px;">
  <div style="max-width:1000px;margin:auto;background:#fff;
              border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">

    <!-- Header -->
    <div style="background:#2c3e50;color:#fff;padding:24px 32px;">
      <h1 style="margin:0;font-size:22px;">QA Hiring Signal Report</h1>
      <p style="margin:6px 0 0;opacity:.8;">{today}</p>
    </div>

    <!-- Stats bar -->
    <div style="padding:16px 32px;background:#ecf0f1;display:flex;gap:32px;align-items:center;">
      <div><strong style="color:#c0392b;font-size:20px;">{high}</strong><br><small>High Priority Companies</small></div>
      <div><strong style="color:#e67e22;font-size:20px;">{med_hi}</strong><br><small>Medium-High</small></div>
      <div><strong style="color:#2980b9;font-size:20px;">{medium}</strong><br><small>Medium</small></div>
      <div style="margin-left:auto;"><strong style="color:#2c3e50;font-size:20px;">{total_companies}</strong><br><small>Companies</small></div>
      <div><strong style="color:#2c3e50;font-size:20px;">{len(jobs)}</strong><br><small>Total Roles</small></div>
    </div>

    <!-- Company highlights -->
    <div style="padding:24px 32px 0;">
      {_highlights_html(company_groups)}
    </div>

    <!-- All jobs detail -->
    <div style="padding:8px 32px 32px;">
      {_detail_html(jobs)}
    </div>

    <div style="padding:16px 32px;background:#f8f9fa;color:#888;font-size:12px;">
      Powered by QA Signal Agent · Company scores 0–10 indicate sales opportunity strength
    </div>
  </div>
</body>
</html>"""


def _build_text(jobs: list[dict]) -> str:
    today = date.today().strftime("%B %d, %Y")
    company_groups = _group_jobs(jobs)

    lines = [f"QA Hiring Signal Report — {today}", "=" * 60, "", "COMPANY HIGHLIGHTS", "-" * 40]

    for g in company_groups:
        label, _ = _priority_label(g["score"])
        roles_str = ", ".join(g["roles"])
        lines += [
            f"[{g['score']}/10] {label} — {g['company']}",
            f"  Roles ({len(g['roles'])}): {roles_str}",
            f"  Location: {g['location']}",
            f"  Why: {g['rationale']}",
        ]
        if g["employee_count"]:
            lines.append(f"  Employees: {g['employee_count']:,}")
        if g["too_large"]:
            lines.append("  Apollo: skipped (large company)")
        elif g["contacts"]:
            for c in g["contacts"]:
                lines.append(f"  Contact: {c['name']} · {c['title']} · {c.get('email','')}")
        lines.append("")

    lines += ["", "ALL JOBS", "-" * 40]
    for job in jobs:
        label, _ = _priority_label(job["score"])
        lines += [
            f"[{job['score']}/10] {job['title']} @ {job['company']}",
            f"  {job['location']} · {job['link']}",
            "",
        ]
    return "\n".join(lines)


def send_digest(jobs: list[dict]) -> bool:
    """Send the daily digest email. Returns True on success."""
    if not jobs:
        logger.info("No new jobs to email.")
        return True

    from_addr = os.environ.get("EMAIL_FROM", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    to_raw    = os.environ.get("EMAIL_TO", "")

    if not from_addr or not password or not to_raw:
        raise EnvironmentError("Set EMAIL_FROM, EMAIL_PASSWORD, and EMAIL_TO in your .env file")

    to_addrs  = [a.strip() for a in to_raw.split(",") if a.strip()]
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    today   = date.today().strftime("%B %d, %Y")
    subject = f"QA Hiring Signals — {len(jobs)} roles across {len(_group_jobs(jobs))} companies — {today}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addrs)
    msg.attach(MIMEText(_build_text(jobs), "plain"))
    msg.attach(MIMEText(_build_html(jobs), "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(from_addr, password)
            server.sendmail(from_addr, to_addrs, msg.as_string())
        logger.info("Digest sent to %s (%d jobs, %d companies)", to_addrs, len(jobs), len(_group_jobs(jobs)))
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP auth failed. Use a Gmail App Password: https://support.google.com/accounts/answer/185833")
        raise
    except Exception as e:
        logger.error("Failed to send email: %s", e)
        raise
