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
        "industry": "",
        "founded_year": None,
        "funding_stage": "",
        "funding": None,
        "product": None,
        "tech_stack": [],
        "leadership": None,
        "repeat_hiring": None,
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
        if job.get("industry") and not g["industry"]:
            g["industry"] = job["industry"]
        if job.get("founded_year") and not g["founded_year"]:
            g["founded_year"] = job["founded_year"]
        if job.get("funding_stage") and not g["funding_stage"]:
            g["funding_stage"] = job["funding_stage"]
        # Signals (take first non-null value seen for this company)
        if job.get("funding") and not g["funding"]:
            g["funding"] = job["funding"]
        if job.get("tech_stack"):
            # Merge tech stacks across jobs, keep unique
            existing = set(g["tech_stack"])
            g["tech_stack"] = list(existing | set(job["tech_stack"]))
        if job.get("leadership") and not g["leadership"]:
            g["leadership"] = job["leadership"]
        if job.get("repeat_hiring") and not g["repeat_hiring"]:
            g["repeat_hiring"] = job["repeat_hiring"]
        if job.get("product") and not g["product"]:
            g["product"] = job["product"]

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

        # Employee count + industry + funding stage + founded year badges
        meta_parts = []
        if g["employee_count"]:
            meta_parts.append(
                f'<span style="background:#ecf0f1;padding:2px 6px;border-radius:3px;font-size:11px;">'
                f'{g["employee_count"]:,} employees</span>'
            )
        elif g["too_large"]:
            meta_parts.append(
                '<span style="background:#fadbd8;padding:2px 6px;border-radius:3px;font-size:11px;">Large MNC</span>'
            )
        if g.get("industry"):
            meta_parts.append(
                f'<span style="background:#eaf4fb;color:#1a5276;padding:2px 6px;border-radius:3px;font-size:11px;">'
                f'{g["industry"]}</span>'
            )
        if g.get("funding_stage"):
            meta_parts.append(
                f'<span style="background:#fef9e7;color:#7d6608;padding:2px 6px;border-radius:3px;font-size:11px;">'
                f'{g["funding_stage"]}</span>'
            )
        if g.get("founded_year"):
            meta_parts.append(
                f'<span style="color:#aab7b8;font-size:11px;">est. {g["founded_year"]}</span>'
            )
        emp_badge = " ".join(meta_parts) if meta_parts else '<span style="color:#bdc3c7;font-size:11px;">size unknown</span>'

        # Contacts
        if g["too_large"]:
            contacts_html = '<span style="color:#bdc3c7;font-size:12px;">Skipped — large company</span>'
        elif g["contacts"]:
            contact_lines = []
            for c in g["contacts"][:3]:
                name_part  = f"<strong>{c['name']}</strong>" if c["name"] else "Unknown"
                title_part = f" · {c['title']}" if c.get("title") else ""
                email_part = f' · <a href="mailto:{c["email"]}" style="color:#2980b9;">{c["email"]}</a>' if c.get("email") else ""
                li_part    = f' · <a href="{c["linkedin_url"]}" style="color:#2980b9;">LinkedIn</a>' if c.get("linkedin_url") else ""
                phone_part = f' · 📞 {c["phone"]}' if c.get("phone") else ""
                contact_lines.append(f"{name_part}{title_part}{email_part}{phone_part}{li_part}")
            contacts_html = "<br>".join(contact_lines)
        else:
            contacts_html = '<span style="color:#bdc3c7;font-size:12px;">No contacts found</span>'

        # ── Signals column ──────────────────────────────────────
        signal_lines = []

        if g.get("funding"):
            signal_lines.append(
                f'<div style="margin-bottom:5px;">'
                f'<span style="background:#d5f5e3;color:#1e8449;padding:2px 6px;'
                f'border-radius:3px;font-size:11px;font-weight:600;">💰 Funding</span> '
                f'<span style="font-size:12px;color:#555;">{g["funding"][:90]}</span>'
                f'</div>'
            )

        if g.get("tech_stack"):
            stack_str = " · ".join(g["tech_stack"][:6])
            signal_lines.append(
                f'<div style="margin-bottom:5px;">'
                f'<span style="background:#d6eaf8;color:#1a5276;padding:2px 6px;'
                f'border-radius:3px;font-size:11px;font-weight:600;">🛠 Stack</span> '
                f'<span style="font-size:12px;color:#555;">{stack_str}</span>'
                f'</div>'
            )

        if g.get("leadership"):
            signal_lines.append(
                f'<div style="margin-bottom:5px;">'
                f'<span style="background:#fdebd0;color:#784212;padding:2px 6px;'
                f'border-radius:3px;font-size:11px;font-weight:600;">👔 Leadership</span> '
                f'<span style="font-size:12px;color:#555;">{g["leadership"]}</span>'
                f'</div>'
            )

        if g.get("repeat_hiring"):
            signal_lines.append(
                f'<div style="margin-bottom:5px;">'
                f'<span style="background:#f9ebea;color:#922b21;padding:2px 6px;'
                f'border-radius:3px;font-size:11px;font-weight:600;">🔁 Repeat</span> '
                f'<span style="font-size:12px;color:#555;">{g["repeat_hiring"]}</span>'
                f'</div>'
            )

        if g.get("product"):
            signal_lines.append(
                f'<div style="margin-bottom:5px;">'
                f'<span style="background:#e8daef;color:#6c3483;padding:2px 6px;'
                f'border-radius:3px;font-size:11px;font-weight:600;">🚀 Product</span> '
                f'<span style="font-size:12px;color:#555;">{g["product"][:90]}</span>'
                f'</div>'
            )

        signals_html = "\n".join(signal_lines) if signal_lines else \
            '<span style="color:#bdc3c7;font-size:12px;">—</span>'

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
          <td style="padding:12px 10px;border-bottom:1px solid #eee;vertical-align:top;">
            {signals_html}
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
          <th style="padding:10px;border-bottom:2px solid #ddd;width:18%;">Company</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:6%;text-align:center;"># Roles</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:20%;">Positions</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:15%;">Signal</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;width:22%;">Intel</th>
          <th style="padding:10px;border-bottom:2px solid #ddd;">QA Contacts</th>
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
        if g.get("industry"):
            lines.append(f"  Industry: {g['industry']}")
        if g.get("funding_stage"):
            lines.append(f"  Stage: {g['funding_stage']}")
        if g.get("founded_year"):
            lines.append(f"  Founded: {g['founded_year']}")
        if g["too_large"]:
            lines.append("  Apollo: skipped (large company)")
        elif g["contacts"]:
            for c in g["contacts"]:
                lines.append(f"  Contact: {c['name']} · {c['title']} · {c.get('email','')}")
        if g.get("funding"):
            lines.append(f"  💰 Funding: {g['funding']}")
        if g.get("tech_stack"):
            lines.append(f"  🛠 Stack: {', '.join(g['tech_stack'])}")
        if g.get("leadership"):
            lines.append(f"  👔 Leadership hiring: {g['leadership']}")
        if g.get("repeat_hiring"):
            lines.append(f"  🔁 Repeat hiring: {g['repeat_hiring']}")
        if g.get("product"):
            lines.append(f"  🚀 Product: {g['product']}")
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
