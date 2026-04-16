"""
Claude API-based filtering and scoring of job postings.
Scores at the COMPANY level — all roles for a company are passed together
so Claude can detect "building a team" signals (multiple hires = high priority).
"""

import json
import logging
import os
from collections import defaultdict

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You are an expert B2B sales analyst for a QA consulting / tooling company
targeting mid-size Indian tech companies (100–2000 employees) that are actively building QA teams.

You will receive a company name, location, and ALL the QA roles they are currently hiring for.
Score the company 0–10 as a sales opportunity using these rules:

SCORE HIGH (8–10):
  - Company is hiring 3+ QA roles simultaneously → strong "building a team" signal → 9–10
  - Company is hiring a QA Manager/Head/Director/Lead AND individual contributors → 8–9
  - Any "Head of QA", "Head of Quality", "Director of QA", "VP Quality" hire → 8–9

SCORE MEDIUM-HIGH (6–7):
  - Company hiring 2 QA roles (even if both IC) → 7
  - Single "QA Manager", "Test Manager", "Engineering Manager QA" → 7
  - Single senior/lead role ("QA Lead", "Test Lead", "Senior QA", "Staff QA", "Principal QA")
    at a company that sounds like a product startup or scaleup → 6–7

SCORE MEDIUM (4–5):
  - Single IC role (QA Engineer, SDET, Automation Engineer, QA Analyst) at a product company
    that is NOT a known large MNC → 5
  - Single senior IC at an unclassifiable company → 5

SCORE LOW (1–3):
  - Company is a known large MNC or IT services firm: TCS, Infosys, Wipro, HCL, Accenture,
    Capgemini, Cognizant, Tech Mahindra, Mphasis, L&T, Deloitte, IBM, Amazon, Google, Microsoft,
    Meta, Apple, Deutsche Bank, Citi, JPMorgan, HSBC, Barclays, Samsung, Ericsson, Nokia,
    Emerson, NEC, Nordson, Tata, Adani, Bajaj, EPAM, TELUS → 2
  - Staffing/recruitment agency posting on behalf of a client
    (Scoutit, Harvey Nash, La Fosse, Evantis, N Consulting, Net2Source, Isoskills, Ampstek,
    RemoteHunter, Crossing Hurdles, AgileGrid, SkillsCapital) → 1–2
  - Contract or freelance roles → 2

SCORE ZERO (0):
  - Non-software QA (Supplier Quality, Hardware, HVAC, Manufacturing, Embedded validation) → 0
  - Location clearly outside India → 0

Respond with ONLY valid JSON, no markdown:
{"score": <integer 0-10>, "rationale": "<one concise sentence explaining the score>"}"""


def _group_by_company(jobs: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for job in jobs:
        groups[job["company"]].append(job)
    return dict(groups)


def _build_company_message(company: str, jobs: list[dict]) -> str:
    location = jobs[0].get("location", "India")
    roles = "\n".join(f"  - {j['title']}" for j in jobs)
    return (
        f"Company: {company}\n"
        f"Location: {location}\n"
        f"Number of QA roles hiring: {len(jobs)}\n"
        f"Roles:\n{roles}"
    )


def _score_company(client: anthropic.Anthropic, company: str, jobs: list[dict]) -> tuple[int, str]:
    """Score a company based on all its open QA roles. Returns (score, rationale)."""
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=128,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": _build_company_message(company, jobs)}],
        )
        raw = message.content[0].text.strip()
        # Strip markdown code fences if Claude wraps the JSON
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        parsed = json.loads(raw)
        return int(parsed.get("score", 0)), parsed.get("rationale", "")
    except json.JSONDecodeError:
        logger.warning("Claude returned non-JSON for company '%s'", company)
        return 3, "Could not parse Claude response"
    except Exception as e:
        logger.error("Claude API error for company '%s': %s", company, e)
        return 3, f"Scoring error: {e}"


def score_jobs(jobs: list[dict]) -> list[dict]:
    """
    Score jobs at the company level with Claude.
    All roles for the same company are evaluated together.
    Returns the full job list with score + rationale applied to each job,
    sorted by score DESC then company name.
    """
    if not jobs:
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)
    groups = _group_by_company(jobs)

    logger.info("Scoring %d companies (%d total jobs) with Claude …", len(groups), len(jobs))

    company_scores: dict[str, tuple[int, str]] = {}
    for i, (company, company_jobs) in enumerate(groups.items(), 1):
        logger.info(
            "Scoring company %d/%d: %s (%d role(s))",
            i, len(groups), company, len(company_jobs),
        )
        score, rationale = _score_company(client, company, company_jobs)
        company_scores[company] = (score, rationale)

    # Apply company score to every individual job
    for job in jobs:
        score, rationale = company_scores.get(job["company"], (3, ""))
        job["score"] = score
        job["rationale"] = rationale

    jobs.sort(key=lambda j: (j.get("score", 0), j.get("company", "")), reverse=True)

    dist = {}
    for j in jobs:
        s = j["score"]
        dist[s] = dist.get(s, 0) + 1
    logger.info("Scoring complete. Score distribution: %s", dist)

    return jobs


def group_by_company(jobs: list[dict]) -> dict[str, list[dict]]:
    """Public helper — returns {company: [jobs]} sorted by score DESC."""
    groups = _group_by_company(jobs)
    return dict(
        sorted(
            groups.items(),
            key=lambda kv: (
                max(j.get("score", 0) for j in kv[1]),
                len(kv[1]),
            ),
            reverse=True,
        )
    )
