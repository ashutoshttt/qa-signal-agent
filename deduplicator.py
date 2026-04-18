"""
SQLite-based deduplication and job storage.
Tracks seen jobs by a stable fingerprint (source + link) to avoid re-alerting.
"""

import hashlib
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "jobs.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables and add enrichment columns if they don't exist."""
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint    TEXT UNIQUE NOT NULL,
                title          TEXT NOT NULL,
                company        TEXT NOT NULL,
                location       TEXT,
                link           TEXT NOT NULL,
                source         TEXT NOT NULL,
                keyword        TEXT,
                score          INTEGER DEFAULT 0,
                rationale      TEXT,
                first_seen     TEXT NOT NULL,
                emailed        INTEGER DEFAULT 0,
                -- Enrichment fields (populated after Apollo/Hunter/signals)
                employee_count INTEGER,
                industry       TEXT,
                funding_stage  TEXT,
                founded_year   INTEGER,
                apollo_url     TEXT,
                funding        TEXT,
                product        TEXT,
                tech_stack     TEXT,
                ai_mentions    TEXT,
                leadership          TEXT,
                repeat_hiring       TEXT,
                contacts            TEXT,
                hiring_velocity     TEXT,
                linkedin_leadership TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fingerprint ON jobs(fingerprint)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_emailed ON jobs(emailed, first_seen)"
        )
        # Add enrichment columns to existing DBs that predate this migration
        enrichment_cols = [
            ("employee_count", "INTEGER"),
            ("industry",       "TEXT"),
            ("funding_stage",  "TEXT"),
            ("founded_year",   "INTEGER"),
            ("apollo_url",     "TEXT"),
            ("funding",        "TEXT"),
            ("product",        "TEXT"),
            ("tech_stack",     "TEXT"),
            ("ai_mentions",    "TEXT"),
            ("leadership",          "TEXT"),
            ("repeat_hiring",       "TEXT"),
            ("contacts",            "TEXT"),
            ("hiring_velocity",     "TEXT"),
            ("linkedin_leadership", "TEXT"),
        ]
        existing = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
        for col, col_type in enrichment_cols:
            if col not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {col_type}")
                logger.debug("Added column: %s", col)
        conn.commit()
    logger.debug("DB initialised at %s", DB_PATH)


def _fingerprint(job: dict) -> str:
    """Stable hash of (source, link) — the canonical dedup key."""
    raw = f"{job['source']}|{job['link']}"
    return hashlib.sha256(raw.encode()).hexdigest()


def filter_new(jobs: list[dict]) -> list[dict]:
    """Return only jobs not already in the DB."""
    if not jobs:
        return []

    fps = {_fingerprint(j): j for j in jobs}
    with _get_conn() as conn:
        placeholders = ",".join("?" * len(fps))
        rows = conn.execute(
            f"SELECT fingerprint FROM jobs WHERE fingerprint IN ({placeholders})",
            list(fps.keys()),
        ).fetchall()

    existing = {r["fingerprint"] for r in rows}
    new_jobs = [j for fp, j in fps.items() if fp not in existing]
    logger.info("Dedup: %d total → %d new", len(jobs), len(new_jobs))
    return new_jobs


def save_jobs(jobs: list[dict]) -> None:
    """Persist scored jobs to the DB."""
    if not jobs:
        return

    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            _fingerprint(j),
            j.get("title", ""),
            j.get("company", ""),
            j.get("location", ""),
            j.get("link", ""),
            j.get("source", ""),
            j.get("keyword", ""),
            j.get("score", 0),
            j.get("rationale", ""),
            now,
        )
        for j in jobs
    ]

    with _get_conn() as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO jobs
              (fingerprint, title, company, location, link, source, keyword,
               score, rationale, first_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        conn.commit()

    logger.info("Saved %d jobs to DB", len(rows))


def save_enrichment(jobs: list[dict]) -> None:
    """Persist enrichment + signal data back to DB for each job."""
    import json as _json
    if not jobs:
        return
    with _get_conn() as conn:
        for j in jobs:
            fp = _fingerprint(j)
            conn.execute(
                """
                UPDATE jobs SET
                    employee_count      = ?,
                    industry            = ?,
                    funding_stage       = ?,
                    founded_year        = ?,
                    apollo_url          = ?,
                    funding             = ?,
                    product             = ?,
                    tech_stack          = ?,
                    ai_mentions         = ?,
                    leadership          = ?,
                    repeat_hiring       = ?,
                    contacts            = ?,
                    hiring_velocity     = ?,
                    linkedin_leadership = ?
                WHERE fingerprint = ?
                """,
                (
                    j.get("employee_count"),
                    j.get("industry", ""),
                    j.get("funding_stage", ""),
                    j.get("founded_year"),
                    j.get("apollo_url", ""),
                    j.get("funding"),
                    j.get("product"),
                    _json.dumps(j.get("tech_stack", [])),
                    _json.dumps(j.get("ai_mentions", [])),
                    j.get("leadership"),
                    j.get("repeat_hiring"),
                    _json.dumps(j.get("contacts", [])),
                    j.get("hiring_velocity"),
                    j.get("linkedin_leadership"),
                    fp,
                ),
            )
        conn.commit()
    logger.info("Saved enrichment for %d jobs", len(jobs))


def get_pending_digest() -> list[dict]:
    """
    Return only net-new jobs that haven't been emailed yet, ordered by score DESC.
    Once a job is emailed it never appears again — keeps each day's digest fresh.
    """
    import json as _json
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, company, location, link, source, score, rationale,
                   employee_count, industry, funding_stage, founded_year, apollo_url,
                   funding, product, tech_stack, ai_mentions, leadership,
                   repeat_hiring, contacts, hiring_velocity, linkedin_leadership
            FROM   jobs
            WHERE  emailed = 0
            ORDER  BY score DESC, first_seen DESC
            """
        ).fetchall()

    jobs = []
    for r in rows:
        j = dict(r)
        # Deserialise JSON fields
        j["tech_stack"]   = _json.loads(j["tech_stack"])   if j.get("tech_stack")   else []
        j["ai_mentions"]  = _json.loads(j["ai_mentions"])  if j.get("ai_mentions")  else []
        j["contacts"]     = _json.loads(j["contacts"])     if j.get("contacts")     else []
        jobs.append(j)
    return jobs


def mark_emailed(job_ids: list[int]) -> None:
    """Mark jobs as included in a digest email."""
    if not job_ids:
        return
    placeholders = ",".join("?" * len(job_ids))
    with _get_conn() as conn:
        conn.execute(
            f"UPDATE jobs SET emailed = 1 WHERE id IN ({placeholders})", job_ids
        )
        conn.commit()
    logger.info("Marked %d jobs as emailed", len(job_ids))
