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
    """Create tables if they don't exist."""
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE NOT NULL,
                title       TEXT NOT NULL,
                company     TEXT NOT NULL,
                location    TEXT,
                link        TEXT NOT NULL,
                source      TEXT NOT NULL,
                keyword     TEXT,
                score       INTEGER DEFAULT 0,
                rationale   TEXT,
                first_seen  TEXT NOT NULL,
                emailed     INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fingerprint ON jobs(fingerprint)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_emailed ON jobs(emailed, first_seen)"
        )
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


def get_pending_digest() -> list[dict]:
    """Return scored, un-emailed jobs ordered by score DESC."""
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, company, location, link, source, score, rationale
            FROM   jobs
            WHERE  emailed = 0
            ORDER  BY score DESC, first_seen DESC
            """
        ).fetchall()

    return [dict(r) for r in rows]


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
