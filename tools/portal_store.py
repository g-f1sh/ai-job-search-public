#!/usr/bin/env python3
"""Durable shared state for the private job-search portal and its agents."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import uuid
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qs, quote, urlsplit, urlunsplit

try:
    from tools.portal_batches import BatchMixin
    from tools.candidate_profile import candidate_required_text
    from tools.verify_pdf import verify_pdf, VerificationError
except ModuleNotFoundError:
    from portal_batches import BatchMixin
    from candidate_profile import candidate_required_text
    from verify_pdf import verify_pdf, VerificationError


APPLICATION_STATUSES = (
    "new",
    "shortlisted",
    "applied",
    "interviewing",
    "offer",
    "rejected_employer",
    "withdrawn",
)
LISTING_STATUSES = ("active", "closed", "unknown")
QUALIFICATION_STATUSES = ("awaiting_evaluation", "recommended", "filtered_out")
RUN_STATES = ("queued", "running", "completed", "failed", "cancelled", "interrupted")
RUN_TASKS = ("search", "refresh_listing", "refresh_bulk", "evaluate", "generate_cv", "chat")
TRACKER_FIELDS = (
    "date",
    "company",
    "sector",
    "role",
    "role_type",
    "channel",
    "status",
    "contact_person",
    "fit_rating",
    "notes",
    "cv_file",
    "cover_letter_file",
    "source",
)
ARTIFACT_TOKEN_RE = re.compile(r"[a-z0-9]+")
ARTIFACT_TOKEN_NOISE = {
    "a",
    "and",
    "at",
    "co",
    "company",
    "cover",
    "cv",
    "sample",
    "candidate",
    "inc",
    "israel",
    "letter",
    "ltd",
    "main",
    "master",
    "of",
    "resume",
    "software",
    "technology",
    "technologies",
    "the",
}


class PortalError(RuntimeError):
    """Base error for validated portal operations."""


class NotFoundError(PortalError):
    pass


class ConflictError(PortalError):
    pass


class ValidationError(PortalError):
    pass


class ClosingSQLiteConnection(sqlite3.Connection):
    """Commit/rollback like sqlite3's context manager, then close deterministically."""

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc, traceback))
        finally:
            self.close()


def utc_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def canonical_job_url(value: object) -> str:
    if not isinstance(value, str):
        return ""
    raw = value.strip()
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return ""
    if parsed.scheme.lower() != "https" or not parsed.netloc or parsed.username or parsed.password:
        return ""
    host = parsed.hostname.casefold() if parsed.hostname else ""
    query = parsed.query
    if host.endswith("linkedin.com"):
        current_id = parse_qs(query).get("currentJobId", [""])[0]
        if current_id.isdigit():
            return f"https://www.linkedin.com/jobs/view/{current_id}"
        parts = [part for part in parsed.path.split("/") if part]
        if "view" in parts:
            index = parts.index("view")
            if index + 1 < len(parts):
                digits = "".join(character for character in parts[index + 1] if character.isdigit())
                if digits:
                    return f"https://www.linkedin.com/jobs/view/{digits}"
    return urlunsplit(("https", parsed.netloc.casefold(), parsed.path.rstrip("/") or "/", query, ""))


def stable_job_id(source_url: object, company: object, title: object) -> str:
    """Create a stable ID from URL, or company+role when no URL exists."""
    url = canonical_job_url(source_url)
    company_text = " ".join(str(company or "").casefold().split())
    title_text = " ".join(str(title or "").casefold().split())
    identity = f"url:{url}" if url else f"role:{company_text}\x1f{title_text}"
    if not url and (not company_text or not title_text):
        raise ValidationError("A job requires a source URL or both company and role")
    return f"job_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]}"


def normalize_application_status(value: object, *, from_seen: bool = False) -> str:
    status = str(value or "").strip().casefold().replace("-", "_").replace(" ", "_")
    direct = {
        "new": "new",
        "shortlist": "shortlisted",
        "shortlisted": "shortlisted",
        "applied": "applied",
        "submitted": "applied",
        "pending": "applied",
        "interview": "interviewing",
        "interviewing": "interviewing",
        "recruiter_screen": "interviewing",
        "recruiter_review": "interviewing",
        "assessment": "interviewing",
        "offer": "offer",
        "offered": "offer",
        "rejected": "rejected_employer",
        "rejected_employer": "rejected_employer",
        "withdrawn": "withdrawn",
        "withdrawn_by_me": "withdrawn",
    }
    if status in direct:
        return direct[status]
    # Ranking is an assessment, not an explicit shortlist/application decision.
    return "new"


def normalize_listing_status(value: object) -> str:
    status = str(value or "").strip().casefold()
    if status in {"closed", "expired", "removed", "inactive"}:
        return "closed"
    if status in {"active", "new", "ranked", "evaluated"}:
        return "active"
    return "unknown"


def legacy_qualification(raw: dict[str, Any]) -> tuple[str, list[str], bool, bool]:
    """Validate an explicit legacy qualification; absence remains an unevaluated discovery."""
    explicit = "qualification_status" in raw or "qualification" in raw
    value = str(raw.get("qualification_status") or raw.get("qualification") or "").strip()
    reasons_value = raw.get("qualification_reasons")
    reasons = (
        [str(item).strip()[:1000] for item in reasons_value if str(item).strip()]
        if isinstance(reasons_value, list)
        else []
    )[:10]
    reviewed = raw.get("responsibilities_reviewed") is True
    if not explicit:
        return "awaiting_evaluation", [], False, False
    if value not in QUALIFICATION_STATUSES:
        return "awaiting_evaluation", ["Invalid legacy qualification was ignored."], False, True
    if value in {"recommended", "filtered_out"} and (not reviewed or not reasons):
        return (
            "awaiting_evaluation",
            ["Qualification requires reviewed responsibilities and specific reasons."],
            False,
            True,
        )
    return value, reasons, reviewed, True


def legacy_tracker_status(status: str) -> str:
    return {
        "new": "new",
        "shortlisted": "shortlisted",
        "applied": "applied",
        "interviewing": "interview",
        "offer": "offer",
        "rejected_employer": "rejected",
        "withdrawn": "withdrawn",
    }[status]


class PortalStore(BatchMixin):
    """SQLite-backed coordination layer with legacy CSV/JSON compatibility."""

    _validation_error = ValidationError
    _conflict_error = ConflictError
    _not_found_error = NotFoundError

    def __init__(
        self,
        repo_root: Path,
        db_path: Path | None = None,
        *,
        migrate: bool = True,
        reconcile: bool = False,
    ):
        self.repo_root = repo_root.resolve()
        self.data_dir = (db_path.parent if db_path else self.repo_root / "portal_data").resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = (db_path or self.data_dir / "portal.sqlite3").resolve()
        self.lock_path = self.data_dir / "portal.lock"
        self._initialize_schema()
        if reconcile:
            self.reconcile_unfinished_runs()
        if migrate:
            self.sync_legacy()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=15,
            isolation_level=None,
            factory=ClosingSQLiteConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    @contextmanager
    def _file_lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _initialize_schema(self) -> None:
        statuses = ",".join(f"'{item}'" for item in APPLICATION_STATUSES)
        listings = ",".join(f"'{item}'" for item in LISTING_STATUSES)
        run_states = ",".join(f"'{item}'" for item in RUN_STATES)
        with self._file_lock(), self._connect() as connection:
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='runs'").fetchone() and not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='batch_children'").fetchone():
                backup = self.data_dir / "backups" / f"portal-before-refresh-batches-{uuid.uuid4().hex}.sqlite3"
                backup.parent.mkdir(parents=True, exist_ok=True)
                with closing(sqlite3.connect(backup)) as destination:
                    connection.backup(destination)
            connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    company TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_url TEXT NOT NULL DEFAULT '',
                    location TEXT NOT NULL DEFAULT '',
                    work_arrangement TEXT NOT NULL DEFAULT '',
                    application_status TEXT NOT NULL DEFAULT 'new'
                        CHECK(application_status IN ({statuses})),
                    listing_status TEXT NOT NULL DEFAULT 'unknown'
                        CHECK(listing_status IN ({listings})),
                    fit_score REAL,
                    fit_recommendation TEXT NOT NULL DEFAULT '',
                    evaluation_reasons TEXT NOT NULL DEFAULT '{{}}',
                    qualification_status TEXT NOT NULL DEFAULT 'awaiting_evaluation'
                        CHECK(qualification_status IN ('awaiting_evaluation','recommended','filtered_out')),
                    qualification_reasons TEXT NOT NULL DEFAULT '[]',
                    qualification_reviewed INTEGER NOT NULL DEFAULT 0,
                    qualification_decided_at TEXT,
                    first_seen TEXT NOT NULL DEFAULT '',
                    last_checked_at TEXT,
                    last_evaluated_at TEXT,
                    next_action TEXT NOT NULL DEFAULT '',
                    source_kind TEXT NOT NULL DEFAULT '',
                    legacy_seen_key TEXT,
                    legacy_tracker_key TEXT,
                    legacy_status TEXT NOT NULL DEFAULT '',
                    cv_file TEXT NOT NULL DEFAULT '',
                    cover_letter_file TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS jobs_application_status ON jobs(application_status);
                CREATE INDEX IF NOT EXISTS jobs_listing_status ON jobs(listing_status);
                CREATE INDEX IF NOT EXISTS jobs_updated_at ON jobs(updated_at DESC);
                CREATE TABLE IF NOT EXISTS legacy_job_snapshots (
                    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    source TEXT NOT NULL CHECK(source IN ('seen','tracker')),
                    facts TEXT NOT NULL,
                    synced_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, source)
                );
                CREATE TABLE IF NOT EXISTS job_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    note TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    idempotency_key TEXT UNIQUE
                );
                CREATE INDEX IF NOT EXISTS job_history_job ON job_history(job_id, id DESC);
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
                    conversation_id TEXT,
                    state TEXT NOT NULL CHECK(state IN ({run_states})),
                    payload TEXT NOT NULL DEFAULT '{{}}',
                    result_summary TEXT NOT NULL DEFAULT '',
                    artifacts TEXT NOT NULL DEFAULT '[]',
                    error TEXT NOT NULL DEFAULT '',
                    codex_thread_id TEXT,
                    codex_turn_id TEXT,
                    idempotency_key TEXT UNIQUE,
                    retry_of TEXT REFERENCES runs(id) ON DELETE SET NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    ended_at TEXT,
                    archived_at TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS runs_state_created ON runs(state, created_at);
                CREATE TABLE IF NOT EXISTS run_requests (
                    idempotency_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_history_archives (
                    idempotency_key TEXT PRIMARY KEY,
                    archived_count INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cv_validations (
                    job_id TEXT PRIMARY KEY REFERENCES jobs(id),
                    path TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    validated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    message TEXT NOT NULL,
                    data TEXT NOT NULL DEFAULT '{{}}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS run_events_run ON run_events(run_id, id);
                CREATE TABLE IF NOT EXISTS batch_children (
                    batch_id TEXT NOT NULL REFERENCES runs(id),
                    run_id TEXT PRIMARY KEY REFERENCES runs(id),
                    job_id TEXT NOT NULL REFERENCES jobs(id)
                );
                CREATE TABLE IF NOT EXISTS run_usage (
                    run_id TEXT PRIMARY KEY REFERENCES runs(id), metrics TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS batch_children_parent ON batch_children(batch_id);
                CREATE TABLE IF NOT EXISTS run_stops (
                    id TEXT PRIMARY KEY, request_key TEXT UNIQUE, batch_id TEXT REFERENCES runs(id),
                    data TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS listing_http_cache (
                    url TEXT PRIMARY KEY, result TEXT NOT NULL, checked_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_sessions (
                    run_id TEXT PRIMARY KEY REFERENCES runs(id),
                    baseline_ids TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_discoveries (
                    run_id TEXT NOT NULL REFERENCES runs(id),
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    raw_json TEXT NOT NULL,
                    is_duplicate INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(run_id, job_id)
                );
                CREATE TABLE IF NOT EXISTS conversations (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
                    codex_thread_id TEXT UNIQUE,
                    source TEXT NOT NULL DEFAULT 'portal',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                    run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
                    role TEXT NOT NULL CHECK(role IN ('user','assistant','system')),
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS messages_conversation ON messages(conversation_id, id);
                CREATE TABLE IF NOT EXISTS approvals (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    rpc_request_id TEXT NOT NULL,
                    method TEXT NOT NULL,
                    params TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('pending','approved','denied','expired')),
                    decision TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS approvals_run ON approvals(run_id, state);
                """
            )
            self._migrate_qualification_schema(connection)
            connection.execute("""INSERT OR IGNORE INTO batch_children
                SELECT p.id,c.id,c.job_id FROM runs c JOIN runs p
                ON p.id=json_extract(c.payload,'$.requested_by')
                WHERE p.task_type='refresh_bulk' AND c.task_type='refresh_listing' AND c.job_id IS NOT NULL""")
            if "archived_at" not in {row["name"] for row in connection.execute("PRAGMA table_info(runs)")}:
                if connection.execute("SELECT 1 FROM runs LIMIT 1").fetchone():
                    backup_path = self.data_dir / "backups" / f"portal-before-run-archive-{uuid.uuid4().hex}.sqlite3"
                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                    with closing(sqlite3.connect(backup_path)) as destination:
                        connection.backup(destination)
                connection.execute("ALTER TABLE runs ADD COLUMN archived_at TEXT")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS jobs_qualification_status ON jobs(qualification_status)"
            )

    def _migrate_qualification_schema(self, connection: sqlite3.Connection) -> None:
        """Add qualification state without treating old title-level fit as a recommendation."""
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(jobs)")
        }
        missing = {
            "qualification_status",
            "qualification_reasons",
            "qualification_reviewed",
            "qualification_decided_at",
        } - columns
        if not missing:
            return

        existing_jobs = int(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0])
        if existing_jobs and not self._metadata(connection, "qualification_backup"):
            stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
            backup_path = self.data_dir / "backups" / f"portal-before-qualification-{stamp}.sqlite3"
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            connection.commit()
            with closing(sqlite3.connect(backup_path)) as destination:
                connection.backup(destination)
            self._set_metadata(
                connection,
                "qualification_backup",
                str(backup_path),
            )

        additions = {
            "qualification_status": (
                "ALTER TABLE jobs ADD COLUMN qualification_status TEXT NOT NULL "
                "DEFAULT 'awaiting_evaluation'"
            ),
            "qualification_reasons": (
                "ALTER TABLE jobs ADD COLUMN qualification_reasons TEXT NOT NULL DEFAULT '[]'"
            ),
            "qualification_reviewed": (
                "ALTER TABLE jobs ADD COLUMN qualification_reviewed INTEGER NOT NULL DEFAULT 0"
            ),
            "qualification_decided_at": (
                "ALTER TABLE jobs ADD COLUMN qualification_decided_at TEXT"
            ),
        }
        connection.execute("BEGIN IMMEDIATE")
        for column, statement in additions.items():
            if column in missing:
                connection.execute(statement)
        now = utc_now()
        for row in connection.execute("SELECT * FROM jobs").fetchall():
            application_status = str(row["application_status"])
            verdict = str(row["fit_recommendation"] or "").strip()
            legacy_status = str(row["legacy_status"] or "").strip().casefold()
            if application_status == "new" and legacy_status == "skipped" and verdict.casefold() not in {
                "",
                "low",
                "medium",
                "high",
            }:
                status = "filtered_out"
                reasons = [verdict]
                reviewed = 1
            else:
                status = "awaiting_evaluation"
                reasons = [
                    "Legacy discovery has not been re-qualified from responsibilities and mandatory requirements."
                ]
                reviewed = 0
            connection.execute(
                """UPDATE jobs SET qualification_status=?,qualification_reasons=?,
                    qualification_reviewed=?,qualification_decided_at=? WHERE id=?""",
                (status, _json(reasons), reviewed, now if reviewed else None, row["id"]),
            )
        self._set_metadata(connection, "qualification_schema_version", "1")
        connection.commit()

    def _metadata(self, connection: sqlite3.Connection, key: str) -> str | None:
        row = connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else None

    def _set_metadata(self, connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO metadata(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def _legacy_digest(self) -> str:
        digest = hashlib.sha256()
        for relative in ("job_scraper/seen_jobs.json", "job_search_tracker.csv"):
            path = self.repo_root / relative
            digest.update(relative.encode())
            if path.is_file():
                digest.update(path.read_bytes())
        return digest.hexdigest()

    def _backup_legacy_once(self, connection: sqlite3.Connection) -> None:
        if self._metadata(connection, "migration_backup"):
            return
        stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        destination = self.data_dir / "backups" / f"portal-migration-{stamp}"
        destination.mkdir(parents=True, exist_ok=False)
        for relative in ("job_scraper/seen_jobs.json", "job_search_tracker.csv"):
            source = self.repo_root / relative
            if source.is_file():
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        self._set_metadata(connection, "migration_backup", str(destination.relative_to(self.repo_root)))

    @staticmethod
    def _legacy_snapshot(
        connection: sqlite3.Connection, job_id: str, source: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT facts FROM legacy_job_snapshots WHERE job_id=? AND source=?",
            (job_id, source),
        ).fetchone()
        return _loads(row["facts"], {}) if row else None

    @staticmethod
    def _save_legacy_snapshot(
        connection: sqlite3.Connection,
        job_id: str,
        source: str,
        facts: dict[str, Any],
        now: str,
    ) -> None:
        connection.execute(
            """INSERT INTO legacy_job_snapshots(job_id,source,facts,synced_at)
               VALUES(?,?,?,?)
               ON CONFLICT(job_id,source) DO UPDATE
               SET facts=excluded.facts,synced_at=excluded.synced_at""",
            (job_id, source, _json(facts), now),
        )

    @staticmethod
    def _legacy_changed(
        previous: dict[str, Any] | None, current: dict[str, Any], *keys: str
    ) -> bool:
        return previous is None or any(previous.get(key) != current.get(key) for key in keys)

    @staticmethod
    def _update_changed_job_fields(
        connection: sqlite3.Connection,
        existing: sqlite3.Row,
        updates: dict[str, Any],
        now: str,
    ) -> sqlite3.Row:
        changed = {key: value for key, value in updates.items() if existing[key] != value}
        if not changed:
            return existing
        assignments = ",".join(f"{key}=?" for key in changed)
        connection.execute(
            f"UPDATE jobs SET {assignments},updated_at=?,version=version+1 WHERE id=?",
            (*changed.values(), now, existing["id"]),
        )
        refreshed = connection.execute("SELECT * FROM jobs WHERE id=?", (existing["id"],)).fetchone()
        assert refreshed is not None
        return refreshed

    def sync_legacy(self, *, force: bool = False) -> int:
        """Repeatably import legacy state without replacing portal-only facts."""
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._backup_legacy_once(connection)
            digest = self._legacy_digest()
            previous_digest = self._metadata(connection, "legacy_digest")
            snapshots_ready = self._metadata(connection, "legacy_snapshot_version") == "1"
            if not force and previous_digest == digest and snapshots_ready:
                connection.commit()
                return 0
            # Databases created before snapshot-aware imports already contain the
            # user's latest portal edits. Seed baselines without replaying stale
            # legacy values over those edits.
            bootstrap_snapshots_only = previous_digest is not None and not snapshots_ready
            now = utc_now()
            imported = 0
            legacy_recommendations = 0
            active_search = connection.execute(
                "SELECT 1 FROM runs WHERE task_type='search' AND state='running' LIMIT 1"
            ).fetchone()
            state_path = self.repo_root / "job_scraper" / "seen_jobs.json"
            try:
                payload = json.loads(state_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
                payload = {}
            seen = payload.get("seen") if isinstance(payload, dict) else {}
            if isinstance(seen, dict):
                for legacy_key, raw in seen.items():
                    if not isinstance(raw, dict):
                        continue
                    company = str(raw.get("company") or "").strip()[:300]
                    title = str(raw.get("title") or "").strip()[:300]
                    if not company or not title:
                        continue
                    url = canonical_job_url(raw.get("url"))
                    job_id = stable_job_id(url, company, title)
                    fit = str(raw.get("fit") or "").strip().lower()
                    recommendation = str(raw.get("rank_verdict") or fit).strip()[:200]
                    reasons = {
                        "strengths": raw.get("strengths") if isinstance(raw.get("strengths"), list) else [],
                        "gaps": raw.get("gaps") if isinstance(raw.get("gaps"), list) else [],
                        "language_gate": raw.get("language_gate"),
                        "language_note": raw.get("language_note"),
                    }
                    score = raw.get("rank_score")
                    if not isinstance(score, (int, float)):
                        score = None
                    existing = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                    application_status = normalize_application_status(raw.get("status"), from_seen=True)
                    listing_status = normalize_listing_status(raw.get("status"))
                    qualification_status, qualification_reasons, qualification_reviewed, qualification_explicit = legacy_qualification(raw)
                    previous = self._legacy_snapshot(connection, job_id, "seen")
                    qualification_block_reason = ""
                    if qualification_status == "recommended" and application_status == "new":
                        tracked_role = connection.execute(
                            """SELECT 1 FROM jobs WHERE lower(company)=lower(?) AND lower(title)=lower(?)
                            AND application_status!='new' AND id!=?""", (company, title, job_id),
                        ).fetchone()
                        if active_search:
                            qualification_block_reason = "Use the validated qualification CLI for this search recommendation."
                        elif tracked_role:
                            qualification_block_reason = "Already tracked company/role; confirm whether this is a different requisition."
                        elif existing is None or (existing["qualification_status"] != "recommended"
                                and (previous is None or previous.get("qualification_status") != "recommended")):
                            if legacy_recommendations >= 8:
                                qualification_block_reason = "Outside the eight-job recommendation shortlist; retained for later review."
                            else:
                                legacy_recommendations += 1
                    elif qualification_status == "recommended" and active_search:
                        qualification_block_reason = "Use the validated qualification CLI for this search recommendation."
                    # Snapshot raw validated source facts, not transient import-policy
                    # decisions. Otherwise ending a run could replay a blocked promotion
                    # or force-sync could downgrade a newer CLI qualification.
                    facts = {
                        "company": company,
                        "title": title,
                        "source_url": url,
                        "location": str(raw.get("location") or "")[:300],
                        "work_arrangement": str(
                            raw.get("work_arrangement") or raw.get("work_mode") or ""
                        )[:100],
                        "application_status": application_status,
                        "listing_status": listing_status,
                        "fit_score": score,
                        "fit_recommendation": recommendation,
                        "evaluation_reasons": reasons,
                        "qualification_status": qualification_status,
                        "qualification_reasons": qualification_reasons,
                        "qualification_reviewed": qualification_reviewed,
                        "qualification_explicit": qualification_explicit,
                        "first_seen": str(raw.get("first_seen") or "")[:30],
                        "last_checked_at": str(raw.get("last_checked_at") or "") or None,
                        "last_evaluated_at": str(
                            raw.get("rank_date") or raw.get("last_evaluated_at") or ""
                        )
                        or None,
                        "next_action": str(raw.get("next_action") or "")[:500],
                        "source_kind": str(raw.get("portal") or "")[:100],
                        "legacy_seen_key": str(legacy_key)[:1000],
                        "legacy_status": str(raw.get("status") or "")[:100],
                    }
                    if existing is None:
                        connection.execute(
                            """INSERT INTO jobs(
                                id,company,title,source_url,location,work_arrangement,
                                application_status,listing_status,fit_score,fit_recommendation,
                                evaluation_reasons,qualification_status,qualification_reasons,
                                qualification_reviewed,qualification_decided_at,
                                first_seen,last_checked_at,last_evaluated_at,
                                next_action,source_kind,legacy_seen_key,legacy_status,created_at,updated_at
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (
                                job_id,
                                facts["company"],
                                facts["title"],
                                facts["source_url"],
                                facts["location"],
                                facts["work_arrangement"],
                                facts["application_status"],
                                facts["listing_status"],
                                facts["fit_score"],
                                facts["fit_recommendation"],
                                _json(facts["evaluation_reasons"]),
                                "awaiting_evaluation" if qualification_block_reason else facts["qualification_status"],
                                _json([qualification_block_reason] if qualification_block_reason else facts["qualification_reasons"]),
                                0 if qualification_block_reason else int(facts["qualification_reviewed"]),
                                now if not qualification_block_reason and facts["qualification_status"] != "awaiting_evaluation" else None,
                                facts["first_seen"],
                                facts["last_checked_at"],
                                facts["last_evaluated_at"],
                                facts["next_action"],
                                facts["source_kind"],
                                facts["legacy_seen_key"],
                                facts["legacy_status"],
                                now,
                                now,
                            ),
                        )
                        imported += 1
                    else:
                        updates: dict[str, Any] = {}
                        if not (bootstrap_snapshots_only and previous is None):
                            for key in (
                                "company",
                                "title",
                                "source_url",
                                "location",
                                "work_arrangement",
                                "first_seen",
                                "next_action",
                                "source_kind",
                                "legacy_seen_key",
                                "legacy_status",
                            ):
                                if self._legacy_changed(previous, facts, key):
                                    updates[key] = facts[key]
                            if self._legacy_changed(previous, facts, "listing_status"):
                                updates["listing_status"] = facts["listing_status"]
                            if facts["last_checked_at"] and self._legacy_changed(
                                previous, facts, "last_checked_at"
                            ):
                                updates["last_checked_at"] = facts["last_checked_at"]
                            if facts["fit_score"] is not None and self._legacy_changed(
                                previous, facts, "fit_score"
                            ):
                                updates["fit_score"] = facts["fit_score"]
                            if facts["fit_recommendation"] and self._legacy_changed(
                                previous, facts, "fit_recommendation"
                            ):
                                updates["fit_recommendation"] = facts["fit_recommendation"]
                            if (
                                any(
                                    value
                                    for value in facts["evaluation_reasons"].values()
                                    if value is not None
                                )
                                and self._legacy_changed(
                                    previous, facts, "evaluation_reasons"
                                )
                            ):
                                updates["evaluation_reasons"] = _json(
                                    facts["evaluation_reasons"]
                                )
                            if facts["last_evaluated_at"] and self._legacy_changed(
                                previous, facts, "last_evaluated_at"
                            ):
                                updates["last_evaluated_at"] = facts["last_evaluated_at"]
                            if not qualification_block_reason and facts["qualification_explicit"] and self._legacy_changed(
                                previous,
                                facts,
                                "qualification_status",
                                "qualification_reasons",
                                "qualification_reviewed",
                            ):
                                updates["qualification_status"] = facts["qualification_status"]
                                updates["qualification_reasons"] = _json(
                                    facts["qualification_reasons"]
                                )
                                updates["qualification_reviewed"] = int(
                                    facts["qualification_reviewed"]
                                )
                                updates["qualification_decided_at"] = (
                                    now
                                    if facts["qualification_status"] != "awaiting_evaluation"
                                    else None
                                )
                                if facts["qualification_reviewed"] and not facts["last_evaluated_at"]:
                                    updates["last_evaluated_at"] = now
                            if (
                                str(existing["application_status"]) == "new"
                                and self._legacy_changed(previous, facts, "application_status")
                            ):
                                updates["application_status"] = facts["application_status"]
                            self._update_changed_job_fields(connection, existing, updates, now)
                    self._save_legacy_snapshot(connection, job_id, "seen", facts, now)

            tracker_path = self.repo_root / "job_search_tracker.csv"
            try:
                with tracker_path.open("r", encoding="utf-8-sig", newline="") as source:
                    tracker_rows = list(csv.DictReader(source))
            except (FileNotFoundError, OSError, UnicodeError, csv.Error):
                tracker_rows = []
            for raw in tracker_rows:
                company = str(raw.get("company") or "").strip()[:300]
                title = str(raw.get("role") or "").strip()[:300]
                if not company or not title:
                    continue
                url = canonical_job_url(raw.get("source"))
                job_id = stable_job_id(url, company, title)
                status = normalize_application_status(raw.get("status"))
                facts = {
                    "company": company,
                    "title": title,
                    "source_url": url,
                    "application_status": status,
                    "fit_recommendation": str(raw.get("fit_rating") or "")[:200],
                    "first_seen": str(raw.get("date") or "")[:30],
                    "source_kind": str(raw.get("channel") or "")[:100],
                    "legacy_tracker_key": job_id,
                    "legacy_status": str(raw.get("status") or "")[:100],
                    "cv_file": str(raw.get("cv_file") or "")[:500],
                    "cover_letter_file": str(raw.get("cover_letter_file") or "")[:500],
                    "notes": str(raw.get("notes") or "")[:4000],
                    "qualification_status": "awaiting_evaluation",
                    "qualification_reasons": [],
                }
                previous = self._legacy_snapshot(connection, job_id, "tracker")
                existing = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                if existing is None:
                    connection.execute(
                        """INSERT INTO jobs(
                            id,company,title,source_url,application_status,listing_status,
                            fit_recommendation,qualification_status,qualification_reasons,
                            qualification_reviewed,qualification_decided_at,
                            first_seen,source_kind,legacy_tracker_key,
                            legacy_status,cv_file,cover_letter_file,created_at,updated_at
                        ) VALUES(?,?,?,?,?,'unknown',?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            job_id,
                            company,
                            title,
                            url,
                            status,
                            facts["fit_recommendation"],
                            facts["qualification_status"],
                            _json(facts["qualification_reasons"]),
                            0,
                            None,
                            facts["first_seen"],
                            facts["source_kind"],
                            job_id,
                            facts["legacy_status"],
                            facts["cv_file"],
                            facts["cover_letter_file"],
                            now,
                            now,
                        ),
                    )
                    imported += 1
                else:
                    old_status = str(existing["application_status"])
                    updates = {}
                    if not (bootstrap_snapshots_only and previous is None):
                        for key in (
                            "company",
                            "title",
                            "source_url",
                            "legacy_tracker_key",
                            "legacy_status",
                        ):
                            if self._legacy_changed(previous, facts, key):
                                updates[key] = facts[key]
                        if self._legacy_changed(previous, facts, "application_status"):
                            updates["application_status"] = facts["application_status"]
                        if (
                            facts["fit_recommendation"]
                            and self._legacy_changed(previous, facts, "fit_recommendation")
                        ):
                            updates["fit_recommendation"] = facts["fit_recommendation"]
                        if not str(existing["first_seen"]) and facts["first_seen"]:
                            updates["first_seen"] = facts["first_seen"]
                        if (
                            facts["source_kind"]
                            and self._legacy_changed(previous, facts, "source_kind")
                        ):
                            updates["source_kind"] = facts["source_kind"]
                        for key in ("cv_file", "cover_letter_file"):
                            if facts[key] and self._legacy_changed(previous, facts, key):
                                updates[key] = facts[key]
                        existing = self._update_changed_job_fields(
                            connection, existing, updates, now
                        )
                    if old_status != str(existing["application_status"]):
                        connection.execute(
                            """INSERT INTO job_history(
                                job_id,event_type,old_value,new_value,note,source,created_at
                            ) VALUES(?,?,?,?,?,?,?)""",
                            (
                                job_id,
                                "application_status",
                                old_status,
                                str(existing["application_status"]),
                                facts["notes"],
                                "legacy_import",
                                now,
                            ),
                        )
                self._save_legacy_snapshot(connection, job_id, "tracker", facts, now)
                notes = facts["notes"].strip()
                if notes and not connection.execute(
                    "SELECT 1 FROM job_history WHERE job_id=? AND source='legacy_import' AND note=?",
                    (job_id, notes[:4000]),
                ).fetchone():
                    connection.execute(
                        "INSERT INTO job_history(job_id,event_type,note,source,created_at) VALUES(?,?,?,?,?)",
                        (job_id, "legacy_note", notes[:4000], "legacy_import", now),
                    )
            self._set_metadata(connection, "legacy_digest", digest)
            self._set_metadata(connection, "legacy_last_synced", now)
            self._set_metadata(connection, "legacy_snapshot_version", "1")
            connection.commit()
            return imported

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["evaluation_reasons"] = _loads(result.get("evaluation_reasons"), {})
        result["qualification_reasons"] = _loads(result.get("qualification_reasons"), [])
        result["qualification_reviewed"] = bool(result.get("qualification_reviewed"))
        candidates = [
            str(result.get(name) or "")
            for name in ("history_updated_at", "last_checked_at", "last_evaluated_at", "first_seen")
        ]
        result["last_relevant_update"] = max((item for item in candidates if item), default="")
        result.pop("history_updated_at", None)
        return result

    def list_jobs(
        self,
        *,
        query: str = "",
        statuses: list[str] | None = None,
        include_archived: bool = False,
        include_discoveries: bool = False,
        listing_status: str | None = None,
    ) -> list[dict[str, Any]]:
        self.sync_legacy()
        clauses: list[str] = []
        params: list[Any] = []
        if query.strip():
            clauses.append("(company LIKE ? OR title LIKE ? OR location LIKE ? OR fit_recommendation LIKE ?)")
            pattern = f"%{query.strip()}%"
            params.extend([pattern] * 4)
        if statuses:
            invalid = set(statuses) - set(APPLICATION_STATUSES)
            if invalid:
                raise ValidationError(f"Unknown application status: {sorted(invalid)[0]}")
            clauses.append(f"application_status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)
        elif not include_archived:
            clauses.append("application_status NOT IN ('rejected_employer','withdrawn')")
        if not include_discoveries:
            clauses.append("((qualification_status='recommended' AND qualification_reviewed=1 "
                           "AND qualification_reasons!='[]') OR application_status!='new')")
        if listing_status:
            if listing_status not in LISTING_STATUSES:
                raise ValidationError("Unknown listing status")
            clauses.append("listing_status=?")
            params.append(listing_status)
        sql = (
            "SELECT jobs.*,"
            "(SELECT MAX(created_at) FROM job_history WHERE job_id=jobs.id) AS history_updated_at "
            "FROM jobs"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC, company COLLATE NOCASE, title COLLATE NOCASE"
        with self._connect() as connection:
            jobs = [self._row_to_job(row) for row in connection.execute(sql, params)]
            for job in jobs:
                job["cv"] = self._cv_state(connection, job)
            return jobs

    def get_job(self, job_id: str) -> dict[str, Any]:
        self.sync_legacy()
        with self._connect() as connection:
            return self._get_job_from_connection(connection, job_id)

    def _get_job_from_connection(
        self, connection: sqlite3.Connection, job_id: str
    ) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("Job was not found")
        job = self._row_to_job(row)
        job["history"] = [
            dict(item)
            for item in connection.execute(
                "SELECT * FROM job_history WHERE job_id=? ORDER BY id DESC", (job_id,)
            )
        ]
        history_times = [str(item.get("created_at") or "") for item in job["history"]]
        job["last_relevant_update"] = max(
            [
                *history_times,
                str(job.get("last_checked_at") or ""),
                str(job.get("last_evaluated_at") or ""),
                str(job.get("first_seen") or ""),
            ],
            default="",
        )
        job["artifacts"] = self._job_artifacts(job)
        job["cv"] = self._cv_state(connection, job)
        job["discoveries"] = [
            {"run_id": item["run_id"], "is_duplicate": bool(item["is_duplicate"]),
             "raw": _loads(item["raw_json"], {})}
            for item in connection.execute(
                "SELECT * FROM search_discoveries WHERE job_id=? ORDER BY created_at DESC", (job_id,)
            )
        ]
        return job

    def _cv_state(self, connection: sqlite3.Connection, job: dict[str, Any]) -> dict[str, Any]:
        latest = connection.execute(
            "SELECT * FROM runs WHERE task_type='generate_cv' AND job_id=? "
            "ORDER BY CASE WHEN state IN ('queued','running') THEN 0 ELSE 1 END,created_at DESC,rowid DESC LIMIT 1",
            (job["id"],),
        ).fetchone()
        result: dict[str, Any] = {"state": "not_generated", "run_id": latest["id"] if latest else None}
        if latest and latest["state"] in ("queued", "running"):
            result["state"] = "queued" if latest["state"] == "queued" else "generating"
            result.update(self._queue_info(connection, latest))
            event = connection.execute(
                "SELECT message,created_at FROM run_events WHERE run_id=? AND kind!='assistant_delta' ORDER BY id DESC LIMIT 1",
                (latest["id"],),
            ).fetchone()
            if event:
                result["progress"] = event["message"][:300]
                result["updated_at"] = event["created_at"]
        elif latest and latest["state"] in ("failed", "cancelled", "interrupted"):
            result.update(state="failed", error=latest["error"] or latest["result_summary"] or latest["state"])
        validation = connection.execute("SELECT * FROM cv_validations WHERE job_id=?", (job["id"],)).fetchone()
        if validation:
            path = (self.repo_root / validation["path"]).resolve()
            try:
                stat = path.stat()
                valid = (path.is_file() and self.repo_root in path.parents and
                         stat.st_size == validation["size"] and stat.st_mtime_ns == validation["mtime_ns"] and
                         hashlib.sha256(path.read_bytes()).hexdigest() == validation["digest"])
            except OSError:
                valid = False
            # A legacy re-import that points at a different artifact must invalidate readiness.
            raw = str(job.get("cv_file") or "").replace("\\", "/")
            if raw.endswith(".tex"):
                raw = raw[:-4] + ".pdf"
            if valid and raw == validation["path"]:
                result.update(url=f"/files/{quote(validation['path'], safe='/')}", path=validation["path"],
                              validated_at=validation["validated_at"], kind="CV", filename=Path(validation["path"]).name)
                if result["state"] == "not_generated":
                    result["state"] = "ready"
            elif latest and latest["state"] == "completed":
                result.update(state="failed", error="CV changed or is missing; generate again to validate it")
        elif latest and latest["state"] == "completed":
            result.update(state="failed", error="Run ended without a validated PDF; retry generation")
        return result

    @staticmethod
    def _queue_info(connection: sqlite3.Connection, run: sqlite3.Row) -> dict[str, Any]:
        if run["state"] != "queued":
            return {"queue_position": None, "waiting_for": None}
        parent = connection.execute("SELECT p.id,p.task_type,p.job_id FROM runs p JOIN batch_children b ON b.batch_id=p.id WHERE b.run_id=?", (run["id"],)).fetchone()
        if parent:
            queued = list(connection.execute("SELECT r.id FROM runs r JOIN batch_children b ON b.run_id=r.id WHERE b.batch_id=? AND r.state='queued' ORDER BY r.created_at,r.rowid", (parent["id"],)))
            running = parent
        else:
            queued = list(connection.execute("SELECT id FROM runs WHERE state='queued' AND id NOT IN (SELECT run_id FROM batch_children) ORDER BY created_at,rowid"))
            running = connection.execute("SELECT id,task_type,job_id FROM runs WHERE state='running' AND id NOT IN (SELECT run_id FROM batch_children) LIMIT 1").fetchone()
        return {"queue_position": next((i+1 for i, row in enumerate(queued) if row["id"]==run["id"]), None),
                "waiting_for": dict(running) if running else None}

    def _job_artifacts(self, job: dict[str, Any]) -> list[dict[str, str]]:
        artifacts: list[dict[str, str]] = []
        for field, kind, directory in (
            ("cv_file", "CV", "cv"),
            ("cover_letter_file", "Cover letter", "cover_letters"),
        ):
            raw = str(job.get(field) or "")
            if not raw:
                continue
            filename = Path(raw.replace("\\", "/")).name
            candidates = [filename]
            if filename.lower().endswith(".tex"):
                candidates.insert(0, f"{filename[:-4]}.pdf")
            for candidate in candidates:
                path = self.repo_root / directory / candidate
                if path.is_file():
                    artifacts.append(
                        {
                            "kind": kind,
                            "filename": candidate,
                            "url": f"/files/{directory}/{quote(candidate)}",
                        }
                    )
                    break
        present = {item["kind"] for item in artifacts}
        for kind, directory in (("CV", "cv"), ("Cover letter", "cover_letters")):
            if kind in present:
                continue
            inferred = self._infer_artifact(job, directory)
            if inferred:
                artifacts.append(
                    {
                        "kind": kind,
                        "filename": inferred,
                        "url": f"/files/{directory}/{quote(inferred)}",
                        "source": "inferred_from_filename",
                    }
                )
        return artifacts

    @staticmethod
    def _artifact_tokens(value: object) -> set[str]:
        tokens: set[str] = set()
        for token in ARTIFACT_TOKEN_RE.findall(str(value or "").casefold()):
            if token in ARTIFACT_TOKEN_NOISE:
                continue
            tokens.add(token)
            if len(token) > 4 and token.endswith("s"):
                tokens.add(token[:-1])
            if token in {"presale", "presales"}:
                tokens.update({"pre", "sale", "sales", "presale", "presales"})
        return tokens

    def _infer_artifact(self, job: dict[str, Any], directory: str) -> str:
        """Conservatively associate pre-portal artifacts without changing stored state."""
        company_tokens = self._artifact_tokens(job.get("company"))
        role_tokens = self._artifact_tokens(job.get("title"))
        best_name = ""
        best_score = -1.0
        artifact_dir = self.repo_root / directory
        if not artifact_dir.is_dir():
            return ""
        for path in artifact_dir.glob("*.pdf"):
            if path.name in {"Candidate_Master_CV.pdf", "main_example.pdf"}:
                continue
            document_tokens = self._artifact_tokens(path.stem)
            company_overlap = len(company_tokens & document_tokens)
            if company_overlap == 0:
                continue
            role_overlap = len(role_tokens & document_tokens)
            role_coverage = role_overlap / max(len(role_tokens), 1)
            if role_overlap < min(2, len(role_tokens)) and role_coverage < 0.5:
                continue
            score = company_overlap * 8 + role_overlap * 3 + role_coverage
            if score > best_score:
                best_name = path.name
                best_score = score
        return best_name

    def _read_tracker(self) -> list[dict[str, str]]:
        path = self.repo_root / "job_search_tracker.csv"
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != list(TRACKER_FIELDS):
                raise ValidationError("Application tracker has an unexpected header")
            return [{field: row.get(field, "") for field in TRACKER_FIELDS} for row in reader]

    def _write_tracker(self, rows: list[dict[str, str]]) -> None:
        path = self.repo_root / "job_search_tracker.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", newline="", dir=path.parent, delete=False
            ) as destination:
                temporary_name = destination.name
                writer = csv.DictWriter(destination, fieldnames=TRACKER_FIELDS, lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temporary_name, path)
        finally:
            if temporary_name:
                Path(temporary_name).unlink(missing_ok=True)

    def _mirror_tracker(
        self,
        job: sqlite3.Row,
        *,
        status: str | None = None,
        note: str = "",
        occurred_at: str,
    ) -> None:
        rows = self._read_tracker()
        target: dict[str, str] | None = None
        for row in rows:
            try:
                row_id = stable_job_id(row.get("source"), row.get("company"), row.get("role"))
            except ValidationError:
                continue
            if row_id == job["id"]:
                target = row
                break
        trackable = status in {"applied", "interviewing", "offer", "rejected_employer", "withdrawn"}
        if target is None and not trackable:
            return
        if target is None:
            target = {field: "" for field in TRACKER_FIELDS}
            target.update(
                {
                    "date": occurred_at[:10],
                    "company": str(job["company"]),
                    "role": str(job["title"]),
                    "channel": "portal",
                    "fit_rating": str(job["fit_recommendation"]),
                    "source": str(job["source_url"]),
                    "cv_file": str(job["cv_file"]),
                    "cover_letter_file": str(job["cover_letter_file"]),
                }
            )
            rows.append(target)
        if status:
            target["status"] = legacy_tracker_status(status)
        if note:
            existing = target.get("notes", "").strip()
            target["notes"] = f"{existing} {occurred_at[:10]}: {note}".strip()
        self._write_tracker(rows)

    def update_application_status(
        self,
        job_id: str,
        status: str,
        *,
        source: str,
        note: str = "",
        idempotency_key: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        if status not in APPLICATION_STATUSES:
            raise ValidationError("Unknown application status")
        if not source.strip():
            raise ValidationError("Update source is required")
        if idempotency_key and len(idempotency_key) > 200:
            raise ValidationError("Idempotency key is too long")
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                prior = connection.execute(
                    "SELECT job_id FROM job_history WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if prior:
                    if prior["job_id"] != job_id:
                        raise ConflictError("Idempotency key belongs to another job")
                    connection.rollback()
                    return self._get_job_from_connection(connection, job_id)
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("Job was not found")
            if expected_version is not None and int(row["version"]) != expected_version:
                raise ConflictError("Job changed since it was loaded")
            old_status = str(row["application_status"])
            if old_status == status:
                connection.commit()
                return self._get_job_from_connection(connection, job_id)
            now = utc_now()
            connection.execute(
                "UPDATE jobs SET application_status=?,updated_at=?,version=version+1 WHERE id=?",
                (status, now, job_id),
            )
            connection.execute(
                """INSERT INTO job_history(
                    job_id,event_type,old_value,new_value,note,source,created_at,idempotency_key
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (job_id, "application_status", old_status, status, note[:4000], source[:100], now, idempotency_key),
            )
            updated = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            assert updated is not None
            self._mirror_tracker(updated, status=status, note=note, occurred_at=now)
            self._set_metadata(connection, "legacy_digest", self._legacy_digest())
            connection.commit()
        return self.get_job(job_id)

    def set_qualification(
        self,
        job_id: str,
        status: str,
        *,
        reasons: list[str],
        responsibilities_reviewed: bool,
        source: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        if status not in QUALIFICATION_STATUSES:
            raise ValidationError("Unknown qualification status")
        cleaned = [str(reason).strip()[:1000] for reason in reasons if str(reason).strip()][:10]
        if status in {"recommended", "filtered_out"}:
            if not responsibilities_reviewed:
                raise ValidationError(
                    "A qualification decision requires review of responsibilities and mandatory requirements"
                )
            if not cleaned:
                raise ValidationError("A qualification decision requires specific reasons")
        now = utc_now()
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("Job was not found")
            # A worker search owns all qualification writes while it runs, including
            # CLI calls that omit --run-id. This keeps the shortlist cap transactional.
            if run_id is None:
                active = connection.execute(
                    "SELECT id FROM runs WHERE task_type='search' AND state='running' LIMIT 1"
                ).fetchone()
                run_id = str(active["id"]) if active else None
            if run_id:
                session = connection.execute(
                    "SELECT baseline_ids FROM search_sessions WHERE run_id=?", (run_id,)
                ).fetchone()
                if session is None:
                    raise ValidationError("Search session has not started")
                baseline = set(_loads(session["baseline_ids"], []))
                tracked_role = connection.execute(
                    """SELECT 1 FROM jobs WHERE lower(company)=lower(?) AND lower(title)=lower(?)
                    AND application_status!='new' AND id!=?""",
                    (row["company"], row["title"], job_id),
                ).fetchone()
                connection.execute(
                    """INSERT OR IGNORE INTO search_discoveries
                        (run_id,job_id,raw_json,is_duplicate,created_at) VALUES(?,?,?,?,?)""",
                    (run_id, job_id, _json(dict(row)), int(job_id in baseline or tracked_role is not None), now),
                )
                candidate = connection.execute(
                    "SELECT is_duplicate FROM search_discoveries WHERE run_id=? AND job_id=?",
                    (run_id, job_id),
                ).fetchone()
                if status == "recommended":
                    if candidate["is_duplicate"]:
                        raise ValidationError("An existing/duplicate job is not a new search recommendation")
                    count = connection.execute(
                        """SELECT COUNT(*) FROM search_discoveries d JOIN jobs j ON j.id=d.job_id
                        WHERE d.run_id=? AND d.is_duplicate=0 AND j.qualification_status='recommended'
                        AND j.id!=?""", (run_id, job_id),
                    ).fetchone()[0]
                    if count >= 8:
                        raise ValidationError("A search run may recommend at most 8 jobs")
            old = str(row["qualification_status"])
            if (old == status and _loads(row["qualification_reasons"], []) == cleaned
                    and bool(row["qualification_reviewed"]) == responsibilities_reviewed):
                connection.commit()
                return self._get_job_from_connection(connection, job_id)
            connection.execute(
                """UPDATE jobs SET qualification_status=?,qualification_reasons=?,
                    qualification_reviewed=?,qualification_decided_at=?,
                    last_evaluated_at=CASE WHEN ? THEN ? ELSE last_evaluated_at END,
                    updated_at=?,version=version+1
                    WHERE id=?""",
                (
                    status,
                    _json(cleaned),
                    int(responsibilities_reviewed),
                    now if status != "awaiting_evaluation" else None,
                    int(responsibilities_reviewed),
                    now,
                    now,
                    job_id,
                ),
            )
            connection.execute(
                """INSERT INTO job_history(
                    job_id,event_type,old_value,new_value,note,source,created_at
                ) VALUES(?,?,?,?,?,?,?)""",
                (
                    job_id,
                    "qualification",
                    old,
                    status,
                    _json(cleaned)[:4000],
                    source[:100],
                    now,
                ),
            )
            connection.commit()
        return self.get_job(job_id)

    def add_note(
        self,
        job_id: str,
        note: str,
        *,
        source: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        note = note.strip()
        if not note or len(note) > 4000:
            raise ValidationError("Note must be between 1 and 4000 characters")
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                prior = connection.execute(
                    "SELECT job_id FROM job_history WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if prior:
                    if prior["job_id"] != job_id:
                        raise ConflictError("Idempotency key belongs to another job")
                    connection.rollback()
                    return self._get_job_from_connection(connection, job_id)
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("Job was not found")
            now = utc_now()
            connection.execute(
                "INSERT INTO job_history(job_id,event_type,note,source,created_at,idempotency_key) VALUES(?,?,?,?,?,?)",
                (job_id, "note", note, source[:100], now, idempotency_key),
            )
            connection.execute("UPDATE jobs SET updated_at=?,version=version+1 WHERE id=?", (now, job_id))
            self._mirror_tracker(row, note=note, occurred_at=now)
            self._set_metadata(connection, "legacy_digest", self._legacy_digest())
            connection.commit()
        return self.get_job(job_id)

    def set_listing_status(
        self, job_id: str, status: str, *, source: str, note: str = "", run_id: str | None = None
    ) -> dict[str, Any]:
        if status not in LISTING_STATUSES:
            raise ValidationError("Unknown listing status")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if run_id:
                run = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
                if not run or run["job_id"] != job_id or run["task_type"] != "refresh_listing" or run["cancel_requested"] or run["state"] != "running":
                    raise ConflictError("Listing check was stopped")
                parent = connection.execute("SELECT batch_id FROM batch_children WHERE run_id=?", (run_id,)).fetchone()
                self._check_parent_fence(connection, parent[0] if parent else None)
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("Job was not found")
            old = str(row["listing_status"])
            now = utc_now()
            connection.execute(
                "UPDATE jobs SET listing_status=?,last_checked_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, now, now, job_id),
            )
            connection.execute(
                "INSERT INTO job_history(job_id,event_type,old_value,new_value,note,source,created_at) VALUES(?,?,?,?,?,?,?)",
                (job_id, "listing_status", old, status, note[:4000], source[:100], now),
            )
            connection.commit()
        return self.get_job(job_id)

    def set_evaluation(
        self,
        job_id: str,
        *,
        score: float | None,
        recommendation: str,
        reasons: dict[str, Any],
        source: str,
    ) -> dict[str, Any]:
        if score is not None and not 0 <= score <= 100:
            raise ValidationError("Fit score must be between 0 and 100")
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("Job was not found")
            connection.execute(
                """UPDATE jobs SET fit_score=?,fit_recommendation=?,evaluation_reasons=?,
                    last_evaluated_at=?,updated_at=?,version=version+1 WHERE id=?""",
                (score, recommendation[:300], _json(reasons), now, now, job_id),
            )
            connection.execute(
                "INSERT INTO job_history(job_id,event_type,new_value,note,source,created_at) VALUES(?,?,?,?,?,?)",
                (job_id, "evaluation", recommendation[:300], _json(reasons)[:4000], source[:100], now),
            )
            connection.commit()
        return self.get_job(job_id)

    def update_job_details(
        self,
        job_id: str,
        *,
        location: str | None = None,
        work_arrangement: str | None = None,
        next_action: str | None = None,
        source: str,
    ) -> dict[str, Any]:
        updates: list[str] = []
        params: list[Any] = []
        for column, value, limit in (
            ("location", location, 300),
            ("work_arrangement", work_arrangement, 100),
            ("next_action", next_action, 500),
        ):
            if value is not None:
                updates.append(f"{column}=?")
                params.append(value.strip()[:limit])
        if not updates:
            raise ValidationError("At least one job detail is required")
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if not connection.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone():
                raise NotFoundError("Job was not found")
            connection.execute(
                f"UPDATE jobs SET {','.join(updates)},updated_at=?,version=version+1 WHERE id=?",
                (*params, now, job_id),
            )
            connection.execute(
                "INSERT INTO job_history(job_id,event_type,note,source,created_at) VALUES(?,?,?,?,?)",
                (job_id, "details", "Job details updated", source[:100], now),
            )
            connection.commit()
        return self.get_job(job_id)

    def set_artifact(
        self, job_id: str, kind: str, relative_path: str, *, source: str
    ) -> dict[str, Any]:
        field = {"cv": "cv_file", "cover_letter": "cover_letter_file"}.get(kind)
        if field is None:
            raise ValidationError("Unknown artifact kind")
        relative = Path(relative_path.replace("\\", "/"))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValidationError("Artifact path must be repository-relative")
        resolved = (self.repo_root / relative).resolve()
        if not resolved.is_file() or self.repo_root not in resolved.parents:
            raise ValidationError("Artifact does not exist in this repository")
        validation = None
        if kind == "cv":
            if relative.suffix.lower() == ".tex":
                relative = relative.with_suffix(".pdf")
                resolved = (self.repo_root / relative).resolve()
            if relative.parts[0] != "cv" or relative.suffix.lower() != ".pdf" or self.repo_root not in resolved.parents:
                raise ValidationError("CV must be a PDF inside cv/")
            try:
                before = resolved.read_bytes()
                verify_pdf(resolved, expected_pages=1, min_chars=300, required_text=candidate_required_text(self.repo_root))
                if resolved.read_bytes() != before:
                    raise ValidationError("CV changed during validation; retry")
                stat = resolved.stat()
                validation = (hashlib.sha256(before).hexdigest(), stat.st_size, stat.st_mtime_ns)
            except (VerificationError, OSError) as exc:
                raise ValidationError(f"CV validation failed: {exc}") from exc
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if not connection.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone():
                raise NotFoundError("Job was not found")
            connection.execute(
                f"UPDATE jobs SET {field}=?,updated_at=?,version=version+1 WHERE id=?",
                (relative.as_posix(), now, job_id),
            )
            if validation:
                connection.execute(
                    "INSERT OR REPLACE INTO cv_validations VALUES(?,?,?,?,?,?)",
                    (job_id, relative.as_posix(), *validation, now),
                )
            connection.execute(
                "INSERT INTO job_history(job_id,event_type,new_value,note,source,created_at) VALUES(?,?,?,?,?,?)",
                (job_id, "artifact", kind, relative.as_posix(), source[:100], now),
            )
            connection.commit()
        return self.get_job(job_id)

    def enqueue_run(
        self,
        task_type: str,
        *,
        job_id: str | None = None,
        conversation_id: str | None = None,
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        retry_of: str | None = None,
        require_new: bool = False,
    ) -> dict[str, Any]:
        if task_type not in RUN_TASKS:
            raise ValidationError("Unknown task type")
        if task_type == "refresh_bulk":
            if retry_of:
                original = self.get_run(retry_of)
                if original["cancel_requested"]:
                    raise ConflictError("Stopped batches cannot be retried; preview a new selection")
            return self.enqueue_refresh_batch(payload, idempotency_key=idempotency_key)
        if task_type == "generate_cv" and not job_id:
            raise ValidationError("CV generation requires a stable job ID")
        if task_type == "refresh_listing" and not job_id:
            raise ValidationError("Listing refresh requires a stable job ID")
        if idempotency_key and len(idempotency_key) > 200:
            raise ValidationError("Idempotency key is too long")
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            parent_id = (payload or {}).get("requested_by")
            if require_new:
                selected = connection.execute("SELECT application_status,qualification_status FROM jobs WHERE id=?", (job_id,)).fetchone()
                if not selected or selected['application_status'] != 'new' or selected['qualification_status'] != 'recommended':
                    raise ConflictError('Job left the qualified New column; not queued')
            if retry_of:
                parent = connection.execute("SELECT batch_id FROM batch_children WHERE run_id=?", (retry_of,)).fetchone()
                parent_id = parent[0] if parent else parent_id
            self._check_parent_fence(connection, parent_id)
            if job_id and not connection.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone():
                raise NotFoundError("Job was not found")
            if conversation_id and not connection.execute(
                "SELECT 1 FROM conversations WHERE id=?", (conversation_id,)
            ).fetchone():
                raise NotFoundError("Conversation was not found")
            if idempotency_key:
                request_match = connection.execute(
                    """SELECT runs.* FROM run_requests
                       JOIN runs ON runs.id=run_requests.run_id
                       WHERE run_requests.idempotency_key=?""",
                    (idempotency_key,),
                ).fetchone()
                if request_match:
                    connection.rollback()
                    return self._run_dict(request_match)
                existing = connection.execute(
                    "SELECT * FROM runs WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if existing:
                    connection.execute(
                        "INSERT OR IGNORE INTO run_requests(idempotency_key,run_id,created_at) VALUES(?,?,?)",
                        (idempotency_key, existing["id"], now),
                    )
                    connection.commit()
                    return self._run_dict(existing)
            if task_type == "chat" and conversation_id and connection.execute(
                "SELECT 1 FROM runs WHERE conversation_id=? AND state IN ('queued','running')",
                (conversation_id,),
            ).fetchone():
                raise ConflictError("This conversation already has an active turn")
            if task_type != "chat":
                active = connection.execute(
                    """SELECT * FROM runs
                       WHERE task_type=? AND COALESCE(job_id,'')=COALESCE(?,'')
                         AND (? OR payload=?) AND state IN ('queued','running')
                       ORDER BY created_at LIMIT 1""",
                    (task_type, job_id, task_type in ("generate_cv", "refresh_listing"), _json(payload or {})),
                ).fetchone()
                if active:
                    if idempotency_key:
                        connection.execute(
                            "INSERT OR IGNORE INTO run_requests(idempotency_key,run_id,created_at) VALUES(?,?,?)",
                            (idempotency_key, active["id"], now),
                        )
                        connection.commit()
                    else:
                        connection.rollback()
                    return self._run_dict(active)
            run_id = f"run_{uuid.uuid4().hex}"
            connection.execute(
                """INSERT INTO runs(
                    id,task_type,job_id,conversation_id,state,payload,idempotency_key,retry_of,
                    created_at,updated_at
                ) VALUES(?,?,?,?, 'queued',?,?,?,?,?)""",
                (run_id, task_type, job_id, conversation_id, _json(payload or {}), idempotency_key, retry_of, now, now),
            )
            connection.execute(
                "INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,?,?,?)",
                (run_id, "queued", "Task queued", now),
            )
            if parent_id and task_type == "refresh_listing":
                parent = connection.execute("SELECT task_type FROM runs WHERE id=?", (parent_id,)).fetchone()
                if parent and parent[0] == "refresh_bulk":
                    connection.execute("INSERT INTO batch_children VALUES(?,?,?)", (parent_id, run_id, job_id))
            if idempotency_key:
                connection.execute(
                    "INSERT INTO run_requests(idempotency_key,run_id,created_at) VALUES(?,?,?)",
                    (idempotency_key, run_id, now),
                )
            row = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            connection.commit()
            assert row is not None
            return self._run_dict(row)

    @staticmethod
    def _run_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = _loads(result.get("payload"), {})
        result["artifacts"] = _loads(result.get("artifacts"), [])
        result["cancel_requested"] = bool(result.get("cancel_requested"))
        return result

    def list_runs(self, limit: int = 100, *, include_archived: bool = False) -> list[dict[str, Any]]:
        """Bound terminal history, never exclude active work because of its age."""
        with self._connect() as connection:
            active = list(connection.execute(
                "SELECT * FROM runs WHERE state IN ('queued','running') ORDER BY created_at DESC,id DESC"
            ))
            history = list(connection.execute(
                "SELECT * FROM runs WHERE state NOT IN ('queued','running') "
                "AND (? OR archived_at IS NULL) ORDER BY created_at DESC,id DESC LIMIT ?",
                (include_archived, min(max(limit, 1), 500)),
            ))
            return [self._run_extra(connection, self._run_dict(row)) for row in active + history]

    def run_history_counts(self) -> dict[str, int]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(CASE WHEN archived_at IS NULL THEN 1 END) AS clearable, "
                "COUNT(CASE WHEN archived_at IS NOT NULL THEN 1 END) AS archived "
                "FROM runs WHERE state IN ('completed','failed','cancelled','interrupted')"
            ).fetchone()
            return dict(row)

    def archive_run_history(
        self, *, source: str = "portal", idempotency_key: str | None = None
    ) -> dict[str, int]:
        """Archive terminal runs atomically; leave execution and linked data untouched."""
        if idempotency_key is not None and (
            not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 200
        ):
            raise ValidationError("Invalid idempotency key")
        if not isinstance(source, str) or not 1 <= len(source) <= 100:
            raise ValidationError("Invalid update source")
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                previous = connection.execute(
                    "SELECT archived_count FROM run_history_archives WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                if previous:
                    connection.commit()
                    return {"archived_count": previous["archived_count"]}
            now = utc_now()
            count = connection.execute(
                "UPDATE runs SET archived_at=? WHERE archived_at IS NULL "
                "AND state IN ('completed','failed','cancelled','interrupted')", (now,),
            ).rowcount
            if idempotency_key:
                connection.execute(
                    "INSERT INTO run_history_archives VALUES(?,?,?,?)",
                    (idempotency_key, count, source, now),
                )
            connection.commit()
            return {"archived_count": count}

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise NotFoundError("Run was not found")
            result = self._run_dict(row)
            self._run_extra(connection, result)
            result.update(self._queue_info(connection, row))
            result["events"] = self.run_events(run_id)
            result["approvals"] = [
                self._approval_dict(item)
                for item in connection.execute(
                    "SELECT * FROM approvals WHERE run_id=? ORDER BY created_at", (run_id,)
                )
            ]
            return result

    def claim_next_run(self, *, cv_limit: int | None = None) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            parallel_cvs = False
            if cv_limit is not None:
                active = connection.execute("SELECT task_type FROM runs WHERE state='running' AND id NOT IN (SELECT run_id FROM batch_children)").fetchall()
                if any(r['task_type'] != 'generate_cv' for r in active) or len(active) >= cv_limit:
                    return None
                parallel_cvs = bool(active)
            row = connection.execute(
                "SELECT * FROM runs WHERE state='queued' AND cancel_requested=0 "
                "AND id NOT IN (SELECT run_id FROM batch_children) "
                "AND (?=0 OR task_type='generate_cv') "
                "AND (conversation_id IS NULL OR conversation_id NOT IN (SELECT conversation_id FROM runs WHERE state='running' AND conversation_id IS NOT NULL)) "
                "ORDER BY created_at,rowid LIMIT 1", (parallel_cvs,)
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            now = utc_now()
            connection.execute(
                "UPDATE runs SET state='running',started_at=?,updated_at=? WHERE id=? AND state='queued'",
                (now, now, row["id"]),
            )
            connection.execute(
                "INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,?,?,?)",
                (row["id"], "running", "Task started", now),
            )
            claimed = connection.execute("SELECT * FROM runs WHERE id=?", (row["id"],)).fetchone()
            connection.commit()
            return self._run_dict(claimed) if claimed else None

    def enqueue_new_cvs(self, *, idempotency_key: str) -> dict[str, Any]:
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 100:
            raise ValidationError('Invalid idempotency key')
        result = {'runs': [], 'ready': 0, 'already_active': 0, 'changed': 0}
        for job in self.list_jobs():
            if job['application_status'] != 'new' or job['qualification_status'] != 'recommended':
                continue
            if job['cv']['state'] == 'ready':
                result['ready'] += 1
                continue
            if job['cv']['state'] in ('queued', 'generating'):
                result['already_active'] += 1
                continue
            try:
                run = self.enqueue_run('generate_cv', job_id=job['id'],
                    idempotency_key=f'new-cvs:{idempotency_key}:{job["id"]}', require_new=True)
                result['runs'].append(run)
            except ConflictError:
                result['changed'] += 1
        result['queued'] = len(result['runs'])
        return result

    def append_run_event(
        self, run_id: str, kind: str, message: str, data: dict[str, Any] | None = None
    ) -> int:
        now = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO run_events(run_id,kind,message,data,created_at) VALUES(?,?,?,?,?)",
                (run_id, kind[:100], message[:8000], _json(data or {}), now),
            )
            connection.execute("UPDATE runs SET updated_at=? WHERE id=?", (now, run_id))
            return int(cursor.lastrowid)

    def begin_search(self, run_id: str) -> None:
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute("SELECT task_type FROM runs WHERE id=?", (run_id,)).fetchone()
            if run is None or run["task_type"] != "search":
                raise ValidationError("A search run is required")
            ids = [row[0] for row in connection.execute("SELECT id FROM jobs")]
            connection.execute(
                "INSERT OR IGNORE INTO search_sessions VALUES(?,?,?)",
                (run_id, _json(ids), utc_now()),
            )
            connection.commit()

    def import_search_candidates(self, run_id: str, candidates: list[dict[str, Any]]) -> dict[str, int]:
        """Retain raw results; titles/scores never qualify a discovery for the board."""
        now = utc_now()
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                "SELECT baseline_ids FROM search_sessions WHERE run_id=?", (run_id,)
            ).fetchone()
            if session is None:
                raise ValidationError("Search session has not started")
            baseline = set(_loads(session["baseline_ids"], []))
            for raw in candidates:
                if not isinstance(raw, dict):
                    raise ValidationError("Discoveries must be job objects")
                company = str(raw.get("company") or "").strip()[:300]
                title = str(raw.get("title") or "").strip()[:300]
                url = canonical_job_url(raw.get("url") or raw.get("source_url"))
                if not company or not title or not url:
                    raise ValidationError("A discovery requires company, title, and HTTPS source URL")
                job_id = stable_job_id(url, company, title)
                duplicate = job_id in baseline
                # Reposts of an already tracked exact role are not new recommendations.
                same_role = connection.execute(
                    """SELECT id FROM jobs WHERE lower(company)=lower(?) AND lower(title)=lower(?)
                    AND application_status!='new' AND id!=?""", (company, title, job_id),
                ).fetchone()
                duplicate = duplicate or same_role is not None
                connection.execute(
                    """INSERT OR IGNORE INTO jobs
                    (id,company,title,source_url,location,work_arrangement,created_at,updated_at)
                    VALUES(?,?,?,?,?,'unknown',?,?)""",
                    (job_id, company, title, url, str(raw.get("location") or "")[:300], now, now),
                )
                connection.execute(
                    """INSERT INTO search_discoveries
                    (run_id,job_id,raw_json,is_duplicate,created_at) VALUES(?,?,?,?,?)
                    ON CONFLICT(run_id,job_id) DO UPDATE SET raw_json=excluded.raw_json""",
                    (run_id, job_id, _json(raw), int(duplicate), now),
                )
            connection.commit()
        return self.search_metrics(run_id)

    def search_metrics(self, run_id: str) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT d.is_duplicate,j.qualification_status FROM search_discoveries d
                JOIN jobs j ON j.id=d.job_id WHERE d.run_id=?""", (run_id,),
            ).fetchall()
        return {
            "discovered": len(rows),
            "duplicates": sum(bool(row["is_duplicate"]) for row in rows),
            **{status: sum(not row["is_duplicate"] and row["qualification_status"] == status
                          for row in rows)
               for status in ("filtered_out", "awaiting_evaluation", "recommended")},
        }

    def finalize_search(self, run_id: str) -> dict[str, int]:
        """Account for legacy imports even when a turn fails before its final summary."""
        with self._file_lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                "SELECT baseline_ids FROM search_sessions WHERE run_id=?", (run_id,)
            ).fetchone()
            if session is None:
                raise ValidationError("Search session has not started")
            baseline = set(_loads(session["baseline_ids"], []))
            for row in connection.execute("SELECT * FROM jobs").fetchall():
                if row["id"] not in baseline:
                    connection.execute(
                        "INSERT OR IGNORE INTO search_discoveries VALUES(?,?,?,?,?)",
                        (run_id, row["id"], _json(dict(row)), 0, utc_now()),
                    )
            connection.commit()
        metrics = self.search_metrics(run_id)
        self.record_search_summary(run_id, **metrics)
        return metrics

    def record_search_summary(
        self,
        run_id: str,
        *,
        discovered: int,
        duplicates: int,
        filtered_out: int,
        awaiting_evaluation: int,
        recommended: int,
    ) -> int:
        metrics = {
            "discovered": discovered,
            "duplicates": duplicates,
            "filtered_out": filtered_out,
            "awaiting_evaluation": awaiting_evaluation,
            "recommended": recommended,
        }
        if any(not isinstance(value, int) or value < 0 for value in metrics.values()):
            raise ValidationError("Search summary counts must be non-negative integers")
        if recommended > 8:
            raise ValidationError("A search run may recommend at most 8 jobs")
        if discovered != duplicates + filtered_out + awaiting_evaluation + recommended:
            raise ValidationError("Search summary counts must account for every discovery")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT task_type FROM runs WHERE id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("Run was not found")
            if row["task_type"] != "search":
                raise ValidationError("Search summaries can only be attached to search runs")
            session = connection.execute("SELECT 1 FROM search_sessions WHERE run_id=?", (run_id,)).fetchone()
        if session and metrics != self.search_metrics(run_id):
            raise ValidationError("Search summary must match persisted discovery decisions")
        message = (
            f"Discovered {discovered} · duplicates {duplicates} · filtered out {filtered_out} · "
            f"awaiting evaluation {awaiting_evaluation} · recommended {recommended}"
        )
        return self.append_run_event(run_id, "search_summary", message, metrics)

    def run_events(self, run_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self._connect() as connection:
            return [
                {**dict(row), "data": _loads(row["data"], {})}
                for row in connection.execute(
                    "SELECT * FROM run_events WHERE run_id=? AND id>? ORDER BY id", (run_id, after)
                )
            ]

    def set_run_codex(self, run_id: str, *, thread_id: str, turn_id: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE runs SET codex_thread_id=?,codex_turn_id=COALESCE(?,codex_turn_id),updated_at=? WHERE id=?",
                (thread_id, turn_id, utc_now(), run_id),
            )

    def finish_run(
        self,
        run_id: str,
        state: str,
        *,
        summary: str = "",
        error: str = "",
        artifacts: list[dict[str, Any]] | None = None,
    ) -> None:
        if state not in {"completed", "failed", "cancelled", "interrupted"}:
            raise ValidationError("Invalid terminal run state")
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute("SELECT state,cancel_requested FROM runs WHERE id=?", (run_id,)).fetchone()
            if current is None:
                raise NotFoundError("Run was not found")
            if current["state"] in {"completed", "failed", "cancelled"}:
                connection.commit()
                return
            if current["cancel_requested"]:
                if state != "interrupted" or not summary:
                    summary = "Stopped during execution"
                state, artifacts = "interrupted", []
            connection.execute(
                """UPDATE runs SET state=?,result_summary=?,error=?,artifacts=?,ended_at=?,updated_at=?
                    WHERE id=?""",
                (state, summary[:12000], error[:12000], _json(artifacts or []), now, now, run_id),
            )
            connection.execute(
                "INSERT INTO run_events(run_id,kind,message,data,created_at) VALUES(?,?,?,?,?)",
                (run_id, state, summary[:8000] or error[:8000] or state.title(), "{}", now),
            )
            connection.commit()

    def request_cancel(self, run_id: str) -> dict[str, Any]:
        if self.get_run(run_id)["task_type"] == "refresh_bulk":
            self.request_stop(batch_id=run_id)
            return self.get_run(run_id)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise NotFoundError("Run was not found")
            if row["state"] == "queued":
                now = utc_now()
                connection.execute(
                    "UPDATE runs SET state='cancelled',cancel_requested=1,result_summary='Cancelled before starting',ended_at=?,updated_at=? WHERE id=?",
                    (now, now, run_id),
                )
                connection.execute(
                    "INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,?,?,?)",
                    (run_id, "cancelled", "Task cancelled before it started", now),
                )
            elif row["state"] == "running":
                connection.execute(
                    "UPDATE runs SET cancel_requested=1,updated_at=? WHERE id=?", (utc_now(), run_id)
                )
            result = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            connection.commit()
            assert result is not None
            return self._run_dict(result)

    def retry_run(self, run_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
        original = self.get_run(run_id)
        if original.get("batch_id") or original["task_type"] == "refresh_bulk":
            raise ConflictError("Batch checks cannot be retried in-place; preview a new independent selection or refresh the selected job")
        invalidated_cv = (original["state"] == "completed" and original["task_type"] == "generate_cv"
                          and self.get_job(original["job_id"])["cv"]["state"] == "failed")
        if original["state"] not in {"failed", "cancelled", "interrupted"} and not invalidated_cv:
            raise ConflictError("Only failed, cancelled, or interrupted runs can be retried")
        return self.enqueue_run(
            original["task_type"],
            job_id=original["job_id"],
            conversation_id=original["conversation_id"],
            payload=original["payload"],
            idempotency_key=idempotency_key,
            retry_of=run_id,
        )

    def reconcile_unfinished_runs(self) -> int:
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute("SELECT id FROM runs WHERE state='running'").fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE runs SET state='interrupted',ended_at=?,updated_at=?,error=? WHERE id=?",
                    (now, now, "Portal restarted while this task was running", row["id"]),
                )
                connection.execute(
                    "INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,?,?,?)",
                    (row["id"], "interrupted", "Portal restarted while this task was running", now),
                )
            connection.execute(
                "UPDATE approvals SET state='expired',resolved_at=? WHERE state='pending'", (now,)
            )
            connection.execute("UPDATE runs SET cancel_requested=1 WHERE task_type='refresh_bulk' AND state='interrupted'")
            children = connection.execute("SELECT r.id FROM runs r JOIN batch_children b ON b.run_id=r.id JOIN runs p ON p.id=b.batch_id WHERE r.state='queued' AND (p.cancel_requested=1 OR p.state IN ('interrupted','failed','cancelled'))").fetchall()
            for child in children:
                connection.execute("UPDATE runs SET state='cancelled',cancel_requested=1,result_summary='Batch interrupted before this check',ended_at=?,updated_at=? WHERE id=?", (now, now, child[0]))
                connection.execute("INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,'cancelled','Batch interrupted before this check',?)", (child[0], now))
            connection.commit()
            return len(rows)

    def create_conversation(
        self,
        title: str,
        *,
        job_id: str | None = None,
        codex_thread_id: str | None = None,
        source: str = "portal",
    ) -> dict[str, Any]:
        title = title.strip()[:200] or "New conversation"
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if job_id and not connection.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone():
                raise NotFoundError("Job was not found")
            if codex_thread_id:
                existing = connection.execute(
                    "SELECT * FROM conversations WHERE codex_thread_id=?", (codex_thread_id,)
                ).fetchone()
                if existing:
                    connection.commit()
                    return dict(existing)
            conversation_id = f"conv_{uuid.uuid4().hex}"
            connection.execute(
                "INSERT INTO conversations(id,title,job_id,codex_thread_id,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (conversation_id, title, job_id, codex_thread_id, source[:50], now, now),
            )
            row = connection.execute("SELECT * FROM conversations WHERE id=?", (conversation_id,)).fetchone()
            connection.commit()
            assert row is not None
            return dict(row)

    def list_conversations(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    """SELECT conversations.*,jobs.company,jobs.title AS job_title
                       FROM conversations LEFT JOIN jobs ON jobs.id=conversations.job_id
                       ORDER BY conversations.updated_at DESC"""
                )
            ]

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM conversations WHERE id=?", (conversation_id,)).fetchone()
            if row is None:
                raise NotFoundError("Conversation was not found")
            result = dict(row)
            result["messages"] = [
                dict(item)
                for item in connection.execute(
                    "SELECT * FROM messages WHERE conversation_id=? ORDER BY id", (conversation_id,)
                )
            ]
            for message in result["messages"]:
                links = []
                for event in connection.execute(
                    "SELECT data FROM run_events WHERE run_id=? AND kind='cv_queued' ORDER BY id",
                    (message["run_id"],),
                ):
                    links.extend(_loads(event["data"], {}).get("runs", []))
                if links:
                    message["cv_runs"] = links
            return result

    def set_conversation_thread(self, conversation_id: str, thread_id: str) -> None:
        with self._connect() as connection:
            try:
                cursor = connection.execute(
                    "UPDATE conversations SET codex_thread_id=?,updated_at=? WHERE id=?",
                    (thread_id, utc_now(), conversation_id),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("Codex thread is already imported") from exc
            if cursor.rowcount != 1:
                raise NotFoundError("Conversation was not found")

    def add_message(
        self, conversation_id: str, role: str, content: str, *, run_id: str | None = None
    ) -> dict[str, Any]:
        if role not in {"user", "assistant", "system"}:
            raise ValidationError("Unknown message role")
        content = content.strip()
        if not content or len(content) > 100_000:
            raise ValidationError("Message must be between 1 and 100000 characters")
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if not connection.execute("SELECT 1 FROM conversations WHERE id=?", (conversation_id,)).fetchone():
                raise NotFoundError("Conversation was not found")
            if run_id:
                existing = connection.execute(
                    "SELECT * FROM messages WHERE run_id=? AND role=? ORDER BY id LIMIT 1",
                    (run_id, role),
                ).fetchone()
                if existing:
                    connection.commit()
                    return dict(existing)
            cursor = connection.execute(
                "INSERT INTO messages(conversation_id,run_id,role,content,created_at) VALUES(?,?,?,?,?)",
                (conversation_id, run_id, role, content, now),
            )
            connection.execute("UPDATE conversations SET updated_at=? WHERE id=?", (now, conversation_id))
            row = connection.execute("SELECT * FROM messages WHERE id=?", (cursor.lastrowid,)).fetchone()
            connection.commit()
            assert row is not None
            return dict(row)

    @staticmethod
    def _approval_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["params"] = _loads(result.get("params"), {})
        raw_request_id = result.get("rpc_request_id")
        try:
            result["rpc_request_id"] = json.loads(raw_request_id)
        except (TypeError, json.JSONDecodeError):
            # Compatibility with approval rows created before request IDs were
            # JSON-encoded to preserve their numeric/string JSON-RPC type.
            result["rpc_request_id"] = raw_request_id
        return result

    def create_approval(
        self, run_id: str, rpc_request_id: object, method: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        approval_id = f"approval_{uuid.uuid4().hex}"
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO approvals(id,run_id,rpc_request_id,method,params,state,created_at) VALUES(?,?,?,?,?,'pending',?)",
                (approval_id, run_id, _json(rpc_request_id), method[:200], _json(params), now),
            )
            row = connection.execute("SELECT * FROM approvals WHERE id=?", (approval_id,)).fetchone()
            assert row is not None
            return self._approval_dict(row)

    def resolve_approval(self, approval_id: str, decision: str) -> dict[str, Any]:
        if decision not in {"approved", "approved_for_session", "denied", "abort"}:
            raise ValidationError("Unknown approval decision")
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM approvals WHERE id=?", (approval_id,)).fetchone()
            if row is None:
                raise NotFoundError("Approval was not found")
            if row["state"] != "pending":
                raise ConflictError("Approval is no longer pending")
            state = "approved" if decision.startswith("approved") else "denied"
            connection.execute(
                "UPDATE approvals SET state=?,decision=?,resolved_at=? WHERE id=?",
                (state, decision, now, approval_id),
            )
            result = connection.execute("SELECT * FROM approvals WHERE id=?", (approval_id,)).fetchone()
            connection.commit()
            assert result is not None
            return self._approval_dict(result)
