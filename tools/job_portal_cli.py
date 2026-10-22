#!/usr/bin/env python3
"""Validated job-state commands shared by the portal, Codex, and CLI users."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.portal_store import (
    APPLICATION_STATUSES,
    LISTING_STATUSES,
    QUALIFICATION_STATUSES,
    ConflictError,
    NotFoundError,
    PortalStore,
    ValidationError,
)


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage private job portal state")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    commands = parser.add_subparsers(dest="command", required=True)

    jobs = commands.add_parser("jobs", help="List jobs")
    jobs.add_argument("--query", default="")
    jobs.add_argument("--status", action="append", choices=APPLICATION_STATUSES)
    jobs.add_argument("--include-archived", action="store_true")
    jobs.add_argument("--include-discoveries", action="store_true")

    get = commands.add_parser("get", help="Read one job by stable ID")
    get.add_argument("job_id")

    status = commands.add_parser("status", help="Update application status")
    status.add_argument("job_id")
    status.add_argument("status", choices=APPLICATION_STATUSES)
    status.add_argument("--note", default="")
    status.add_argument("--source", default="agent_cli")
    status.add_argument("--idempotency-key")
    status.add_argument("--expected-version", type=int)

    note = commands.add_parser("note", help="Add a job note")
    note.add_argument("job_id")
    note.add_argument("note")
    note.add_argument("--source", default="agent_cli")
    note.add_argument("--idempotency-key")

    listing = commands.add_parser("listing", help="Set listing availability")
    listing.add_argument("job_id")
    listing.add_argument("status", choices=LISTING_STATUSES)
    listing.add_argument("--note", default="")
    listing.add_argument("--source", default="agent_cli")

    evaluation = commands.add_parser("evaluation", help="Record a structured evaluation")
    evaluation.add_argument("job_id")
    evaluation.add_argument("--score", type=float)
    evaluation.add_argument("--recommendation", required=True)
    evaluation.add_argument("--reasons-json", default="{}")
    evaluation.add_argument("--source", default="agent_cli")

    qualification = commands.add_parser(
        "qualification", help="Record a responsibilities-based qualification decision"
    )
    qualification.add_argument("job_id")
    qualification.add_argument("status", choices=QUALIFICATION_STATUSES)
    qualification.add_argument("--reasons-json", required=True)
    qualification.add_argument("--responsibilities-reviewed", action="store_true")
    qualification.add_argument("--source", default="agent_cli")
    qualification.add_argument("--run-id")

    discoveries = commands.add_parser("discoveries", help="Persist raw search results without recommending them")
    discoveries.add_argument("run_id")
    discoveries.add_argument("--input", type=Path, required=True, help="JSON array of raw job objects")

    details = commands.add_parser("details", help="Update location, work arrangement, or next action")
    details.add_argument("job_id")
    details.add_argument("--location")
    details.add_argument("--work-arrangement")
    details.add_argument("--next-action")
    details.add_argument("--source", default="agent_cli")

    artifact = commands.add_parser("artifact", help="Associate an existing artifact with a job")
    artifact.add_argument("job_id")
    artifact.add_argument("kind", choices=("cv", "cover_letter"))
    artifact.add_argument("path")
    artifact.add_argument("--source", default="agent_cli")

    generate_cv = commands.add_parser("generate-cv", help="Enqueue a dedicated CV task without changing application status")
    generate_cv.add_argument("job_id")
    generate_cv.add_argument("--idempotency-key")
    generate_new = commands.add_parser('generate-new-cvs', help='Enqueue missing CVs for qualified New jobs; skip ready/active tasks')
    generate_new.add_argument('--idempotency-key', required=True)

    refresh_listing = commands.add_parser("refresh-listing", help="Enqueue an HTTP-first availability check")
    refresh_listing.add_argument("job_id")
    refresh_listing.add_argument("--idempotency-key")
    for command in ("refresh-preview", "refresh-batch"):
        refresh = commands.add_parser(command, help="Preview/select a stale availability-only batch")
        refresh.add_argument("--freshness-hours", type=int)
        refresh.add_argument("--limit", type=int)
        refresh.add_argument("--max-ai-fallbacks", type=int)
        refresh.add_argument("--include-tracked", action="store_true")
        refresh.add_argument("--job-id", action="append")
        refresh.add_argument("--idempotency-key")
    stop_runs = commands.add_parser("stop-runs", help="Persist stop-all or a batch cancellation fence")
    stop_runs.add_argument("--batch-id")
    stop_runs.add_argument("--idempotency-key")

    sync = commands.add_parser("sync", help="Import legacy CSV/JSON changes")
    sync.add_argument("--force", action="store_true")

    summary = commands.add_parser("search-summary", help="Record persisted search counts")
    summary.add_argument("run_id")
    for name in (
        "discovered",
        "duplicates",
        "filtered-out",
        "awaiting-evaluation",
        "recommended",
    ):
        summary.add_argument(f"--{name}", type=int, required=True)

    conversation = commands.add_parser("import-thread", help="Map an existing Codex thread into the portal")
    conversation.add_argument("thread_id")
    conversation.add_argument("--title", default="Imported Codex conversation")
    conversation.add_argument("--job-id")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        store = PortalStore(args.repo)
        if args.command == "jobs":
            _print(
                store.list_jobs(
                    query=args.query,
                    statuses=args.status,
                    include_archived=args.include_archived,
                    include_discoveries=args.include_discoveries,
                )
            )
        elif args.command == "get":
            _print(store.get_job(args.job_id))
        elif args.command == "status":
            _print(
                store.update_application_status(
                    args.job_id,
                    args.status,
                    note=args.note,
                    source=args.source,
                    idempotency_key=args.idempotency_key or f"cli-{uuid.uuid4().hex}",
                    expected_version=args.expected_version,
                )
            )
        elif args.command == "note":
            _print(
                store.add_note(
                    args.job_id,
                    args.note,
                    source=args.source,
                    idempotency_key=args.idempotency_key or f"cli-{uuid.uuid4().hex}",
                )
            )
        elif args.command == "listing":
            _print(store.set_listing_status(args.job_id, args.status, source=args.source, note=args.note))
        elif args.command == "evaluation":
            reasons = json.loads(args.reasons_json)
            if not isinstance(reasons, dict):
                raise ValidationError("Evaluation reasons must be a JSON object")
            _print(
                store.set_evaluation(
                    args.job_id,
                    score=args.score,
                    recommendation=args.recommendation,
                    reasons=reasons,
                    source=args.source,
                )
            )
        elif args.command == "qualification":
            reasons = json.loads(args.reasons_json)
            if not isinstance(reasons, list):
                raise ValidationError("Qualification reasons must be a JSON array")
            _print(
                store.set_qualification(
                    args.job_id,
                    args.status,
                    reasons=reasons,
                    responsibilities_reviewed=args.responsibilities_reviewed,
                    source=args.source,
                    run_id=args.run_id,
                )
            )
        elif args.command == "discoveries":
            candidates = json.loads(args.input.read_text(encoding="utf-8"))
            if not isinstance(candidates, list):
                raise ValidationError("Discoveries input must be a JSON array")
            _print(store.import_search_candidates(args.run_id, candidates))
        elif args.command == "details":
            _print(
                store.update_job_details(
                    args.job_id,
                    location=args.location,
                    work_arrangement=args.work_arrangement,
                    next_action=args.next_action,
                    source=args.source,
                )
            )
        elif args.command == "artifact":
            _print(store.set_artifact(args.job_id, args.kind, args.path, source=args.source))
        elif args.command == "generate-cv":
            _print(store.enqueue_run("generate_cv", job_id=args.job_id,
                                     idempotency_key=args.idempotency_key))
        elif args.command == 'generate-new-cvs':
            _print(store.enqueue_new_cvs(idempotency_key=args.idempotency_key))
        elif args.command == "refresh-listing":
            _print(store.enqueue_run("refresh_listing", job_id=args.job_id, idempotency_key=args.idempotency_key))
        elif args.command in ("refresh-preview", "refresh-batch"):
            options = {k: v for k, v in {"freshness_hours": args.freshness_hours, "limit": args.limit,
                       "max_ai_fallbacks": args.max_ai_fallbacks, "job_ids": args.job_id,
                       "include_tracked": args.include_tracked}.items() if v is not None}
            _print(store.preview_refresh_batch(options) if args.command == "refresh-preview" else
                   store.enqueue_refresh_batch(options, idempotency_key=args.idempotency_key))
        elif args.command == "stop-runs":
            _print(store.request_stop(batch_id=args.batch_id, idempotency_key=args.idempotency_key))
        elif args.command == "sync":
            _print({"imported": store.sync_legacy(force=args.force)})
        elif args.command == "search-summary":
            _print(
                {
                    "event_id": store.record_search_summary(
                        args.run_id,
                        discovered=args.discovered,
                        duplicates=args.duplicates,
                        filtered_out=args.filtered_out,
                        awaiting_evaluation=args.awaiting_evaluation,
                        recommended=args.recommended,
                    )
                }
            )
        elif args.command == "import-thread":
            _print(
                store.create_conversation(
                    args.title,
                    job_id=args.job_id,
                    codex_thread_id=args.thread_id,
                    source="imported_cli",
                )
            )
        return 0
    except (ValidationError, NotFoundError, ConflictError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
