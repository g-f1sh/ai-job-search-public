"""Tests for durable portal state, history, locking, and run persistence."""

from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path

from tools.portal_store import ConflictError, PortalStore, ValidationError, stable_job_id


TRACKER_HEADER = (
    "date,company,sector,role,role_type,channel,status,contact_person,fit_rating,notes,"
    "cv_file,cover_letter_file,source\n"
)


class PortalStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "cv").mkdir()
        (self.root / "cover_letters").mkdir()
        (self.root / "job_scraper").mkdir()
        (self.root / "job_scraper" / "seen_jobs.json").write_text(
            json.dumps(
                {
                    "seen": {
                        "lead": {
                            "company": "Acme",
                            "title": "Solutions Engineer",
                            "url": "https://example.com/jobs/42",
                            "location": "Tel Aviv",
                            "fit": "high",
                            "status": "shortlisted",
                            "rank_score": 84,
                            "rank_verdict": "Strong fit",
                            "strengths": ["Customer-facing implementation"],
                            "gaps": ["Product-specific experience"],
                            "first_seen": "2026-10-01",
                        },
                        "closed": {
                            "company": "Closed Co",
                            "title": "Implementation Manager",
                            "url": "https://example.com/jobs/closed",
                            "fit": "medium",
                            "status": "expired",
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        (self.root / "job_search_tracker.csv").write_text(TRACKER_HEADER, encoding="utf-8")
        self.store = PortalStore(self.root)
        self.job = next(job for job in self.store.list_jobs(include_archived=True) if job["company"] == "Acme")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_run_history_archiving_retains_active_work_chat_logs_and_artifacts(self) -> None:
        conversation = self.store.create_conversation("Archive test")
        terminal = []
        artifact = self.root / "cv" / "archive-test.pdf"
        artifact.write_bytes(b"original CV")
        for state in ("completed", "failed", "cancelled", "interrupted"):
            run = self.store.enqueue_run("search", payload={"test": state})
            self.store.finish_run(run["id"], state, summary=state, artifacts=[{"path": str(artifact)}])
            self.store.add_message(conversation["id"], "assistant", state, run_id=run["id"])
            terminal.append(self.store.get_run(run["id"]))
        running = self.store.enqueue_run("search", payload={"test": "running"})
        self.store.claim_next_run()
        queued = self.store.enqueue_run("search", payload={"test": "queued"})
        active_before = [self.store.get_run(r["id"]) for r in (running, queued)]
        chat_before = self.store.get_conversation(conversation["id"])
        self.assertEqual(self.store.archive_run_history(idempotency_key="clear"), {"archived_count": 4})
        self.assertEqual(self.store.run_history_counts(), {"clearable": 0, "archived": 4})
        self.assertEqual({r["id"] for r in self.store.list_runs(limit=1)}, {running["id"], queued["id"]})
        self.assertEqual(len(self.store.list_runs(include_archived=True)), 6)
        for before in terminal:
            after = self.store.get_run(before["id"])
            self.assertTrue(after.pop("archived_at"))
            before.pop("archived_at")
            self.assertEqual(after, before)
        self.assertEqual([self.store.get_run(r["id"]) for r in (running, queued)], active_before)
        self.assertEqual(self.store.get_conversation(conversation["id"]), chat_before)
        self.assertEqual(artifact.read_bytes(), b"original CV")
        reopened = PortalStore(self.root)
        self.assertEqual(reopened.run_history_counts()["archived"], 4)
        retry = reopened.retry_run(terminal[1]["id"])
        self.assertIsNone(retry["archived_at"])

    def test_archive_requests_are_idempotent_and_do_not_sweep_newly_finished_work(self) -> None:
        first = self.store.enqueue_run("search", payload={"test": 1})
        self.store.finish_run(first["id"], "completed")
        self.assertEqual(self.store.archive_run_history(idempotency_key="same")["archived_count"], 1)
        later = self.store.enqueue_run("search", payload={"test": 2})
        self.store.finish_run(later["id"], "completed")
        self.assertEqual(self.store.archive_run_history(idempotency_key="same")["archived_count"], 1)
        self.assertIsNone(self.store.get_run(later["id"])["archived_at"])
        self.assertEqual(self.store.archive_run_history(idempotency_key="next")["archived_count"], 1)
        self.assertEqual(self.store.archive_run_history()["archived_count"], 0)
        for invalid in ("", [], {}, "x" * 201):
            with self.assertRaises(ValidationError):
                self.store.archive_run_history(idempotency_key=invalid)

    def test_concurrent_archives_and_worker_completion_serialize_without_data_loss(self) -> None:
        runs = [self.store.enqueue_run("search", payload={"test": i}) for i in range(10)]
        for run in runs:
            self.store.finish_run(run["id"], "completed")
        active = self.store.enqueue_run("search", payload={"test": "active"})
        self.store.claim_next_run()
        barrier = threading.Barrier(3)
        counts, errors = [], []
        def clear():
            try:
                barrier.wait()
                counts.append(self.store.archive_run_history()["archived_count"])
            except Exception as error:
                errors.append(error)
        threads = [threading.Thread(target=clear) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        self.store.finish_run(active["id"], "completed", summary="Worker completed normally")
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertFalse(errors)
        self.assertIn(sum(counts), (10, 11))
        self.assertEqual(len(self.store.list_runs(include_archived=True)), 11)
        finished = self.store.get_run(active["id"])
        self.assertEqual(finished["state"], "completed")
        self.assertFalse(finished["cancel_requested"])
        self.assertEqual(finished["events"][-1]["message"], "Worker completed normally")

    def test_run_archive_migration_is_additive_backed_up_and_repeatable(self) -> None:
        run = self.store.enqueue_run("search")
        self.store.finish_run(run["id"], "completed")
        with self.store._connect() as connection:
            connection.execute("ALTER TABLE runs DROP COLUMN archived_at")
        migrated = PortalStore(self.root)
        self.assertIsNone(migrated.get_run(run["id"])["archived_at"])
        backups = list((self.root / "portal_data" / "backups").glob("portal-before-run-archive-*.sqlite3"))
        self.assertEqual(len(backups), 1)
        with closing(sqlite3.connect(backups[0])) as backup:
            self.assertEqual(backup.execute("SELECT state FROM runs WHERE id=?", (run["id"],)).fetchone()[0], "completed")
        PortalStore(self.root)
        self.assertEqual(len(list(backups[0].parent.glob("portal-before-run-archive-*.sqlite3"))), 1)

    def test_stable_ids_use_url_or_company_and_role_not_company_alone(self) -> None:
        first = stable_job_id("", "Same Company", "Role One")
        second = stable_job_id("", "Same Company", "Role Two")
        canonical = stable_job_id(
            "https://www.linkedin.com/jobs/search-results/?currentJobId=4470402470&trackingId=x",
            "Different",
            "Different",
        )
        direct = stable_job_id(
            "https://www.linkedin.com/jobs/view/4470402470", "Company", "Role"
        )

        self.assertNotEqual(first, second)
        self.assertEqual(canonical, direct)

    def test_migration_separates_listing_closure_from_employer_rejection(self) -> None:
        closed = next(
            job
            for job in self.store.list_jobs(
                include_archived=True, include_discoveries=True
            )
            if job["company"] == "Closed Co"
        )

        self.assertEqual(closed["listing_status"], "closed")
        self.assertEqual(closed["application_status"], "new")
        self.assertTrue(list((self.root / "portal_data" / "backups").glob("portal-migration-*")))

    def test_discoveries_require_persisted_responsibilities_based_qualification(self) -> None:
        discovery = next(
            job
            for job in self.store.list_jobs(
                include_archived=True, include_discoveries=True
            )
            if job["company"] == "Closed Co"
        )
        self.assertEqual(discovery["qualification_status"], "awaiting_evaluation")
        self.assertNotIn(
            discovery["id"],
            {job["id"] for job in self.store.list_jobs(include_archived=True)},
        )

        with self.assertRaises(ValidationError):
            self.store.set_qualification(
                discovery["id"],
                "recommended",
                reasons=["Title match only"],
                responsibilities_reviewed=False,
                source="test",
            )
        qualified = self.store.set_qualification(
            discovery["id"],
            "recommended",
            reasons=["Responsibilities require customer implementation and training."],
            responsibilities_reviewed=True,
            source="test",
        )

        self.assertTrue(qualified["qualification_reviewed"])
        self.assertIn(
            discovery["id"],
            {job["id"] for job in self.store.list_jobs(include_archived=True)},
        )

    def test_tracked_status_remains_visible_even_if_qualification_is_filtered(self) -> None:
        self.store.set_qualification(
            self.job["id"],
            "filtered_out",
            reasons=["Mandatory specialist certification is not established."],
            responsibilities_reviewed=True,
            source="test",
        )
        visible = {job["id"] for job in self.store.list_jobs(include_archived=True)}
        self.assertIn(self.job["id"], visible)

    def test_ranked_score_does_not_imply_shortlisting_or_qualification(self) -> None:
        path = self.root / "job_scraper" / "seen_jobs.json"
        payload = json.loads(path.read_text())
        payload["seen"]["scored"] = {
            "company": "Ranked", "title": "Unreviewed Developer",
            "url": "https://example.com/scored", "status": "ranked", "rank_score": 99,
        }
        path.write_text(json.dumps(payload))
        self.store.sync_legacy(force=True)
        job = self.store.get_job(stable_job_id("https://example.com/scored", "", ""))
        self.assertEqual(job["application_status"], "new")
        self.assertEqual(job["qualification_status"], "awaiting_evaluation")
        self.assertNotIn(job["id"], {j["id"] for j in self.store.list_jobs()})
        self.assertEqual(self.store.get_job(self.job["id"])["application_status"], "shortlisted")

    def test_qualification_migration_backs_up_and_preserves_tracked_history(self) -> None:
        self.store.add_note(self.job["id"], "Keep this history", source="test")
        before = self.store.get_job(self.job["id"])["history"]
        with closing(sqlite3.connect(self.store.db_path)) as connection, connection:
            connection.execute("DROP INDEX jobs_qualification_status")
            for name in ("qualification_status", "qualification_reasons", "qualification_reviewed", "qualification_decided_at"):
                connection.execute(f"ALTER TABLE jobs DROP COLUMN {name}")
        migrated = PortalStore(self.root)
        job = migrated.get_job(self.job["id"])
        self.assertEqual(job["history"], before)
        self.assertEqual(job["application_status"], "shortlisted")
        self.assertEqual(job["qualification_status"], "awaiting_evaluation")
        self.assertFalse(job["qualification_reviewed"])
        backups = list((self.root / "portal_data/backups").glob("portal-before-qualification-*.sqlite3"))
        self.assertEqual(len(backups), 1)
        with closing(sqlite3.connect(backups[0])) as backup:
            self.assertNotIn("qualification_status", [r[1] for r in backup.execute("PRAGMA table_info(jobs)")])
        PortalStore(self.root).sync_legacy(force=True)
        self.assertEqual(len(list((self.root / "portal_data/backups").glob("portal-before-qualification-*.sqlite3"))), 1)

    def test_search_pool_is_durable_idempotent_capped_and_reconciled(self) -> None:
        run = self.store.enqueue_run("search", payload={}, idempotency_key="pool")
        self.store.begin_search(run["id"])
        candidates = [{"company": f"Candidate {i}", "title": "Implementation Engineer",
                       "url": f"https://example.com/candidate/{i}", "description": "Full raw text",
                       "rank_score": 100, "qualification_status": "recommended"} for i in range(10)]
        candidates.append({"company": "Acme", "title": "Solutions Engineer", "url": self.job["source_url"]})
        first = self.store.import_search_candidates(run["id"], candidates)
        again = self.store.import_search_candidates(run["id"], candidates)
        self.assertEqual(first, again)
        self.assertEqual(first, {"discovered": 11, "duplicates": 1, "filtered_out": 0,
                                 "awaiting_evaluation": 10, "recommended": 0})
        for candidate in candidates[:8]:
            job_id = stable_job_id(candidate["url"], "", "")
            self.store.set_qualification(job_id, "recommended", reasons=["Reviewed customer implementation responsibilities"],
                                         responsibilities_reviewed=True, source="test", run_id=run["id"])
        with self.assertRaises(ValidationError):
            self.store.set_qualification(stable_job_id(candidates[8]["url"], "", ""), "recommended",
                                         reasons=["Fit"], responsibilities_reviewed=True, source="test", run_id=run["id"])
        with self.assertRaises(ValidationError):
            self.store.set_qualification(self.job["id"], "recommended", reasons=["Fit"],
                                         responsibilities_reviewed=True, source="test", run_id=run["id"])
        restarted = PortalStore(self.root)
        metrics = restarted.finalize_search(run["id"])
        self.assertEqual(metrics["recommended"], 8)
        self.assertEqual(metrics["awaiting_evaluation"], 2)
        job = restarted.get_job(stable_job_id(candidates[0]["url"], "", ""))
        self.assertEqual(job["discoveries"][0]["raw"]["description"], "Full raw text")
        self.assertEqual(restarted.run_events(run["id"])[-1]["data"], metrics)

    def test_search_qualification_limit_is_safe_under_conflicting_writes(self) -> None:
        run = self.store.enqueue_run("search", payload={}, idempotency_key="parallel-pool")
        self.store.begin_search(run["id"])
        candidates = [{"company": f"Parallel {i}", "title": "TAM", "url": f"https://example.com/parallel/{i}"}
                      for i in range(10)]
        self.store.import_search_candidates(run["id"], candidates)
        failures = []
        def qualify(candidate):
            try:
                self.store.set_qualification(stable_job_id(candidate["url"], "", ""), "recommended",
                                             reasons=["Reviewed customer troubleshooting"], responsibilities_reviewed=True,
                                             source="test", run_id=run["id"])
            except ValidationError as exc:
                failures.append(str(exc))
        threads = [threading.Thread(target=qualify, args=(candidate,)) for candidate in candidates]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(self.store.search_metrics(run["id"])["recommended"], 8)
        self.assertEqual(len(failures), 2)

    def test_legacy_recommendations_cannot_bypass_active_search_gate_or_cap(self) -> None:
        run = self.store.enqueue_run("search", payload={}, idempotency_key="legacy-cap")
        self.store.claim_next_run()
        self.store.begin_search(run["id"])
        path = self.root / "job_scraper/seen_jobs.json"
        payload = json.loads(path.read_text())
        for i in range(12):
            payload["seen"][f"bulk-{i}"] = {
                "company": f"Bulk {i}", "title": "Implementation", "url": f"https://example.com/bulk/{i}",
                "status": "ranked", "rank_score": 99, "qualification_status": "recommended",
                "qualification_reasons": ["Reviewed implementation responsibilities"], "responsibilities_reviewed": True,
            }
        path.write_text(json.dumps(payload))
        self.store.sync_legacy(force=True)
        self.assertNotIn("Bulk 0", {j["company"] for j in self.store.list_jobs()})
        self.store.finalize_search(run["id"])
        self.store.finish_run(run["id"], "completed", summary="No qualified CLI decisions")
        # Ending the run/force-sync must not replay blocked legacy promotions.
        self.store.sync_legacy(force=True)
        self.assertNotIn("Bulk 0", {j["company"] for j in self.store.list_jobs()})
        # A standalone legacy shortlist is also limited and requires reviewed reasons.
        with closing(sqlite3.connect(self.store.db_path)) as connection, connection:
            connection.execute("DELETE FROM search_discoveries")
            connection.execute("DELETE FROM legacy_job_snapshots WHERE job_id IN (SELECT id FROM jobs WHERE company LIKE 'Bulk %')")
            connection.execute("DELETE FROM jobs WHERE company LIKE 'Bulk %'")
        self.store.sync_legacy(force=True)
        bulk = [j for j in self.store.list_jobs() if j["company"].startswith("Bulk ")]
        self.assertEqual(len(bulk), 8)

    def test_qualification_survives_tracker_force_sync_and_repeated_decisions_are_idempotent(self) -> None:
        self.store.update_application_status(self.job["id"], "applied", source="test")
        first = self.store.set_qualification(self.job["id"], "filtered_out", reasons=["Mandatory specialization gap"],
                                            responsibilities_reviewed=True, source="test")
        self.store.sync_legacy(force=True)
        synced = self.store.get_job(self.job["id"])
        second = self.store.set_qualification(self.job["id"], "filtered_out", reasons=["Mandatory specialization gap"],
                                             responsibilities_reviewed=True, source="test")
        self.assertEqual(second["qualification_status"], "filtered_out")
        self.assertEqual(second["application_status"], "applied")
        self.assertEqual(second["version"], synced["version"])
        self.assertEqual(sum(h["event_type"] == "qualification" for h in first["history"]), 1)

    def test_force_sync_preserves_newer_portal_listing_and_evaluation(self) -> None:
        self.store.set_listing_status(
            self.job["id"], "closed", source="worker", note="Verified closed"
        )
        portal = self.store.set_evaluation(
            self.job["id"],
            score=87,
            recommendation="Strong portal evaluation",
            reasons={"strengths": ["Implementation ownership"]},
            source="worker",
        )

        self.store.sync_legacy(force=True)
        preserved = self.store.get_job(self.job["id"])

        self.assertEqual(preserved["listing_status"], "closed")
        self.assertEqual(preserved["fit_score"], 87)
        self.assertEqual(preserved["fit_recommendation"], "Strong portal evaluation")
        self.assertEqual(preserved["last_checked_at"], portal["last_checked_at"])
        self.assertEqual(preserved["last_evaluated_at"], portal["last_evaluated_at"])

    def test_changed_legacy_facts_can_replace_their_prior_snapshot(self) -> None:
        self.store.set_listing_status(self.job["id"], "active", source="portal")
        self.store.set_evaluation(
            self.job["id"],
            score=87,
            recommendation="Portal result",
            reasons={"strengths": ["Delivery"]},
            source="portal",
        )
        state_path = self.root / "job_scraper" / "seen_jobs.json"
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        payload["seen"]["lead"].update(
            {
                "status": "expired",
                "rank_score": 91,
                "rank_verdict": "Updated legacy evaluation",
                "last_checked_at": "2026-10-06T08:00:00+03:00",
                "rank_date": "2026-10-06T08:05:00+03:00",
            }
        )
        state_path.write_text(json.dumps(payload), encoding="utf-8")

        self.store.sync_legacy(force=True)
        updated = self.store.get_job(self.job["id"])

        self.assertEqual(updated["listing_status"], "closed")
        self.assertEqual(updated["fit_score"], 91)
        self.assertEqual(updated["fit_recommendation"], "Updated legacy evaluation")
        self.assertEqual(updated["last_checked_at"], "2026-10-06T08:00:00+03:00")
        self.assertEqual(updated["last_evaluated_at"], "2026-10-06T08:05:00+03:00")

    def test_legacy_timestamp_change_does_not_replay_unchanged_status_or_score(self) -> None:
        self.store.set_listing_status(self.job["id"], "closed", source="portal")
        self.store.set_evaluation(
            self.job["id"],
            score=87,
            recommendation="Portal result",
            reasons={"strengths": ["Delivery"]},
            source="portal",
        )
        state_path = self.root / "job_scraper" / "seen_jobs.json"
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        payload["seen"]["lead"].update(
            {
                "last_checked_at": "2026-10-06T08:00:00+03:00",
                "rank_date": "2026-10-06T08:05:00+03:00",
            }
        )
        state_path.write_text(json.dumps(payload), encoding="utf-8")

        self.store.sync_legacy(force=True)
        updated = self.store.get_job(self.job["id"])

        self.assertEqual(updated["listing_status"], "closed")
        self.assertEqual(updated["fit_score"], 87)
        self.assertEqual(updated["fit_recommendation"], "Portal result")
        self.assertEqual(updated["last_checked_at"], "2026-10-06T08:00:00+03:00")
        self.assertEqual(updated["last_evaluated_at"], "2026-10-06T08:05:00+03:00")

    def test_status_change_has_history_csv_mirror_idempotency_and_conflict_check(self) -> None:
        updated = self.store.update_application_status(
            self.job["id"],
            "applied",
            source="test",
            note="Submitted through careers page",
            idempotency_key="status-request-1",
            expected_version=self.job["version"],
        )
        repeated = self.store.update_application_status(
            self.job["id"],
            "applied",
            source="test",
            note="Must not duplicate",
            idempotency_key="status-request-1",
        )
        same_status_new_request = self.store.update_application_status(
            self.job["id"],
            "applied",
            source="test",
            note="Must also remain a no-op",
            idempotency_key="status-request-2",
        )

        self.assertEqual(updated["application_status"], "applied")
        status_events = [
            item
            for item in same_status_new_request["history"]
            if item["event_type"] == "application_status"
        ]
        self.assertEqual(len(status_events), 1)
        with (self.root / "job_search_tracker.csv").open(encoding="utf-8", newline="") as source:
            tracker = list(csv.DictReader(source))
        self.assertEqual(len(tracker), 1)
        self.assertEqual(tracker[0]["status"], "applied")
        self.assertIn("Submitted through careers page", tracker[0]["notes"])
        with self.assertRaises(ConflictError):
            self.store.update_application_status(
                self.job["id"],
                "interviewing",
                source="test",
                idempotency_key="status-request-3",
                expected_version=self.job["version"],
            )

    def test_concurrent_notes_do_not_lose_history_or_tracker_updates(self) -> None:
        self.store.update_application_status(
            self.job["id"], "applied", source="test", idempotency_key="applied"
        )
        errors: list[Exception] = []

        def write(index: int) -> None:
            try:
                self.store.add_note(
                    self.job["id"],
                    f"Concurrent note {index}",
                    source="test",
                    idempotency_key=f"note-{index}",
                )
            except Exception as exc:  # pragma: no cover - captured for assertion
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        self.assertEqual(errors, [])
        notes = [item for item in self.store.get_job(self.job["id"])["history"] if item["event_type"] == "note"]
        self.assertEqual(len(notes), 8)
        tracker_text = (self.root / "job_search_tracker.csv").read_text(encoding="utf-8")
        for index in range(8):
            self.assertIn(f"Concurrent note {index}", tracker_text)

    def test_runs_are_deduplicated_reconciled_cancelled_and_retried(self) -> None:
        first = self.store.enqueue_run("evaluate", job_id=self.job["id"], idempotency_key="eval-1")
        duplicate = self.store.enqueue_run("evaluate", job_id=self.job["id"], idempotency_key="eval-1")
        duplicate_click = self.store.enqueue_run(
            "evaluate", job_id=self.job["id"], idempotency_key="eval-second-click"
        )
        self.assertEqual(first["id"], duplicate["id"])
        self.assertEqual(first["id"], duplicate_click["id"])
        claimed = self.store.claim_next_run()
        self.assertEqual(claimed["state"], "running")

        ordinary_cli_store = PortalStore(self.root)
        self.assertEqual(ordinary_cli_store.get_run(first["id"])["state"], "running")

        restarted = PortalStore(self.root, reconcile=True)
        self.assertEqual(restarted.get_run(first["id"])["state"], "interrupted")
        delayed_duplicate = restarted.enqueue_run(
            "evaluate", job_id=self.job["id"], idempotency_key="eval-second-click"
        )
        self.assertEqual(delayed_duplicate["id"], first["id"])
        retry = restarted.retry_run(first["id"], "eval-retry")
        self.assertEqual(retry["retry_of"], first["id"])
        cancelled = restarted.request_cancel(retry["id"])
        self.assertEqual(cancelled["state"], "cancelled")

    def test_search_summary_is_persisted_and_recommendations_are_capped(self) -> None:
        run = self.store.enqueue_run("search", idempotency_key="search-summary")
        event_id = self.store.record_search_summary(
            run["id"],
            discovered=20,
            duplicates=5,
            filtered_out=7,
            awaiting_evaluation=3,
            recommended=5,
        )
        event = next(
            item for item in self.store.run_events(run["id"]) if item["id"] == event_id
        )
        self.assertEqual(event["kind"], "search_summary")
        self.assertEqual(event["data"]["recommended"], 5)

        with self.assertRaises(ValidationError):
            self.store.record_search_summary(
                run["id"],
                discovered=20,
                duplicates=0,
                filtered_out=0,
                awaiting_evaluation=0,
                recommended=9,
            )

    def test_conversation_turns_are_serialized(self) -> None:
        conversation = self.store.create_conversation("General")
        first = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Hello"},
            idempotency_key="chat-1",
        )
        self.assertEqual(first["state"], "queued")
        duplicate = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Hello"},
            idempotency_key="chat-1",
        )
        self.assertEqual(duplicate["id"], first["id"])
        first_message = self.store.add_message(
            conversation["id"], "user", "Hello", run_id=first["id"]
        )
        repeated_message = self.store.add_message(
            conversation["id"], "user", "Hello", run_id=first["id"]
        )
        self.assertEqual(repeated_message["id"], first_message["id"])
        with self.assertRaises(ConflictError):
            self.store.enqueue_run(
                "chat",
                conversation_id=conversation["id"],
                payload={"message": "Second"},
                idempotency_key="chat-2",
            )

    def test_existing_tailored_cv_is_inferred_without_mutating_job_identity(self) -> None:
        filename = "Candidate_CV_acme_solutions_engineer.pdf"
        (self.root / "cv" / filename).write_bytes(b"%PDF-1.4\n%%EOF\n")

        job = self.store.get_job(self.job["id"])

        self.assertEqual(job["artifacts"][0]["filename"], filename)
        self.assertEqual(job["artifacts"][0]["source"], "inferred_from_filename")
        self.assertEqual(job["id"], self.job["id"])


if __name__ == "__main__":
    unittest.main()
