"""Availability checks, durable batches, stop fences, and bounded inference."""
from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from tools.portal_codex import CodexAppServer
from tools.portal_listing_check import ListingChecker, CheckStopped, classify_listing, public_url
from tools.portal_store import PortalStore, ConflictError, ValidationError
from tools.portal_worker import PortalWorker
from tests.test_portal_worker import FakeCodex, TRACKER_HEADER


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "job_scraper").mkdir()
        (self.root / "job_search_tracker.csv").write_text(TRACKER_HEADER)
        (self.root / "job_scraper" / "seen_jobs.json").write_text(json.dumps({"seen": {
            str(i): {"company": f"Company {i}", "title": "Implementation Engineer", "url": f"https://example.com/jobs/{i}",
                     "status": "new", "qualification_status": "recommended", "qualification_reasons": ["Implementation responsibilities match"], "responsibilities_reviewed": True}
            for i in range(15)}}))
        self.store = PortalStore(self.root)
        self.jobs = sorted(self.store.list_jobs(include_discoveries=True), key=lambda j: int(j["company"].split()[-1]))
        for job in self.jobs:
            self.store.set_qualification(job["id"], "recommended", reasons=["Implementation responsibilities match"], responsibilities_reviewed=True, source="test")

    def tearDown(self):
        self.temp.cleanup()

    def test_selective_preview_and_default_size(self):
        self.store.set_listing_status(self.jobs[0]["id"], "active", source="test")
        self.store.set_listing_status(self.jobs[1]["id"], "closed", source="test")
        for i, status in ((2, "rejected_employer"), (3, "withdrawn"), (4, "applied"), (5, "interviewing"), (6, "offer")):
            self.store.update_application_status(self.jobs[i]["id"], status, source="test")
        self.store.set_qualification(self.jobs[7]["id"], status="filtered_out", reasons=["Wrong mandatory specialization"], responsibilities_reviewed=True, source="test")
        self.store.enqueue_run("refresh_listing", job_id=self.jobs[8]["id"])
        self.assertEqual(self.store.preview_refresh_batch()["eligible_count"], 6)
        extra = self.store.preview_refresh_batch({"include_tracked": True})
        self.assertEqual(extra["eligible_count"], 8)
        batch = self.store.enqueue_refresh_batch()
        self.assertEqual(len(batch["children"]), 6)
        self.assertEqual(batch["payload"]["freshness_hours"], 24)
        self.assertEqual(batch["payload"]["max_ai_fallbacks"], 2)
        self.assertFalse(any(c["job_id"] in {j["id"] for j in self.jobs[:9]} for c in batch["children"]))

    def test_explicit_selection_requires_valid_ids_and_current_eligibility(self):
        with self.assertRaises(ValidationError):
            self.store.enqueue_refresh_batch({"job_ids": []})
        with self.assertRaises(ConflictError):
            self.store.enqueue_refresh_batch({"job_ids": ["job_" + "a" * 20]})
        with self.assertRaises(ValidationError):
            self.store.enqueue_refresh_batch({"max_ai_fallbacks": -1})
        with self.assertRaises(ValidationError):
            self.store.enqueue_refresh_batch({"job_ids": [j["id"] for j in self.jobs]})
        batch = self.store.enqueue_refresh_batch({"limit": 15, "job_ids": [j["id"] for j in self.jobs]})
        self.assertEqual(batch["counts"]["total"], 15)

    def test_duplicate_batches_and_per_job_refresh_tasks(self):
        batches = []
        threads = [threading.Thread(target=lambda: batches.append(self.store.enqueue_refresh_batch(idempotency_key="click-" + threading.current_thread().name))) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len({b["id"] for b in batches}), 1)
        batch = self.store.get_run(batches[0]["id"])
        self.assertEqual(batch["counts"]["total"], 10)
        child = batch["children"][0]
        independent = self.store.enqueue_run("evaluate", job_id=self.jobs[12]["id"])
        self.assertEqual(self.store.get_run(independent["id"])["queue_position"], 2)  # batch, not ten standalone children
        child_details = self.store.get_run(child["id"])
        self.assertEqual(child_details["queue_position"], 1)
        self.assertEqual(child_details["waiting_for"]["id"], batch["id"])
        repeated = self.store.enqueue_run("refresh_listing", job_id=child["job_id"], payload={"arbitrary": "other origin"})
        self.assertEqual(repeated["id"], child["id"])
        self.store.request_stop(batch_id=batch["id"])
        repeated_batch = self.store.enqueue_refresh_batch(idempotency_key="click-" + threads[0].name)
        self.assertEqual(repeated_batch["id"], batch["id"])

    def test_stop_batch_fences_enqueue_claim_retry_and_results(self):
        batch = self.store.enqueue_refresh_batch({"limit": 2})
        self.store.claim_next_run()
        first, second = batch["children"]
        self.store.claim_batch_child(batch["id"], first["id"])
        independent = self.store.enqueue_run("evaluate", job_id=self.jobs[12]["id"])
        stop = self.store.request_stop(batch_id=batch["id"], idempotency_key="stop-once")
        self.assertEqual(stop["cancelled_queued"], 1)
        self.assertEqual(stop["stopping"], 2)  # batch owner + its active check
        self.assertEqual(self.store.get_run(independent["id"])["state"], "queued")
        self.assertEqual(self.store.get_run(first["id"])["display_state"], "stopping")
        for action in (
            lambda: self.store.enqueue_run("refresh_listing", job_id=self.jobs[13]["id"], payload={"requested_by": batch["id"]}),
            lambda: self.store.retry_run(second["id"]),
            lambda: self.store.claim_batch_child(batch["id"], second["id"]),
            lambda: self.store.set_listing_status(first["job_id"], "closed", source="worker", run_id=first["id"]),
        ):
            with self.assertRaises(ConflictError):
                action()
        self.store.finish_run(first["id"], "completed", summary="False success")
        self.store.finish_run(batch["id"], "completed", summary="False success")
        self.assertEqual(self.store.get_stop(stop["id"])["interrupted_active"], 2)
        self.assertEqual(self.store.request_stop(batch_id=batch["id"], idempotency_key="stop-once")["id"], stop["id"])
        self.assertEqual(self.store.get_job(first["job_id"])["application_status"], "new")
        self.assertEqual(self.store.get_job(first["job_id"])["listing_status"], "active")
        self.assertEqual(len(self.store.get_run(batch["id"])["children"]), 2)

    def test_enqueue_and_cancel_race_is_serialized(self):
        for _ in range(4):
            batch = self.store.enqueue_refresh_batch({"limit": 1})
            errors = []
            gate = threading.Barrier(2)
            def enqueue():
                gate.wait()
                try:
                    self.store.enqueue_run("refresh_listing", job_id=self.jobs[14]["id"], payload={"requested_by": batch["id"]})
                except ConflictError:
                    errors.append("fenced")
            t = threading.Thread(target=enqueue)
            t.start()
            gate.wait()
            self.store.request_stop(batch_id=batch["id"])
            t.join()
            self.assertTrue(all(c["state"] == "cancelled" for c in self.store.get_run(batch["id"])["children"]))
            with self.assertRaises(ConflictError):
                self.store.enqueue_run("refresh_listing", job_id=self.jobs[13]["id"], payload={"requested_by": batch["id"]})

    def test_stop_all_keeps_history_and_chat_and_reconciles_restart(self):
        batch = self.store.enqueue_refresh_batch({"limit": 2})
        self.store.claim_next_run()
        self.store.claim_batch_child(batch["id"], batch["children"][0]["id"])
        c = self.store.create_conversation("Preserved")
        self.store.add_message(c["id"], "assistant", "Earlier conversation")
        queued = self.store.enqueue_run("chat", conversation_id=c["id"], payload={"message": "hello"})
        stop = self.store.request_stop(idempotency_key="all")
        self.assertEqual(stop["cancelled_queued"], 2)
        restarted = PortalStore(self.root, reconcile=True)
        self.assertEqual(restarted.get_stop(stop["id"])["interrupted_active"], 2)
        self.assertEqual(restarted.get_run(queued["id"])["state"], "cancelled")
        self.assertEqual(restarted.get_conversation(c["id"])["messages"][0]["content"], "Earlier conversation")
        self.assertTrue(all(r["archived_at"] is None for r in restarted.list_runs()))
        self.assertIsNone(restarted.claim_next_run())

    def test_restart_without_stop_cancels_orphan_children(self):
        batch = self.store.enqueue_refresh_batch()
        self.store.claim_next_run()
        reopened = PortalStore(self.root, reconcile=True)
        self.assertEqual(reopened.get_run(batch["id"])["state"], "interrupted")
        self.assertTrue(all(c["state"] == "cancelled" for c in reopened.get_run(batch["id"])["children"]))
        self.assertIsNone(reopened.claim_next_run())

    def test_child_claim_racing_stop_cannot_escape_fence(self):
        for _ in range(5):
            batch = self.store.enqueue_refresh_batch({"limit": 1})
            self.store.claim_next_run()
            gate = threading.Barrier(2)
            def claim():
                gate.wait()
                try:
                    self.store.claim_batch_child(batch["id"], batch["children"][0]["id"])
                except ConflictError:
                    pass
            t = threading.Thread(target=claim)
            t.start()
            gate.wait()
            self.store.request_stop(batch_id=batch["id"])
            t.join()
            child = self.store.get_run(batch["children"][0]["id"])
            self.assertTrue(child["cancel_requested"])
            self.assertIn(child["display_state"], ("cancelled", "stopping"))
            self.store.finish_run(child["id"], "completed")
            self.store.finish_run(batch["id"], "completed")
            self.assertNotIn(self.store.get_run(child["id"])["state"], ("queued", "running", "completed"))

    def test_cancellation_during_ai_interrupts_turn_and_children(self):
        batch = self.store.enqueue_refresh_batch({"limit": 2, "max_ai_fallbacks": 2})
        fake = FakeCodex(approval=True)
        fake.start_availability_thread = fake.start_thread
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=.01)
        worker.listing_checker = Mock()
        worker.listing_checker.check.return_value = {"status": "unknown", "reason": "Accessible evidence", "needs_ai": True, "excerpt": "Applications open."}
        worker.start()
        try:
            deadline = time.monotonic() + 3
            while not self.store.get_run(batch["id"])["codex_turn_id"] and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(self.store.get_run(batch["id"])["codex_turn_id"])
            stop = worker.stop_runs(batch_id=batch["id"])
            deadline = time.monotonic() + 3
            while self.store.get_stop(stop["id"])["stopping"] and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(fake.interrupted)
            self.assertEqual(self.store.get_stop(stop["id"])["stopping"], 0)
            latest = self.store.get_run(batch["id"])
            self.assertEqual(latest["state"], "interrupted")
            self.assertTrue(all(c["state"] == "interrupted" for c in latest["children"]))
            self.assertTrue(all(self.store.get_job(c["job_id"])["last_checked_at"] is None for c in latest["children"]))
        finally:
            worker.stop()

    def test_reported_tokens_and_final_response_not_intro_are_persisted(self):
        run = self.store.enqueue_run("refresh_listing", job_id=self.jobs[0]["id"])
        claimed = self.store.claim_next_run()
        class AvailabilityCodex(FakeCodex):
            start_availability_thread = FakeCodex.start_thread
            def start_turn(inner, thread_id, text, **kwargs):
                inner.turn_calls += 1
                rows = json.loads(text[text.index('[{"id"'):])
                final = json.dumps({"results": [{"job_id": r["id"], "status": "active", "reason": "Applications explicitly open"} for r in rows]})
                for listener in list(inner.listeners):
                    listener("item/agentMessage/delta", {"threadId": thread_id, "delta": "I'll check this for you."})
                    listener("item/completed", {"threadId": thread_id, "item": {"type": "agentMessage", "phase": "commentary", "text": "I'll check this for you."}})
                    for total in (100, 150):
                        listener("thread/tokenUsage/updated", {"threadId": thread_id, "turnId": "turn-1", "tokenUsage": {"total": {"totalTokens": total, "inputTokens": total-20, "outputTokens": 20, "cachedInputTokens": 0, "reasoningOutputTokens": 0}, "last": {}}})
                    listener("item/completed", {"threadId": thread_id, "item": {"type": "agentMessage", "phase": "final_answer", "text": final}})
                    listener("turn/completed", {"threadId": thread_id, "turn": {"id": "turn-1", "status": "completed"}})
                return "turn-1"
        fake = AvailabilityCodex()
        worker = PortalWorker(self.store, self.root, codex=fake)
        worker.listing_checker = Mock()
        worker.listing_checker.check.return_value = {"status": "unknown", "reason": "Accessible evidence", "needs_ai": True, "excerpt": "Applications open."}
        worker._execute(claimed)
        done = self.store.get_run(run["id"])
        self.assertEqual(done["state"], "completed")
        self.assertNotIn("I'll check", done["result_summary"])
        self.assertIn("Listing active", done["result_summary"])
        self.assertEqual(done["usage"]["reported_tokens"]["totalTokens"], 150)
        self.assertEqual(done["usage"]["usage_scope"], "fresh availability thread")

    def test_legacy_usage_is_unknown_and_intro_not_shown_as_result(self):
        run = self.store.enqueue_run("refresh_listing", job_id=self.jobs[0]["id"])
        self.store.claim_next_run()
        self.store.set_run_codex(run["id"], thread_id="legacy-thread", turn_id="legacy-turn")
        intro = "I'll read AGENTS.md, retrieve this job, and check its source listing."
        self.store.finish_run(run["id"], "completed", summary=intro)
        result = self.store.get_run(run["id"])
        self.assertFalse(result["usage"]["metrics_recorded"])
        self.assertNotIn("I'll read", result["display_summary"])
        self.assertEqual(result["result_summary"], intro)  # original history preserved

    def test_check_failure_does_not_pretend_batch_completed_or_leave_children_live(self):
        batch = self.store.enqueue_refresh_batch({"limit": 2})
        worker = PortalWorker(self.store, self.root, codex=FakeCodex())
        worker.listing_checker = Mock()
        worker.listing_checker.check.side_effect = RuntimeError("adapter failed unexpectedly")
        worker._execute(self.store.claim_next_run())
        latest = self.store.get_run(batch["id"])
        self.assertEqual(latest["state"], "failed")
        self.assertTrue(all(c["state"] == "failed" for c in latest["children"]))
        self.assertEqual(latest["counts"]["failed"], 2)

    def test_interrupt_failure_is_reported_and_persisted(self):
        batch = self.store.enqueue_refresh_batch({"limit": 1})
        self.store.claim_next_run()
        self.store.set_run_codex(batch["id"], thread_id="thread-x", turn_id="turn-x")
        fake = FakeCodex()
        fake.interrupt_turn = Mock(side_effect=RuntimeError("unavailable"))
        # A failed RPC should be visible, not treated as successful interruption.
        from tools.portal_codex import CodexUnavailable
        fake.interrupt_turn.side_effect = CodexUnavailable("bridge unavailable")
        worker = PortalWorker(self.store, self.root, codex=fake)
        stop = worker.stop_runs(batch_id=batch["id"])
        deadline = time.monotonic() + 2
        while not self.store.get_stop(stop["id"])["failures"] and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(len(self.store.get_stop(stop["id"])["failures"]), 1)
        self.assertEqual(self.store.get_stop(stop["id"])["interrupted_active"], 0)
        self.store.finish_run(batch["id"], "interrupted")

    def test_batch_checks_http_then_one_limited_ai_turn_without_status_or_fit_changes(self):
        batch = self.store.enqueue_refresh_batch({"limit": 4, "max_ai_fallbacks": 2})
        claimed = self.store.claim_next_run()
        fake = FakeCodex()
        fake.start_availability_thread = fake.start_thread
        worker = PortalWorker(self.store, self.root, codex=fake)
        worker.listing_checker = Mock()
        worker.listing_checker.check.return_value = {"status": "unknown", "reason": "Interpret fetched evidence", "needs_ai": True, "excerpt": "Responsibilities: onboarding and implementation. Applications open.", "cached": False}
        seen = []
        def interpretation(owner, prompt, **kwargs):
            seen.append(prompt)
            ids = json.loads(prompt[prompt.index('[{"id"'):])
            return {"status": "completed", "error": "", "response": json.dumps({"results": [{"job_id": j["id"], "status": "active", "reason": "Visible application instructions"} for j in ids]})}
        worker._execute_codex = interpretation
        before = {j["id"]: (j["application_status"], j["fit_score"], j["last_evaluated_at"], j["cv_file"]) for j in self.store.list_jobs()}
        worker._execute(claimed)
        done = self.store.get_run(batch["id"])
        self.assertEqual(done["state"], "completed")
        self.assertEqual(done["usage"]["deterministic_checks"], 4)
        self.assertEqual(done["usage"]["ai_fallbacks"], 2)
        self.assertEqual(done["usage"]["unresolved"], 2)
        self.assertEqual(len(seen), 1)
        self.assertNotIn("CLAUDE.md", seen[0])
        after = {j["id"]: (j["application_status"], j["fit_score"], j["last_evaluated_at"], j["cv_file"]) for j in self.store.list_jobs()}
        self.assertEqual(before, after)

    def test_zero_ai_limit_and_blocked_requests_do_not_use_codex(self):
        batch = self.store.enqueue_refresh_batch({"limit": 3, "max_ai_fallbacks": 0})
        worker = PortalWorker(self.store, self.root, codex=FakeCodex())
        worker._execute_codex = Mock(side_effect=AssertionError("AI must not run"))
        worker.listing_checker = Mock()
        worker.listing_checker.check.side_effect = [
            {"status": "unknown", "reason": "Blocked", "needs_ai": False},
            {"status": "unknown", "reason": "Missing content", "needs_ai": False},
            {"status": "unknown", "reason": "Ambiguous accessible content", "needs_ai": True, "excerpt": "..."},
        ]
        worker._execute(self.store.claim_next_run())
        done = self.store.get_run(batch["id"])
        self.assertEqual(done["usage"]["ai_fallbacks"], 0)
        self.assertEqual(done["usage"]["unresolved"], 3)
        worker._execute_codex.assert_not_called()

    def test_cancel_during_http_execution_no_late_job_writes(self):
        batch = self.store.enqueue_refresh_batch({"limit": 2})
        worker = PortalWorker(self.store, self.root, codex=FakeCodex())
        entered, released = threading.Event(), threading.Event()
        def check(job, stopped):
            entered.set()
            released.wait(2)
            return {"status": "closed", "reason": "Late response", "needs_ai": False}
        worker.listing_checker = Mock()
        worker.listing_checker.check.side_effect = check
        claimed = self.store.claim_next_run()
        t = threading.Thread(target=worker._execute, args=(claimed,))
        t.start()
        self.assertTrue(entered.wait(2))
        stop = worker.stop_runs(batch_id=batch["id"])
        self.assertEqual(stop["cancelled_queued"], 1)
        released.set()
        t.join(3)
        self.assertFalse(t.is_alive())
        latest = self.store.get_run(batch["id"])
        self.assertEqual(latest["state"], "interrupted")
        self.assertEqual(self.store.get_stop(stop["id"])["stopping"], 0)
        self.assertTrue(all(self.store.get_job(c["job_id"])["last_checked_at"] is None for c in latest["children"]))

    def test_available_interpretation_thread_is_minimal_and_usage_is_actual(self):
        app = CodexAppServer(self.root)
        app.request = Mock(return_value={"thread": {"id": "only-availability"}})
        self.assertEqual(app.start_availability_thread()["id"], "only-availability")
        params = app.request.call_args.args[1]
        self.assertEqual(params["config"]["project_doc_max_bytes"], 0)
        self.assertEqual(params["approvalPolicy"], "never")
        self.assertEqual(params["sandbox"], "danger-full-access")
        self.assertNotIn("CLAUDE.md", params["baseInstructions"])

    def test_http_cache_timeout_and_redirect_security(self):
        job = self.jobs[0]
        checker = ListingChecker(self.store, rate_seconds=0)
        response = Mock()
        response.headers.get_content_charset.return_value = "utf-8"
        response.read1.side_effect = [b'<h1>Implementation Engineer</h1><button>Apply now</button>', b'']
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        opener = Mock()
        opener.open.return_value = response
        with patch("tools.portal_listing_check.public_url", side_effect=lambda url: url), patch("tools.portal_listing_check.build_opener", return_value=opener):
            self.assertEqual(checker.check(job)["status"], "active")
            self.assertTrue(checker.check(job)["cached"])
        self.assertEqual(opener.open.call_count, 1)
        with patch("tools.portal_listing_check.public_url", side_effect=lambda url: url), patch("tools.portal_listing_check.build_opener", return_value=opener):
            opener.open.side_effect = TimeoutError("time out")
            unknown = checker.check(self.jobs[1])
            self.assertEqual(unknown["status"], "unknown")
            self.assertFalse(unknown["needs_ai"])
        with self.assertRaises(CheckStopped):
            checker.check(job, lambda: True)
        for url in ("file:///etc/passwd", "http://127.0.0.1:8765", "http://localhost"):
            with self.assertRaises(ValueError):
                public_url(url)

    def test_classification_uses_real_evidence_not_http_success(self):
        job = self.jobs[0]
        for content in ("", "<html><div id='app'></div></html>", "Sign in to view Implementation Engineer", "Implementation Engineer Verify you are human", "Implementation Engineer enable JavaScript", "Other Engineer Apply now"):
            result = classify_listing(job, content)
            self.assertEqual(result["status"], "unknown")
            self.assertFalse(result["needs_ai"])
        self.assertEqual(classify_listing(job, "Implementation Engineer No longer accepting applications")["status"], "closed")
        schema = {"@type": "JobPosting", "title": job["title"], "description": "Implement", "hiringOrganization": {"name": "Acme"}}
        html = '<script type="application/ld+json">' + json.dumps(schema) + '</script>'
        self.assertEqual(classify_listing(job, html)["status"], "active")
        schema["validThrough"] = "2000-01-01"
        self.assertEqual(classify_listing(job, '<script type="application/ld+json">' + json.dumps(schema) + '</script>')["status"], "closed")
        self.assertTrue(classify_listing(job, "Implementation Engineer " + "accessible responsibilities " * 50)["needs_ai"])


if __name__ == "__main__":
    unittest.main()
