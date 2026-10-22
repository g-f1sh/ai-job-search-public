"""CV tasks are explicit, stable-ID-bound, validated, and independent of status."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.portal_store import PortalStore, ValidationError
from tools.portal_worker import PortalWorker
from tools.verify_pdf import VerificationError
from tests.test_portal_worker import FakeCodex, TRACKER_HEADER


class PortalCVTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "job_scraper").mkdir()
        (self.root / "cv").mkdir()
        (self.root / "job_search_tracker.csv").write_text(TRACKER_HEADER)
        (self.root / "job_scraper/seen_jobs.json").write_text(json.dumps({"seen": {
            "acmecloud": {"company": "AcmeCloud", "title": "Partner Technical Account Manager",
                       "url": "https://example.com/acmecloud", "status": "new", "qualification_status": "recommended",
                       "qualification_reasons": ["Reviewed partner implementation and training"], "responsibilities_reviewed": True},
            "other": {"company": "Other", "title": "Implementation Specialist", "url": "https://example.com/other", "status": "interviewing"}
        }}))
        self.store = PortalStore(self.root)
        self.job = next(j for j in self.store.list_jobs() if j["company"] == "AcmeCloud")
        self.validation = patch("tools.portal_store.verify_pdf")
        self.verify = self.validation.start()
        self.pdf = self.root / "cv/Candidate_CV_acmecloud_partner_technical_account_manager.pdf"

    def tearDown(self):
        self.validation.stop()
        self.temporary.cleanup()

    def draft(self):
        self.pdf.write_bytes(b"%PDF-1.4 candidate-specific test artifact")
        return self.store.set_artifact(self.job["id"], "cv", self.pdf.relative_to(self.root).as_posix(), source="test")

    def execute(self, run, fake=None):
        worker = PortalWorker(self.store, self.root, codex=fake or FakeCodex())
        claimed = self.store.claim_next_run()
        self.assertEqual(claimed["id"], run["id"])
        worker._execute(claimed)
        return self.store.get_run(run["id"])

    def test_cv_states_queue_details_and_status_are_independent(self):
        self.assertEqual(self.job["cv"]["state"], "not_generated")
        search = self.store.enqueue_run("search")
        self.store.claim_next_run()
        cv = self.store.enqueue_run("generate_cv", job_id=self.job["id"])
        job = self.store.get_job(self.job["id"])
        self.assertEqual(job["application_status"], "new")
        self.assertEqual(job["cv"]["state"], "queued")
        self.assertEqual(job["cv"]["queue_position"], 1)
        self.assertEqual(job["cv"]["waiting_for"]["id"], search["id"])
        self.assertEqual(self.store.get_run(cv["id"])["waiting_for"]["id"], search["id"])
        self.store.finish_run(search["id"], "completed")
        self.store.claim_next_run()
        self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "generating")
        self.store.finish_run(cv["id"], "failed", error="No usable PDF")
        failed = self.store.get_job(self.job["id"])
        self.assertEqual(failed["cv"]["state"], "failed")
        self.assertIn("No usable PDF", failed["cv"]["error"])
        self.assertEqual(failed["application_status"], "new")

    def test_duplicate_cv_tasks_ignore_payload_and_conversation_differences(self):
        barrier = threading.Barrier(3)
        ids = []
        def enqueue(n):
            barrier.wait()
            ids.append(self.store.enqueue_run("generate_cv", job_id=self.job["id"], payload={"request": n}, idempotency_key=str(n))["id"])
        threads = [threading.Thread(target=enqueue, args=(n,)) for n in (1, 2)]
        for t in threads:
            t.start()
        barrier.wait()
        for t in threads:
            t.join(3)
            self.assertFalse(t.is_alive())
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(self.store.enqueue_run("generate_cv", job_id=self.job["id"], idempotency_key="2")["id"], ids[0])
        with self.assertRaises(ValidationError):
            self.store.enqueue_run("generate_cv")

    def test_queued_cv_can_be_cancelled_and_retried_without_application_changes(self):
        run = self.store.enqueue_run("generate_cv", job_id=self.job["id"])
        worker = PortalWorker(self.store, self.root, codex=FakeCodex())
        worker.cancel(run["id"])
        self.assertEqual(self.store.get_run(run["id"])["state"], "cancelled")
        self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "failed")
        retry = self.store.retry_run(run["id"], "cv-retry")
        self.assertEqual(retry["job_id"], self.job["id"])
        self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "queued")
        self.assertEqual(self.store.get_job(self.job["id"])["application_status"], "new")
        self.assertFalse(self.pdf.exists())

    def test_artifact_validation_is_required_and_sources_normalize_to_pdf(self):
        tex = self.pdf.with_suffix(".tex")
        tex.write_text("draft only")
        with self.assertRaises(ValidationError):
            self.store.set_artifact(self.job["id"], "cv", "cv/missing.pdf", source="test")
        self.pdf.write_bytes(b"test PDF")
        self.verify.side_effect = VerificationError("expected 1 page, found 2")
        with self.assertRaisesRegex(ValidationError, "found 2"):
            self.store.set_artifact(self.job["id"], "cv", tex.relative_to(self.root).as_posix(), source="test")
        self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "not_generated")
        self.verify.side_effect = None
        ready = self.store.set_artifact(self.job["id"], "cv", tex.relative_to(self.root).as_posix(), source="test")
        self.assertEqual(ready["cv_file"], self.pdf.relative_to(self.root).as_posix())
        self.assertEqual(ready["cv"]["state"], "ready")
        self.verify.assert_called_with(self.pdf, expected_pages=1, min_chars=300, required_text=())
        self.assertEqual(ready["application_status"], "new")
        self.assertEqual(PortalStore(self.root).get_job(self.job["id"])["cv"]["state"], "ready")
        self.pdf.write_bytes(b"modified")
        self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "not_generated")

    def test_artifact_validation_uses_the_configured_candidate_identity(self):
        profile = self.root / '.claude/skills/job-application-assistant/01-candidate-profile.md'
        profile.parent.mkdir(parents=True)
        profile.write_text('- **Name:** Alex Example\n', encoding='utf-8')
        self.draft()
        self.verify.assert_called_with(self.pdf, expected_pages=1, min_chars=300, required_text=('Alex Example',))

    def test_existing_pdf_recovery_uses_dedicated_run_without_codex_or_duplication(self):
        ready = self.draft()
        before = self.pdf.read_bytes()
        fake = FakeCodex()
        run = self.store.enqueue_run("generate_cv", job_id=self.job["id"])
        result = self.execute(run, fake)
        self.assertEqual(result["state"], "completed")
        self.assertEqual(fake.turn_calls, 0)
        self.assertEqual(result["artifacts"][0]["url"], ready["cv"]["url"])
        self.assertEqual(self.pdf.read_bytes(), before)
        self.assertEqual(self.store.get_job(self.job["id"])["application_status"], "new")

    def test_agent_completion_without_pdf_fails_not_success(self):
        conversation = self.store.create_conversation("CV failure")
        run = self.store.enqueue_run("generate_cv", job_id=self.job["id"], conversation_id=conversation["id"])
        worker = PortalWorker(self.store, self.root, codex=FakeCodex(), codex_factory=FakeCodex, poll_interval=0.01)
        worker.start()
        try:
            import time
            deadline = time.monotonic() + 3
            while self.store.get_run(run["id"])["state"] not in ("failed", "completed") and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertEqual(self.store.get_run(run["id"])["state"], "failed")
            self.assertIn("associated PDF", self.store.get_run(run["id"])["error"])
            self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "failed")
            reply = self.store.get_conversation(conversation["id"])["messages"][-1]["content"]
            self.assertIn("CV generation failed", reply)
            self.assertNotIn("Hello from Codex", reply)
        finally:
            worker.stop()

    def test_generated_cv_completion_validates_and_associates_selected_job_only(self):
        run = self.store.enqueue_run("generate_cv", job_id=self.job["id"])
        result = self.execute(run, FakeCodex(before_complete=self.draft))
        self.assertEqual(result["state"], "completed")
        self.assertTrue(result["artifacts"][0]["url"].endswith(".pdf"))
        selected = self.store.get_job(self.job["id"])
        self.assertEqual(selected["cv"]["state"], "ready")
        self.assertEqual(selected["application_status"], "new")
        other = next(j for j in self.store.list_jobs() if j["company"] == "Other")
        self.assertEqual(other["cv"]["state"], "not_generated")
        self.assertEqual(other["application_status"], "interviewing")
        self.pdf.unlink()
        self.assertEqual(self.store.get_job(self.job["id"])["cv"]["state"], "failed")
        retry = self.store.retry_run(run["id"])
        self.assertEqual(retry["state"], "queued")

    def chat(self, message, *, job_id=None, conversation_id=None):
        conversation = self.store.create_conversation("General", job_id=job_id) if not conversation_id else {"id": conversation_id}
        run = self.store.enqueue_run("chat", job_id=job_id, conversation_id=conversation["id"], payload={"message": message})
        self.store.add_message(conversation["id"], "user", message, run_id=run["id"])
        return conversation, run

    def test_ambiguous_chat_requests_selection_without_generation(self):
        conversation, run = self.chat("Create Cv's for the jobs available")
        result = self.execute(run)
        self.assertEqual(result["state"], "completed")
        self.assertTrue(result["result_summary"].startswith("Which jobs should get a CV?"))
        self.assertEqual(len(self.store.list_runs()), 1)
        conversation, reply = self.chat("AcmeCloud", conversation_id=conversation["id"])
        self.execute(reply)
        cvs = [r for r in self.store.list_runs() if r["task_type"] == "generate_cv"]
        self.assertEqual(len(cvs), 1)
        self.assertEqual(cvs[0]["job_id"], self.job["id"])

    def test_identified_chat_uses_same_cv_queue_and_links_existing_active_task(self):
        active = self.store.enqueue_run("generate_cv", job_id=self.job["id"])
        self.store.claim_next_run()
        conversation, run = self.chat("Please create a CV for AcmeCloud")
        self.execute(run)
        cvs = [r for r in self.store.list_runs() if r["task_type"] == "generate_cv"]
        self.assertEqual([r["id"] for r in cvs], [active["id"]])
        messages = self.store.get_conversation(conversation["id"])["messages"]
        self.assertEqual(messages[-1]["cv_runs"][0]["id"], active["id"])

    def test_negated_or_explanatory_chat_does_not_generate_cv(self):
        worker = PortalWorker(self.store, self.root, codex=FakeCodex())
        for message in ("Don't generate CVs", "How do I create a CV for AcmeCloud?", "Search for new jobs"):
            _, run = self.chat(message)
            self.assertFalse(worker._route_cv_chat(run))

    def test_negated_reply_to_selection_does_not_authorize_generation(self):
        conversation, first = self.chat("Create CVs for available jobs")
        self.execute(first)
        worker = PortalWorker(self.store, self.root, codex=FakeCodex())
        for message in ("Don't create a CV for AcmeCloud", "How do I create a CV for AcmeCloud?", "Search for new jobs"):
            _, run = self.chat(message, conversation_id=conversation["id"])
            self.assertFalse(worker._route_cv_chat(run))
            self.store.finish_run(run["id"], "completed")
        self.assertFalse(any(r["task_type"] == "generate_cv" for r in self.store.list_runs()))

    def test_job_chat_can_generate_for_selected_job_and_polite_regenerate_is_explicit(self):
        _, run = self.chat("Create a CV", job_id=self.job["id"])
        self.execute(run)
        cv = next(r for r in self.store.list_runs() if r["task_type"] == "generate_cv")
        self.assertEqual(cv["job_id"], self.job["id"])
        self.store.finish_run(cv["id"], "cancelled")
        _, run = self.chat("Please regenerate a CV for AcmeCloud")
        self.execute(run)
        latest = next(r for r in self.store.list_runs() if r["state"] == "queued")
        self.assertTrue(latest["payload"]["regenerate"])

    def test_search_is_not_an_automatic_cv_workflow(self):
        run = self.store.enqueue_run("search")
        self.execute(run)
        self.assertFalse(any(r["task_type"] == "generate_cv" for r in self.store.list_runs()))
        prompt = PortalWorker(self.store, self.root, codex=FakeCodex())._prompt_for(run)
        self.assertIn("Do not generate CVs", prompt)
