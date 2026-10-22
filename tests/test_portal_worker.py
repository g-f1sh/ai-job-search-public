"""Tests for persistent worker chat, streamed events, approvals, and failures."""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from tools.portal_codex import CodexUnavailable
from tools.portal_store import PortalStore
from tools.portal_worker import PortalWorker


TRACKER_HEADER = (
    "date,company,sector,role,role_type,channel,status,contact_person,fit_rating,notes,"
    "cv_file,cover_letter_file,source\n"
)


class FakeCodex:
    def __init__(
        self,
        *,
        fail: bool = False,
        approval: bool = False,
        disconnect_once: bool = False,
        before_complete: Any = None,
    ):
        self.fail = fail
        self.approval = approval
        self.disconnect_once = disconnect_once
        self.before_complete = before_complete
        self.listeners: list[Any] = []
        self.server_callback: Any = None
        self.resumed: list[str] = []
        self.responded: list[tuple[object, dict[str, Any]]] = []
        self.approval_event = threading.Event()
        self.interrupted = False
        self.running = True
        self.turn_calls = 0
        self.thread_calls = 0

    def set_server_request_callback(self, callback: Any) -> None:
        self.server_callback = callback

    def add_listener(self, callback: Any) -> None:
        self.listeners.append(callback)

    def remove_listener(self, callback: Any) -> None:
        self.listeners.remove(callback)

    def start_thread(self) -> dict[str, str]:
        self.running = True
        self.thread_calls += 1
        thread_id = "thread-new" if self.thread_calls == 1 else f"thread-new-{self.thread_calls}"
        return {"id": thread_id}

    def resume_thread(self, thread_id: str) -> dict[str, str]:
        self.running = True
        self.resumed.append(thread_id)
        return {"id": thread_id}

    def start_turn(self, thread_id: str, text: str, *, client_message_id: str | None = None, availability: bool = False) -> str:
        if self.fail:
            raise CodexUnavailable("mock failure")
        self.turn_calls += 1
        turn_call = self.turn_calls

        def emit() -> None:
            time.sleep(0.02)
            if self.disconnect_once and turn_call == 1:
                for callback in list(self.listeners):
                    callback(
                        "item/agentMessage/delta",
                        {"threadId": thread_id, "turnId": "turn-1", "delta": "Partial"},
                    )
                self.running = False
                for callback in list(self.listeners):
                    callback(
                        "server/disconnected",
                        {
                            "error": "Codex App Server connection closed unexpectedly (exit code 17)",
                            "returncode": 17,
                        },
                    )
                return
            if self.approval:
                self.server_callback(
                    "rpc-approval",
                    "item/commandExecution/requestApproval",
                    {
                        "threadId": thread_id,
                        "turnId": "turn-1",
                        "itemId": "item-1",
                        "command": "python3 tools/job_portal_cli.py jobs",
                    },
                )
                self.approval_event.wait(2)
            if self.before_complete:
                self.before_complete()
            for callback in list(self.listeners):
                callback("item/commandExecution/outputDelta", {"threadId": thread_id, "delta": "Compiling one-page PDF"})
                callback(
                    "item/agentMessage/delta",
                    {"threadId": thread_id, "turnId": "turn-1", "delta": "Hello from Codex"},
                )
                callback(
                    "turn/completed",
                    {
                        "threadId": thread_id,
                        "turn": {
                            "id": "turn-1",
                            "status": "interrupted" if self.interrupted else "completed",
                            "error": None,
                        },
                    },
                )

        threading.Thread(target=emit, daemon=True).start()
        return "turn-1"

    def interrupt_turn(self, thread_id: str, turn_id: str) -> None:
        self.interrupted = True
        self.approval_event.set()

    def approval_response(self, method: str, decision: str) -> dict[str, str]:
        return {"decision": decision}

    def respond(self, request_id: object, result: dict[str, Any]) -> None:
        self.responded.append((request_id, result))
        self.approval_event.set()

    def stop(self) -> None:
        self.running = False
        self.approval_event.set()

    def recent_stderr(self) -> str:
        return "mock app-server stderr" if not self.running else ""


class PortalWorkerTests(unittest.TestCase):
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
                            "url": "https://example.com/job",
                            "status": "new",
                            "fit": "high",
                            "qualification_status": "recommended",
                            "qualification_reasons": ["Customer-facing implementation role"],
                            "responsibilities_reviewed": True,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        (self.root / "job_search_tracker.csv").write_text(TRACKER_HEADER, encoding="utf-8")
        self.store = PortalStore(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def wait_for(self, run_id: str, states: set[str], timeout: float = 3) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            run = self.store.get_run(run_id)
            if run["state"] in states:
                return run
            time.sleep(0.02)
        self.fail(f"run {run_id} did not reach {states}")

    def test_new_and_resumed_conversation_streams_and_persists_messages(self) -> None:
        fake = FakeCodex()
        conversation = self.store.create_conversation("General")
        run = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Hello"},
            idempotency_key="chat-new",
        )
        self.store.add_message(conversation["id"], "user", "Hello", run_id=run["id"])
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        worker.wake()
        completed = self.wait_for(run["id"], {"completed"})
        worker.stop()

        self.assertEqual(completed["codex_thread_id"], "thread-new")
        messages = self.store.get_conversation(conversation["id"])["messages"]
        self.assertEqual([message["role"] for message in messages], ["user", "assistant"])
        self.assertEqual(messages[-1]["content"], "Hello from Codex")
        self.assertTrue(any(event["kind"] == "assistant_delta" for event in completed["events"]))
        self.assertTrue(any(event["kind"] == "command_output" and "Compiling" in event["message"] for event in completed["events"]))

        resumed_fake = FakeCodex()
        second = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Continue"},
            idempotency_key="chat-resume",
        )
        self.store.add_message(conversation["id"], "user", "Continue", run_id=second["id"])
        resumed_worker = PortalWorker(
            self.store, self.root, codex=resumed_fake, poll_interval=0.01
        )
        resumed_worker.start()
        resumed_worker.wake()
        self.wait_for(second["id"], {"completed"})
        resumed_worker.stop()
        self.assertEqual(resumed_fake.resumed, ["thread-new"])

    def test_approval_is_persisted_and_can_resume_turn(self) -> None:
        fake = FakeCodex(approval=True)
        conversation = self.store.create_conversation("Approval")
        run = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Run it"},
            idempotency_key="chat-approval",
        )
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        worker.wake()
        deadline = time.monotonic() + 2
        approval = None
        while time.monotonic() < deadline:
            approvals = self.store.get_run(run["id"])["approvals"]
            if approvals:
                approval = approvals[0]
                break
            time.sleep(0.02)
        self.assertIsNotNone(approval)
        worker.resolve_approval(approval["id"], "approved")
        self.wait_for(run["id"], {"completed"})
        worker.stop()
        self.assertEqual(fake.responded[0][0], "rpc-approval")

    def test_codex_failure_is_recorded_with_useful_error(self) -> None:
        fake = FakeCodex(fail=True)
        conversation = self.store.create_conversation("Failure")
        run = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Fail"},
            idempotency_key="chat-fail",
        )
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        worker.wake()
        failed = self.wait_for(run["id"], {"failed"})
        worker.stop()
        self.assertIn("mock failure", failed["error"])

    def test_running_turn_can_be_cancelled(self) -> None:
        fake = FakeCodex(approval=True)
        conversation = self.store.create_conversation("Cancellation")
        run = self.store.enqueue_run(
            "chat",
            conversation_id=conversation["id"],
            payload={"message": "Wait for approval"},
            idempotency_key="chat-cancel",
        )
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        worker.wake()
        self.wait_for(run["id"], {"running"})

        worker.cancel(run["id"])
        cancelled = self.wait_for(run["id"], {"interrupted"})
        worker.stop()

        self.assertTrue(cancelled["cancel_requested"])
        self.assertTrue(fake.interrupted)

    def test_worker_completion_preserves_portal_facts_during_legacy_sync(self) -> None:
        job = self.store.list_jobs(include_archived=True)[0]

        def write_portal_results() -> None:
            self.store.set_listing_status(job["id"], "closed", source="worker")
            self.store.set_evaluation(
                job["id"],
                score=87,
                recommendation="Worker evaluation",
                reasons={"strengths": ["Technical delivery"]},
                source="worker",
            )

        fake = FakeCodex(before_complete=write_portal_results)
        run = self.store.enqueue_run(
            "evaluate", job_id=job["id"], idempotency_key="worker-sync-regression"
        )
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        worker.wake()
        self.wait_for(run["id"], {"completed"})
        worker.stop()

        preserved = self.store.get_job(job["id"])
        self.assertEqual(preserved["listing_status"], "closed")
        self.assertEqual(preserved["fit_score"], 87)
        self.assertEqual(preserved["fit_recommendation"], "Worker evaluation")

    def test_search_completion_persists_discoveries_and_summary_without_agent_summary(self) -> None:
        run = self.store.enqueue_run("search", payload={}, idempotency_key="search-import")
        def import_results():
            raw = [{"company": "Raw Result", "title": "Developer",
                    "url": "https://example.com/raw", "description": "Mandatory deep full-stack development"}]
            self.store.import_search_candidates(run["id"], raw)
        fake = FakeCodex(before_complete=import_results)
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        try:
            completed = self.wait_for(run["id"], {"completed"})
        finally:
            worker.stop()
        summary = [e for e in completed["events"] if e["kind"] == "search_summary"][-1]
        self.assertEqual(summary["data"], {"discovered": 1, "duplicates": 0, "filtered_out": 0,
                                          "awaiting_evaluation": 1, "recommended": 0})
        self.assertNotIn("Raw Result", {j["company"] for j in self.store.list_jobs()})
        self.assertIn("Raw Result", {j["company"] for j in self.store.list_jobs(include_discoveries=True)})

    def test_search_completion_and_later_force_sync_preserve_cli_qualification(self) -> None:
        run = self.store.enqueue_run("search", payload={}, idempotency_key="search-qualify")
        def qualify_and_write_legacy():
            candidate = {"company": "Qualified", "title": "Implementation Engineer",
                         "url": "https://example.com/qualified"}
            self.store.import_search_candidates(run["id"], [candidate])
            job = next(j for j in self.store.list_jobs(include_discoveries=True) if j["company"] == "Qualified")
            self.store.set_qualification(job["id"], "recommended", reasons=["Reviewed customer implementation and training"],
                                         responsibilities_reviewed=True, source="agent_cli")
            path = self.root / "job_scraper/seen_jobs.json"
            payload = json.loads(path.read_text())
            payload["seen"]["qualified"] = {**candidate, "status": "ranked", "rank_score": 87,
                                            "qualification_status": "recommended",
                                            "qualification_reasons": ["Legacy representation of the recommendation"],
                                            "responsibilities_reviewed": True}
            path.write_text(json.dumps(payload))
        fake = FakeCodex(before_complete=qualify_and_write_legacy)
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        try:
            completed = self.wait_for(run["id"], {"completed"})
        finally:
            worker.stop()
        self.store.sync_legacy(force=True)
        job = next(j for j in self.store.list_jobs() if j["company"] == "Qualified")
        self.assertEqual(job["qualification_reasons"], ["Reviewed customer implementation and training"])
        self.assertEqual(job["fit_score"], 87)
        self.assertEqual(job["application_status"], "new")
        self.assertEqual(next(e for e in completed["events"] if e["kind"] == "search_summary")["data"]["recommended"], 1)

    def test_failed_search_still_records_persisted_summary(self) -> None:
        run = self.store.enqueue_run("search", payload={}, idempotency_key="search-failure")
        fake = FakeCodex(fail=True)
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        try:
            failed = self.wait_for(run["id"], {"failed"})
        finally:
            worker.stop()
        summary = next(e for e in failed["events"] if e["kind"] == "search_summary")
        self.assertEqual(summary["data"]["discovered"], 0)
        self.assertIn("mock failure", failed["error"])

    def test_mid_turn_disconnect_fails_run_and_worker_processes_next_task(self) -> None:
        fake = FakeCodex(disconnect_once=True)
        first_conversation = self.store.create_conversation("Disconnect")
        second_conversation = self.store.create_conversation("Recovery")
        first = self.store.enqueue_run(
            "chat",
            conversation_id=first_conversation["id"],
            payload={"message": "First"},
            idempotency_key="disconnect-first",
        )
        second = self.store.enqueue_run(
            "chat",
            conversation_id=second_conversation["id"],
            payload={"message": "Second"},
            idempotency_key="disconnect-second",
        )
        worker = PortalWorker(self.store, self.root, codex=fake, poll_interval=0.01)
        worker.start()
        worker.wake()

        failed = self.wait_for(first["id"], {"failed"})
        completed = self.wait_for(second["id"], {"completed"})
        worker.stop()

        self.assertIn("exit code 17", failed["error"])
        self.assertTrue(any(event["kind"] == "codex_disconnected" for event in failed["events"]))
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(fake.turn_calls, 2)


if __name__ == "__main__":
    unittest.main()
