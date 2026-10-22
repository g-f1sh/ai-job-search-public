"""Authenticated portal API integration tests."""

from __future__ import annotations

import http.cookiejar
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

from tools.cv_webview import create_server


TRACKER_HEADER = (
    "date,company,sector,role,role_type,channel,status,contact_person,fit_rating,notes,"
    "cv_file,cover_letter_file,source\n"
)


class PortalHTTPAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "cv").mkdir()
        (self.root / "cover_letters").mkdir()
        (self.root / "job_scraper").mkdir()
        self.job_url = "https://example.com/jobs/solutions"
        (self.root / "job_scraper" / "seen_jobs.json").write_text(
            json.dumps(
                {
                    "seen": {
                        "role": {
                            "company": "Acme",
                            "title": "Solutions Engineer",
                            "url": self.job_url,
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
        (self.root / "cv" / "Acme_Solutions_Engineer.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")
        environment = {"PORTAL_AUTH_TOKEN": "test-portal-secret", "PORTAL_COOKIE_SECURE": "0"}
        self.env = patch.dict(os.environ, environment)
        self.env.start()
        # HTTP checks exercise the real store/handlers, never live Codex execution.
        self.server = create_server(self.root, port=0, worker=Mock())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.jar = http.cookiejar.CookieJar()
        self.client = build_opener(HTTPCookieProcessor(self.jar))

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.env.stop()
        self.temporary.cleanup()

    def test_bulk_new_cvs_auth_write_validation_and_duplicate_clicks(self):
        path = '/api/cvs/generate-new'
        with self.assertRaises(HTTPError) as denied:
            self.post(path, {'idempotency_key': 'bulk'})
        self.assertEqual(denied.exception.code, 401)
        denied.exception.close()
        self.post('/api/auth/login', {'token': 'test-portal-secret'}).close()
        self.server.portal_worker.cv_concurrency = 4
        with self.assertRaises(HTTPError) as denied:
            self.post(path, {'idempotency_key': 'bulk'}, verified=False)
        self.assertEqual(denied.exception.code, 403)
        denied.exception.close()
        with self.assertRaises(HTTPError) as invalid:
            self.post(path, {})
        self.assertEqual(invalid.exception.code, 400)
        invalid.exception.close()
        with self.post(path, {'idempotency_key': 'bulk'}) as response:
            first = json.load(response)
        with self.post(path, {'idempotency_key': 'bulk-again'}) as response:
            second = json.load(response)
        self.assertEqual((first['queued'], second['queued'], second['already_active']), (1, 0, 1))
        self.assertEqual(first['concurrency'], 4)
        self.assertEqual(self.server.portal_store.get_job(first['runs'][0]['job_id'])['application_status'], 'new')

    @staticmethod
    def body(error: HTTPError) -> dict[str, object]:
        return json.loads(error.read())

    def post(self, path: str, body: dict[str, object], *, verified: bool = True):
        headers = {"Content-Type": "application/json"}
        if verified:
            headers["X-Requested-With"] = "job-portal"
        return self.client.open(
            Request(
                self.base + path,
                data=json.dumps(body).encode(),
                headers=headers,
                method="POST",
            ),
            timeout=3,
        )

    def login(self) -> None:
        with self.post("/api/auth/login", {"token": "test-portal-secret"}) as response:
            self.assertTrue(json.load(response)["authenticated"])

    def test_refresh_preview_batches_and_stops_require_authenticated_verified_writes(self) -> None:
        store = self.server.portal_store
        self.server.portal_worker.stop_runs.side_effect = lambda **kw: store.request_stop(**kw)
        for authenticated, verified, code in ((False, True, 401), (True, False, 403)):
            if authenticated:
                self.login()
            for path in ("/api/listings/refresh-preview", "/api/listings/refresh-eligible", "/api/runs/stop-all"):
                with self.assertRaises(HTTPError) as failure:
                    self.post(path, {}, verified=verified)
                self.assertEqual(failure.exception.code, code)
                failure.exception.close()
        with self.post("/api/listings/refresh-preview", {}) as response:
            preview = json.load(response)
        self.assertEqual(preview["eligible_count"], 1)
        job_id = preview["jobs"][0]["id"]
        options = {"job_ids": [job_id], "idempotency_key": "batch-a"}
        with self.post("/api/listings/refresh-eligible", options) as response:
            batch = json.load(response)["run"]
        with self.post("/api/listings/refresh-eligible", {**options, "idempotency_key": "batch-b"}) as response:
            self.assertEqual(json.load(response)["run"]["id"], batch["id"])
        self.assertEqual(batch["children"][0]["company"], "Acme")
        store.claim_next_run()
        with self.post(f"/api/runs/{batch['id']}/stop-batch", {"idempotency_key": "stop-batch"}) as response:
            stop = json.load(response)
        self.assertEqual(stop["cancelled_queued"], 1)
        with self.client.open(self.base + "/api/stops/" + stop["id"]) as response:
            self.assertEqual(json.load(response)["id"], stop["id"])
        with self.assertRaises(HTTPError) as failure:
            urlopen(self.base + "/api/stops/" + stop["id"])
        self.assertEqual(failure.exception.code, 401)
        failure.exception.close()
        with self.assertRaises(HTTPError) as failure:
            self.post(f"/api/runs/{batch['id']}/stop-batch", {}, verified=False)
        self.assertEqual(failure.exception.code, 403)
        failure.exception.close()
        with self.assertRaises(HTTPError) as failure:
            self.post(f"/api/runs/{batch['children'][0]['id']}/retry", {})
        self.assertEqual(failure.exception.code, 409)
        failure.exception.close()
        self.assertEqual(store.get_job(job_id)["application_status"], "new")
        self.assertIsNone(store.get_job(job_id)["last_evaluated_at"])

    def test_archiving_requires_auth_verified_write_and_preserves_linked_data(self) -> None:
        store = self.server.portal_store
        conversation = store.create_conversation("Preserved chat")
        finished = store.enqueue_run("search", payload={"test": "finished"})
        store.finish_run(finished["id"], "completed", artifacts=[{"path": "cv/Acme_Solutions_Engineer.pdf"}])
        store.add_message(conversation["id"], "assistant", "Existing reply", run_id=finished["id"])
        active = store.enqueue_run("search", payload={"test": "running"})
        store.claim_next_run()
        queued = store.enqueue_run("search", payload={"test": "queued"})
        chat_before = store.get_conversation(conversation["id"])
        active_before = store.get_run(active["id"])
        for authenticated, verified, code in ((False, True, 401), (True, False, 403)):
            if authenticated:
                self.login()
            with self.assertRaises(HTTPError) as raised:
                self.post("/api/runs/archive-history", {"idempotency_key": "clear"}, verified=verified)
            self.assertEqual(raised.exception.code, code)
            raised.exception.close()
            self.assertIsNone(store.get_run(finished["id"])["archived_at"])
        with self.assertRaises(HTTPError) as raised:
            self.post("/api/runs/archive-history", {"idempotency_key": {"invalid": True}})
        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()
        for _ in range(2):
            with self.post("/api/runs/archive-history", {"idempotency_key": "clear"}) as response:
                self.assertEqual(json.load(response), {"archived_count": 1})
        with self.client.open(self.base + "/api/runs") as response:
            body = json.load(response)
            self.assertEqual({r["id"] for r in body["runs"]}, {active["id"], queued["id"]})
            self.assertEqual(body["history"], {"clearable": 0, "archived": 1})
        with self.client.open(self.base + "/api/runs?include_archived=1") as response:
            self.assertEqual(len(json.load(response)["runs"]), 3)
        with self.client.open(self.base + f"/api/runs/{finished['id']}") as response:
            run = json.load(response)
            self.assertTrue(run["archived_at"])
            self.assertTrue(run["events"])
            self.assertEqual(run["artifacts"][0]["path"], "cv/Acme_Solutions_Engineer.pdf")
        with self.client.open(self.base + f"/api/conversations/{conversation['id']}") as response:
            self.assertEqual(json.load(response), chat_before)
        with self.client.open(self.base + "/files/cv/Acme_Solutions_Engineer.pdf") as response:
            self.assertTrue(response.read().startswith(b"%PDF"))
        self.assertEqual(store.get_run(active["id"]), active_before)
        self.assertEqual(store.get_run(queued["id"])["state"], "queued")

    def test_private_data_artifacts_and_streaming_require_authentication(self) -> None:
        for path in ("/api/jobs", "/api/runs/run_00000000000000000000000000000000/events"):
            with self.assertRaises(HTTPError) as raised:
                urlopen(self.base + path, timeout=2)
            self.assertEqual(raised.exception.code, 401)
            raised.exception.close()
        with self.assertRaises(HTTPError) as raised:
            urlopen(self.base + "/files/cv/Acme_Solutions_Engineer.pdf", timeout=2)
        self.assertEqual(raised.exception.code, 401)
        raised.exception.close()

    def test_generate_cv_is_protected_deduplicated_and_does_not_require_shortlisting(self) -> None:
        store = self.server.portal_store
        job = store.list_jobs()[0]
        path = f"/api/jobs/{job['id']}/generate-cv"
        with self.assertRaises(HTTPError) as raised:
            self.post(path, {"idempotency_key": "unauthorized-cv"})
        self.assertEqual(raised.exception.code, 401)
        raised.exception.close()
        self.login()
        with self.assertRaises(HTTPError) as raised:
            self.post(path, {"idempotency_key": "unverified-cv"}, verified=False)
        self.assertEqual(raised.exception.code, 403)
        raised.exception.close()
        run_ids = []
        for key in ("cv-click-one", "cv-click-two"):
            with self.post(path, {"idempotency_key": key}) as response:
                self.assertEqual(response.status, 202)
                run = json.load(response)["run"]
                self.assertEqual(run["task_type"], "generate_cv")
                self.assertEqual(run["job_id"], job["id"])
                run_ids.append(run["id"])
        self.assertEqual(len(set(run_ids)), 1)
        self.assertEqual(store.get_job(job["id"])["application_status"], "new")
        with self.client.open(self.base + f"/api/jobs/{job['id']}") as response:
            body = json.load(response)
            self.assertEqual(body["cv"]["state"], "queued")
            self.assertEqual(body["cv"]["run_id"], run_ids[0])

    def test_bounded_real_event_stream_closes_and_reconnects_with_cursor(self) -> None:
        self.login()
        store = self.server.portal_store
        run = store.enqueue_run("search", payload={"test": "long-running"})
        with patch.object(self.server.RequestHandlerClass, "run_stream_seconds", 0.05, create=True):
            with self.client.open(self.base + f"/api/runs/{run['id']}/events?after=0", timeout=2) as response:
                self.assertEqual(response.headers["Connection"], "close")
                # Without true EOF this hangs until socket timeout even though the
                # handler returned. Browser EventSource likewise never reconnects.
                initial = response.read().decode()
            queued_id = store.get_run(run["id"])["events"][-1]["id"]
            self.assertIn(f"id: {queued_id}", initial)
            self.assertIn("Task queued", initial)
            store.append_run_event(run["id"], "activity", "Still working after the stream rotated")
            with self.client.open(self.base + f"/api/runs/{run['id']}/events?after={queued_id}", timeout=2) as response:
                resumed = response.read().decode()
            self.assertIn("Still working", resumed)
            self.assertNotIn("Task queued", resumed)
            self.assertEqual(store.get_run(run["id"])["state"], "queued")
            store.finish_run(run["id"], "completed", summary="Finished independently of browser")
            with self.client.open(self.base + f"/api/runs/{run['id']}/events?after={queued_id}", timeout=2) as response:
                terminal = response.read().decode()
            self.assertIn('"terminal": true', terminal)
            self.assertIn('"state": "completed"', terminal)

    def test_login_cookie_and_verified_write_update_status_history(self) -> None:
        self.login()
        with self.client.open(self.base + "/api/jobs", timeout=2) as response:
            job = json.load(response)["jobs"][0]

        with self.assertRaises(HTTPError) as raised:
            self.post(
                f"/api/jobs/{job['id']}/status",
                {"status": "applied", "expected_version": job["version"]},
                verified=False,
            )
        self.assertEqual(raised.exception.code, 403)
        raised.exception.close()

        with self.post(
            f"/api/jobs/{job['id']}/status",
            {
                "status": "applied",
                "expected_version": job["version"],
                "idempotency_key": "status-http-test",
                "note": "Submitted from phone",
            },
        ) as response:
            updated = json.load(response)["job"]

        self.assertEqual(updated["application_status"], "applied")
        self.assertEqual(updated["history"][0]["source"], "portal")
        self.assertEqual(updated["history"][0]["note"], "Submitted from phone")

    def test_empty_chat_message_does_not_leave_a_queued_run(self) -> None:
        self.login()
        with self.post("/api/conversations", {"title": "General"}) as response:
            conversation = json.load(response)["conversation"]
        with self.assertRaises(HTTPError) as raised:
            self.post(f"/api/conversations/{conversation['id']}/messages", {"message": "  "})
        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()
        with self.client.open(self.base + "/api/runs", timeout=2) as response:
            self.assertEqual(json.load(response)["runs"], [])


if __name__ == "__main__":
    unittest.main()
