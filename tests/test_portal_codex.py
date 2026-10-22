"""Codex App Server transport lifecycle regression tests."""

from __future__ import annotations

import io
import unittest
from pathlib import Path
from typing import Any

from tools.portal_codex import CodexAppServer


class ExitedProcess:
    def __init__(self, returncode: int = 17):
        self.stdout = io.StringIO("")
        self.stderr = io.StringIO("fatal transport failure\n")
        self.stdin = io.StringIO()
        self.returncode = returncode

    def poll(self) -> int:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        return self.returncode


class CodexTransportTests(unittest.TestCase):
    def test_threads_use_portal_scoped_full_access_without_approvals(self) -> None:
        client = CodexAppServer(Path.cwd())
        requests: list[tuple[str, dict[str, Any], float]] = []

        def request(method: str, params: dict[str, Any], *, timeout: float = 30):
            requests.append((method, params, timeout))
            return {"thread": {"id": params.get("threadId", "thread-new")}}

        client.request = request  # type: ignore[method-assign]

        self.assertEqual(client.start_thread()["id"], "thread-new")
        self.assertEqual(client.resume_thread("thread-existing")["id"], "thread-existing")

        for method, params, _timeout in requests:
            self.assertIn(method, {"thread/start", "thread/resume"})
            self.assertEqual(params["approvalPolicy"], "never")
            self.assertEqual(params["sandbox"], "danger-full-access")
            self.assertNotIn("approvalsReviewer", params)

    def test_eof_emits_disconnect_and_releases_dead_process(self) -> None:
        client = CodexAppServer(Path.cwd())
        process = ExitedProcess()
        client.process = process  # type: ignore[assignment]
        client._stderr_lines.append("fatal transport failure")
        events: list[tuple[str, dict[str, Any]]] = []
        client.add_listener(lambda method, params: events.append((method, params)))

        client._read_loop()

        self.assertIsNone(client.process)
        self.assertFalse(client.running)
        self.assertEqual(events[0][0], "server/disconnected")
        self.assertEqual(events[0][1]["returncode"], 17)
        self.assertIn("fatal transport failure", events[0][1]["error"])


if __name__ == "__main__":
    unittest.main()
