"""Static regression checks for the dependency-free portal event lifecycle."""

from __future__ import annotations

import re
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from tools.portal_ui import PORTAL_HTML


class PortalUIEventLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        match = re.search(r"<script>(.*)</script>", PORTAL_HTML, re.DOTALL)
        if match is None:
            raise AssertionError("Portal script was not found")
        cls.script = match.group(1)

    def test_frontend_javascript_parses(self) -> None:
        result = subprocess.run(
            ["node", "--check"],
            input=self.script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_real_browser_progress_recovery_and_scrollable_kanban_at_responsive_widths(self) -> None:
        chrome = shutil.which("chromium") or shutil.which("google-chrome")
        if not chrome:
            cached = list((Path.home() / ".cache/puppeteer/chrome").glob("*/chrome-linux64/chrome"))
            chrome = str(cached[0]) if cached else None
        if not chrome:
            self.skipTest("Chrome unavailable; static JavaScript checks still run")
        with tempfile.TemporaryDirectory(prefix="portal-browser-") as profile:
            result = subprocess.run(
                ["node", str(Path(__file__).with_name("portal_browser_smoke.cjs"))],
                input=json.dumps({"chrome": chrome, "profile": profile, "html": PORTAL_HTML}),
                text=True, capture_output=True, timeout=90, check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        reports = output["logs"]
        self.assertEqual([r["width"] for r in reports], [1440, 390])
        self.assertTrue(all(r["scrollAnchoring"] and r["reconnect"] for r in reports))
        boards = output["kanban"]
        self.assertEqual([r["width"] for r in boards], [1440, 1024, 900, 768, 390])
        self.assertTrue(all(r["independentScrolling"] and r["edgeAutoscroll"]
                            and r["persistentDrop"] and r["rollback"] for r in boards[:3]))
        self.assertTrue(all(r["pageScrolling"] for r in boards[3:]))
        runs = output["runs"]
        self.assertEqual([r["width"] for r in runs], [1440, 1024, 900, 768, 390])
        self.assertTrue(all(r["independentScrolling"] and r["fixedToolbar"] for r in runs[:3]))
        self.assertTrue(all(r["pageScrolling"] for r in runs[3:]))
        self.assertTrue(all(r["archiving"] and r["activePreserved"] and r["chatAccess"]
                            and r["refresh"] and r["rollback"] for r in runs))
        cvs = output["cv"]
        self.assertEqual([r["width"] for r in cvs], [1440, 390])
        self.assertTrue(all(r["newJobAction"] and r["queued"] and r["generating"]
                            and r["ready"] and r["failed"] and r["statusPreserved"]
                            and r["progress"] and r["chatLink"] for r in cvs))
        self.assertEqual([r["width"] for r in output["refresh"]], [1440, 390])
        self.assertEqual([r['width'] for r in output['bulkCv']], [1440, 390])
        self.assertTrue(all(r['button'] and r['dedup'] and r['feedback'] and r['statusPreserved'] for r in output['bulkCv']))
        self.assertTrue(all(r["preview"] and r["grouping"] and r["explicitSelection"] and
                            r["dedup"] and r["stopping"] and r["accurateStates"] and r["stopAll"] for r in output["refresh"]))

    def test_run_streams_are_keyed_cursor_aware_and_not_recursively_reopened(self) -> None:
        self.assertIn("runSubscriptions:new Map()", self.script)
        self.assertIn("runCursors:new Map()", self.script)
        self.assertIn("events?after=${after}", self.script)
        self.assertIn("pollRunSubscription(sub)", self.script)
        self.assertNotIn("watchRun(id,()=>openRun(id))", self.script)
        open_run = re.search(r"async function openRun\(id\)\{([^\n]+)", self.script)
        self.assertIsNotNone(open_run)
        self.assertNotIn("openRun(id)", open_run.group(1))

    def test_run_log_is_newest_first_for_initial_live_and_recovered_events(self) -> None:
        self.assertIn(
            "sort((a,b)=>Number(b.id)-Number(a.id))",
            self.script,
        )
        self.assertIn("next.insertAdjacentHTML('beforebegin',runEventMarkup(event))", self.script)
        self.assertIn("if(payload.event)prependRunEvent(payload.event)", self.script)
        self.assertIn("for(const event of run.events.filter", self.script)
        self.assertIn("restoreRunLogScroll(log,snapshot)", self.script)
        self.assertIn("readingOlder:log.scrollTop>8", self.script)

    def test_run_log_order_does_not_change_chat_message_order(self) -> None:
        self.assertIn("c.messages.map(m=>", self.script)
        self.assertNotIn("sortedRunEvents(c.messages)", self.script)

    def test_chat_streams_deltas_and_guards_conversation_identity(self) -> None:
        self.assertIn("appendChatDelta(conversationId,runId", self.script)
        self.assertIn(
            "S.conversation!==conversationId||S.activeRun!==runId", self.script
        )
        self.assertIn("await refreshConversationMessages(conversationId)", self.script)
        self.assertIn("const request=++S.conversationRequest", self.script)


if __name__ == "__main__":
    unittest.main()
