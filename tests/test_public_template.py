import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("public_check", ROOT / "tools/check_public_template.py")
public_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public_check)


class PublicTemplateTests(unittest.TestCase):
    def test_personalized_name_and_tracked_tracker_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            (root / "CLAUDE.md").write_text("- **Name:** Alex Example\n", encoding="utf-8")
            (root / "job_search_tracker.csv").write_text("company,status\nExample,applied\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            errors = public_check.check(root)
            self.assertTrue(any("candidate identity" in e for e in errors))
            self.assertTrue(any("job_search_tracker.csv" in e for e in errors))

    def test_repository_template_passes(self):
        self.assertEqual(public_check.check(ROOT), [])
