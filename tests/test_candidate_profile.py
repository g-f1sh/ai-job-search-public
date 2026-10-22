import tempfile
import unittest
from pathlib import Path
from tools.candidate_profile import candidate_required_text


class CandidateProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile = self.root / ".claude/skills/job-application-assistant/01-candidate-profile.md"
        self.profile.parent.mkdir(parents=True)

    def test_unconfigured_profile_does_not_require_placeholder_name(self):
        self.profile.write_text("- **Name:** [YOUR_NAME]\n", encoding="utf-8")
        self.assertEqual(candidate_required_text(self.root), ())

    def test_configured_identity_is_used(self):
        self.profile.write_text("- **Name:** Alex Example\n", encoding="utf-8")
        self.assertEqual(candidate_required_text(self.root), ("Alex Example",))

    def test_canonical_profile_has_precedence(self):
        self.profile.write_text("- **Name:** Alex Example\n", encoding="utf-8")
        (self.root / "CLAUDE.md").write_text("- **Name:** Jordan Sample\n", encoding="utf-8")
        self.assertEqual(candidate_required_text(self.root), ("Alex Example",))

    def test_workspace_fallback_and_inline_comment(self):
        self.profile.write_text("- **Name:** [YOUR_NAME]\n", encoding="utf-8")
        (self.root / "CLAUDE.md").write_text("- **Name:** Jordan Sample <!-- setup -->\n", encoding="utf-8")
        self.assertEqual(candidate_required_text(self.root), ("Jordan Sample",))

    def test_missing_profiles_have_no_hardcoded_identity(self):
        self.assertEqual(candidate_required_text(self.root), ())
