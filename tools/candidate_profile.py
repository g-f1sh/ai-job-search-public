"""Read configured candidate identity without embedding a developer's identity."""

from pathlib import Path
import re


def candidate_required_text(repo_root: Path) -> tuple[str, ...]:
    """Return the configured name, or no identity requirement before setup.

    PDF page/text validation still applies when setup has not supplied a name.
    Canonical profile values take precedence over the workspace overview.
    """
    for path in (
        repo_root / ".claude/skills/job-application-assistant/01-candidate-profile.md",
        repo_root / "CLAUDE.md",
    ):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        match = re.search(r"^\s*-\s*\*\*Name:\*\*\s*(.+)$", text, re.MULTILINE)
        if not match:
            continue
        name = match.group(1).split("<!--", 1)[0].strip()
        if name and not re.search(r"[\[\]<>]", name) and name.casefold() not in {
            "not provided", "unknown", "your name", "not configured"
        }:
            return (name,)
    return ()
