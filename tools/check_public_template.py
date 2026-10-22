#!/usr/bin/env python3
"""Fail before publishing a personalized workspace as a public template."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent


def check(root: Path) -> list[str]:
    errors = []
    required = {
        "CLAUDE.md": "[YOUR_NAME]",
        ".claude/skills/job-application-assistant/01-candidate-profile.md": "<!-- SETUP",
        ".claude/skills/job-application-assistant/04-job-evaluation.md": "[YOUR_PRIMARY_SKILLS]",
        "cv/main_example.tex": "[YOUR_NAME]",
        "cover_letters/cover_example.tex": "[YOUR NAME]",
    }
    for name, token in required.items():
        path = root / name
        if not path.is_file() or token not in path.read_text(encoding="utf-8"):
            errors.append(f"{name}: template placeholder missing; review personalization")
    for name in ("CLAUDE.md", ".claude/skills/job-application-assistant/01-candidate-profile.md"):
        path = root / name
        if path.is_file():
            names = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip().startswith("- **Name:**")]
            if names != ["- **Name:** [YOUR_NAME]"]:
                errors.append(f"{name}: candidate identity must be a blank placeholder")
    result = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], capture_output=True)
    if result.returncode:
        errors.append("Initialize and stage the publication tree before checking tracked files")
        return errors
    for name in result.stdout.decode("utf-8").split("\0"):
        if not name:
            continue
        path = Path(name)
        forbidden = (
            path.suffix.lower() in {".pdf", ".docx", ".db", ".sqlite", ".pem", ".key", ".pfx", ".p12", ".har"}
            or name in {"job_search_tracker.csv", "job_scraper/seen_jobs.json"}
            or name.startswith(("portal_data/", "portal_runs/", "gmail_sync/", "reports/"))
            or name.startswith("documents/") and name not in {"documents/README.md"} and not name.endswith("/.gitkeep")
            or name.startswith("cv/") and name != "cv/main_example.tex"
            or path.name.startswith(".env")
        )
        if forbidden:
            errors.append(f"{name}: private/generated data must not be tracked in the public template")
    return errors


def main() -> int:
    errors = check(ROOT)
    for error in errors:
        print(error)
    if not errors:
        print("Public template checks passed: placeholders and tracked output boundaries")
    return bool(errors)


if __name__ == "__main__":
    sys.exit(main())
