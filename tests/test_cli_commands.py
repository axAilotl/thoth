import subprocess
import sys
from pathlib import Path


def test_removed_playwright_commands_are_rejected():
    repo_root = Path(__file__).resolve().parents[1]

    for command in ("download", "full"):
        result = subprocess.run(
            [sys.executable, "thoth.py", command, "--help"],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "invalid choice" in result.stderr


def test_web_clipper_commands_are_still_wired():
    repo_root = Path(__file__).resolve().parents[1]

    for command in ("web-clipper", "ingest-queue"):
        result = subprocess.run(
            [sys.executable, "thoth.py", command, "--help"],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert command in result.stdout
