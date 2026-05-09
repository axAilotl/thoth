import json
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


def test_capabilities_json_is_data_only_stdout():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "capabilities", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["tool"] == "thoth"
    assert payload["agent_surfaces"]["robot_triage"] == "python thoth.py --robot-triage"
    assert "Configuration validation failed" not in result.stdout


def test_robot_triage_json_includes_health_and_commands():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "--robot-triage"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["tool"] == "thoth"
    assert "health" in payload
    assert any(command["name"] == "stats" for command in payload["commands"])


def test_stats_json_and_json_typo_correction_are_parseable():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "stats", "--jsno"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["tool"] == "thoth"
    assert "graphql_cache" in payload
    assert "Interpreted `--jsno` as `--json`" in result.stderr


def test_command_typo_error_teaches_correct_command():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "stat", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "Did you mean `stats`?" in result.stderr
    assert "Suggested command: `thoth.py stats --help`" in result.stderr


def test_delete_requires_confirmation_or_dry_run():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "delete", "1234567890"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to delete artifacts without explicit confirmation" in result.stderr
    assert "python thoth.py delete 1234567890 --dry-run" in result.stderr
    assert "python thoth.py delete 1234567890 --yes" in result.stderr


def test_wiki_read_commands_support_json():
    repo_root = Path(__file__).resolve().parents[1]

    query = subprocess.run(
        [sys.executable, "thoth.py", "wiki-query", "unlikely query token", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert query.returncode == 0
    query_payload = json.loads(query.stdout)
    assert query_payload["surface"] == "wiki-query"
    assert "hits" in query_payload

    lint = subprocess.run(
        [sys.executable, "thoth.py", "wiki-lint", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert lint.returncode in {0, 1}
    lint_payload = json.loads(lint.stdout)
    assert lint_payload["surface"] == "wiki-lint"
    assert "report" in lint_payload


def test_archivist_benchmark_json_allows_empty_scope():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "archivist", "--benchmark", "--limit", "0", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["surface"] == "archivist benchmark"
    assert payload["topics"] == []
