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
    assert (
        payload["agent_surfaces"]["web_clipper_plan_json"]
        == "python thoth.py web-clipper --plan --json"
    )
    assert (
        payload["agent_surfaces"]["ingest_queue_plan_json"]
        == "python thoth.py ingest-queue --plan --json"
    )
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


def test_mutating_ingestion_commands_have_plan_json_surfaces():
    repo_root = Path(__file__).resolve().parents[1]

    web_clipper = subprocess.run(
        [sys.executable, "thoth.py", "web-clipper", "--plan", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert web_clipper.returncode == 0
    web_payload = json.loads(web_clipper.stdout)
    assert web_payload["surface"] == "web-clipper plan"
    assert "ready" in web_payload
    assert isinstance(web_payload["records"], list)
    assert web_payload["mutation"]["will_index_files"] is False
    assert "Configuration validation failed" not in web_clipper.stdout

    ingest_queue = subprocess.run(
        [sys.executable, "thoth.py", "ingest-queue", "--plan", "--json", "--limit", "1"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert ingest_queue.returncode == 0
    ingest_payload = json.loads(ingest_queue.stdout)
    assert ingest_payload["surface"] == "ingest-queue plan"
    assert ingest_payload["limit"] == 1
    assert isinstance(ingest_payload["entries"], list)
    assert ingest_payload["mutation"]["will_dispatch_artifacts"] is False
    assert "Configuration validation failed" not in ingest_queue.stdout

    x_api_sync = subprocess.run(
        [
            sys.executable,
            "thoth.py",
            "x-api-sync",
            "--plan",
            "--json",
            "--max-pages",
            "1",
            "--max-results",
            "10",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert x_api_sync.returncode == 0
    x_payload = json.loads(x_api_sync.stdout)
    assert x_payload["surface"] == "x-api-sync plan"
    assert x_payload["parameters"]["max_pages"] == 1
    assert x_payload["parameters"]["max_results"] == 10
    assert x_payload["mutation"]["will_contact_x_api"] is False
    assert "Configuration validation failed" not in x_api_sync.stdout
