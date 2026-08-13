import json
import os
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api

BUILTIN_CONNECTOR_NAMES = [
    "arxiv",
    "github",
    "huggingface",
    "imported_markdown",
    "omi",
    "pi_skills",
    "skill_outputs",
    "web_clipper",
    "x_api",
    "youtube",
]

PLUGIN_MANIFEST = {
    "name": "meeting_notes",
    "source_name": "meeting_notes",
    "display_name": "Meeting Notes Export",
    "artifact_types": ["transcript"],
    "inputs": ["local_files:meeting_notes_export"],
    "outputs": ["artifact_queue:transcript"],
    "capabilities": ["transcripts", "queue"],
    "config_keys": ["sources.meeting_notes.export_dir"],
    "auth": [],
    "queue_capability": True,
    "queue_behavior": "queues_artifacts",
    "safety_mode": "local_ingest_queue",
    "allowed_side_effects": [
        "local_file_read",
        "raw_file_write",
        "artifact_queue_write",
    ],
    "entrypoint": "collectors.personal.meeting_notes:MeetingNotesConnector",
    "cli_command": "connectors run meeting_notes",
    "config_namespace": "sources.meeting_notes",
}


def _api_client(tmp_path: Path, monkeypatch, settings: dict) -> TestClient:
    base_config_path = tmp_path / "config.example.json"
    base_config_path.write_text(json.dumps(settings), encoding="utf-8")
    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", base_config_path)
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")
    return TestClient(thoth_api.app)


def test_get_connectors_returns_builtin_tree_with_policy_and_budgets(
    tmp_path: Path, monkeypatch
):
    settings = {
        "sources": {"web_clipper": {"enabled": False}},
        "connectors": {
            "allowlist": ["arxiv", "github"],
            "budgets": {"per_connector": {"arxiv": {"max_files_per_run": 25}}},
        },
    }
    client = _api_client(tmp_path, monkeypatch, settings)

    response = client.get("/api/connectors")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == len(payload["connectors"])
    names = [item["name"] for item in payload["connectors"]]
    assert names == BUILTIN_CONNECTOR_NAMES
    for connector in payload["connectors"]:
        assert isinstance(connector["enabled"], bool)
        assert set(connector["policy"]) == {"allowlist", "pins"}
        assert set(connector["policy"]["allowlist"]) == {
            "configured",
            "allowed",
            "matched",
        }
        assert set(connector["policy"]["pins"]) == {"configured", "matched", "drift"}
        assert connector["budgets"]["connector"] == connector["name"]
        assert "max_files_per_run" in connector["budgets"]["limits"]

    by_name = {item["name"]: item for item in payload["connectors"]}
    assert by_name["web_clipper"]["enabled"] is False
    assert by_name["arxiv"]["enabled"] is True
    assert by_name["arxiv"]["policy"]["allowlist"]["configured"] is True
    assert by_name["arxiv"]["policy"]["allowlist"]["allowed"] is True
    assert by_name["youtube"]["policy"]["allowlist"]["allowed"] is False
    assert by_name["arxiv"]["budgets"]["configured"] is True
    assert by_name["arxiv"]["budgets"]["limits"]["max_files_per_run"] == 25


def test_get_connectors_includes_drop_in_plugin_manifest(
    tmp_path: Path, monkeypatch
):
    plugin_dir = tmp_path / "plugins" / "meeting_notes"
    plugin_dir.mkdir(parents=True)
    manifest_path = plugin_dir / "connector.json"
    manifest_path.write_text(json.dumps(PLUGIN_MANIFEST), encoding="utf-8")
    settings = {"connectors": {"plugin_dirs": [str(tmp_path / "plugins")]}}
    client = _api_client(tmp_path, monkeypatch, settings)

    response = client.get("/api/connectors")

    assert response.status_code == 200
    payload = response.json()
    by_name = {item["name"]: item for item in payload["connectors"]}
    assert payload["total"] == len(BUILTIN_CONNECTOR_NAMES) + 1
    plugin = by_name["meeting_notes"]
    assert plugin["origin"] == str(manifest_path)
    assert plugin["enabled"] is True
    assert plugin["policy"]["allowlist"]["configured"] is False
    assert plugin["budgets"]["configured"] is False


def test_get_connectors_surfaces_manifest_errors(tmp_path: Path, monkeypatch):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "broken.connector.json").write_text(
        json.dumps({"name": "broken"}), encoding="utf-8"
    )
    settings = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}
    client = _api_client(tmp_path, monkeypatch, settings)

    response = client.get("/api/connectors")

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "broken.connector.json" in detail
    assert "source_name" in detail


def _run_connectors_cli(*cli_args: str, env_extra: dict | None = None):
    repo_root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "thoth.py", "connectors", *cli_args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
    )


def test_connectors_list_human_output_shows_state_policy_and_budgets():
    result = _run_connectors_cli("list")

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("Connectors")
    lines = [line for line in result.stdout.splitlines() if line.startswith("- ")]
    assert len(lines) == len(BUILTIN_CONNECTOR_NAMES)
    for name in BUILTIN_CONNECTOR_NAMES:
        line = next(item for item in lines if item.startswith(f"- {name} "))
        assert "enabled" in line or "disabled" in line
        assert "allowlist=" in line
        assert "pin=" in line
        assert "budgets=" in line
    assert "- x_api [disabled" in result.stdout
    assert "- arxiv [enabled" in result.stdout


def test_connectors_list_json_includes_policy_and_budgets():
    result = _run_connectors_cli("list", "--json")

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    for connector in payload["connectors"]:
        assert "policy" in connector
        assert "budgets" in connector
        assert isinstance(connector["enabled"], bool)


def test_connectors_list_cli_fails_closed_on_bad_plugin_manifest(tmp_path: Path):
    (tmp_path / "broken.connector.json").write_text(
        json.dumps({"name": "broken"}), encoding="utf-8"
    )

    result = _run_connectors_cli(
        "list", env_extra={"THOTH_CONNECTOR_PATH": str(tmp_path)}
    )

    assert result.returncode != 0
    assert "connector registry error" in result.stderr
    assert "broken.connector.json" in result.stderr
