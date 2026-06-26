import json
from pathlib import Path

import pytest

from core.agent_surface import AgentSurfaceService
from core.config import Config
from core.mcp_server import ThothMCPServer
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


def _config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("sources.skill_outputs.enabled", True)
    return config


def test_connector_run_history_dedupes_rerun_outputs(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = tmp_path / "skill-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifact_type": "transcript",
                "artifact_id": "history-note",
                "payload": {
                    "title": "History Note",
                    "raw_transcript": "Connector run history fixture.",
                    "processed_transcript": "Connector run history fixture.",
                },
            }
        ),
        encoding="utf-8",
    )
    service = AgentSurfaceService(config, layout=layout, db=db)

    first = service.run_connector(
        "skill_outputs",
        execute=True,
        options={"output_paths": [str(output_path)]},
    )
    second = service.run_connector(
        "skill_outputs",
        execute=True,
        options={"output_paths": [str(output_path)]},
    )

    assert first["result"]["queued_count"] == 1
    assert second["result"]["queued_count"] == 1
    assert first["history"]["run"]["output_count"] == 1
    assert second["history"]["run"]["output_count"] == 1
    assert (
        first["history"]["checkpoint"]["checkpoint_id"]
        == second["history"]["checkpoint"]["checkpoint_id"]
    )
    assert second["history"]["checkpoint"]["output_count"] == 1
    assert [entry.artifact_id for entry in db.list_ingestion_entries(limit=10)] == [
        "history-note"
    ]


def test_connector_run_history_records_failed_attempt_reason_and_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    def fail_connector(_options):
        raise ValueError("temporary connector outage")

    monkeypatch.setattr(service, "_run_arxiv_connector", fail_connector)

    with pytest.raises(ValueError, match="temporary connector outage"):
        service.run_connector(
            "arxiv",
            execute=True,
            options={"topics": "agents", "limit": 1},
        )

    history = service.list_connector_runs(connector_name="arxiv", limit=5)

    assert history["runs"][0]["status"] == "failed"
    assert history["runs"][0]["failure_reason"] == "temporary connector outage"
    assert history["runs"][0]["retry_state"]["retryable"] is True
    assert history["runs"][0]["next_retry_at"]
    assert history["checkpoints"][0]["failure_reason"] == "temporary connector outage"


def test_connector_checkpoint_records_resume_token_from_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    def run_connector(_options):
        return {
            "queued_count": 1,
            "queued": [{"artifact_id": "paper-1", "artifact_type": "paper"}],
            "checkpoint": {
                "pagination_token": "cursor-2",
                "last_result_count": 1,
            },
        }

    monkeypatch.setattr(service, "_run_arxiv_connector", run_connector)

    payload = service.run_connector(
        "arxiv",
        execute=True,
        options={"topics": "agents", "limit": 1},
    )
    plan = service.run_connector(
        "arxiv",
        options={"topics": "agents", "limit": 1},
    )

    assert payload["history"]["run"]["resume_token"] == "cursor-2"
    assert payload["history"]["checkpoint"]["resume_token"] == "cursor-2"
    assert payload["history"]["checkpoint"]["state"]["checkpoint"][
        "pagination_token"
    ] == "cursor-2"
    assert plan["history"]["checkpoint"]["last_run_id"] == (
        payload["history"]["run"]["run_id"]
    )


def test_mcp_lists_connector_runs(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)
    server = ThothMCPServer(service)

    tools = {tool["name"] for tool in server.list_tools()["tools"]}

    assert "list_connector_runs" in tools
    response = server.call_tool("list_connector_runs", {"limit": 5})
    payload = json.loads(response["content"][0]["text"])
    assert payload == {"checkpoints": [], "runs": [], "total": 0}
