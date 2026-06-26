import json
import subprocess
import sys
from pathlib import Path

from core.agent_surface import AgentSurfaceService
from core.artifacts import PaperArtifact, RepositoryArtifact
from core.config import Config
from core.mcp_server import ThothMCPServer
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.wiki_updater import CompiledWikiUpdater


def _config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return config


def test_agent_surface_queries_wiki_with_provenance(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    artifact = RepositoryArtifact(
        id="gh_1",
        source_type="github",
        repo_name="owner/agent-repo",
        description="Agent-facing repository",
        raw_content='{"id": 1, "full_name": "owner/agent-repo"}',
    )
    CompiledWikiUpdater(config, layout=layout, db=db).update_from_artifact(artifact)

    service = AgentSurfaceService(config, layout=layout, db=db)
    result = service.query_wiki("agent repo", limit=5)

    assert result["hits"]
    hit = result["hits"][0]
    assert hit["title"] == "owner/agent-repo"
    assert hit["provenance"]["artifact_id"] == "gh_1"
    assert hit["provenance"]["source_type"] == "github"


def test_agent_surface_artifact_lookup_returns_canonical_provenance(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    artifact = PaperArtifact(
        id="2401.12345",
        source_type="arxiv",
        title="Agent Paper",
        raw_content='{"id": "2401.12345"}',
        arxiv_id="2401.12345",
        pdf_url="https://arxiv.org/pdf/2401.12345.pdf",
        custom_metadata={"raw_payload_path": "raw/arxiv/2401.12345.json"},
    )
    db.upsert_ingestion_entry(
        IngestionQueueEntry(
            artifact_id="paper-queued",
            artifact_type="paper",
            source="arxiv",
            payload_json=json.dumps(artifact.to_dict()),
            capabilities_json=json.dumps(list(artifact.capabilities)),
            created_at="2026-04-04T00:00:00",
        )
    )

    service = AgentSurfaceService(config, layout=layout, db=db)
    listed = service.list_artifacts(limit=10)
    detail = service.get_artifact("paper-queued")
    provenance = service.get_artifact_provenance("paper-queued")

    assert listed["artifacts"][0]["artifact_id"] == "paper-queued"
    assert detail["canonical_record"]["artifact_id"] == "2401.12345"
    assert provenance["provenance"]["queue_id"] == "paper-queued"
    assert provenance["provenance"]["raw_payload"]["path"] == (
        "raw/arxiv/2401.12345.json"
    )


def test_mcp_server_lists_and_calls_core_tools(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)
    server = ThothMCPServer(service)

    tools = server.list_tools()["tools"]
    assert {tool["name"] for tool in tools} >= {
        "wiki_query",
        "list_artifacts",
        "get_artifact",
        "get_artifact_provenance",
        "list_connectors",
        "run_connector",
        "research_missing_papers",
    }
    db.upsert_ingestion_entry(
        IngestionQueueEntry(
            artifact_id="mcp-paper",
            artifact_type="paper",
            source="arxiv",
            payload_json=json.dumps(
                PaperArtifact(
                    id="2601.00001",
                    source_type="arxiv",
                    title="MCP Paper",
                    raw_content='{"id": "2601.00001"}',
                    arxiv_id="2601.00001",
                ).to_dict()
            ),
            created_at="2026-04-04T00:00:00",
        )
    )

    response = server.call_tool("list_connectors", {})
    payload = json.loads(response["content"][0]["text"])
    assert payload["total"] == 9

    response = server.call_tool(
        "run_connector",
        {"connector_name": "arxiv", "options": {"topics": "agents"}},
    )
    payload = json.loads(response["content"][0]["text"])
    assert payload["status"] == "planned"
    assert payload["connector"]["name"] == "arxiv"

    response = server.call_tool("get_artifact", {"artifact_id": "mcp-paper"})
    payload = json.loads(response["content"][0]["text"])
    assert payload["queue"]["artifact_id"] == "mcp-paper"
    assert payload["canonical_record"]["artifact_id"] == "2601.00001"

    rpc_response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
        }
    )
    assert rpc_response["result"]["tools"][0]["name"] == "wiki_query"


def test_stable_agent_cli_groups_are_wired():
    repo_root = Path(__file__).resolve().parents[1]
    commands = (
        ["artifacts", "list", "--json", "--limit", "1"],
        ["query", "wiki", "no-such-query", "--json", "--limit", "1"],
        ["connectors", "run", "arxiv", "--topics", "agents", "--json"],
        ["ingest", "queue", "--help"],
        ["wiki", "lint", "--help"],
    )

    for command in commands:
        result = subprocess.run(
            [sys.executable, "thoth.py", *command],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
