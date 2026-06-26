import asyncio
import json
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api
from core.agent_response import AGENT_QUERY_RESPONSE_TYPE
from core.artifacts import RepositoryArtifact
from core.config import Config
from core.metadata_db import MetadataDB
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


def test_query_wiki_api_uses_agent_safe_response_model(tmp_path: Path, monkeypatch):
    async def noop_async(*args, **kwargs):
        return None

    config = _config(tmp_path)
    base_config_path = tmp_path / "config.example.json"
    base_config_path.write_text(json.dumps(config.data), encoding="utf-8")
    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", base_config_path)
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")
    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", lambda runtime_config: None)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "archivist_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(thoth_api, "resolve_x_api_sync_config", lambda: None)
    thoth_api._shutdown_event = asyncio.Event()

    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    artifact = RepositoryArtifact(
        id="api-repo",
        source_type="github",
        repo_name="owner/api-agent-repo",
        description="Agent API query response",
        raw_content='{"id": 1, "full_name": "owner/api-agent-repo"}',
    )
    CompiledWikiUpdater(config, layout=layout, db=db).update_from_artifact(artifact)

    with TestClient(thoth_api.app) as client:
        response = client.get(
            "/api/query/wiki",
            params={"query": "api agent repo", "limit": 5},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["response_type"] == AGENT_QUERY_RESPONSE_TYPE
    assert payload["action_boundary"]["retrieval_payload_path"] == "retrieval.hits"
    assert payload["retrieval"]["query"] == "api agent repo"
    assert payload["retrieval"]["hits"][0]["title"] == "owner/api-agent-repo"
    assert payload["citations"][0]["supports_result_id"] == (
        payload["retrieval"]["hits"][0]["result_id"]
    )
