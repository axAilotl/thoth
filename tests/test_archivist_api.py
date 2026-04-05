import json
import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import thoth_api


@pytest.fixture(autouse=True)
def patch_background_tasks(monkeypatch):
    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(
        thoth_api,
        "resolve_x_api_sync_config",
        lambda: {
            "enabled": False,
            "interval_hours": 8,
            "run_on_startup": False,
            "max_results": 100,
            "max_pages": None,
            "resume_from_checkpoint": True,
        },
    )


def _write_runtime_config(tmp_path: Path) -> None:
    runtime_config = {
        "paths": {
            "vault_dir": str(tmp_path / "vault"),
            "raw_dir": "raw",
            "library_dir": "library",
            "wiki_dir": "wiki",
            "system_dir": str(tmp_path / ".thoth_system"),
            "cache_dir": "graphql_cache",
            "archivist_topics_file": "topics/archivist_topics.yaml",
        },
        "database": {
            "path": "meta.db",
        },
    }
    (tmp_path / "config.json").write_text(
        json.dumps(runtime_config),
        encoding="utf-8",
    )


def test_archivist_registry_endpoint_returns_topics_and_state(monkeypatch, tmp_path: Path):
    _write_runtime_config(tmp_path)
    registry_path = tmp_path / "topics" / "archivist_topics.yaml"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        """
version: 1
defaults:
  cadence_hours: 6
topics:
  - id: companion-ai
    title: Companion AI Research
    output_path: pages/topic-companion-ai.md
    include_roots:
      - tweets
      - papers
    include_tags:
      - companion_ai
      - persona
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(thoth_api, "RUNTIME_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/archivist/registry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["registry_path"] == str(registry_path)
    assert "Companion AI Research" in payload["raw_text"]
    assert payload["defaults"]["cadence_hours"] == 6
    assert payload["topics"][0]["id"] == "companion-ai"
    assert payload["topics"][0]["state"]["last_run_at"] is None


def test_archivist_registry_endpoint_bootstraps_live_registry_from_example(
    monkeypatch, tmp_path: Path
):
    _write_runtime_config(tmp_path)
    example_path = tmp_path / "archivist_topics.example.yaml"
    example_path.write_text(
        """
version: 1
topics:
  - id: seeded-topic
    title: Seeded Topic
    output_path: pages/topic-seeded-topic.md
    include_roots:
      - tweets
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(thoth_api, "RUNTIME_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/archivist/registry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["seeded_from_example"] is True
    assert payload["topics"][0]["id"] == "seeded-topic"
    registry_path = tmp_path / "topics" / "archivist_topics.yaml"
    assert registry_path.exists()
    assert registry_path.read_text(encoding="utf-8") == example_path.read_text(encoding="utf-8")


def test_archivist_registry_save_and_force_cycle(monkeypatch, tmp_path: Path):
    _write_runtime_config(tmp_path)
    monkeypatch.setattr(thoth_api, "RUNTIME_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")

    content = """
version: 1
topics:
  - id: model-evals-and-benchmarks
    title: Model Evals and Benchmarks
    output_path: pages/topic-model-evals-and-benchmarks.md
    include_roots:
      - papers
    allow_manual_force: true
""".strip()

    with TestClient(thoth_api.app) as client:
        save_response = client.put(
            "/api/archivist/registry",
            json={"content": content},
        )
        assert save_response.status_code == 200
        save_payload = save_response.json()
        assert save_payload["status"] == "ok"
        assert save_payload["topics"][0]["id"] == "model-evals-and-benchmarks"

        force_response = client.post(
            "/api/archivist/topics/model-evals-and-benchmarks/force",
            json={"reason": "test"},
        )
        assert force_response.status_code == 200
        force_payload = force_response.json()
        assert force_payload["state"]["force_requested_at"] is not None
        assert force_payload["state"]["force_reason"] == "test"

        clear_response = client.delete("/api/archivist/topics/model-evals-and-benchmarks/force")
        assert clear_response.status_code == 200
        clear_payload = clear_response.json()
        assert clear_payload["state"]["force_requested_at"] is None

    registry_path = tmp_path / "topics" / "archivist_topics.yaml"
    assert registry_path.read_text(encoding="utf-8") == content


def test_archivist_registry_rejects_invalid_yaml(monkeypatch, tmp_path: Path):
    _write_runtime_config(tmp_path)
    monkeypatch.setattr(thoth_api, "RUNTIME_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")

    with TestClient(thoth_api.app) as client:
        response = client.put(
            "/api/archivist/registry",
            json={"content": "topics: [\n"},
        )

    assert response.status_code == 400
    payload = response.json()
    assert "Failed to parse archivist topic registry" in payload["detail"]


def test_archivist_topic_run_endpoint_executes_immediately(monkeypatch, tmp_path: Path):
    _write_runtime_config(tmp_path)
    monkeypatch.setattr(thoth_api, "RUNTIME_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")

    async def fake_run_archivist_compilation(**kwargs):
        assert kwargs["topic_ids"] == ["companion-ai"]
        assert kwargs["force"] is True
        assert kwargs["dry_run"] is False
        assert kwargs["limit"] == 1
        return {
            "status": "ok",
            "force": True,
            "dry_run": False,
            "limit": 1,
            "topic_ids": ["companion-ai"],
            "results": [
                {
                    "topic_id": "companion-ai",
                    "status": "compiled",
                    "reason": "forced",
                    "page_path": str(tmp_path / "wiki" / "pages" / "topic-companion-ai.md"),
                    "candidate_count": 3,
                    "source_paths": ["tweets/example.md"],
                    "model_provider": "openrouter",
                    "model": "archivist",
                }
            ],
            "summary": {
                "compiled": 1,
                "skipped": 0,
                "dry_run": 0,
                "total": 1,
            },
        }

    monkeypatch.setattr(thoth_api, "run_archivist_compilation", fake_run_archivist_compilation)

    with TestClient(thoth_api.app) as client:
        response = client.post(
            "/api/archivist/topics/companion-ai/run",
            json={"force": True},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["summary"]["compiled"] == 1
    assert payload["results"][0]["status"] == "compiled"


@pytest.mark.anyio
async def test_archivist_scheduler_runs_due_topics(monkeypatch):
    class FakeStateDB:
        def __init__(self):
            self.payloads = {}

        def get_automation_state(self, state_key):
            return self.payloads.get(state_key)

        def upsert_automation_state(self, state_key, payload):
            self.payloads[state_key] = dict(payload)

    fake_db = FakeStateDB()
    calls = []

    monkeypatch.setattr(thoth_api, "get_metadata_db", lambda: fake_db)
    monkeypatch.setattr(
        thoth_api,
        "resolve_archivist_sync_config",
        lambda: {
            "enabled": True,
            "interval_hours": 12,
            "run_on_startup": True,
        },
    )

    async def fake_run_archivist_compilation(**kwargs):
        calls.append(kwargs)
        thoth_api._shutdown_event.set()
        return {"status": "ok", "summary": {"compiled": 1, "skipped": 0, "dry_run": 0, "total": 1}}

    monkeypatch.setattr(thoth_api, "run_archivist_compilation", fake_run_archivist_compilation)

    thoth_api._shutdown_event = asyncio.Event()
    await asyncio.wait_for(thoth_api.archivist_scheduler(), timeout=1.0)

    assert calls == [{}]
