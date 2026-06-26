from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api


def _patch_background_tasks(monkeypatch):
    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "archivist_scheduler", noop_async)
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


def test_settings_api_returns_runtime_summary(monkeypatch, tmp_path: Path):
    _patch_background_tasks(monkeypatch)
    config_data = {
        "paths": {
            "vault_dir": str(tmp_path / "vault"),
            "system_dir": ".thoth_system",
            "cache_dir": "graphql_cache",
            "raw_dir": "raw",
            "library_dir": "library",
            "wiki_dir": "wiki",
            "archivist_topics_file": "archivist_topics.yaml",
        },
        "database": {
            "path": "meta.db",
        },
        "sources": {
            "pi_skills": {
                "enabled": True,
                "skills": [
                    {
                        "id": "knowledge-collation",
                        "artifact_types": ["transcript"],
                        "inputs": ["operator_prompt", "local_files:allowed_input_roots"],
                        "outputs": ["skill_output_envelopes", "artifact_queue:transcript"],
                        "auth": ["llm.providers.pi"],
                        "safety_mode": "no_tools_json",
                        "queue_behavior": "queues_artifacts",
                        "allowed_side_effects": [
                            "llm_api_call",
                            "local_file_read",
                            "local_file_write",
                            "artifact_queue_write",
                        ],
                    }
                ],
            },
            "web_clipper": {
                "enabled": True,
                "note_dirs": ["imports/notes"],
                "attachment_dirs": ["imports/assets"],
            }
        },
    }
    (tmp_path / "archivist_topics.yaml").write_text(
        """
version: 1
topics:
  - id: test-topic
    title: Test Topic
    output_path: pages/topic-test-topic.md
    include_roots:
      - tweets
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(thoth_api, "load_runtime_settings", lambda: config_data)
    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", tmp_path / "config.example.json")
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/settings")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime"]["layout"]["wiki_root"] == str(tmp_path / "wiki")
    assert payload["runtime"]["archivist"]["topics"] == ["test-topic"]
    assert payload["runtime"]["web_clipper"]["watch_dirs"] == [
        str(tmp_path / "vault" / "imports" / "notes"),
        str(tmp_path / "vault" / "imports" / "assets"),
    ]
    assert payload["runtime"]["groups"]["connectors"]["total"] == 9
    assert payload["runtime"]["groups"]["skills"]["total"] == 1
    assert payload["runtime"]["groups"]["storage"]["raw_root"] == (
        str(tmp_path / "vault" / "raw")
    )
    assert payload["runtime"]["groups"]["wiki"]["okf_target"] == "v0.1"
    assert payload["config_files"] == {
        "base": str(tmp_path / "config.example.json"),
        "local": str(tmp_path / "config.json"),
        "control": str(thoth_api.CONTROL_CONFIG_PATH),
    }
