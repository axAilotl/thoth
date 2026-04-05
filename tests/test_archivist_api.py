import json
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api


def _write_base_config(tmp_path: Path) -> None:
    base_config = {
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
    (tmp_path / "config.example.json").write_text(
        json.dumps(base_config),
        encoding="utf-8",
    )


def test_archivist_registry_endpoint_returns_topics_and_state(monkeypatch, tmp_path: Path):
    _write_base_config(tmp_path)
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

    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", tmp_path / "config.example.json")
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")
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
    _write_base_config(tmp_path)
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

    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", tmp_path / "config.example.json")
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")
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
    _write_base_config(tmp_path)
    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", tmp_path / "config.example.json")
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")
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
    _write_base_config(tmp_path)
    monkeypatch.setattr(thoth_api, "BASE_CONFIG_PATH", tmp_path / "config.example.json")
    monkeypatch.setattr(thoth_api, "LOCAL_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(thoth_api, "CONTROL_CONFIG_PATH", tmp_path / "control.json")

    with TestClient(thoth_api.app) as client:
        response = client.put(
            "/api/archivist/registry",
            json={"content": "topics: [\n"},
        )

    assert response.status_code == 400
    payload = response.json()
    assert "Failed to parse archivist topic registry" in payload["detail"]
