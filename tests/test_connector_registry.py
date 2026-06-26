import json
from pathlib import Path

import pytest

from core.config import Config
from core.connector_registry import ConnectorManifestError, load_connector_registry


def test_builtin_connector_registry_exposes_core_sources(tmp_path: Path):
    config = Config()
    config.data = {
        "sources": {
            "x_api": {"enabled": True},
            "web_clipper": {"enabled": False},
        }
    }

    registry = load_connector_registry(config, project_root=tmp_path)
    names = [manifest.name for manifest in registry.list()]

    assert names[:9] == [
        "x_api",
        "arxiv",
        "github",
        "huggingface",
        "web_clipper",
        "youtube",
        "omi",
        "skill_outputs",
        "pi_skills",
    ]
    assert registry.get("arxiv").artifact_types == ("paper",)
    assert registry.get("arxiv").inputs == ("remote_api:arxiv",)
    assert registry.get("arxiv").outputs == ("artifact_queue:paper",)
    assert registry.get("github").queue_capability is True
    assert registry.get("github").queue_behavior == "queues_artifacts"
    assert registry.get("skill_outputs").safety_mode == "queue_only"
    assert registry.get("x_api").is_enabled(config) is True
    assert registry.get("web_clipper").is_enabled(config) is False
    assert registry.get("omi").artifact_types == ("transcript",)
    assert registry.get("imported_markdown").artifact_types == ("markdown",)
    assert registry.get("manual_import").name == "imported_markdown"
    assert registry.get("personal_transcripts").name == "omi"
    assert registry.get("last30days-skill").name == "skill_outputs"
    assert registry.get("pi_skill").name == "pi_skills"


def test_plugin_connector_manifest_is_loaded_after_builtins(tmp_path: Path):
    plugin_dir = tmp_path / "plugins" / "meeting_notes"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "connector.json").write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(tmp_path / "plugins")]}}

    registry = load_connector_registry(config, project_root=tmp_path)
    manifest = registry.get("meeting_notes")

    assert [item.name for item in registry.list()][-1] == "meeting_notes"
    assert manifest.source_names == ("meeting_notes",)
    assert manifest.origin == str(plugin_dir / "connector.json")


def test_invalid_plugin_connector_manifest_fails_closed(tmp_path: Path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "broken.connector.json").write_text(
        json.dumps(
            {
                "name": "broken",
                "source_name": "broken",
                "artifact_types": ["paper"],
                "inputs": ["remote_api:broken"],
                "outputs": ["artifact_queue:paper"],
                "auth": [],
                "queue_behavior": "queues_artifacts",
                "safety_mode": "network_ingest_queue",
                "allowed_side_effects": [
                    "network_read",
                    "artifact_queue_write",
                ],
                "entrypoint": "collectors.broken:Broken",
            }
        ),
        encoding="utf-8",
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}

    with pytest.raises(ConnectorManifestError, match="queue_capability"):
        load_connector_registry(config, project_root=tmp_path)


def test_plugin_connector_manifest_requires_safety_metadata(tmp_path: Path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "missing-safety.connector.json").write_text(
        json.dumps(
            {
                "name": "missing_safety",
                "source_name": "missing_safety",
                "artifact_types": ["paper"],
                "inputs": ["remote_api:papers"],
                "outputs": ["artifact_queue:paper"],
                "auth": [],
                "queue_capability": True,
                "queue_behavior": "queues_artifacts",
                "allowed_side_effects": ["network_read", "artifact_queue_write"],
                "entrypoint": "collectors.missing_safety:MissingSafety",
            }
        ),
        encoding="utf-8",
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}

    with pytest.raises(ConnectorManifestError, match="safety_mode"):
        load_connector_registry(config, project_root=tmp_path)


def test_plugin_connector_manifest_rejects_wiki_write_side_effect(tmp_path: Path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "unsafe.connector.json").write_text(
        json.dumps(
            {
                "name": "unsafe",
                "source_name": "unsafe",
                "artifact_types": ["paper"],
                "inputs": ["local_files:unsafe"],
                "outputs": ["artifact_queue:paper"],
                "auth": [],
                "queue_capability": True,
                "queue_behavior": "queues_artifacts",
                "safety_mode": "local_ingest_queue",
                "allowed_side_effects": ["direct_wiki_write"],
                "entrypoint": "collectors.unsafe:Unsafe",
            }
        ),
        encoding="utf-8",
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}

    with pytest.raises(ConnectorManifestError, match="direct wiki writes"):
        load_connector_registry(config, project_root=tmp_path)


def test_plugin_connector_manifest_rejects_direct_wiki_outputs(tmp_path: Path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "unsafe-output.connector.json").write_text(
        json.dumps(
            {
                "name": "unsafe_output",
                "source_name": "unsafe_output",
                "artifact_types": ["paper"],
                "inputs": ["local_files:unsafe"],
                "outputs": ["artifact_queue:paper", "wiki/pages/unsafe.md"],
                "auth": [],
                "queue_capability": True,
                "queue_behavior": "queues_artifacts",
                "safety_mode": "local_ingest_queue",
                "allowed_side_effects": ["local_file_read", "artifact_queue_write"],
                "entrypoint": "collectors.unsafe:Unsafe",
            }
        ),
        encoding="utf-8",
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}

    with pytest.raises(ConnectorManifestError, match="direct wiki outputs"):
        load_connector_registry(config, project_root=tmp_path)


def test_config_example_exposes_all_builtin_connector_names():
    repo_root = Path(__file__).resolve().parents[1]
    config_data = json.loads((repo_root / "config.example.json").read_text(encoding="utf-8"))
    schema_data = json.loads((repo_root / "config.schema.json").read_text(encoding="utf-8"))
    source_config = config_data["sources"]
    source_schema = schema_data["properties"]["sources"]["properties"]
    registry = load_connector_registry(project_root=repo_root)

    for manifest in registry.list():
        namespace = manifest.config_namespace
        if not namespace or not namespace.startswith("sources."):
            continue
        source_key = namespace.split(".", 1)[1]
        assert source_key in source_config
        assert source_key in source_schema

    assert "research_graph" in config_data
    assert "research_graph" in schema_data["properties"]
