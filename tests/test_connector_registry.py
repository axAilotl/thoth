import json
from dataclasses import asdict
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

    assert names == [
        "arxiv",
        "corpus_index",
        "github",
        "huggingface",
        "imported_markdown",
        "inbox",
        "omi",
        "pi_skills",
        "skill_outputs",
        "web_clipper",
        "wiki_reconcile",
        "x_api",
        "youtube",
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
    assert registry.get("manual_import").to_dict(config=config)["runner"] == (
        "imported_markdown"
    )
    assert registry.get("personal_transcripts").name == "omi"
    assert registry.get("last30days-skill").name == "skill_outputs"
    assert registry.get("pi_skill").name == "pi_skills"
    # The capture source's class-level collector names resolve to the
    # manifests whose ccf blocks lane the dual-write mirror's artifacts.
    assert registry.get("skill_output_connector").name == "skill_outputs"
    assert registry.get("skill_output_connector").ccf.lane == "mixed"
    assert registry.get("pi_skill_connector").name == "pi_skills"
    assert registry.get("pi_skill_connector").ccf.lane == "mixed"


def test_builtin_manifests_are_colocated_with_collectors(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
    manifest_paths = sorted((repo_root / "collectors").glob("*.connector.json"))

    assert len(manifest_paths) == 13

    registry = load_connector_registry(project_root=tmp_path)
    builtins = [manifest for manifest in registry.list() if manifest.origin == "builtin"]

    assert [manifest.name for manifest in builtins] == [
        json.loads(path.read_text(encoding="utf-8"))["name"]
        for path in manifest_paths
    ]


def test_builtin_and_plugin_manifests_share_discovery_parity(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
    payload = json.loads(
        (repo_root / "collectors" / "arxiv.connector.json").read_text(encoding="utf-8")
    )
    payload["name"] = "arxiv_copy"
    payload["source_name"] = "arxiv_copy"
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    plugin_path = plugin_dir / "arxiv_copy.connector.json"
    plugin_path.write_text(json.dumps(payload), encoding="utf-8")
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}

    registry = load_connector_registry(config, project_root=tmp_path)
    builtin = registry.get("arxiv")
    plugin = registry.get("arxiv_copy")

    assert builtin.origin == "builtin"
    assert plugin.origin == str(plugin_path)
    assert asdict(plugin) == {
        **asdict(builtin),
        "name": "arxiv_copy",
        "source_name": "arxiv_copy",
        "origin": str(plugin_path),
    }


def test_plugin_connector_manifest_cannot_shadow_builtin_name(tmp_path: Path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "arxiv.connector.json").write_text(
        json.dumps(
            {
                "name": "arxiv",
                "source_name": "arxiv",
                "artifact_types": ["paper"],
                "inputs": ["remote_api:arxiv"],
                "outputs": ["artifact_queue:paper"],
                "auth": [],
                "queue_capability": True,
                "queue_behavior": "queues_artifacts",
                "safety_mode": "network_ingest_queue",
                "allowed_side_effects": ["network_read", "artifact_queue_write"],
                "entrypoint": "collectors.arxiv_collector:ArXivCollector",
            }
        ),
        encoding="utf-8",
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}

    with pytest.raises(ConnectorManifestError, match="duplicate connector name 'arxiv'"):
        load_connector_registry(config, project_root=tmp_path)


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


# ---------------------------------------------------------------------------
# Optional manifest ccf block (lane declaration for the dual-write mirror)
# ---------------------------------------------------------------------------


def test_builtin_manifests_declare_ccf_lanes(tmp_path: Path):
    registry = load_connector_registry(project_root=tmp_path)

    lanes = {manifest.name: manifest.ccf.lane for manifest in registry.list()}
    assert lanes == {
        "arxiv": "paper",
        "corpus_index": "mixed",
        "github": "repository",
        "huggingface": "repository",
        "imported_markdown": "markdown",
        "inbox": "markdown",
        "omi": "transcript",
        "pi_skills": "mixed",
        "skill_outputs": "mixed",
        "web_clipper": "web_clipper",
        "wiki_reconcile": "markdown",
        "x_api": "tweet",
        "youtube": "video",
    }
    for manifest in registry.list():
        assert manifest.ccf.artifact_role == ({"corpus_index": "derived_index", "wiki_reconcile": "wiki_revision"}.get(manifest.name, "raw_capture"))
        assert manifest.ccf.extensions == {}

    as_dict = registry.get("arxiv").to_dict()["ccf"]
    assert as_dict == {
        "lane": "paper",
        "artifact_role": "raw_capture",
        "extensions": {},
    }


def _write_ccf_plugin(tmp_path: Path, ccf_block) -> Config:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(exist_ok=True)
    payload = {
        "name": "lane_plugin",
        "source_name": "lane_plugin",
        "artifact_types": ["paper"],
        "inputs": ["local_files:lane_plugin"],
        "outputs": ["artifact_queue:paper"],
        "auth": [],
        "queue_capability": True,
        "queue_behavior": "queues_artifacts",
        "safety_mode": "local_ingest_queue",
        "allowed_side_effects": ["local_file_read", "artifact_queue_write"],
        "entrypoint": "collectors.personal.lane_plugin:LanePlugin",
    }
    if ccf_block is not ...:
        payload["ccf"] = ccf_block
    (plugin_dir / "lane_plugin.connector.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    config = Config()
    config.data = {"connectors": {"plugin_dirs": [str(plugin_dir)]}}
    return config


def test_plugin_manifest_without_ccf_block_loads_with_none(tmp_path: Path):
    config = _write_ccf_plugin(tmp_path, ...)

    registry = load_connector_registry(config, project_root=tmp_path)

    assert registry.get("lane_plugin").ccf is None
    assert registry.get("lane_plugin").to_dict()["ccf"] is None


def test_plugin_manifest_ccf_block_round_trips(tmp_path: Path):
    config = _write_ccf_plugin(
        tmp_path,
        {
            "lane": "transcript",
            "artifact_role": "raw_capture",
            "extensions": {"thoth.lane": "transcript", "acme.channel": "calls"},
        },
    )

    registry = load_connector_registry(config, project_root=tmp_path)
    block = registry.get("lane_plugin").ccf

    assert block.lane == "transcript"
    assert block.artifact_role == "raw_capture"
    assert block.extensions == {"thoth.lane": "transcript", "acme.channel": "calls"}
    assert block.to_dict()["extensions"]["acme.channel"] == "calls"


@pytest.mark.parametrize(
    ("ccf_block", "match"),
    [
        ("paper", "must be an object"),
        ({"artifact_role": "raw_capture"}, "requires a non-empty lane"),
        (
            {"lane": "  ", "artifact_role": "raw_capture"},
            "requires a non-empty lane",
        ),
        (
            {"lane": "hologram", "artifact_role": "raw_capture"},
            "unknown ccf lane 'hologram'",
        ),
        ({"lane": "paper"}, "artifact_role"),
        ({"lane": "paper", "artifact_role": ""}, "artifact_role"),
        ({"lane": "paper", "artifact_role": "Raw Capture!"}, "artifact_role"),
        ({"lane": "paper", "artifact_role": "raw_capture", "role": "x"}, "unknown fields"),
        (
            {"lane": "paper", "artifact_role": "raw_capture", "extensions": []},
            "extensions must be an object",
        ),
        (
            {
                "lane": "paper",
                "artifact_role": "raw_capture",
                "extensions": {"lane": "paper"},
            },
            "namespaced",
        ),
        (
            {
                "lane": "paper",
                "artifact_role": "raw_capture",
                "extensions": {"thoth.meta": {"nested": True}},
            },
            "scalar",
        ),
    ],
)
def test_plugin_manifest_ccf_block_fails_closed(tmp_path: Path, ccf_block, match):
    config = _write_ccf_plugin(tmp_path, ccf_block)

    with pytest.raises(ConnectorManifestError, match=match) as excinfo:
        load_connector_registry(config, project_root=tmp_path)

    # The error names the manifest origin so operators can find the file.
    assert "lane_plugin.connector.json" in str(excinfo.value)
