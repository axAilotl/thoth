import asyncio
import json
from pathlib import Path

import pytest

from collectors.skill_output_connector import SkillOutputConnector
from core.agent_surface import AgentSurfaceService
from core.connector_runners import ConnectorRunnerError
from core.config import Config
from core.connector_budgets import ConnectorBudgetError
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


def _config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("wiki.publish_source_pages", True)
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


def test_skill_output_connector_queues_enveloped_artifact_without_wiki_access(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = tmp_path / "last30days-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifacts": [
                    {
                        "source_name": "last30days-skill",
                        "artifact_type": "transcript",
                        "artifact_id": "last30days-2026-06",
                        "capabilities": ["transcript", "skill_output"],
                        "payload": {
                            "title": "Last 30 Days",
                            "summary": "A synthesized personal activity summary.",
                            "raw_transcript": "Collected notes from the last month.",
                            "processed_transcript": "Collected notes from the last month.",
                            "tags": ["last30days", "personal-knowledge"],
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    connector = SkillOutputConnector(config, layout=layout, db=db)

    result = asyncio.run(connector.collect(output_paths=[output_path]))

    assert result.records[0].artifact_id == "last30days-2026-06"
    raw_output_path = result.records[0].raw_output_path
    assert raw_output_path.exists()
    assert raw_output_path.parent == (
        layout.raw_root / "skill_outputs" / "last30days-skill"
    )
    assert not (layout.wiki_root / "pages").exists()

    entry = db.get_ingestion_entry("last30days-2026-06")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert payload["source_type"] == "last30days-skill"
    assert payload["custom_metadata"]["raw_payload_path"].startswith(
        "raw/skill_outputs/last30days-skill/last30days-output-"
    )

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert [item.artifact_type for item in processed] == ["transcript"]
    wiki_page = layout.wiki_root / "pages" / "transcript-last30days-2026-06.md"
    assert wiki_page.exists()
    assert "A synthesized personal activity summary." in wiki_page.read_text(
        encoding="utf-8"
    )


def test_skill_output_connector_rejects_direct_wiki_write_fields(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = tmp_path / "bad-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifact_type": "transcript",
                "artifact_id": "bad-skill-output",
                "payload": {
                    "title": "Bad",
                    "raw_transcript": "Should not queue.",
                    "wiki_path": "wiki/pages/bad.md",
                },
            }
        ),
        encoding="utf-8",
    )
    connector = SkillOutputConnector(config, layout=layout, db=db)

    with pytest.raises(ValueError, match="direct wiki write fields"):
        asyncio.run(connector.collect(output_paths=[output_path]))

    assert db.get_ingestion_entry("bad-skill-output") is None


def test_skill_output_connector_rejects_direct_wiki_path_values(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = tmp_path / "bad-path-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifact_type": "transcript",
                "artifact_id": "bad-skill-output-path",
                "payload": {
                    "title": "Bad",
                    "raw_transcript": "Should not queue.",
                    "custom_metadata": {
                        "destination": "wiki/pages/bad.md",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    connector = SkillOutputConnector(config, layout=layout, db=db)

    with pytest.raises(ValueError, match="direct wiki paths"):
        asyncio.run(connector.collect(output_paths=[output_path]))

    assert db.get_ingestion_entry("bad-skill-output-path") is None


def test_skill_output_connector_allows_prose_wikipedia_urls(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = tmp_path / "wikipedia-url-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifact_type": "transcript",
                "artifact_id": "skill-output-with-wikipedia-url",
                "payload": {
                    "title": "Citation Note",
                    "raw_transcript": (
                        "See https://en.wikipedia.org/wiki/Python_(programming_language) "
                        "for background."
                    ),
                },
            }
        ),
        encoding="utf-8",
    )
    connector = SkillOutputConnector(config, layout=layout, db=db)

    result = asyncio.run(connector.collect(output_paths=[output_path]))

    assert result.records[0].artifact_id == "skill-output-with-wikipedia-url"
    assert db.get_ingestion_entry("skill-output-with-wikipedia-url") is not None


def test_skill_output_connector_stops_when_transcript_chunk_budget_exceeded(
    tmp_path: Path,
):
    config = _config(tmp_path)
    config.set(
        "connectors.budgets.per_connector.skill_outputs.max_transcript_chunks_per_run",
        1,
    )
    config.set("connectors.budgets.per_connector.skill_outputs.transcript_chunk_chars", 10)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = tmp_path / "chunky-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifact_type": "transcript",
                "artifact_id": "chunky-skill-output",
                "payload": {
                    "title": "Chunky",
                    "raw_transcript": "x" * 25,
                },
            }
        ),
        encoding="utf-8",
    )
    connector = SkillOutputConnector(config, layout=layout, db=db)

    with pytest.raises(ConnectorBudgetError, match="max_transcript_chunks_per_run"):
        asyncio.run(connector.collect(output_paths=[output_path]))

    assert db.get_ingestion_entry("chunky-skill-output") is None


def test_skill_output_agent_surface_requires_output_source(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    with pytest.raises(ConnectorRunnerError, match="requires output_paths or output_dirs"):
        service.run_connector("skill_outputs", execute=True)


def _write_envelope(tmp_path: Path, envelope: dict, name: str = "envelope.json") -> Path:
    output_path = tmp_path / name
    output_path.write_text(json.dumps(envelope), encoding="utf-8")
    return output_path


def _collect_one(tmp_path: Path, envelope: dict):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = _write_envelope(tmp_path, envelope)
    connector = SkillOutputConnector(config, layout=layout, db=db)
    return connector, db, asyncio.run(connector.collect(output_paths=[output_path]))


def test_skill_output_connector_threads_envelope_ccf_into_queue_payload(tmp_path: Path):
    # Fold form (no explicit "payload" key): v1.1 fields must not fold in.
    _, db, result = _collect_one(
        tmp_path,
        {
            "artifact_type": "transcript",
            "artifact_id": "laned-skill-output",
            "lane": "transcript",
            "ccf": {"thoth.lane": "transcript", "acme.channel": "calls"},
            "title": "Laned",
            "raw_transcript": "Transcript body.",
        },
    )

    assert result.records[0].artifact_id == "laned-skill-output"
    entry = db.get_ingestion_entry("laned-skill-output")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert payload["title"] == "Laned"
    assert payload["normalized_metadata"]["ccf"] == {
        "lane": "transcript",
        "extensions": {"thoth.lane": "transcript", "acme.channel": "calls"},
    }
    # The v1.1 envelope fields do not leak into the queued payload body.
    assert "lane" not in payload
    assert "ccf" not in payload


def test_skill_output_connector_v1_envelope_has_no_ccf_metadata(tmp_path: Path):
    _, db, result = _collect_one(
        tmp_path,
        {
            "artifact_type": "web_clipper",
            "artifact_id": "v1-skill-output",
            "payload": {"url": "https://example.com/clip"},
        },
    )

    assert result.records[0].artifact_id == "v1-skill-output"
    entry = db.get_ingestion_entry("v1-skill-output")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert "ccf" not in payload["normalized_metadata"]


@pytest.mark.parametrize(
    ("envelope", "match"),
    [
        (
            {"artifact_type": "transcript", "lane": "hologram", "payload": {}},
            "unknown ccf lane 'hologram'",
        ),
        (
            {"artifact_type": "transcript", "lane": "  ", "payload": {}},
            "non-empty",
        ),
        (
            {"artifact_type": "transcript", "ccf": ["thoth.lane"], "payload": {}},
            "extensions must be an object",
        ),
        (
            {"artifact_type": "transcript", "ccf": {"lane": "paper"}, "payload": {}},
            "namespaced",
        ),
        (
            {
                "artifact_type": "transcript",
                "ccf": {"thoth.meta": {"nested": True}},
                "payload": {},
            },
            "scalar",
        ),
    ],
)
def test_skill_output_connector_rejects_invalid_envelope_ccf(
    tmp_path: Path, envelope, match
):
    envelope.setdefault("artifact_id", "bad-ccf-envelope")
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    output_path = _write_envelope(tmp_path, envelope)
    connector = SkillOutputConnector(config, layout=layout, db=db)

    with pytest.raises(ValueError, match=match):
        asyncio.run(connector.collect(output_paths=[output_path]))

    assert db.get_ingestion_entry("bad-ccf-envelope") is None
