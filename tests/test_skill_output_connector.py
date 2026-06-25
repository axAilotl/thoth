import asyncio
import json
from pathlib import Path

import pytest

from collectors.skill_output_connector import SkillOutputConnector
from core.agent_surface import AgentSurfaceError, AgentSurfaceService
from core.config import Config
from core.ingestion_runtime import KnowledgeArtifactRuntime
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


def test_skill_output_agent_surface_requires_output_source(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    with pytest.raises(AgentSurfaceError, match="requires output_paths or output_dirs"):
        service.run_connector("skill_outputs", execute=True)
