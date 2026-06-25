import asyncio
import json
from pathlib import Path

import pytest

from collectors.personal_transcript_connector import PersonalTranscriptConnector
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
    config.set("sources.omi.enabled", True)
    return config


def test_personal_transcript_connector_preserves_and_queues_omi_export(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    export_path = tmp_path / "omi-export.json"
    export_path.write_text(
        json.dumps(
            {
                "sessions": [
                    {
                        "id": "session-1",
                        "title": "Local-first KB notes",
                        "device_id": "omi-device-1",
                        "started_at": "2026-04-04T10:00:00Z",
                        "summary": "Discussed preserving user-owned data.",
                        "segments": [
                            {
                                "timestamp": "2026-04-04T10:00:00Z",
                                "speaker": "Ada",
                                "text": "Keep raw exports immutable.",
                            },
                            {
                                "timestamp": "2026-04-04T10:01:00Z",
                                "speaker": "Ada",
                                "text": "Compile topic pages from normalized transcripts.",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    connector = PersonalTranscriptConnector(config, layout=layout, db=db)

    result = asyncio.run(connector.collect(export_paths=[export_path]))

    assert result.records[0].session_id == "session-1"
    raw_export_path = result.records[0].raw_export_path
    assert raw_export_path.exists()
    assert raw_export_path.parent == layout.raw_root / "personal_transcripts" / "omi"
    transcript_path = result.records[0].transcript_path
    assert transcript_path.exists()
    assert transcript_path.relative_to(layout.vault_root).as_posix() == (
        "transcripts/personal/omi_session-1.md"
    )

    entry = db.get_ingestion_entry("omi_transcript_session-1")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert payload["session_id"] == "session-1"
    assert payload["device_id"] == "omi-device-1"
    assert payload["speaker"] == "Ada"
    assert payload["custom_metadata"]["raw_payload_path"].startswith(
        "raw/personal_transcripts/omi/omi-export-"
    )

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert [item.artifact_type for item in processed] == ["transcript"]
    wiki_page = layout.wiki_root / "pages" / "transcript-omi-transcript-session-1.md"
    assert wiki_page.exists()
    wiki_text = wiki_page.read_text(encoding="utf-8")
    assert "Discussed preserving user-owned data." in wiki_text
    assert "Session ID: `session-1`" in wiki_text
    assert "Device ID: `omi-device-1`" in wiki_text
    assert "Speaker: Ada" in wiki_text


def test_personal_transcript_connector_fails_closed_without_exports(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    connector = PersonalTranscriptConnector(config, layout=layout, db=db)

    with pytest.raises(ValueError, match="requires export_paths or export_dirs"):
        asyncio.run(connector.collect())


def test_omi_agent_surface_requires_export_source(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    with pytest.raises(AgentSurfaceError, match="requires export_paths or export_dirs"):
        service.run_connector("omi", execute=True)
