import asyncio
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from collectors.imported_markdown_connector import ImportedMarkdownConnector
from collectors.personal_transcript_connector import PersonalTranscriptConnector
from collectors.skill_output_connector import SkillOutputConnector
from collectors.web_clipper_collector import WebClipperCollector
from collectors.web_clipper_parser import parse_web_clipper_markdown
from collectors.youtube_connector import YouTubeConnector
from core.config import Config
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.wiki_updater import CompiledWikiUpdater
from processors.youtube_processor import YouTubeVideo


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "golden_connectors"
EXPECTED_SOURCE_CLASSES = {
    "omi",
    "youtube",
    "github",
    "huggingface",
    "arxiv",
    "web_clip",
    "pi_skill_output",
    "imported_markdown",
}


def _load_manifest() -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / "manifest.json").read_text(encoding="utf-8"))


def _fixture_text(relative_path: str) -> str:
    return (FIXTURE_ROOT / relative_path).read_text(encoding="utf-8")


def _fixture_json(relative_path: str) -> dict[str, Any]:
    return json.loads(_fixture_text(relative_path))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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
    config.set("sources.youtube.enabled", True)
    config.set("youtube.enable_transcripts", True)
    config.set("youtube.enable_embeddings", False)
    config.set("sources.skill_outputs.enabled", True)
    config.set("sources.web_clipper.note_dirs", ["Clippings"])
    config.set("sources.web_clipper.attachment_dirs", ["clipper-assets"])
    config.set("llm.tasks.summary.enabled", False)
    return config


def _runtime(tmp_path: Path) -> tuple[Config, Any, MetadataDB]:
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    db = MetadataDB(str(layout.database_path))
    return config, layout, db


class FixtureYouTubeProcessor:
    def __init__(self, transcripts_dir: Path):
        self.transcripts_dir = transcripts_dir
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)

    def extract_video_id(self, url: str) -> str | None:
        if "v=" in url:
            return url.split("v=", 1)[-1].split("&", 1)[0]
        if "youtu.be/" in url:
            return url.rsplit("/", 1)[-1].split("?", 1)[0]
        return None

    def find_existing_transcript_files(self, video_id: str) -> list[Path]:
        return sorted(self.transcripts_dir.glob(f"youtube_{video_id}_*.md"))

    async def process_video(
        self,
        video_id: str,
        resume_metadata: bool = True,
        resume_transcripts: bool = True,
        source_label: str | None = None,
    ):
        transcript_path = self.transcripts_dir / f"youtube_{video_id}_Golden.md"
        transcript_path.write_text(
            "# Golden connector video\n\n## Transcript\nGolden transcript text.\n",
            encoding="utf-8",
        )
        return (
            YouTubeVideo(
                video_id=video_id,
                title="Golden connector video",
                description="Small deterministic YouTube fixture.",
                published_at="2026-04-04T00:00:00Z",
                channel_id="golden-channel",
                channel_title="Golden Fixtures",
                duration="PT1M",
                view_count=8,
                thumbnail_url="https://example.com/golden-thumb.jpg",
                transcript="[00:00] Golden raw transcript text.",
                formatted_transcript="Golden transcript text.",
                transcript_summary="Golden YouTube transcript summary.",
                transcript_tags="youtube, fixtures",
            ),
            {
                "metadata_seconds": 0.0,
                "transcript_seconds": 0.0,
                "transcript_attempts": 1,
                "transcript_completed": 1,
                "transcript_failed": 0,
            },
        )


def test_golden_connector_manifest_inventory_is_stable_and_local():
    manifest = _load_manifest()

    assert manifest["schema_version"] == 1
    assert manifest["event_assertions"]["status"] == "active"

    fixtures = manifest["fixtures"]
    assert {item["source_class"] for item in fixtures} == EXPECTED_SOURCE_CLASSES
    assert len({item["id"] for item in fixtures}) == len(fixtures)

    for item in fixtures:
        relative_path = item["path"]
        assert not Path(relative_path).is_absolute()
        fixture_path = FIXTURE_ROOT / relative_path
        assert fixture_path.exists()
        assert _sha256(fixture_path) == item["sha256"]
        assert fixture_path.stat().st_size < 4096
        expected = item["expected"]
        assert expected["artifact_id"]
        assert expected["native_id"]
        assert "wiki_supported" in expected

    imported_markdown = next(
        item for item in fixtures if item["source_class"] == "imported_markdown"
    )
    text = _fixture_text(imported_markdown["path"])
    assert "Imported Markdown Golden Note" in text
    assert imported_markdown["expected"]["wiki_supported"] is False


def test_golden_omi_fixture_preserves_raw_queues_and_compiles_wiki(tmp_path: Path):
    config, layout, db = _runtime(tmp_path)
    export_path = FIXTURE_ROOT / "omi" / "export.json"
    connector = PersonalTranscriptConnector(config, layout=layout, db=db)

    result = asyncio.run(connector.collect(export_paths=[export_path]))

    assert len(result.records) == 1
    record = result.records[0]
    assert record.session_id == "golden-omi-session"
    assert record.raw_export_path.read_bytes() == export_path.read_bytes()

    entry = db.get_ingestion_entry("omi_transcript_golden-omi-session")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert payload["session_id"] == "golden-omi-session"
    assert payload["device_id"] == "omi-golden-device"
    assert payload["custom_metadata"]["raw_payload_path"].startswith(
        "raw/personal_transcripts/omi/export-"
    )

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    artifact = runtime.materialize_artifact(entry)
    canonical = artifact.canonical_record()
    assert canonical["source_identity"]["source_name"] == "omi"
    assert canonical["raw_payload"]["path"].startswith(
        "raw/personal_transcripts/omi/export-"
    )

    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert [item.artifact_type for item in processed] == ["transcript"]
    wiki_page = layout.wiki_root / "pages" / "transcript-omi-transcript-golden-omi-session.md"
    assert wiki_page.exists()
    wiki_text = wiki_page.read_text(encoding="utf-8")
    assert "Discussed fixture coverage for connector regressions." in wiki_text
    assert "Session ID: `golden-omi-session`" in wiki_text


def test_golden_youtube_fixture_preserves_raw_queues_and_compiles_wiki(tmp_path: Path):
    config, layout, db = _runtime(tmp_path)
    processor = FixtureYouTubeProcessor(layout.vault_root / "transcripts")
    connector = YouTubeConnector(config, layout=layout, db=db, processor=processor)

    result = asyncio.run(
        connector.collect(
            export_paths=[FIXTURE_ROOT / "youtube" / "watch_later.html"],
        )
    )

    assert len(result.records) == 1
    record = result.records[0]
    assert record.video_id == "goldenYt01"
    raw_payload = json.loads(record.raw_payload_path.read_text(encoding="utf-8"))
    assert raw_payload["source_url"] == "https://www.youtube.com/watch?v=goldenYt01"
    assert raw_payload["video"]["title"] == "Golden connector video"

    video_entry = db.get_ingestion_entry("yt_video_goldenYt01")
    transcript_entry = db.get_ingestion_entry("yt_transcript_goldenYt01")
    assert video_entry is not None
    assert transcript_entry is not None

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    transcript_artifact = runtime.materialize_artifact(transcript_entry)
    canonical = transcript_artifact.canonical_record()
    assert canonical["source_identity"]["source_name"] == "youtube"
    assert canonical["raw_payload"]["path"] == "raw/youtube/goldenYt01.json"
    assert canonical["derived_outputs"] == [
        {
            "output_type": "markdown",
            "path": "transcripts/youtube_goldenYt01_Golden.md",
        }
    ]

    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert {item.artifact_type for item in processed} == {"video", "transcript"}
    transcript_page = layout.wiki_root / "pages" / "transcript-yt-transcript-goldenyt01.md"
    video_page = layout.wiki_root / "pages" / "video-goldenyt01.md"
    assert transcript_page.exists()
    assert video_page.exists()
    assert "Golden YouTube transcript summary." in transcript_page.read_text(
        encoding="utf-8"
    )


def test_golden_web_clip_fixture_preserves_raw_queues_and_compiles_wiki(tmp_path: Path):
    config, layout, db = _runtime(tmp_path)
    note_dir = layout.vault_root / "Clippings"
    attachment_dir = layout.vault_root / "clipper-assets"
    note_dir.mkdir(parents=True, exist_ok=True)
    attachment_dir.mkdir(parents=True, exist_ok=True)
    note_path = note_dir / "capture_note.md"
    shutil.copy2(FIXTURE_ROOT / "web_clips" / "capture_note.md", note_path)
    connector = WebClipperCollector(config, layout=layout, db=db)

    discovered = connector.collect()

    assert len(discovered) == 1
    artifact = discovered[0].artifact
    assert artifact is not None
    assert artifact.raw_content == note_path.read_text(encoding="utf-8")
    assert artifact.title == "Golden Web Clip"
    assert artifact.source_url == "https://example.com/golden-clip"

    entry = db.get_ingestion_entry("webclip:Clippings/capture_note.md")
    assert entry is not None
    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    canonical = runtime.materialize_artifact(entry).canonical_record()
    assert canonical["source_identity"]["source_name"] == "web_clipper"
    assert canonical["raw_payload"]["path"] == str(note_path)
    assert canonical["provenance"]["raw_payload"]["path"] == str(note_path)

    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert [item.artifact_type for item in processed] == ["web_clipper"]
    wiki_page = layout.wiki_root / "pages" / "clip-golden-web-clip.md"
    assert wiki_page.exists()
    wiki_text = wiki_page.read_text(encoding="utf-8")
    assert "Golden Web Clip" in wiki_text
    assert "Clippings/capture_note.md" in wiki_text


def test_golden_pi_skill_fixture_preserves_raw_queues_and_compiles_wiki(tmp_path: Path):
    config, layout, db = _runtime(tmp_path)
    connector = SkillOutputConnector(config, layout=layout, db=db)
    output_path = FIXTURE_ROOT / "pi_skill_outputs" / "output.json"

    result = asyncio.run(connector.collect(output_paths=[output_path]))

    assert len(result.records) == 1
    record = result.records[0]
    assert record.artifact_id == "pi-golden-note"
    assert record.raw_output_path.read_bytes() == output_path.read_bytes()

    entry = db.get_ingestion_entry("pi-golden-note")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert payload["source_type"] == "pi_skill:golden-notes"
    assert payload["custom_metadata"]["raw_payload_path"].startswith(
        "raw/skill_outputs/pi-skill-golden-notes/output-"
    )

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    canonical = runtime.materialize_artifact(entry).canonical_record()
    assert canonical["source_identity"]["source_name"] == "pi_skill:golden-notes"
    assert canonical["raw_payload"]["content_key"] == "raw_content"
    assert "path" not in canonical["raw_payload"]

    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert [item.artifact_type for item in processed] == ["transcript"]
    wiki_page = layout.wiki_root / "pages" / "transcript-pi-golden-note.md"
    assert wiki_page.exists()
    assert "A deterministic Pi skill output fixture." in wiki_page.read_text(
        encoding="utf-8"
    )


def test_golden_repository_and_arxiv_fixtures_materialize_with_provenance(
    tmp_path: Path,
):
    config, layout, db = _runtime(tmp_path)
    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    updater = CompiledWikiUpdater(config, layout=layout, db=db)

    cases = [
        (
            "github",
            "repository",
            "github/repository.json",
            {
                "id": "thoth-fixtures/golden-repo",
                "repo_name": "thoth-fixtures/golden-repo",
                "source_type": "github",
                "stars": 7,
                "raw_payload_path": "raw/github/repository.json",
            },
            "repo-thoth-fixtures-golden-repo",
        ),
        (
            "huggingface",
            "repository",
            "huggingface/repository.json",
            {
                "repo_name": "thoth-fixtures/golden-model",
                "source_type": "huggingface",
                "stars": 3,
                "topics": ["text-generation", "fixtures"],
                "raw_payload_path": "raw/huggingface/repository.json",
            },
            "repo-thoth-fixtures-golden-model",
        ),
        (
            "arxiv",
            "paper",
            "arxiv/paper.json",
            {
                "source_type": "arxiv",
                "raw_payload_path": "raw/arxiv/paper.json",
            },
            "paper-2604-00001",
        ),
    ]

    for source, artifact_type, relative_path, overrides, expected_slug in cases:
        payload = _fixture_json(relative_path)
        payload.update(overrides)
        payload["raw_content"] = _fixture_text(relative_path)
        entry = IngestionQueueEntry(
            artifact_id=payload["id"],
            artifact_type=artifact_type,
            source=source,
            payload_json=json.dumps(payload, ensure_ascii=False),
            created_at="2026-04-04T00:00:00Z",
            capabilities_json=json.dumps([artifact_type, "fixture"]),
        )

        artifact = runtime.materialize_artifact(entry)
        canonical = artifact.canonical_record()
        assert canonical["source_identity"]["source_name"] == source
        assert canonical["source_identity"]["native_id"] == payload["id"]
        assert canonical["raw_payload"]["path"] == overrides["raw_payload_path"]
        assert canonical["normalized_metadata"]["queue_source"] == source

        result = updater.update_from_artifact(
            artifact,
            dispatch_details={"fixture": True},
        )

        assert result.slug == expected_slug
        assert result.page_path.exists()
        page_text = result.page_path.read_text(encoding="utf-8")
        assert payload["id"] in page_text
        assert f"Source: `{source}`" in page_text


def test_imported_markdown_fixture_preserves_raw_and_queues_capture_only(
    tmp_path: Path,
):
    config, layout, db = _runtime(tmp_path)
    path = FIXTURE_ROOT / "imported_markdown" / "manual_note.md"
    text = path.read_text(encoding="utf-8")
    connector = ImportedMarkdownConnector(config, layout=layout, db=db)

    parsed = parse_web_clipper_markdown(text, source_path=path)
    result = asyncio.run(connector.collect(import_paths=[path]))

    assert parsed.raw_content == text
    assert parsed.title == "Imported Markdown Golden Note"
    assert parsed.frontmatter["source"] == "manual_import"
    assert len(result.records) == 1
    record = result.records[0]
    assert record.artifact_id.startswith("manual-imported-markdown-golden-note-")
    assert len(record.artifact_id.rsplit("-", 1)[-1]) == 12
    assert record.raw_markdown_path.read_bytes() == path.read_bytes()

    entry = db.get_ingestion_entry(record.artifact_id)
    assert entry is not None
    assert entry.artifact_type == "markdown"
    assert entry.status == "pending"
    payload = json.loads(entry.payload_json)
    assert payload["title"] == "Imported Markdown Golden Note"
    assert payload["raw_content"] == text
    assert payload["custom_metadata"]["raw_payload_path"].startswith(
        "raw/imported_markdown/manual-import/manual-note-"
    )

    processed = asyncio.run(
        KnowledgeArtifactRuntime(config, layout=layout, db=db)
        .process_pending_ingestions_once()
    )
    processed_entry = db.get_ingestion_entry(record.artifact_id)
    assert [item.artifact_type for item in processed] == ["markdown"]
    assert processed[0].status == "skipped"
    assert processed_entry.status == "processed"

    pages_dir = layout.wiki_root / "pages"
    assert not pages_dir.exists() or not list(pages_dir.glob("*imported-markdown*"))
