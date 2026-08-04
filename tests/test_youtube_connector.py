import asyncio
from pathlib import Path

from collectors.youtube_connector import YouTubeConnector
from core.config import Config
from core.connector_budgets import ConnectorBudgetError
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from processors.youtube_processor import YouTubeProcessor, YouTubeVideo


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
    config.set("sources.youtube.enabled", True)
    config.set("youtube.enable_transcripts", True)
    config.set("youtube.enable_embeddings", False)
    return config


class FakeYouTubeProcessor:
    def __init__(self, transcripts_dir: Path):
        self.transcripts_dir = transcripts_dir
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)
        self.api_timeout = 1.0
        self.session = None

    def extract_video_id(self, url: str) -> str | None:
        if "youtu.be/" in url:
            return url.rsplit("/", 1)[-1].split("?", 1)[0]
        if "v=" in url:
            return url.split("v=", 1)[-1].split("&", 1)[0]
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
        transcript_path = self.transcripts_dir / f"youtube_{video_id}_Fixture.md"
        transcript_path.write_text(
            "# Fixture Video\n\n## Transcript\nProcessed transcript text.\n",
            encoding="utf-8",
        )
        return (
            YouTubeVideo(
                video_id=video_id,
                title="Fixture Video",
                description="A fixture video.",
                published_at="2026-04-04T00:00:00Z",
                channel_id="channel-1",
                channel_title="Fixture Channel",
                duration="PT1M",
                view_count=42,
                thumbnail_url="https://example.test/thumb.jpg",
                transcript="[00:00] Raw transcript text.",
                formatted_transcript="Processed transcript text.",
                transcript_summary="Fixture summary.",
                transcript_tags="fixtures, videos",
            ),
            {
                "metadata_seconds": 0.0,
                "transcript_seconds": 0.0,
                "transcript_attempts": 1,
                "transcript_completed": 1,
                "transcript_failed": 0,
            },
        )


def test_youtube_connector_queues_raw_video_and_transcript_artifacts(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    export_path = tmp_path / "watch-later.html"
    export_path.write_text(
        '<a href="https://www.youtube.com/watch?v=abc123">Fixture</a>',
        encoding="utf-8",
    )
    processor = FakeYouTubeProcessor(layout.vault_root / "transcripts")
    connector = YouTubeConnector(config, layout=layout, db=db, processor=processor)

    result = asyncio.run(
        connector.collect(
            urls=["https://youtu.be/abc123"],
            export_paths=[export_path],
        )
    )

    assert result.records[0].video_id == "abc123"
    raw_path = layout.raw_root / "youtube" / "abc123.json"
    assert raw_path.exists()
    assert db.get_ingestion_entry("yt_video_abc123") is not None
    assert db.get_ingestion_entry("yt_transcript_abc123") is not None

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    processed = asyncio.run(runtime.process_pending_ingestions_once())

    assert {entry.artifact_type for entry in processed} == {"video", "transcript"}
    transcript_page = layout.wiki_root / "pages" / "transcript-yt-transcript-abc123.md"
    assert transcript_page.exists()
    page_text = transcript_page.read_text(encoding="utf-8")
    assert "Fixture summary." in page_text
    assert "transcripts/youtube_abc123_Fixture.md" in page_text


def test_youtube_connector_accepts_playlist_urls_via_adapter(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    processor = FakeYouTubeProcessor(layout.vault_root / "transcripts")
    connector = YouTubeConnector(config, layout=layout, db=db, processor=processor)

    async def fake_playlist_urls(_playlist_url: str) -> list[str]:
        return ["https://youtu.be/pl123"]

    connector._urls_from_playlist = fake_playlist_urls

    result = asyncio.run(
        connector.collect(
            playlist_urls=["https://www.youtube.com/playlist?list=PL123"],
        )
    )

    assert result.records[0].video_id == "pl123"
    assert db.get_ingestion_entry("yt_video_pl123") is not None


def test_youtube_connector_skips_one_video_when_budget_exceeded(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    processor = FakeYouTubeProcessor(layout.vault_root / "transcripts")
    connector = YouTubeConnector(config, layout=layout, db=db, processor=processor)
    original_collect_video = connector._collect_video

    async def collect_or_budget_error(video_id, **kwargs):
        if video_id == "overbudget":
            raise ConnectorBudgetError(
                "video exceeded transcript budget",
                connector_name="youtube",
                field="max_output_tokens_per_run",
                subject=video_id,
            )
        return await original_collect_video(video_id, **kwargs)

    connector._collect_video = collect_or_budget_error

    result = asyncio.run(
        connector.collect(
            urls=[
                "https://youtu.be/overbudget",
                "https://youtu.be/afterbudget",
            ]
        )
    )

    assert [record.video_id for record in result.records] == ["afterbudget"]
    assert result.skipped_urls == ("https://youtu.be/overbudget",)
    assert result.errors[0]["video_id"] == "overbudget"
    assert result.errors[0]["error_type"] == "ConnectorBudgetError"
    assert db.get_ingestion_entry("yt_video_afterbudget") is not None
    assert db.get_ingestion_entry("yt_video_overbudget") is None


def test_youtube_connector_video_archival_is_config_gated(tmp_path: Path, monkeypatch):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    processor = FakeYouTubeProcessor(layout.vault_root / "transcripts")
    connector = YouTubeConnector(config, layout=layout, db=db, processor=processor)

    def archive_should_not_run(_source_url: str, _video_id: str) -> Path:
        raise AssertionError("archive should not run without config or CLI opt-in")

    monkeypatch.setattr(connector, "_archive_video", archive_should_not_run)

    result = asyncio.run(connector.collect(urls=["https://youtu.be/noarchive"]))

    assert result.records[0].archive_path is None

    archive_root = layout.library_root / "youtube" / "videos"

    def fake_archive(_source_url: str, video_id: str) -> Path:
        archive_root.mkdir(parents=True, exist_ok=True)
        archive_path = archive_root / f"{video_id}.mp4"
        archive_path.write_text("video", encoding="utf-8")
        return archive_path

    monkeypatch.setattr(connector, "_archive_video", fake_archive)
    config.set("sources.youtube.archive_video", True)

    result = asyncio.run(connector.collect(urls=["https://youtu.be/archive1"]))

    assert result.records[0].archive_path == archive_root / "archive1.mp4"
    video_entry = db.get_ingestion_entry("yt_video_archive1")
    assert video_entry is not None
    assert "library/youtube/videos/archive1.mp4" in video_entry.payload_json


def test_youtube_processor_metadata_fallback_uses_blank_timestamp(
    tmp_path: Path,
    monkeypatch,
):
    processor = YouTubeProcessor(vault_path=str(tmp_path / "vault"))
    processor.transcript_llm_processor = None

    async def no_metadata(_video_id: str):
        return None

    async def transcript(_video_id: str):
        return "[00:00] Fallback transcript text."

    monkeypatch.setattr(processor, "get_video_info", no_metadata)
    monkeypatch.setattr(processor, "get_video_transcript", transcript)

    video, _metrics = asyncio.run(
        processor.process_video(
            "fallback1",
            resume_metadata=False,
            resume_transcripts=False,
            source_label="unit test",
        )
    )

    assert video is not None
    assert video.published_at == ""
    assert list((tmp_path / "vault" / "transcripts").glob("youtube_fallback1_*.md"))
