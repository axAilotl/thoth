import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.config import config
from core.data_models import Tweet
from core.llm_cache import LLMCache
from core.metadata_db import MetadataDB
from processors.pipeline_processor import PipelineProcessor
from processors.transcript_llm_processor import TranscriptLLMProcessor


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def _configure_runtime_paths(tmp_path: Path) -> None:
    vault_root = tmp_path / "vault"
    system_root = tmp_path / ".thoth_system"
    config.data = {}
    config.set("paths.vault_dir", str(vault_root))
    config.set("paths.system_dir", str(system_root))
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", str(tmp_path / "wiki"))
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("database.enabled", True)
    config.set("vault_dir", str(vault_root))
    config.set("system_dir", str(system_root))
    config.set("processing.enable_llm_features", True)
    config.set("youtube.enable_transcripts", True)
    config.set("youtube.enable_embeddings", False)
    config.set("youtube.enable_llm_transcript_processing", True)
    config.set("youtube.transcript_chunk_size", 999999)
    config.set("llm.tasks.transcript.enabled", True)
    config.set("llm.tasks.transcript.retry_interval_hours", 12)
    config.set("llm.tasks.transcript.fallback", [{"provider": "openai"}])


def _build_tweet(tweet_id: str = "123", text: str = "watch https://youtu.be/abc123") -> Tweet:
    return Tweet(
        id=tweet_id,
        full_text=text,
        created_at="2026-04-04T00:00:00Z",
        screen_name="alice",
        name="Alice",
    )


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content
        self.error = None


class _FakeLLMInterface:
    calls = 0

    def __init__(self, llm_config):
        self.llm_config = llm_config

    def resolve_task_route(self, task: str):
        assert task == "transcript"
        return ("openai", "gpt-test", {})

    async def generate(self, **kwargs):
        type(self).calls += 1
        return _FakeResponse(
            json.dumps(
                {
                    "text": "clean transcript",
                    "summary": "summary",
                    "tags": "alpha, beta",
                }
            )
        )


def test_transcript_llm_skips_recent_failed_chunk_and_uses_raw_fallback(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))
    monkeypatch.setattr(
        "processors.transcript_llm_processor.pipeline_registry.is_enabled",
        lambda name: True,
    )
    monkeypatch.setattr(
        "processors.transcript_llm_processor.get_metadata_db",
        lambda: metadata_db,
    )
    monkeypatch.setattr(
        "processors.transcript_llm_processor.llm_cache",
        LLMCache(str(tmp_path / "llm_cache")),
    )
    monkeypatch.setattr(
        "processors.transcript_llm_processor.LLMInterface",
        _FakeLLMInterface,
    )
    _FakeLLMInterface.calls = 0

    processor = TranscriptLLMProcessor()
    transcript_text = "[00:00] hello world"
    chunk_hash = processor._hash_content(transcript_text)
    metadata_db.upsert_transcript_chunk(
        "video-123",
        1,
        chunk_hash,
        json.dumps({"status": "failed", "reason": "invalid_json", "chunk_index": 1}),
        "openai:gpt-test",
    )

    result = asyncio.run(
        processor.process_transcript(transcript_text, context_id="video-123")
    )

    assert _FakeLLMInterface.calls == 0
    assert result is not None
    assert result["text"] == transcript_text
    assert result["summary"] == ""
    assert result["tags"] == ""
    assert result["chunk_metadata"]["fallback_used"] is True
    assert result["chunk_metadata"]["failed_chunks"] == [1]


def test_transcript_llm_retries_after_failed_chunk_cooldown_expires(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))
    monkeypatch.setattr(
        "processors.transcript_llm_processor.pipeline_registry.is_enabled",
        lambda name: True,
    )
    monkeypatch.setattr(
        "processors.transcript_llm_processor.get_metadata_db",
        lambda: metadata_db,
    )
    monkeypatch.setattr(
        "processors.transcript_llm_processor.llm_cache",
        LLMCache(str(tmp_path / "llm_cache")),
    )
    monkeypatch.setattr(
        "processors.transcript_llm_processor.LLMInterface",
        _FakeLLMInterface,
    )
    _FakeLLMInterface.calls = 0

    processor = TranscriptLLMProcessor()
    transcript_text = "[00:00] hello world"
    chunk_hash = processor._hash_content(transcript_text)
    metadata_db.upsert_transcript_chunk(
        "video-456",
        1,
        chunk_hash,
        json.dumps({"status": "failed", "reason": "invalid_json", "chunk_index": 1}),
        "openai:gpt-test",
    )

    stale_timestamp = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    with metadata_db._get_connection() as conn:
        conn.execute(
            """
            UPDATE transcript_chunk_cache
            SET updated_at = ?
            WHERE context_id = ? AND chunk_index = ?
            """,
            (stale_timestamp, "video-456", 1),
        )

    result = asyncio.run(
        processor.process_transcript(transcript_text, context_id="video-456")
    )

    assert _FakeLLMInterface.calls == 1
    assert result is not None
    assert result["text"] == "clean transcript"
    assert result["summary"] == "summary"
    assert result["tags"] == "alpha, beta"
    assert result["chunk_metadata"]["chunks_processed"] == 1


def test_pipeline_should_process_youtube_skips_when_transcript_markdown_exists(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    processor = object.__new__(PipelineProcessor)
    transcript_dir = tmp_path / "vault" / "transcripts"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    (transcript_dir / "youtube_abc123_cached.md").write_text(
        "# cached transcript\n",
        encoding="utf-8",
    )
    processor.youtube_processor = SimpleNamespace(
        extract_youtube_urls=lambda text: ["https://youtu.be/abc123"],
        extract_video_id=lambda url: "abc123",
        has_existing_transcript=lambda video_id: True,
    )
    monkeypatch.setattr(
        "processors.pipeline_processor.pipeline_registry.is_enabled",
        lambda name: True,
    )

    assert processor._should_process_youtube(_build_tweet(), resume=True) is False
