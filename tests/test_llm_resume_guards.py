from copy import deepcopy
from pathlib import Path

import pytest

from core.config import config
from core.data_models import Tweet
from core.filename_utils import get_filename_normalizer
from processors.llm_processor import LLMProcessor
from processors.pipeline_processor import PipelineProcessor


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def _configure_runtime_paths(tmp_path: Path) -> None:
    vault_root = tmp_path / "vault"
    config.data = {}
    config.set("paths.vault_dir", str(vault_root))
    config.set("paths.images_dir", "images")
    config.set("paths.videos_dir", "videos")
    config.set("paths.media_dir", "media")
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("database.enabled", False)
    config.set("processing.enable_llm_features", True)
    config.set("llm.tasks.tags.enabled", True)
    config.set("llm.tasks.summary.enabled", True)
    config.set("llm.tasks.alt_text.enabled", False)


def _build_tweet(tweet_id: str = "123") -> Tweet:
    return Tweet(
        id=tweet_id,
        full_text="This is a sufficiently long tweet body used for resume checks.",
        created_at="2026-04-04T00:00:00Z",
        screen_name="alice",
        name="Alice",
    )


def test_pipeline_should_process_llm_skips_when_markdown_has_current_llm_sections(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    processor = PipelineProcessor(vault_path=str(tmp_path / "vault"))
    monkeypatch.setattr(processor.llm_processor, "is_enabled", lambda: True)
    monkeypatch.setattr(
        "processors.pipeline_processor.pipeline_registry.any_enabled",
        lambda names: True,
    )

    tweet = _build_tweet()
    normalizer = get_filename_normalizer()
    tweet_file = (
        tmp_path
        / "vault"
        / "tweets"
        / normalizer.generate_tweet_filename(tweet.id, tweet.screen_name)
    )
    tweet_file.parent.mkdir(parents=True, exist_ok=True)
    tweet_file.write_text("# Tweet\n\n## Summary\ncached summary\n\n## Tags\n#ai\n", encoding="utf-8")

    assert processor._should_process_llm(tweet, resume=True) is False


def test_thread_resume_reads_existing_thread_markdown_sections(
    tmp_path: Path,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    processor = LLMProcessor()

    tweet_one = _build_tweet("100")
    tweet_one.thread_id = "thread-1"
    tweet_two = _build_tweet("101")
    tweet_two.thread_id = "thread-1"

    normalizer = get_filename_normalizer()
    thread_file = (
        tmp_path
        / "vault"
        / "threads"
        / normalizer.generate_thread_filename(tweet_one.thread_id, tweet_one.screen_name)
    )
    thread_file.parent.mkdir(parents=True, exist_ok=True)
    thread_file.write_text("# Thread\n\n## Summary\ncached thread summary\n", encoding="utf-8")

    assert processor._thread_has_llm_features([tweet_one, tweet_two]) is True


def test_llm_processor_has_llm_features_reads_existing_tweet_markdown(
    tmp_path: Path,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    processor = LLMProcessor()
    tweet = _build_tweet("222")

    normalizer = get_filename_normalizer()
    tweet_file = (
        tmp_path
        / "vault"
        / "tweets"
        / normalizer.generate_tweet_filename(tweet.id, tweet.screen_name)
    )
    tweet_file.parent.mkdir(parents=True, exist_ok=True)
    tweet_file.write_text("# Tweet\n\n## Tags\n#ai\n", encoding="utf-8")

    assert processor._has_llm_features(tweet) is True
