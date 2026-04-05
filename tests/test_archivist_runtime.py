from pathlib import Path

import pytest

from core.archivist_compiler import ArchivistCompileResult
from core.archivist_runtime import resolve_archivist_sync_config, run_archivist_topics
from core.config import Config


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("database.path", "meta.db")
    config.set("automation.archivist.enabled", True)
    config.set("automation.archivist.interval_hours", 12)
    config.set("automation.archivist.run_on_startup", False)
    return config


def test_resolve_archivist_sync_config_enforces_non_live_minimum(tmp_path: Path):
    config = make_config(tmp_path)

    resolved = resolve_archivist_sync_config(config)

    assert resolved["enabled"] is True
    assert resolved["interval_hours"] == 12.0
    assert resolved["run_on_startup"] is False

    config.set("automation.archivist.interval_hours", 4)
    with pytest.raises(ValueError):
        resolve_archivist_sync_config(config)


@pytest.mark.anyio
async def test_run_archivist_topics_returns_serialized_summary(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)

    class FakeCompiler:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, *, topic_ids=None, force=False, dry_run=False, limit=None):
            assert topic_ids == ["companion-ai"]
            assert force is True
            assert dry_run is False
            assert limit == 1
            return [
                ArchivistCompileResult(
                    topic_id="companion-ai",
                    status="compiled",
                    reason="forced",
                    page_path=tmp_path / "wiki" / "pages" / "topic-companion-ai.md",
                    candidate_count=2,
                    source_paths=("tweets/example.md", "papers/example.md"),
                    model_provider="openrouter",
                    model="archivist",
                )
            ]

    monkeypatch.setattr("core.archivist_runtime.ArchivistCompiler", FakeCompiler)

    payload = await run_archivist_topics(
        config,
        project_root=tmp_path,
        topic_ids=["companion-ai"],
        force=True,
        dry_run=False,
        limit=1,
    )

    assert payload["status"] == "ok"
    assert payload["summary"]["compiled"] == 1
    assert payload["results"][0]["page_path"] == str(
        tmp_path / "wiki" / "pages" / "topic-companion-ai.md"
    )
