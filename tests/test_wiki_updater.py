from copy import deepcopy
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.artifacts import RepositoryArtifact
from core.config import config
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.wiki_io import read_document
from core.wiki_updater import CompiledWikiUpdater


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _configure_runtime_config(tmp_path: Path) -> None:
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")


def test_wiki_updater_creates_repository_page_and_refreshes_index(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    stars_dir = layout.vault_root / "stars"
    repos_dir = layout.vault_root / "repos"
    stars_dir.mkdir(parents=True, exist_ok=True)
    repos_dir.mkdir(parents=True, exist_ok=True)
    (stars_dir / "owner_repo_summary.md").write_text("# summary\n", encoding="utf-8")
    (repos_dir / "github_owner_repo_README.md").write_text("# readme\n", encoding="utf-8")

    updater = CompiledWikiUpdater(config, layout=layout)
    result = updater.update_from_artifact(
        RepositoryArtifact(
            id="gh_1",
            source_type="github",
            repo_name="owner/repo",
            description="Repository summary",
            stars=12,
            language="python",
            topics=["agents"],
        ),
        dispatch_details={"repo_name": "owner/repo"},
    )

    page_content = result.page_path.read_text(encoding="utf-8")
    index_content = (layout.wiki_root / "index.md").read_text(encoding="utf-8")
    log_content = (layout.wiki_root / "log.md").read_text(encoding="utf-8")

    assert result.slug == "repo-owner-repo"
    assert "Repository summary" in page_content
    assert "stars/owner_repo_summary.md" in page_content
    assert "[owner/repo](pages/repo-owner-repo.md)" in index_content
    assert "Created `repo-owner-repo` from `github:gh_1`." in log_content


@pytest.mark.anyio
async def test_bookmark_runtime_does_not_create_compiled_tweet_pages(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    runtime = KnowledgeArtifactRuntime()

    async def fake_process_tweets_pipeline(*args, **kwargs):
        return SimpleNamespace(processed_tweets=1)

    runtime._pipeline = SimpleNamespace(process_tweets_pipeline=fake_process_tweets_pipeline)

    fake_loader = SimpleNamespace(
        load_cached_enhancements=lambda tweet_ids: {},
        _load_tweet_from_cache=lambda cache_file, tweet_id: None,
        extract_all_thread_tweets_from_cache=lambda cache_file: [],
    )
    monkeypatch.setattr("processors.cache_loader.CacheLoader", lambda: fake_loader)
    monkeypatch.setattr("core.graphql_cache.maybe_cleanup_graphql_cache", lambda *args, **kwargs: None)

    result = await runtime.process_bookmark_payload(
        {
            "tweet_id": "123",
            "tweet_data": {"author": "alice", "text": "hello wiki"},
            "timestamp": "2026-04-04T00:00:00",
            "source": "browser_extension",
        },
        resume=False,
    )

    layout = build_path_layout(config)
    wiki_page = layout.wiki_root / "pages" / "tweet-123.md"

    assert result.tweet_id == "123"
    assert not wiki_page.exists()


def test_wiki_updater_prunes_legacy_tweet_pages_when_refreshing_index(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    wiki_pages_dir = layout.wiki_root / "pages"
    wiki_pages_dir.mkdir(parents=True, exist_ok=True)
    legacy_page = wiki_pages_dir / "tweet-123.md"
    legacy_page.write_text(
        "---\n"
        "thoth_type: wiki_page\n"
        "title: Tweet 123 by alice\n"
        "slug: tweet-123\n"
        "kind: concept\n"
        "summary: hello wiki\n"
        "---\n"
        "\n"
        "# Tweet 123 by alice\n",
        encoding="utf-8",
    )

    updater = CompiledWikiUpdater(config, layout=layout)
    updater.refresh_index()

    assert not legacy_page.exists()
    index_content = (layout.wiki_root / "index.md").read_text(encoding="utf-8")
    assert "tweet-123.md" not in index_content
    log_content = (layout.wiki_root / "log.md").read_text(encoding="utf-8")
    assert "Pruned legacy compiled tweet wiki pages" in log_content


def test_ingestion_runtime_updates_wiki_after_dispatch(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    db = MetadataDB()
    runtime = KnowledgeArtifactRuntime(db=db)

    entry = IngestionQueueEntry(
        artifact_id="gh_1",
        artifact_type="repository",
        source="github",
        payload_json='{"id":"gh_1","source_type":"github","repo_name":"owner/repo","description":"Repo description","stars":3,"language":"python","topics":["ai"],"raw_content":"{\\"id\\":1,\\"full_name\\":\\"owner/repo\\",\\"stargazers_count\\":3,\\"forks_count\\":0,\\"language\\":\\"python\\",\\"topics\\":[\\"ai\\"],\\"created_at\\":\\"2026-04-04T00:00:00\\",\\"updated_at\\":\\"2026-04-04T00:00:00\\",\\"pushed_at\\":\\"2026-04-04T00:00:00\\",\\"license\\":null}"}',
        created_at="2026-04-04T00:00:00",
    )
    assert db.upsert_ingestion_entry(entry)

    async def fake_dispatch(artifact):
        return SimpleNamespace(
            artifact_id=artifact.id,
            artifact_type="repository",
            source="github",
            status="processed",
            processed_at="2026-04-04T00:00:00",
            details={"repo_name": "owner/repo"},
        )

    monkeypatch.setattr(runtime, "dispatch_artifact", fake_dispatch)

    dispatch_results = asyncio.run(runtime.process_pending_ingestions_once())
    layout = build_path_layout(config)
    wiki_page = layout.wiki_root / "pages" / "repo-owner-repo.md"

    assert len(dispatch_results) == 1
    assert wiki_page.exists()
    assert "Repo description" in wiki_page.read_text(encoding="utf-8")


def test_wiki_updater_emits_deterministic_provenance_and_security_frontmatter(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    stars_dir = layout.vault_root / "stars"
    repos_dir = layout.vault_root / "repos"
    stars_dir.mkdir(parents=True, exist_ok=True)
    repos_dir.mkdir(parents=True, exist_ok=True)
    (stars_dir / "owner_repo_summary.md").write_text("# summary\n", encoding="utf-8")
    (repos_dir / "github_owner_repo_README.md").write_text("# readme\n", encoding="utf-8")

    result = CompiledWikiUpdater(config, layout=layout).update_from_artifact(
        RepositoryArtifact(
            id="gh_1",
            source_type="github",
            repo_name="owner/repo",
            description="Repository summary",
            stars=12,
            language="python",
            topics=["zeta", "alpha", "zeta"],
            custom_metadata={
                "event_ids": ["event-b", "event-a", "event-b"],
                "security_findings": [
                    {"pattern_id": "prompt_override", "scope": "strict"},
                ],
            },
            normalized_metadata={
                "redaction": {
                    "finding_count": 1,
                    "categories": {"api_key": 1},
                    "findings": [{"category": "api_key", "pattern_id": "generic"}],
                }
            },
        ),
        dispatch_details={"repo_name": "owner/repo"},
    )

    document = read_document(result.page_path)

    assert result.source_paths == (
        "repos/github_owner_repo_README.md",
        "stars/owner_repo_summary.md",
    )
    assert document.frontmatter["type"] == "Entity"
    assert document.frontmatter["id"] == "repo-owner-repo"
    assert document.frontmatter["thoth_id"] == "repo-owner-repo"
    assert document.frontmatter["thoth_artifact_id"] == "gh_1"
    assert document.frontmatter["thoth_source_paths"] == [
        "repos/github_owner_repo_README.md",
        "stars/owner_repo_summary.md",
    ]
    assert document.frontmatter["thoth_event_ids"] == ["event-a", "event-b"]
    assert document.frontmatter["thoth_security_findings"] == [
        {"category": "api_key", "pattern_id": "generic", "source": "redaction"},
        {"pattern_id": "prompt_override", "scope": "strict", "source": "security_findings"},
    ]
    assert "- Topics: `alpha`, `zeta`" in document.body

    sources_section = document.body.split("## Sources", maxsplit=1)[1].split(
        "# Citations", maxsplit=1
    )[0]
    citations_section = document.body.split("# Citations", maxsplit=1)[1]
    assert sources_section.index("repos/github_owner_repo_README.md") < sources_section.index(
        "stars/owner_repo_summary.md"
    )
    assert citations_section.index("[1] [Canonical resource]") < citations_section.index(
        "[2] [repos/github_owner_repo_README.md]"
    )
    assert citations_section.index(
        "[2] [repos/github_owner_repo_README.md]"
    ) < citations_section.index("[3] [stars/owner_repo_summary.md]")
