from copy import deepcopy
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.artifacts import RepositoryArtifact
from core.capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    CaptureSession,
    CaptureSource,
    RawArtifactRef,
    SecurityFinding,
)
from core.config import config
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.prompt_security import (
    THOTH_SECURITY_PATTERN_IDS_KEY,
    THOTH_SECURITY_POLICY_KEY,
)
from core.wiki_io import read_document
from core.wiki_lint import WikiLintRunner
from core.wiki_updater import CompiledWikiUpdater
from test_capture_event_store import FakeCaptureConnection
from tests.security_hostile_fixtures import hostile_text


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


def _capture_store_with_public_and_restricted_events(layout):
    public_raw = layout.raw_root / "capture" / "public.json"
    public_raw.parent.mkdir(parents=True, exist_ok=True)
    public_raw.write_text(
        '{"raw_content":"do not copy this raw transcript"}\n',
        encoding="utf-8",
    )
    restricted_raw = layout.raw_root / "capture" / "restricted.json"
    restricted_raw.write_text('{"text":"private note"}\n', encoding="utf-8")

    store = CaptureEventStore(
        FakeCaptureConnection(),
        schema="capture_unit",
        raw_roots=[layout.raw_root],
    )
    source = store.upsert_source(
        CaptureSource(
            source_id="source-public",
            source_name="manual-notes",
            source_type="manual",
            base_uri="https://example.test/manual",
        )
    )
    session = store.upsert_session(
        CaptureSession(
            source_id=source.source_id,
            session_id="session-public",
            native_session_id="sync-2026-04-04",
            session_type="manual",
            started_at="2026-04-04T10:00:00Z",
        )
    )
    public_event = store.upsert_event(
        CaptureEvent(
            event_id="event-public",
            source_id=source.source_id,
            session_id=session.session_id,
            event_type="note",
            native_event_id="note-public",
            occurred_at="2026-04-04T10:15:00Z",
            captured_at="2026-04-04T10:16:00Z",
            payload={
                "title": "Public capture note",
                "raw_content": "do not render this payload blob",
                "normalized_metadata": {
                    "people": [{"id": "ada", "name": "Ada Lovelace"}],
                    "projects": [{"id": "thoth", "name": "Thoth"}],
                },
            },
            privacy={"classification": "public"},
        )
    )
    public_ref = store.upsert_raw_ref(
        RawArtifactRef.from_file(
            public_raw,
            source_id=source.source_id,
            session_id=session.session_id,
            event_id=public_event.event_id,
            raw_roots=[layout.raw_root],
        )
    )
    store.upsert_artifact_link(
        ArtifactLink(
            event_id=public_event.event_id,
            raw_ref_id=public_ref.raw_ref_id,
            artifact_id="artifact-public",
            artifact_type="note",
        )
    )
    store.upsert_security_finding(
        SecurityFinding(
            event_id=public_event.event_id,
            finding_type="classifier",
            severity="low",
            status="open",
            fingerprint="low-classifier",
            details={"pattern_id": "benign-marker", "scope": "context"},
        )
    )

    restricted_event = store.upsert_event(
        CaptureEvent(
            event_id="event-private",
            source_id=source.source_id,
            session_id=session.session_id,
            event_type="note",
            native_event_id="note-private",
            occurred_at="2026-04-04T11:00:00Z",
            captured_at="2026-04-04T11:01:00Z",
            payload={"title": "Private capture note"},
            privacy={"classification": "private"},
        )
    )
    store.upsert_raw_ref(
        RawArtifactRef.from_file(
            restricted_raw,
            source_id=source.source_id,
            session_id=session.session_id,
            event_id=restricted_event.event_id,
            raw_roots=[layout.raw_root],
        )
    )
    return store


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


def test_wiki_updater_blocks_quarantined_artifacts_and_index_entries(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    updater = CompiledWikiUpdater(config, layout=layout)

    risky = RepositoryArtifact(
        id="gh_risky",
        source_type="github",
        repo_name="owner/risky",
        description="Risky repository",
        raw_content=hostile_text("base64_like_payload"),
    )

    with pytest.raises(ValueError, match="security review"):
        updater.update_from_artifact(risky)

    pages_dir = layout.wiki_root / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    quarantined_page = pages_dir / "repo-quarantined.md"
    quarantined_page.write_text(
        "---\n"
        "title: Quarantined Repo\n"
        "thoth_slug: repo-quarantined\n"
        "thoth_security_policy:\n"
        "  status: needs_review\n"
        "  reason: high_risk_finding\n"
        "---\n"
        "\n"
        "# Quarantined Repo\n",
        encoding="utf-8",
    )

    updater.refresh_index()

    index_content = (layout.wiki_root / "index.md").read_text(encoding="utf-8")
    assert "repo-quarantined.md" not in index_content
    assert THOTH_SECURITY_POLICY_KEY in risky.normalized_metadata
    assert "base64_prompt_payload" in risky.normalized_metadata[
        THOTH_SECURITY_PATTERN_IDS_KEY
    ]


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
    influence_sources = document.frontmatter["thoth_influence_sources"]
    assert [item["source_path"] for item in influence_sources] == [
        "repos/github_owner_repo_README.md",
        "stars/owner_repo_summary.md",
    ]
    assert all(item["artifact_id"] == "gh_1" for item in influence_sources)
    assert all(item["source_type"] == "github" for item in influence_sources)
    assert all(item["sha256"] for item in influence_sources)
    assert document.frontmatter["thoth_input_hash"]
    assert [item["source_path"] for item in document.frontmatter["thoth_input_manifest"]] == [
        "repos/github_owner_repo_README.md",
        "stars/owner_repo_summary.md",
    ]
    assert document.frontmatter["thoth_change_provenance"]["reason"] == "initial_compile"
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


def test_wiki_lint_reports_source_file_changes_after_compile(
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
    readme_path = repos_dir / "github_owner_repo_README.md"
    readme_path.write_text("# readme\n", encoding="utf-8")

    CompiledWikiUpdater(config, layout=layout).update_from_artifact(
        RepositoryArtifact(
            id="gh_1",
            source_type="github",
            repo_name="owner/repo",
            description="Repository summary",
            stars=12,
            language="python",
        ),
        dispatch_details={"repo_name": "owner/repo"},
    )

    readme_path.write_text("# changed readme\n", encoding="utf-8")
    report = WikiLintRunner(config, layout=layout).lint(stale_after_days=999999)

    stale_issue = next(
        issue for issue in report.issues if issue.code == "stale-page-inputs"
    )
    assert stale_issue.severity == "warning"
    assert stale_issue.details["recorded_input_hash"] != stale_issue.details[
        "current_input_hash"
    ]
    assert any(
        "Source file repos/github_owner_repo_README.md hash changed."
        == change["reason"]
        for change in stale_issue.details["changes"]
    )


def test_wiki_updater_compiles_capture_event_rollup_pages_with_filters(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    store = _capture_store_with_public_and_restricted_events(layout)

    updater = CompiledWikiUpdater(config, layout=layout)
    results = updater.update_from_capture_events(store)

    slugs = {result.slug for result in results}
    assert slugs == {
        "capture-daily-2026-04-04",
        "capture-weekly-2026-w14",
        "capture-source-manual-notes",
        "capture-session-session-public",
        "person-ada",
        "project-thoth",
    }

    daily_page = layout.wiki_root / "pages" / "capture-daily-2026-04-04.md"
    document = read_document(daily_page)
    assert document.frontmatter["type"] == "Topic"
    assert document.frontmatter["thoth_capture_page_type"] == "daily"
    assert document.frontmatter["thoth_capture_page_key"] == "2026-04-04"
    assert document.frontmatter["thoth_capture_event_count"] == 1
    assert document.frontmatter["thoth_event_ids"] == ["event-public"]
    assert document.frontmatter["thoth_source_ids"] == ["source-public"]
    assert document.frontmatter["thoth_session_ids"] == ["session-public"]
    assert document.frontmatter["thoth_source_paths"] == [
        "raw/capture/public.json"
    ]
    assert document.frontmatter["thoth_input_hash"]
    input_kinds = {
        item["input_kind"]
        for item in document.frontmatter["thoth_input_manifest"]
    }
    assert input_kinds == {"capture_event", "raw_ref"}
    assert any(
        item["event_id"] == "event-public"
        for item in document.frontmatter["thoth_influence_sources"]
    )
    assert document.frontmatter["thoth_security_findings"] == [
        {
            "event_id": "event-public",
            "finding_id": document.frontmatter["thoth_security_findings"][0][
                "finding_id"
            ],
            "finding_type": "classifier",
            "severity": "low",
            "status": "open",
            "fingerprint": "low-classifier",
            "pattern_id": "benign-marker",
            "scope": "context",
        }
    ]
    assert "## Sources" in document.body
    assert "# Citations" in document.body
    assert "raw/capture/public.json" in document.body
    assert "Capture event event-public" in document.body
    assert "event-private" not in document.body
    assert "do not copy this raw transcript" not in document.body
    assert "do not render this payload blob" not in document.body

    assert (layout.wiki_root / "pages" / "person-ada.md").exists()
    assert (layout.wiki_root / "pages" / "project-thoth.md").exists()

    report = WikiLintRunner(config, layout=layout).lint(stale_after_days=999999)
    assert not report.has_errors


def test_wiki_lint_reports_capture_event_changes_after_compile(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    store = _capture_store_with_public_and_restricted_events(layout)

    CompiledWikiUpdater(config, layout=layout).update_from_capture_events(store)
    store.upsert_event(
        CaptureEvent(
            event_id="event-public",
            source_id="source-public",
            session_id="session-public",
            event_type="note",
            native_event_id="note-public",
            occurred_at="2026-04-04T10:15:00Z",
            captured_at="2026-04-04T10:16:00Z",
            payload={
                "title": "Public capture note updated",
                "normalized_metadata": {
                    "people": [{"id": "ada", "name": "Ada Lovelace"}],
                    "projects": [{"id": "thoth", "name": "Thoth"}],
                },
            },
            privacy={"classification": "public"},
        )
    )

    report = WikiLintRunner(
        config,
        layout=layout,
        event_store=store,
    ).lint(stale_after_days=999999)

    stale_issue = next(
        issue
        for issue in report.issues
        if issue.code == "stale-page-inputs"
        and issue.page_path.name == "capture-daily-2026-04-04.md"
    )
    assert any(
        "Capture event event-public hash changed." == change["reason"]
        for change in stale_issue.details["changes"]
    )


def test_wiki_updater_requires_audit_reason_for_restricted_capture_events(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)
    store = _capture_store_with_public_and_restricted_events(layout)
    updater = CompiledWikiUpdater(config, layout=layout)

    with pytest.raises(ValueError, match="audit_reason"):
        updater.update_from_capture_events(
            store,
            include_restricted_events=True,
        )

    results = updater.update_from_capture_events(
        store,
        include_restricted_events=True,
        audit_reason="operator reviewed private note",
    )

    daily_page = next(
        result.page_path
        for result in results
        if result.slug == "capture-daily-2026-04-04"
    )
    document = read_document(daily_page)
    assert document.frontmatter["thoth_event_ids"] == [
        "event-private",
        "event-public",
    ]
    assert document.frontmatter["thoth_capture_event_count"] == 2
    assert document.frontmatter["thoth_capture_audit"] == {
        "compiled_at": document.frontmatter["thoth_capture_audit"]["compiled_at"],
        "include_restricted_events": True,
        "reason": "operator reviewed private note",
    }
    assert "event-private" in document.body
