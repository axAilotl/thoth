import json
from copy import deepcopy
import asyncio
from types import SimpleNamespace
from pathlib import Path

import pytest

from core.artifacts import (
    ArtifactRelationship,
    DerivedOutput,
    KnowledgeArtifact,
    PaperArtifact,
    RepositoryArtifact,
    TweetArtifact,
    WebClipperArtifact,
)
from core.config import config
from core.ingestion_runtime import (
    BookmarkDispatchResult,
    IngestionDispatchResult,
    KnowledgeArtifactRuntime,
    UnsupportedArtifactTypeError,
)
from core.metadata_db import IngestionQueueEntry, MetadataDB


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


def test_materialize_artifact_supports_known_types(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    runtime = KnowledgeArtifactRuntime()

    tweet_entry = IngestionQueueEntry(
        artifact_id="tweet-1",
        artifact_type="tweet",
        source="browser_extension",
        payload_json='{"tweet_id":"123","tweet_data":{"author":"alice","text":"hello"},"timestamp":"2026-04-04T00:00:00","source":"browser_extension"}',
        created_at="2026-04-04T00:00:00",
    )
    paper_entry = IngestionQueueEntry(
        artifact_id="paper-1",
        artifact_type="paper",
        source="arxiv",
        payload_json='{"id":"2401.12345","source_type":"arxiv","title":"Paper","pdf_url":"https://arxiv.org/pdf/2401.12345.pdf"}',
        created_at="2026-04-04T00:00:00",
    )
    repo_entry = IngestionQueueEntry(
        artifact_id="repo-1",
        artifact_type="repository",
        source="github",
        payload_json='{"id":"gh_1","source_type":"github","repo_name":"owner/repo","full_name":"owner/repo","raw_content":"{\\"id\\": 1, \\"full_name\\": \\"owner/repo\\", \\"stargazers_count\\": 1, \\"forks_count\\": 0, \\"language\\": null, \\"topics\\": [], \\"created_at\\": \\"2026-04-04T00:00:00\\", \\"updated_at\\": \\"2026-04-04T00:00:00\\", \\"pushed_at\\": \\"2026-04-04T00:00:00\\", \\"license\\": null}"}',
        created_at="2026-04-04T00:00:00",
    )
    webclip_artifact = WebClipperArtifact(
        id="webclip:imports/notes/capture.md",
        source_type="web_clipper",
        raw_content="---\ntitle: captured note\n---\n\n# captured note\n",
        created_at="2026-04-04T00:00:00",
        ingested_at="2026-04-04T00:00:00",
        source_path="/tmp/vault/imports/notes/capture.md",
        source_relative_path="imports/notes/capture.md",
        file_type="note",
        title="captured note",
        frontmatter={"title": "captured note"},
        body="# captured note\n",
        source_language="en",
        source_url="https://example.com/capture",
    )
    webclip_entry = IngestionQueueEntry(
        artifact_id="webclip-1",
        artifact_type="web_clipper",
        source="web_clipper",
        payload_json=json.dumps(webclip_artifact.to_dict()),
        created_at="2026-04-04T00:00:00",
    )

    assert isinstance(runtime.materialize_artifact(tweet_entry), TweetArtifact)
    assert isinstance(runtime.materialize_artifact(paper_entry), PaperArtifact)
    assert isinstance(runtime.materialize_artifact(repo_entry), RepositoryArtifact)
    assert isinstance(runtime.materialize_artifact(webclip_entry), WebClipperArtifact)

    with pytest.raises(UnsupportedArtifactTypeError):
        runtime.materialize_artifact(
            IngestionQueueEntry(
                artifact_id="bad-1",
                artifact_type="unsupported",
                source="manual",
                payload_json="{}",
                created_at="2026-04-04T00:00:00",
            )
        )


def test_materialized_artifacts_include_canonical_queue_contract(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    runtime = KnowledgeArtifactRuntime()
    entry = IngestionQueueEntry(
        artifact_id="paper-queued",
        artifact_type="paper",
        source="arxiv_rss",
        payload_json=json.dumps(
            {
                "id": "2401.12345",
                "source_type": "arxiv",
                "title": "Paper",
                "pdf_url": "https://arxiv.org/pdf/2401.12345.pdf",
                "raw_payload_path": "raw/arxiv/2401.12345.json",
                "raw_payload_sha256": "abc123",
            }
        ),
        capabilities_json=json.dumps(["pdf_download", "citation_graph"]),
        created_at="2026-04-04T00:00:00",
    )

    artifact = runtime.materialize_artifact(entry)
    record = artifact.canonical_record()

    assert record["artifact_id"] == "2401.12345"
    assert record["source_identity"] == {
        "source_name": "arxiv_rss",
        "source_type": "arxiv",
        "native_id": "2401.12345",
        "uri": "https://arxiv.org/pdf/2401.12345.pdf",
        "collector": "arxiv_rss",
    }
    assert record["raw_payload"]["path"] == "raw/arxiv/2401.12345.json"
    assert record["raw_payload"]["sha256"] == "abc123"
    assert record["provenance"]["queue_id"] == "paper-queued"
    assert record["provenance"]["collector"] == "arxiv_rss"
    assert record["capabilities"] == ["pdf_download", "citation_graph"]
    assert record["normalized_metadata"]["queue_id"] == "paper-queued"
    assert record["normalized_metadata"]["queue_source"] == "arxiv_rss"


def test_web_clipper_materialization_preserves_raw_and_derived_locations(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    runtime = KnowledgeArtifactRuntime()
    artifact = WebClipperArtifact(
        id="webclip:Clippings/capture.md",
        source_type="web_clipper",
        raw_content="# capture\n",
        ingested_at="2026-04-04T00:00:00",
        source_path="/tmp/vault/Clippings/capture.md",
        source_relative_path="Clippings/capture.md",
        file_type="note",
        title="capture",
        source_checksum="def456",
        source_size_bytes=42,
        source_url="https://example.com/capture",
        output_paths={"translation": "translations/Clippings/capture.en.md"},
    )
    entry = IngestionQueueEntry(
        artifact_id=artifact.id,
        artifact_type="web_clipper",
        source="web_clipper",
        payload_json=json.dumps(artifact.to_dict()),
        created_at="2026-04-04T00:00:00",
    )

    materialized = runtime.materialize_artifact(entry)
    record = materialized.canonical_record()

    assert record["raw_payload"]["path"] == "/tmp/vault/Clippings/capture.md"
    assert record["raw_payload"]["sha256"] == "def456"
    assert record["raw_payload"]["size_bytes"] == 42
    assert record["derived_outputs"] == [
        {
            "output_type": "translation",
            "path": "translations/Clippings/capture.en.md",
        }
    ]
    assert record["provenance"]["evidence_paths"] == [
        "translations/Clippings/capture.en.md"
    ]


def test_serialized_tweet_artifact_materializes_with_canonical_outputs(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    runtime = KnowledgeArtifactRuntime()
    artifact = TweetArtifact(
        id="123",
        source_type="twitter",
        raw_content='{"id":"123"}',
        created_at="2026-04-04T00:00:00",
        screen_name="alice",
        name="Alice",
        full_text="hello",
        engagement={"favorite_count": 3, "retweet_count": 2, "reply_count": 1},
        custom_metadata={"raw_payload_path": "raw/twitter/123.json"},
        output_paths={"markdown": "wiki/tweets/123.md"},
    )
    entry = IngestionQueueEntry(
        artifact_id="tweet-queued",
        artifact_type="tweet",
        source="x_api",
        payload_json=json.dumps(artifact.to_dict()),
        created_at="2026-04-04T00:00:00",
    )

    materialized = runtime.materialize_artifact(entry)
    record = materialized.canonical_record()

    assert isinstance(materialized, TweetArtifact)
    assert materialized.engagement == {
        "favorite_count": 3,
        "retweet_count": 2,
        "reply_count": 1,
    }
    assert record["source_identity"] == {
        "source_name": "x_api",
        "source_type": "twitter",
        "native_id": "123",
        "collector": "x_api",
    }
    assert record["raw_payload"]["path"] == "raw/twitter/123.json"
    assert record["derived_outputs"] == [
        {"output_type": "markdown", "path": "wiki/tweets/123.md"}
    ]
    assert record["provenance"]["evidence_paths"] == ["wiki/tweets/123.md"]


def test_knowledge_artifact_canonical_record_serializes_relationships_and_outputs():
    artifact = KnowledgeArtifact(
        id="manual-1",
        source_type="manual",
        raw_content="raw",
        created_at="2026-04-04T00:00:00",
        output_paths={"markdown": "wiki/pages/manual-1.md"},
        derived_outputs=(
            DerivedOutput(output_type="summary", path="summaries/manual-1.md"),
        ),
        relationships=(
            ArtifactRelationship(
                relationship_type="references",
                target_id="paper-1",
                target_type="paper",
                source_evidence="manual note",
            ),
        ),
    )

    record = artifact.canonical_record()

    assert record["source_identity"]["source_name"] == "manual"
    assert record["raw_payload"]["content_key"] == "raw_content"
    assert record["derived_outputs"] == [
        {"output_type": "summary", "path": "summaries/manual-1.md"},
        {"output_type": "markdown", "path": "wiki/pages/manual-1.md"},
    ]
    assert record["relationships"] == [
        {
            "relationship_type": "references",
            "target_id": "paper-1",
            "target_type": "paper",
            "source_evidence": "manual note",
        }
    ]


def test_process_pending_ingestions_marks_processed(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    db = MetadataDB()
    runtime = KnowledgeArtifactRuntime(db=db)

    repo_entry = IngestionQueueEntry(
        artifact_id="repo-queued",
        artifact_type="repository",
        source="github",
        payload_json='{"id":"gh_1","source_type":"github","repo_name":"owner/repo","full_name":"owner/repo","raw_content":"{\\"id\\": 1, \\"full_name\\": \\"owner/repo\\", \\"stargazers_count\\": 1, \\"forks_count\\": 0, \\"language\\": null, \\"topics\\": [], \\"created_at\\": \\"2026-04-04T00:00:00\\", \\"updated_at\\": \\"2026-04-04T00:00:00\\", \\"pushed_at\\": \\"2026-04-04T00:00:00\\", \\"license\\": null}"}',
        created_at="2026-04-04T00:00:00",
    )
    assert db.upsert_ingestion_entry(repo_entry)

    async def fake_dispatch(artifact):
        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="repository",
            source="github",
            status="processed",
            processed_at="2026-04-04T00:00:00",
            details={"repo_name": "owner/repo"},
        )

    monkeypatch.setattr(runtime, "dispatch_artifact", fake_dispatch)

    results = asyncio.run(runtime.process_pending_ingestions_once())

    assert len(results) == 1
    assert results[0].status == "processed"
    assert db.get_ingestion_entry("repo-queued").status == "processed"


@pytest.mark.anyio
async def test_bookmark_payload_uses_shared_runtime(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    runtime = KnowledgeArtifactRuntime()
    
    async def fake_process_tweets_pipeline(*args, **kwargs):
        return SimpleNamespace(processed_tweets=1)

    runtime._pipeline = SimpleNamespace(
        process_tweets_pipeline=fake_process_tweets_pipeline
    )

    fake_loader = SimpleNamespace(
        load_cached_enhancements=lambda tweet_ids: {},
        _load_tweet_from_cache=lambda cache_file, tweet_id: None,
        extract_all_thread_tweets_from_cache=lambda cache_file: [],
    )
    monkeypatch.setattr("processors.cache_loader.CacheLoader", lambda: fake_loader)
    monkeypatch.setattr(
        "core.graphql_cache.maybe_cleanup_graphql_cache",
        lambda *args, **kwargs: None,
    )

    result = await runtime.process_bookmark_payload(
        {
            "tweet_id": "123",
            "tweet_data": {"author": "alice", "text": "hello"},
            "timestamp": "2026-04-04T00:00:00",
            "source": "browser_extension",
        },
        resume=False,
    )

    assert isinstance(result, BookmarkDispatchResult)
    assert result.tweet_id == "123"
    assert result.tweet_count == 1
    assert result.url_mapping_count == 0
