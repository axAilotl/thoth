from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

from core.archivist_selection import select_archivist_candidates
from core.archivist_retrieval.models import (
    ArchivistCorpusDocument,
    ArchivistRetrievalPolicy,
)
from core.archivist_topics import ArchivistTopicDefinition
from core.config import Config
from core.metadata_db import FileMetadata, IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.archivist_retrieval.service import select_archivist_candidates_async
from core.prompt_security import THOTH_SECURITY_PATTERN_IDS_KEY
from tests.security_hostile_fixtures import hostile_text


class FakeEmbeddingLLM:
    def __init__(self):
        self.embed_calls = []

    def resolve_task_route(self, task: str):
        if task == "embedding":
            return ("local", "embedding", {})
        raise AssertionError(task)

    async def embed_texts(self, texts, *, provider=None, model=None):
        self.embed_calls.append(list(texts))
        vectors = []
        for text in texts:
            lowered = text.lower()
            vectors.append(
                [
                    float(lowered.count("companion") + lowered.count("persona")),
                    float(lowered.count("memory") + lowered.count("reflection")),
                    float(lowered.count("security") + lowered.count("attack")),
                ]
            )
        return SimpleNamespace(vectors=vectors, error=None, provider=provider, model=model)


def make_config(tmp_path: Path) -> tuple[Config, MetadataDB]:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", str(tmp_path / ".thoth_system" / "meta.db"))
    db_path = tmp_path / ".thoth_system" / "meta.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return config, MetadataDB(str(db_path))


def test_archivist_corpus_document_preserves_zero_source_trust(tmp_path: Path):
    _config, db = make_config(tmp_path)
    document = ArchivistCorpusDocument(
        candidate_key="vault:repos/zero.md",
        path=tmp_path / "vault" / "repos" / "zero.md",
        scope="vault",
        scope_relative_path="repos/zero.md",
        source_type="repository",
        file_type="markdown",
        title="Zero Trust",
        tags=("retrieval",),
        content_text="Explicitly untrusted retrieval source.",
        source_hash="zero-source-hash",
        size_bytes=10,
        updated_at="2026-04-04T00:00:00Z",
        source_trust_score=0.0,
        source_trust_reason="operator_untrusted",
    )

    db.upsert_archivist_corpus_document(document)

    assert (
        db.get_archivist_corpus_document(document.candidate_key).source_trust_score
        == 0.0
    )


def test_archivist_inventory_reuses_unchanged_documents_between_runs(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    tweets_dir = layout.vault_root / "tweets"
    tweets_dir.mkdir(parents=True, exist_ok=True)
    (tweets_dir / "companion.md").write_text(
        "---\n"
        "type: tweet\n"
        "---\n"
        "\n"
        "Companion AI memory loops.\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion",
        output_path="pages/topic-companion.md",
        include_roots=("tweets",),
        include_terms=("companion",),
    )

    first = select_archivist_candidates(topic, config=config, layout=layout, db=db)
    second = select_archivist_candidates(topic, config=config, layout=layout, db=db)

    assert len(first.candidates) == 1
    assert first.indexed_count == 1
    assert second.indexed_count == 0


def test_archivist_full_text_query_mode_searches_beyond_required_tags(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    repos_dir = layout.vault_root / "repos"
    clippings_dir = layout.vault_root / "clippings"
    repos_dir.mkdir(parents=True, exist_ok=True)
    clippings_dir.mkdir(parents=True, exist_ok=True)
    (repos_dir / "memory_toolkit.md").write_text(
        "# Persona Memory Toolkit\n\nCompanion personas rely on stable reflection and memory loops.\n",
        encoding="utf-8",
    )
    (clippings_dir / "companion_clip.md").write_text(
        "# Agent reflection\n\nA clipping about companion AI introspection and persona scaffolding.\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion AI Research",
        output_path="pages/topic-companion.md",
        include_roots=("repos", "clippings"),
        source_types=("repository", "note"),
        include_tags=("companion_ai",),
        include_terms=("companion ai", "persona", "introspection"),
        retrieval=ArchivistRetrievalPolicy(
            mode="full_text",
            tag_mode="query",
            term_mode="query",
            full_text_limit=20,
            rerank_limit=20,
        ),
    )

    result = select_archivist_candidates(topic, config=config, layout=layout, db=db)

    assert len(result.candidates) == 2
    assert {candidate.scope_relative_path for candidate in result.candidates} == {
        "repos/memory_toolkit.md",
        "clippings/companion_clip.md",
    }


def test_archivist_retrieval_excludes_quarantined_source_ids(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    repos_dir = layout.vault_root / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)
    safe_path = repos_dir / "safe.md"
    quarantined_path = repos_dir / "quarantined.md"
    safe_path.write_text(
        "# Safe Persona Memory\n\nCompanion persona memory loops.\n",
        encoding="utf-8",
    )
    quarantined_path.write_text(
        "# Quarantined Persona Memory\n\n"
        "Companion persona memory loops.\n\n"
        f"{hostile_text('fake_citations')}\n",
        encoding="utf-8",
    )
    db.upsert_file(
        FileMetadata(
            path=str(quarantined_path),
            file_type="readme",
            size_bytes=quarantined_path.stat().st_size,
            updated_at="2026-04-04T00:00:00",
            source_id="repo-review",
        )
    )
    db.upsert_ingestion_entry(
        IngestionQueueEntry(
            artifact_id="repo-review",
            artifact_type="repository",
            source="github",
            payload_json=json.dumps(
                {
                    "id": "repo-review",
                    "source_type": "github",
                    "repo_name": "owner/review",
                    "description": hostile_text("fake_citations"),
                }
            ),
            created_at="2026-04-04T00:00:00",
        )
    )
    entry = db.get_ingestion_entry("repo-review")
    assert entry is not None
    assert entry.status == "needs_review"
    metadata = json.loads(entry.payload_json)["normalized_metadata"]
    assert "fake_citation_injection" in metadata[THOTH_SECURITY_PATTERN_IDS_KEY]

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion",
        output_path="pages/topic-companion.md",
        include_roots=("repos",),
        source_types=("repository",),
        include_terms=("companion", "persona"),
    )

    result = select_archivist_candidates(topic, config=config, layout=layout, db=db)

    assert {candidate.scope_relative_path for candidate in result.candidates} == {
        "repos/safe.md",
    }
    assert db.get_archivist_corpus_document("vault:repos/quarantined.md") is None


def test_archivist_retrieval_blocks_hostile_source_text_before_context(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    repos_dir = layout.vault_root / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)
    (repos_dir / "safe.md").write_text(
        "# Safe Persona Memory\n\nCompanion persona memory loops.\n",
        encoding="utf-8",
    )
    (repos_dir / "hostile.md").write_text(
        "# Hostile Persona Memory\n\n"
        "Companion persona memory loops.\n\n"
        f"{hostile_text('fake_citations')}\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion",
        output_path="pages/topic-companion.md",
        include_roots=("repos",),
        source_types=("repository",),
        include_terms=("companion", "persona"),
    )

    result = select_archivist_candidates(topic, config=config, layout=layout, db=db)

    assert [candidate.scope_relative_path for candidate in result.candidates] == [
        "repos/safe.md",
    ]
    assert db.get_archivist_corpus_document("vault:repos/hostile.md") is None


def test_archivist_retrieval_caps_repeated_source_in_context(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    repos_dir = layout.vault_root / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)
    source_a_paths = []
    for index in range(3):
        path = repos_dir / f"source_a_{index}.md"
        path.write_text(
            f"# Source A {index}\n\nCompanion persona memory retrieval.\n",
            encoding="utf-8",
        )
        source_a_paths.append(path)
    source_b_path = repos_dir / "source_b.md"
    source_b_path.write_text(
        "# Source B\n\nCompanion persona memory retrieval.\n",
        encoding="utf-8",
    )
    for path in source_a_paths:
        db.upsert_file(
            FileMetadata(
                path=str(path),
                file_type="readme",
                size_bytes=path.stat().st_size,
                updated_at="2026-04-04T00:00:00",
                source_id="repo-a",
            )
        )
    db.upsert_file(
        FileMetadata(
            path=str(source_b_path),
            file_type="readme",
            size_bytes=source_b_path.stat().st_size,
            updated_at="2026-04-04T00:00:00",
            source_id="repo-b",
        )
    )

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion",
        output_path="pages/topic-companion.md",
        include_roots=("repos",),
        source_types=("repository",),
        include_terms=("companion", "persona"),
        max_sources=3,
    )

    result = select_archivist_candidates(topic, config=config, layout=layout, db=db)
    source_ids = [candidate.source_id for candidate in result.candidates]

    assert len(result.candidates) == 3
    assert source_ids.count("repo-a") == 2
    assert source_ids.count("repo-b") == 1


def test_archivist_semantic_retrieval_uses_embedding_route(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    repos_dir = layout.vault_root / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)
    (repos_dir / "companion.md").write_text(
        "# Companion Memory\n\nPersona continuity and memory reflection for companion agents.\n",
        encoding="utf-8",
    )
    (repos_dir / "security.md").write_text(
        "# Security Audit\n\nPrompt injection attack surface and model hardening.\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion AI Research",
        output_path="pages/topic-companion.md",
        include_roots=("repos",),
        source_types=("repository",),
        include_terms=("companion ai", "persona", "memory"),
        retrieval=ArchivistRetrievalPolicy(
            mode="semantic",
            tag_mode="query",
            term_mode="query",
            semantic_limit=10,
            rerank_limit=10,
            max_new_embeddings_per_run=10,
        ),
    )

    result = __import__("asyncio").run(
        select_archivist_candidates_async(
            topic,
            config=config,
            layout=layout,
            db=db,
            llm_interface=FakeEmbeddingLLM(),
        )
    )

    assert result.retrieval_mode == "semantic"
    assert result.candidates[0].scope_relative_path == "repos/companion.md"
    assert result.candidates[0].semantic_score is not None


def test_archivist_embedding_lookup_filters_provenance_trust_and_stale_state(
    tmp_path: Path,
):
    _config, db = make_config(tmp_path)
    base_document = ArchivistCorpusDocument(
        candidate_key="vault:repos/safe.md",
        path=tmp_path / "vault" / "repos" / "safe.md",
        scope="vault",
        scope_relative_path="repos/safe.md",
        source_type="repository",
        file_type="markdown",
        title="Safe Repository",
        tags=("retrieval",),
        content_text="Companion memory retrieval.",
        source_hash="safe-source-hash",
        size_bytes=10,
        updated_at="2026-04-04T00:00:00Z",
        source_id="repo-safe",
        source_key="repository:repo-safe",
        artifact_id="artifact-safe",
        event_id="event-safe",
        privacy_class="public",
        retention_class="retain",
    )
    low_trust_document = replace(
        base_document,
        candidate_key="vault:repos/low.md",
        scope_relative_path="repos/low.md",
        source_hash="low-source-hash",
        source_id="repo-low",
        source_key="repository:repo-low",
        artifact_id="artifact-low",
        event_id="event-low",
        source_trust_score=0.65,
        source_trust_reason="prompt_security_low_risk_wrapped",
    )
    stale_document = replace(
        base_document,
        candidate_key="vault:repos/stale.md",
        scope_relative_path="repos/stale.md",
        source_hash="current-source-hash",
        source_id="repo-stale",
        source_key="repository:repo-stale",
        artifact_id="artifact-stale",
        event_id="event-stale",
    )
    quarantined_document = replace(
        base_document,
        candidate_key="vault:repos/quarantined.md",
        scope_relative_path="repos/quarantined.md",
        source_hash="quarantine-source-hash",
        source_id="repo-quarantined",
        source_key="repository:repo-quarantined",
        artifact_id="artifact-quarantined",
        event_id="event-quarantined",
    )

    for document, source_hash in (
        (base_document, base_document.embedding_source_hash()),
        (low_trust_document, low_trust_document.embedding_source_hash()),
        (stale_document, "old-source-hash"),
        (quarantined_document, quarantined_document.embedding_source_hash()),
    ):
        db.upsert_archivist_corpus_embedding(
            candidate_key=document.candidate_key,
            provider="local",
            model="embedding",
            source_hash=source_hash,
            provenance=document.embedding_provenance(),
            vector=[1.0, 0.0, 0.0],
        )

    with db._get_connection() as conn:
        conn.execute(
            """
            UPDATE archivist_corpus_embeddings
            SET security_state = 'quarantined'
            WHERE candidate_key = ?
            """,
            (quarantined_document.candidate_key,),
        )

    candidate_keys = tuple(
        document.candidate_key
        for document in (
            base_document,
            low_trust_document,
            stale_document,
            quarantined_document,
        )
    )
    expected_hashes = {
        document.candidate_key: document.embedding_source_hash()
        for document in (
            base_document,
            low_trust_document,
            stale_document,
            quarantined_document,
        )
    }

    trusted = db.get_archivist_corpus_embeddings(
        candidate_keys=candidate_keys,
        provider="local",
        model="embedding",
        expected_source_hashes=expected_hashes,
        source_types=("repository",),
        artifact_ids=("artifact-safe", "artifact-low"),
        trust_tiers=("trusted",),
        min_trust_score=0.9,
        privacy_classes=("public",),
    )

    assert list(trusted) == [base_document.candidate_key]
    assert trusted[base_document.candidate_key]["artifact_id"] == "artifact-safe"
    assert trusted[base_document.candidate_key]["event_id"] == "event-safe"
    assert trusted[base_document.candidate_key]["trust_tier"] == "trusted"

    low_risk = db.get_archivist_corpus_embeddings(
        candidate_keys=candidate_keys,
        provider="local",
        model="embedding",
        expected_source_hashes=expected_hashes,
        source_ids=("repo-low",),
        trust_tiers=("low_risk",),
    )
    assert list(low_risk) == [low_trust_document.candidate_key]

    assert stale_document.candidate_key not in db.get_archivist_corpus_embeddings(
        candidate_keys=candidate_keys,
        provider="local",
        model="embedding",
        expected_source_hashes=expected_hashes,
    )
    stale_included = db.get_archivist_corpus_embeddings(
        candidate_keys=(stale_document.candidate_key,),
        provider="local",
        model="embedding",
        expected_source_hashes=expected_hashes,
        exclude_stale=False,
    )
    assert stale_included[stale_document.candidate_key]["stale"] is True

    assert quarantined_document.candidate_key not in db.get_archivist_corpus_embeddings(
        candidate_keys=candidate_keys,
        provider="local",
        model="embedding",
        expected_source_hashes=expected_hashes,
    )
    quarantined_included = db.get_archivist_corpus_embeddings(
        candidate_keys=(quarantined_document.candidate_key,),
        provider="local",
        model="embedding",
        expected_source_hashes=expected_hashes,
        exclude_security_states=(),
    )
    assert (
        quarantined_included[quarantined_document.candidate_key]["security_state"]
        == "quarantined"
    )


def test_archivist_semantic_retrieval_skips_restricted_and_secret_embeddings(
    tmp_path: Path,
):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    repos_dir = layout.vault_root / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)
    (repos_dir / "safe.md").write_text(
        "# Safe Persona Memory\n\nCompanion persona memory reflection.\n",
        encoding="utf-8",
    )
    (repos_dir / "restricted.md").write_text(
        "---\n"
        "privacy_class: restricted\n"
        "retention_class: short\n"
        "---\n"
        "\n"
        "# Restricted Persona Memory\n\nCompanion persona memory reflection.\n",
        encoding="utf-8",
    )
    secret = "OPENAI_API_KEY=sk-proj-" + "a" * 32
    (repos_dir / "secret.md").write_text(
        f"# Secret Persona Memory\n\nCompanion persona memory reflection.\n{secret}\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion",
        title="Companion AI Research",
        output_path="pages/topic-companion.md",
        include_roots=("repos",),
        source_types=("repository",),
        include_terms=("companion", "persona", "memory"),
        retrieval=ArchivistRetrievalPolicy(
            mode="semantic",
            tag_mode="query",
            term_mode="query",
            semantic_limit=10,
            rerank_limit=10,
            max_new_embeddings_per_run=10,
        ),
    )
    llm = FakeEmbeddingLLM()

    result = __import__("asyncio").run(
        select_archivist_candidates_async(
            topic,
            config=config,
            layout=layout,
            db=db,
            llm_interface=llm,
        )
    )

    assert [candidate.scope_relative_path for candidate in result.candidates] == [
        "repos/safe.md",
    ]
    indexed = db.list_archivist_corpus_documents(source_types=("repository",))
    embeddings = db.list_archivist_corpus_embeddings_for_candidate_keys(
        tuple(document.candidate_key for document in indexed)
    )
    assert [embedding["candidate_key"] for embedding in embeddings] == [
        "vault:repos/safe.md",
    ]
    embedded_texts = "\n".join(text for call in llm.embed_calls for text in call)
    assert "Restricted Persona Memory" not in embedded_texts
    assert secret not in embedded_texts
