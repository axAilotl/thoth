from pathlib import Path
from types import SimpleNamespace

from core.archivist_selection import select_archivist_candidates
from core.archivist_retrieval.models import ArchivistRetrievalPolicy
from core.archivist_topics import ArchivistTopicDefinition
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.archivist_retrieval.service import select_archivist_candidates_async


class FakeEmbeddingLLM:
    def resolve_task_route(self, task: str):
        if task == "embedding":
            return ("local", "embedding", {})
        raise AssertionError(task)

    async def embed_texts(self, texts, *, provider=None, model=None):
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
