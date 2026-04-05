from pathlib import Path
from types import SimpleNamespace

from core.archivist_benchmark import benchmark_archivist_topics
from core.config import Config
from core.metadata_db import MetadataDB


class FakeEmbeddingLLM:
    def resolve_task_route(self, task: str):
        if task == "embedding":
            return ("local", "embedding", {})
        raise AssertionError(task)

    async def embed_texts(self, texts, *, provider=None, model=None):
        return SimpleNamespace(
            vectors=[[float(text.lower().count("companion")), 1.0] for text in texts],
            error=None,
            provider=provider,
            model=model,
        )


def test_archivist_benchmark_reports_candidate_diagnostics(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("paths.archivist_topics_file", "archivist_topics.yaml")
    config.set("database.path", str(tmp_path / ".thoth_system" / "meta.db"))

    (tmp_path / "archivist_topics.yaml").write_text(
        """
version: 1
topics:
  - id: companion
    title: Companion AI Research
    output_path: pages/topic-companion.md
    include_roots:
      - repos
    source_types:
      - repository
    include_terms:
      - companion
    retrieval:
      mode: full_text
      tag_mode: query
      term_mode: query
""".strip(),
        encoding="utf-8",
    )
    repos_dir = tmp_path / "vault" / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)
    (repos_dir / "companion.md").write_text(
        "# Companion Research\n\nCompanion agents and memory.\n",
        encoding="utf-8",
    )

    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    results = __import__("asyncio").run(
        benchmark_archivist_topics(
            config,
            project_root=tmp_path,
            db=db,
            llm_interface=FakeEmbeddingLLM(),
        )
    )

    assert len(results) == 1
    result = results[0]
    assert result.topic_id == "companion"
    assert result.candidate_count == 1
    assert result.indexed_count == 1
    assert result.top_candidate_paths == ("repos/companion.md",)
