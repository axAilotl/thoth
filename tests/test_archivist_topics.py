from pathlib import Path

import pytest

from core.archivist_topics import (
    ArchivistTopicConfigError,
    ARCHIVIST_TOPICS_FILENAME,
    ARCHIVIST_TOPICS_EXAMPLE_FILENAME,
    load_archivist_topic_registry,
    resolve_archivist_topics_example_path,
    resolve_archivist_topics_path,
    seed_archivist_topic_registry_from_example,
)
from core.config import Config


def _make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return config


def _configure_validateable_runtime(config: Config, tmp_path: Path) -> None:
    bookmarks = tmp_path / "bookmarks.json"
    cookies = tmp_path / "cookies.json"
    bookmarks.write_text("[]\n", encoding="utf-8")
    cookies.write_text("{}\n", encoding="utf-8")
    config.set("paths.bookmarks_file", str(bookmarks))
    config.set("paths.cookies_file", str(cookies))
    config.set("paths.images_dir", "images")
    config.set("paths.videos_dir", "videos")
    config.set("paths.media_dir", "media")


def test_archivist_registry_loads_defaults_and_normalizes_fields(tmp_path: Path):
    config = _make_config(tmp_path)
    registry_path = tmp_path / "archivist_topics.yaml"
    registry_path.write_text(
        "version: 1\n"
        "defaults:\n"
        "  cadence_hours: 12\n"
        "  max_sources: 120\n"
        "topics:\n"
        "  - id: companion-ai-research\n"
        "    title: Companion AI Research\n"
        "    output_path: pages/topic-companion-ai-research.md\n"
        "    include_roots:\n"
        "      - tweets\n"
        "      - papers\n"
        "    exclude_roots:\n"
        "      - transcripts\n"
        "    source_types:\n"
        "      - Tweet\n"
        "      - Paper\n"
        "    include_tags:\n"
        "      - '#Companion-AI'\n"
        "      - personas\n"
        "    exclude_tags:\n"
        "      - personal\n"
        "    include_terms:\n"
        "      - Companion   AI\n"
        "      - assistant agents\n"
        "  - id: model-evals-and-benchmarks\n"
        "    title: Model Evals and Benchmarks\n"
        "    output_path: pages/topic-model-evals-and-benchmarks.md\n"
        "    include_roots:\n"
        "      - tweets\n"
        "      - papers\n"
        "    cadence_hours: 24\n"
        "    max_sources: 60\n",
        encoding="utf-8",
    )

    registry = load_archivist_topic_registry(config, project_root=tmp_path, required=True)

    assert registry.source_path == registry_path
    assert len(registry.topics) == 2

    companion = registry.get_topic("companion-ai-research")
    assert companion is not None
    assert companion.output_path == "pages/topic-companion-ai-research.md"
    assert companion.include_roots == ("tweets", "papers")
    assert companion.exclude_roots == ("transcripts",)
    assert companion.source_types == ("tweet", "paper")
    assert companion.include_tags == ("companion_ai", "personas")
    assert companion.exclude_tags == ("personal",)
    assert companion.include_terms == ("companion ai", "assistant agents")
    assert companion.cadence_hours == 12
    assert companion.max_sources == 120
    assert companion.allow_manual_force is True
    assert companion.retrieval.mode == "full_text"
    assert companion.retrieval.tag_mode == "required"
    assert companion.retrieval.term_mode == "required"

    evals = registry.get_topic("model-evals-and-benchmarks")
    assert evals is not None
    assert evals.include_roots == ("tweets", "papers")
    assert evals.cadence_hours == 24
    assert evals.max_sources == 60


def test_archivist_registry_uses_default_project_relative_path(tmp_path: Path):
    config = _make_config(tmp_path)
    registry_path = tmp_path / ARCHIVIST_TOPICS_FILENAME
    registry_path.write_text(
        "version: 1\n"
        "topics:\n"
        "  - id: companion-ai-research\n"
        "    title: Companion AI Research\n"
        "    output_path: pages/topic-companion-ai-research.md\n"
        "    include_roots:\n"
        "      - tweets\n",
        encoding="utf-8",
    )

    resolved_path = resolve_archivist_topics_path(config, project_root=tmp_path)
    registry = load_archivist_topic_registry(config, project_root=tmp_path)

    assert resolved_path == registry_path
    assert registry.source_path == registry_path
    assert registry.topics[0].id == "companion-ai-research"


def test_archivist_registry_missing_optional_file_returns_empty_registry(tmp_path: Path):
    config = _make_config(tmp_path)

    registry = load_archivist_topic_registry(config, project_root=tmp_path)

    assert registry.topics == ()
    assert registry.source_path == tmp_path / ARCHIVIST_TOPICS_FILENAME


def test_archivist_registry_can_seed_live_file_from_example(tmp_path: Path):
    config = _make_config(tmp_path)
    example_path = tmp_path / ARCHIVIST_TOPICS_EXAMPLE_FILENAME
    example_path.write_text(
        "version: 1\n"
        "topics:\n"
        "  - id: companion-ai-research\n"
        "    title: Companion AI Research\n"
        "    output_path: pages/topic-companion-ai-research.md\n"
        "    include_roots:\n"
        "      - tweets\n",
        encoding="utf-8",
    )

    seeded_path = seed_archivist_topic_registry_from_example(
        config,
        project_root=tmp_path,
    )
    registry_path = resolve_archivist_topics_path(config, project_root=tmp_path)

    assert resolve_archivist_topics_example_path(project_root=tmp_path) == example_path
    assert seeded_path == registry_path
    assert registry_path.read_text(encoding="utf-8") == example_path.read_text(encoding="utf-8")


def test_archivist_registry_missing_explicit_file_fails_closed(tmp_path: Path):
    config = _make_config(tmp_path)
    config.set("paths.archivist_topics_file", "topics/archivist.yaml")

    with pytest.raises(ArchivistTopicConfigError, match="file not found"):
        load_archivist_topic_registry(config, project_root=tmp_path)


def test_archivist_registry_rejects_invalid_paths_and_duplicates(tmp_path: Path):
    config = _make_config(tmp_path)
    registry_path = tmp_path / "archivist_topics.yaml"
    registry_path.write_text(
        "version: 1\n"
        "topics:\n"
        "  - id: bad-topic\n"
        "    title: Bad Topic\n"
        "    output_path: ../escape.md\n"
        "    include_roots:\n"
        "      - tweets\n",
        encoding="utf-8",
    )

    with pytest.raises(ArchivistTopicConfigError, match="cannot escape their root"):
        load_archivist_topic_registry(config, project_root=tmp_path, required=True)

    registry_path.write_text(
        "version: 1\n"
        "topics:\n"
        "  - id: bad-topic\n"
        "    title: Bad Topic\n"
        "    output_path: notes/escape.md\n"
        "    include_roots:\n"
        "      - tweets\n",
        encoding="utf-8",
    )

    with pytest.raises(ArchivistTopicConfigError, match="must live under wiki/pages"):
        load_archivist_topic_registry(config, project_root=tmp_path, required=True)

    registry_path.write_text(
        "version: 1\n"
        "topics:\n"
        "  - id: duplicate-topic\n"
        "    title: First Topic\n"
        "    output_path: pages/topic-one.md\n"
        "    include_roots:\n"
        "      - tweets\n"
        "  - id: duplicate-topic\n"
        "    title: Second Topic\n"
        "    output_path: pages/topic-two.md\n"
        "    include_roots:\n"
        "      - papers\n",
        encoding="utf-8",
    )

    with pytest.raises(ArchivistTopicConfigError, match="Duplicate archivist topic id"):
        load_archivist_topic_registry(config, project_root=tmp_path, required=True)


def test_config_validate_surfaces_archivist_registry_errors(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = _make_config(tmp_path)
    _configure_validateable_runtime(config, tmp_path)
    config.set("paths.archivist_topics_file", "topics/archivist.yaml")

    errors = config.validate()

    assert any("Archivist topic registry file not found" in error for error in errors)
