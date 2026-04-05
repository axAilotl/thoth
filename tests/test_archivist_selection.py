from pathlib import Path

from core.archivist_selection import select_archivist_candidates
from core.archivist_topics import ArchivistTopicDefinition
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


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


def test_archivist_selection_enforces_root_gates_before_content_filters(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()

    (layout.vault_root / "tweets").mkdir(parents=True, exist_ok=True)
    (layout.vault_root / "papers").mkdir(parents=True, exist_ok=True)
    (layout.vault_root / "transcripts").mkdir(parents=True, exist_ok=True)
    (layout.vault_root / "journals").mkdir(parents=True, exist_ok=True)

    tweet_path = layout.vault_root / "tweets" / "companion.md"
    tweet_path.write_text(
        "---\n"
        "type: tweet\n"
        "tags:\n"
        "  - companion_ai\n"
        "---\n"
        "\n"
        "# Companion AI\n"
        "\n"
        "Notes about personas and introspection.\n",
        encoding="utf-8",
    )
    paper_path = layout.vault_root / "papers" / "companion-ai-whitepaper.pdf"
    paper_path.write_bytes(b"%PDF-1.4\ncompanion ai whitepaper\n")
    (layout.vault_root / "transcripts" / "video.md").write_text(
        "# Expensive transcript\n\nDo not include this source.\n",
        encoding="utf-8",
    )
    (layout.vault_root / "journals" / "daily.md").write_text(
        "# Personal journal\n\nPrivate source.\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion-ai-research",
        title="Companion AI Research",
        output_path="pages/topic-companion-ai-research.md",
        include_roots=("tweets", "papers", "transcripts", "journals"),
        exclude_roots=("transcripts", "journals"),
    )

    result = select_archivist_candidates(
        topic,
        config=config,
        layout=layout,
        db=db,
    )

    assert result.missing_roots == ()
    assert {candidate.path for candidate in result.candidates} == {tweet_path, paper_path}
    assert {candidate.source_type for candidate in result.candidates} == {"tweet", "paper"}


def test_archivist_selection_applies_source_type_tag_and_term_filters(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()

    config.set("sources.web_clipper.note_dirs", ["research-imports"])
    note_dir = layout.vault_root / "research-imports"
    note_dir.mkdir(parents=True, exist_ok=True)
    matching = note_dir / "matching.md"
    matching.write_text(
        "---\n"
        "title: Companion systems\n"
        "tags:\n"
        "  - companion_ai\n"
        "  - personas\n"
        "---\n"
        "\n"
        "# Companion systems\n"
        "\n"
        "This note covers introspection and personas for companion agents.\n",
        encoding="utf-8",
    )
    ignored = note_dir / "ignored.md"
    ignored.write_text(
        "---\n"
        "title: Sales bots\n"
        "tags:\n"
        "  - sales\n"
        "---\n"
        "\n"
        "# Sales bots\n"
        "\n"
        "This note is about customer support automation.\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="companion-ai-research",
        title="Companion AI Research",
        output_path="pages/topic-companion-ai-research.md",
        include_roots=("research-imports",),
        source_types=("web_clipper",),
        include_tags=("companion_ai",),
        include_terms=("introspection",),
        exclude_terms=("customer support",),
    )

    result = select_archivist_candidates(
        topic,
        config=config,
        layout=layout,
        db=db,
    )

    assert [candidate.path for candidate in result.candidates] == [matching]
    assert result.candidates[0].tags == ("companion_ai", "personas")
    assert result.candidates[0].source_type == "web_clipper"


def test_archivist_selection_reports_missing_roots_without_broadening_scope(tmp_path: Path):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    (layout.vault_root / "tweets").mkdir(parents=True, exist_ok=True)

    tweet_path = layout.vault_root / "tweets" / "signal.md"
    tweet_path.write_text(
        "---\n"
        "type: tweet\n"
        "---\n"
        "\n"
        "Signal.\n",
        encoding="utf-8",
    )

    topic = ArchivistTopicDefinition(
        id="signal",
        title="Signal",
        output_path="pages/topic-signal.md",
        include_roots=("tweets", "missing-root"),
    )

    result = select_archivist_candidates(
        topic,
        config=config,
        layout=layout,
        db=db,
    )

    assert result.scanned_roots == ("tweets",)
    assert result.missing_roots == ("missing-root",)
    assert [candidate.path for candidate in result.candidates] == [tweet_path]
