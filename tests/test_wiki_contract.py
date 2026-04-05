from pathlib import Path

import pytest

from core.config import Config
from core.wiki_contract import (
    WikiPageSpec,
    build_wiki_contract,
    is_legacy_tweet_slug,
    normalize_wiki_slug,
)


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.wiki_dir", "wiki")
    config.set("database.path", "meta.db")
    return config


def test_normalize_wiki_slug():
    assert normalize_wiki_slug("LLM Knowledge Base") == "llm-knowledge-base"
    assert normalize_wiki_slug("  Entities / Topics  ") == "entities-topics"


def test_normalize_wiki_slug_rejects_empty_value():
    with pytest.raises(ValueError, match="cannot be empty"):
        normalize_wiki_slug("   ")


def test_is_legacy_tweet_slug():
    assert is_legacy_tweet_slug("tweet-123")
    assert not is_legacy_tweet_slug("repo-owner-repo")


def test_wiki_contract_paths_and_frontmatter(tmp_path: Path):
    contract = build_wiki_contract(make_config(tmp_path), project_root=tmp_path)

    spec = WikiPageSpec(
        title="LLM Knowledge Base",
        slug="llm-knowledge-base",
        kind="topic",
        summary="Compiled notes for the wiki loop.",
        aliases=("knowledge-base",),
        source_paths=("raw/bookmarks/item.md",),
        related_slugs=("agents",),
        updated_at="2026-04-04T00:00:00Z",
    )

    assert contract.root == tmp_path / "wiki"
    assert contract.index_path == tmp_path / "wiki" / "index.md"
    assert contract.log_path == tmp_path / "wiki" / "log.md"
    assert contract.page_path_for(spec) == tmp_path / "wiki" / "pages" / "llm-knowledge-base.md"

    frontmatter = contract.frontmatter_for(spec)
    assert frontmatter["thoth_type"] == "wiki_page"
    assert frontmatter["title"] == "LLM Knowledge Base"
    assert frontmatter["slug"] == "llm-knowledge-base"
    assert frontmatter["kind"] == "topic"
    assert frontmatter["aliases"] == ["knowledge-base"]
    assert frontmatter["source_paths"] == ["raw/bookmarks/item.md"]
    assert frontmatter["related_slugs"] == ["agents"]


def test_wiki_contract_rejects_reserved_and_invalid_slugs(tmp_path: Path):
    contract = build_wiki_contract(make_config(tmp_path), project_root=tmp_path)

    with pytest.raises(ValueError, match="reserved"):
        contract.page_path("index")

    with pytest.raises(ValueError, match="normalized"):
        contract.page_path("Not Canonical")


def test_wiki_contract_rejects_invalid_page_kind(tmp_path: Path):
    contract = build_wiki_contract(make_config(tmp_path), project_root=tmp_path)

    spec = WikiPageSpec(title="Bad Kind", slug="bad-kind", kind="unknown")
    with pytest.raises(ValueError, match="Unsupported wiki page kind"):
        contract.validate_page_spec(spec)
