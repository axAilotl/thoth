from copy import deepcopy
from pathlib import Path

from core.config import config
from core.path_layout import build_path_layout
from core.wiki_contract import WikiPageSpec, build_wiki_contract
from core.wiki_io import atomic_write_text, render_frontmatter
from core.wiki_query import WikiQueryRunner


def make_config(tmp_path: Path):
    original = deepcopy(config.data)
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return original


def _write_page(contract, spec, body: str) -> Path:
    page_path = contract.page_path_for(spec)
    content = render_frontmatter(contract.frontmatter_for(spec)).rstrip() + "\n\n" + body
    atomic_write_text(page_path, content)
    return page_path


def test_wiki_query_searches_and_writes_back_curated_pages(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)

        _write_page(
            contract,
            WikiPageSpec(
                title="Owner Repo",
                slug="repo-owner-repo",
                kind="entity",
                summary="Repository summary",
                source_paths=("stars/owner_repo_summary.md",),
                related_slugs=("owner-repo",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Owner Repo\n\nThis page discusses agentic workflows and retrieval.",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Unrelated Note",
                slug="unrelated-note",
                kind="topic",
                summary="A disconnected note",
                source_paths=("notes/unrelated.md",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Unrelated Note\n\nNo query term here.",
        )

        runner = WikiQueryRunner(config, layout=layout, contract=contract)
        result = runner.search("agentic workflows", limit=5)

        assert len(result.hits) == 1
        assert result.hits[0].slug == "repo-owner-repo"
        assert "body" in result.hits[0].matched_fields or "phrase" in result.hits[0].matched_fields

        write_back = runner.curated_write_back(
            "agentic workflows",
            limit=5,
            selected_slugs=["repo-owner-repo"],
            curated_notes="Curated answer for the wiki loop.",
        )

        query_page = layout.wiki_root / "pages" / "query-agentic-workflows.md"
        index_path = layout.wiki_root / "index.md"

        assert write_back.page_path == query_page
        assert query_page.exists()
        page_content = query_page.read_text(encoding="utf-8")
        assert "thoth_type: wiki_query" in page_content
        assert "Curated answer for the wiki loop." in page_content
        assert "repo-owner-repo" in page_content
        assert index_path.read_text(encoding="utf-8").count("query-agentic-workflows.md") == 1
    finally:
        config.data = original
