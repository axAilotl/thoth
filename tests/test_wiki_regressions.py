from copy import deepcopy
from pathlib import Path
import shutil

import pytest

from core.artifacts import RepositoryArtifact
from core.config import config
from core.path_layout import build_path_layout
from core.wiki_contract import build_wiki_contract
from core.wiki_io import read_document
from core.wiki_lint import WikiLintRunner
from core.wiki_query import WikiQueryRunner
from core.wiki_updater import CompiledWikiUpdater


FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "wiki"


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


@pytest.fixture
def wiki_env(tmp_path: Path, monkeypatch, restore_runtime_config):
    monkeypatch.chdir(tmp_path)
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    layout = build_path_layout(config)
    contract = build_wiki_contract(config)
    return layout, contract


def _copy_fixture(relative_path: str, destination: Path) -> Path:
    source = FIXTURES_ROOT / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination


def _copy_page_fixtures(layout, names: tuple[str, ...]) -> None:
    for name in names:
        _copy_fixture(f"pages/{name}", layout.wiki_root / "pages" / name)


def _copy_source_fixtures(layout) -> None:
    for source_name in (
        "sources/stars/owner_repo_summary.md",
        "sources/repos/github_owner_repo_README.md",
    ):
        _copy_fixture(source_name, layout.vault_root / source_name.removeprefix("sources/"))


def test_compiled_wiki_updater_preserves_created_at_and_refreshes_index(
    wiki_env,
):
    layout, _contract = wiki_env
    _copy_page_fixtures(layout, ("repo-owner-repo.md",))
    _copy_source_fixtures(layout)

    page_path = layout.wiki_root / "pages" / "repo-owner-repo.md"
    original_created_at = read_document(page_path).frontmatter["created_at"]

    result = CompiledWikiUpdater(config, layout=layout).update_from_artifact(
        RepositoryArtifact(
            id="gh_1",
            source_type="github",
            repo_name="owner/repo",
            description="Updated repository description",
            stars=12,
            language="python",
            topics=["agents"],
        ),
        dispatch_details={"repo_name": "owner/repo"},
    )

    updated_document = read_document(result.page_path)
    index_content = (layout.wiki_root / "index.md").read_text(encoding="utf-8")
    log_content = (layout.wiki_root / "log.md").read_text(encoding="utf-8")

    assert result.action == "updated"
    assert result.source_paths == (
        "stars/owner_repo_summary.md",
        "repos/github_owner_repo_README.md",
    )
    assert updated_document.frontmatter["created_at"] == original_created_at
    assert updated_document.frontmatter["updated_at"] != original_created_at
    assert "Updated repository description" in updated_document.body
    assert index_content.count("repo-owner-repo.md") == 1
    assert "Updated `repo-owner-repo` from `github:gh_1`." in log_content


def test_wiki_query_curated_write_back_rewrites_existing_page(
    wiki_env,
):
    layout, contract = wiki_env
    _copy_page_fixtures(layout, ("repo-owner-repo.md", "unrelated-note.md"))

    runner = WikiQueryRunner(config, layout=layout, contract=contract)
    first = runner.curated_write_back(
        "agentic workflows",
        limit=5,
        selected_slugs=["REPO-OWNER-REPO"],
        curated_notes="First curated note.",
    )
    first_document = read_document(first.page_path)
    created_at = first_document.frontmatter["created_at"]

    second = runner.curated_write_back(
        "agentic workflows",
        limit=5,
        selected_slugs=["repo-owner-repo"],
        curated_notes="Updated curated note.",
        curated_title="Agentic Workflows",
    )
    second_document = read_document(second.page_path)
    index_content = (layout.wiki_root / "index.md").read_text(encoding="utf-8")

    assert first.page_path == second.page_path
    assert first.selected_slugs == ("repo-owner-repo",)
    assert first.hit_count == 1
    assert first_document.frontmatter["thoth_type"] == "wiki_query"
    assert first_document.frontmatter["query_terms"] == ["agentic", "workflows"]
    assert first_document.frontmatter["curated"] is True
    assert second_document.frontmatter["created_at"] == created_at
    assert "Updated curated note." in second_document.body
    assert index_content.count("query-agentic-workflows.md") == 1


def test_wiki_lint_accepts_query_pages_and_reports_invalid_timestamps(
    wiki_env,
):
    layout, contract = wiki_env
    _copy_page_fixtures(layout, ("repo-owner-repo.md", "unrelated-note.md", "bad-timestamp.md"))

    query_runner = WikiQueryRunner(config, layout=layout, contract=contract)
    write_back = query_runner.curated_write_back(
        "agentic workflows",
        limit=5,
        selected_slugs=["repo-owner-repo"],
        curated_notes="Curated answer for lint coverage.",
    )

    report = WikiLintRunner(config, layout=layout, contract=contract).lint(
        stale_after_days=30
    )

    codes = {issue.code for issue in report.issues}
    query_page_issues = [
        issue for issue in report.issues if issue.page_path == write_back.page_path
    ]

    assert report.pages_checked == 4
    assert "invalid-updated-at" in codes
    assert "invalid-record-type" not in codes
    assert not any(issue.code == "orphan-page" for issue in query_page_issues)


def test_query_and_lint_ignore_legacy_compiled_tweet_pages(wiki_env):
    layout, contract = wiki_env
    _copy_page_fixtures(layout, ("repo-owner-repo.md",))
    legacy_page = layout.wiki_root / "pages" / "tweet-123.md"
    legacy_page.write_text(
        "---\n"
        "thoth_type: wiki_page\n"
        "title: Tweet 123 by alice\n"
        "slug: tweet-123\n"
        "kind: concept\n"
        "summary: legacy tweet page\n"
        "---\n"
        "\n"
        "# Tweet 123 by alice\n",
        encoding="utf-8",
    )

    query_result = WikiQueryRunner(config, layout=layout, contract=contract).search(
        "tweet 123",
        limit=10,
    )
    lint_report = WikiLintRunner(config, layout=layout, contract=contract).lint()

    assert legacy_page.exists()
    assert all(hit.slug != "tweet-123" for hit in query_result.hits)
    assert lint_report.pages_checked == 1
