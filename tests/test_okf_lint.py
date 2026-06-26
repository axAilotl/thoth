from copy import deepcopy
from pathlib import Path

from core.config import config
from core.okf import OKFLintRunner
from core.path_layout import build_path_layout
from core.wiki_contract import WikiPageSpec, build_wiki_contract
from core.wiki_io import atomic_write_text, render_frontmatter
from core.wiki_scaffold import append_wiki_log_entry, ensure_wiki_scaffold


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


def test_okf_lint_accepts_generated_scaffold_and_concept_page(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        scaffold = ensure_wiki_scaffold(config, project_root=tmp_path)
        append_wiki_log_entry(
            scaffold,
            "Compiled `owner-repo`.",
            timestamp="2026-04-04T00:00:00Z",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Owner Repo",
                slug="owner-repo",
                kind="entity",
                summary="Repository summary",
                source_paths=("stars/owner_repo_summary.md",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Owner Repo\n\n"
            "Repository notes.\n\n"
            "# Citations\n\n"
            "[1] [summary](../../vault/stars/owner_repo_summary.md)",
        )

        report = OKFLintRunner(config, layout=layout, contract=contract).lint()

        assert report.okf_version == "0.1"
        assert report.concepts_checked == 1
        assert report.reserved_files_checked == 2
        assert report.issues == ()
    finally:
        config.data = original


def test_okf_lint_reports_missing_type_and_reserved_frontmatter(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        ensure_wiki_scaffold(config, project_root=tmp_path)

        bad_page = contract.pages_dir / "missing-type.md"
        atomic_write_text(
            bad_page,
            "---\n"
            "title: Missing Type\n"
            "---\n\n"
            "# Missing Type\n",
        )
        atomic_write_text(
            contract.index_path,
            "---\n"
            "thoth_type: wiki_index\n"
            "---\n\n"
            "# Index\n",
        )
        atomic_write_text(
            contract.log_path,
            "# Log\n\n"
            "## 2026-04-04T00:00:00Z\n\n"
            "* **Update**: bad date heading\n",
        )

        report = OKFLintRunner(config, layout=layout, contract=contract).lint()

        codes = {issue.code for issue in report.issues}
        assert report.has_errors
        assert "missing-type" in codes
        assert "reserved-frontmatter" in codes
        assert "invalid-log-date" in codes
    finally:
        config.data = original


def test_okf_lint_reports_invalid_frontmatter(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        ensure_wiki_scaffold(config, project_root=tmp_path)

        atomic_write_text(
            contract.pages_dir / "invalid-frontmatter.md",
            "---\n"
            "type: [unterminated\n"
            "---\n\n"
            "# Invalid Frontmatter\n",
        )

        report = OKFLintRunner(config, layout=layout, contract=contract).lint()

        assert report.has_errors
        assert {issue.code for issue in report.issues} == {"invalid-frontmatter"}
    finally:
        config.data = original


def test_okf_lint_tolerates_broken_markdown_links(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        ensure_wiki_scaffold(config, project_root=tmp_path)

        atomic_write_text(
            contract.pages_dir / "broken-link.md",
            "---\n"
            "type: Topic\n"
            "id: broken-link\n"
            "---\n\n"
            "# Broken Link\n\n"
            "This OKF concept links to [missing evidence](does-not-exist.md).\n",
        )

        report = OKFLintRunner(config, layout=layout, contract=contract).lint()

        assert report.concepts_checked == 1
        assert report.issues == ()
    finally:
        config.data = original
