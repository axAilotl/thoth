from copy import deepcopy
from pathlib import Path

from core.config import config
from core.path_layout import build_path_layout
from core.wiki_contract import WikiPageSpec, build_wiki_contract
from core.wiki_io import atomic_write_text, render_frontmatter
from core.wiki_lint import WikiLintRunner


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


def test_wiki_lint_reports_contradictions_staleness_and_orphans(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)

        _write_page(
            contract,
            WikiPageSpec(
                title="Alpha Note",
                slug="alpha-note",
                kind="entity",
                source_paths=("raw/shared/source.md",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Alpha Note\n\nLinks to [Beta](beta-note.md).",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Beta Note",
                slug="beta-note",
                kind="concept",
                source_paths=("raw/shared/source.md",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Beta Note\n\nDifferent interpretation of the same source.",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Old Note",
                slug="old-note",
                kind="topic",
                source_paths=("raw/old/source.md",),
                updated_at="2020-01-01T00:00:00Z",
            ),
            "# Old Note\n\nThis note is stale.",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Orphan Note",
                slug="orphan-note",
                kind="topic",
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Orphan Note\n\nNo provenance or backlinks.",
        )

        report = WikiLintRunner(config, layout=layout, contract=contract).lint(
            stale_after_days=30
        )

        codes = {issue.code for issue in report.issues}
        severities = {issue.code: issue.severity for issue in report.issues}

        assert report.pages_checked == 4
        assert "contradicting-source-path" in codes
        assert severities["contradicting-source-path"] == "error"
        assert "stale-page" in codes
        assert severities["stale-page"] == "warning"
        assert "orphan-page" in codes
        assert severities["orphan-page"] == "warning"
    finally:
        config.data = original
