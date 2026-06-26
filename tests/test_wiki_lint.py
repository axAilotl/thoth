from copy import deepcopy
from pathlib import Path

from core.admin_lint import run_admin_lint
from core.artifacts import RepositoryArtifact
from core.config import config
from core.path_layout import build_path_layout
from core.wiki_contract import WikiPageSpec, build_wiki_contract
from core.wiki_io import atomic_write_text, render_frontmatter
from core.wiki_lint import WikiLintRunner
from core.wiki_updater import CompiledWikiUpdater


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


def _write_generated_page(contract, slug: str, frontmatter: dict, body: str) -> Path:
    page_path = contract.pages_dir / f"{slug}.md"
    content = render_frontmatter(frontmatter).rstrip() + "\n\n" + body
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


def test_wiki_lint_reports_generated_quality_failures(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)

        missing_provenance = contract.frontmatter_for(
            WikiPageSpec(
                title="Missing Provenance",
                slug="missing-provenance",
                kind="topic",
                summary="Generated page without source metadata.",
                updated_at="2026-06-26T00:00:00Z",
            )
        )
        _write_generated_page(
            contract,
            "missing-provenance",
            missing_provenance,
            "# Missing Provenance\n",
        )

        invalid_summary = contract.frontmatter_for(
            WikiPageSpec(
                title="Invalid Summary",
                slug="invalid-summary",
                kind="topic",
                summary="Valid description.",
                source_paths=("raw/missing/source.md",),
                updated_at="2026-06-26T00:00:00Z",
            )
        )
        invalid_summary["thoth_summary"] = ["not", "a", "string"]
        _write_generated_page(
            contract,
            "invalid-summary",
            invalid_summary,
            "# Invalid Summary\n\n"
            "## Sources\n\n"
            "- [raw/missing/source.md](../../vault/raw/missing/source.md)\n",
        )

        malformed_sections = contract.frontmatter_for(
            WikiPageSpec(
                title="Malformed Sections",
                slug="malformed-sections",
                kind="entity",
                summary="Generated page with malformed evidence sections.",
                source_paths=("stars/owner_repo_summary.md",),
                resource="https://example.com/owner/repo",
                artifact_id="gh_1",
                source_type="github",
                updated_at="2026-06-26T00:00:00Z",
            )
        )
        _write_generated_page(
            contract,
            "malformed-sections",
            malformed_sections,
            "# Malformed Sections\n\n"
            "## Sources\n\n"
            "- stars/owner_repo_summary.md\n\n"
            "# Citations\n\n"
            "[2] [Wrong label](../../vault/stars/owner_repo_summary.md)\n",
        )

        duplicate = contract.frontmatter_for(
            WikiPageSpec(
                title="Duplicate Identity",
                slug="duplicate-identity",
                kind="topic",
                summary="Same generated identity in two files.",
                source_paths=("notes/source.md",),
                updated_at="2026-06-26T00:00:00Z",
            )
        )
        for filename in ("duplicate-a", "duplicate-b"):
            _write_generated_page(
                contract,
                filename,
                duplicate,
                "# Duplicate Identity\n\n"
                "## Sources\n\n"
                "- [notes/source.md](../../vault/notes/source.md)\n",
            )

        report = WikiLintRunner(config, layout=layout, contract=contract).lint(
            stale_after_days=999999
        )

        codes = {issue.code for issue in report.issues}
        assert "missing-provenance" in codes
        assert "invalid-summary-schema" in codes
        assert "malformed-source-section" in codes
        assert "malformed-citations" in codes
        assert "duplicate-page-slug" in codes
        assert "duplicate-page-id" in codes
    finally:
        config.data = original


def test_wiki_lint_tolerates_broken_links_when_metadata_is_valid(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        frontmatter = contract.frontmatter_for(
            WikiPageSpec(
                title="Broken Link Allowed",
                slug="broken-link-allowed",
                kind="entity",
                summary="Broken markdown targets are tolerated by OKF.",
                source_paths=("raw/missing/source.md",),
                resource="https://example.com/canonical",
                artifact_id="artifact-1",
                source_type="fixture",
                updated_at="2026-06-26T00:00:00Z",
            )
        )
        _write_generated_page(
            contract,
            "broken-link-allowed",
            frontmatter,
            "# Broken Link Allowed\n\n"
            "Links to [Missing Wiki Page](missing-page.md).\n\n"
            "## Sources\n\n"
            "- [raw/missing/source.md](../../vault/raw/missing/source.md)\n\n"
            "# Citations\n\n"
            "[1] [Canonical resource](https://example.com/canonical)\n"
            "[2] [raw/missing/source.md](../../vault/raw/missing/source.md)\n",
        )

        report = WikiLintRunner(config, layout=layout, contract=contract).lint(
            stale_after_days=999999
        )

        assert report.pages_checked == 1
        assert report.issues == ()
    finally:
        config.data = original


def test_wiki_lint_reports_invalid_frontmatter(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        atomic_write_text(
            contract.pages_dir / "invalid-frontmatter.md",
            "---\n"
            "title: [unterminated\n"
            "---\n\n"
            "# Invalid Frontmatter\n",
        )

        report = WikiLintRunner(config, layout=layout, contract=contract).lint()

        assert report.pages_checked == 1
        assert {issue.code for issue in report.issues} == {"invalid-frontmatter"}
    finally:
        config.data = original


def test_admin_wiki_lint_serializes_stale_input_provenance(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        stars_dir = layout.vault_root / "stars"
        repos_dir = layout.vault_root / "repos"
        stars_dir.mkdir(parents=True, exist_ok=True)
        repos_dir.mkdir(parents=True, exist_ok=True)
        (stars_dir / "owner_repo_summary.md").write_text("# summary\n", encoding="utf-8")
        readme_path = repos_dir / "github_owner_repo_README.md"
        readme_path.write_text("# readme\n", encoding="utf-8")

        CompiledWikiUpdater(config, layout=layout).update_from_artifact(
            RepositoryArtifact(
                id="gh_1",
                source_type="github",
                repo_name="owner/repo",
                description="Repository summary",
                stars=12,
                language="python",
            ),
            dispatch_details={"repo_name": "owner/repo"},
        )
        readme_path.write_text("# changed readme\n", encoding="utf-8")

        payload = run_admin_lint(
            config.data,
            project_root=tmp_path,
            lint_kind="wiki",
        )

        stale_issue = next(
            issue for issue in payload["issues"] if issue["code"] == "stale-page-inputs"
        )
        assert Path(payload["report_path"]).exists()
        assert stale_issue["details"]["recorded_input_hash"] != stale_issue["details"][
            "current_input_hash"
        ]
        assert any(
            change["reason"] == "Source file repos/github_owner_repo_README.md hash changed."
            for change in stale_issue["details"]["changes"]
        )
    finally:
        config.data = original
