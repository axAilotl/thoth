from pathlib import Path
from types import SimpleNamespace

import pytest

from core.metadata_db import MetadataDB
from core.source_records import SourceRecordStore
from core.vault_maintenance import apply_source_page_plan, plan_source_pages


def setup(tmp_path):
    obsidian = tmp_path / "obsidian"
    layout = SimpleNamespace(wiki_root=obsidian / "wiki", vault_root=obsidian / "knowledge_vault")
    (layout.wiki_root / "pages").mkdir(parents=True)
    (layout.vault_root / "papers").mkdir(parents=True)
    source = layout.vault_root / "papers/paper.pdf"
    source.write_bytes(b"original source bytes")
    page = layout.wiki_root / "pages/clip-paper-123.md"
    page.write_text("---\nthoth_type: wiki_page\nthoth_id: clip-paper-123\nthoth_kind: concept\n"
                    "thoth_artifact_id: webclip:papers/paper.pdf\nthoth_source_type: web_clipper\n"
                    "thoth_source_paths:\n- papers/paper.pdf\nunknown_metadata: retain me\n---\n"
                    "# Paper\nAn abstract and potentially valuable annotations.\n")
    db = MetadataDB(str(tmp_path / "meta.db"))
    return obsidian, layout, source, page, db


def test_source_migration_is_lossless_idempotent_and_outside_vault(tmp_path):
    obsidian, layout, source, page, db = setup(tmp_path)
    original = page.read_bytes()
    before = source.read_bytes()
    (layout.wiki_root / "log.md").write_text("# Wiki Maintenance Log\nCreated `clip-paper-123`\n")
    plan = plan_source_pages(layout, obsidian_root=obsidian)
    assert len(plan["pages"]) == 1 and not plan["pages"][0]["blocked_by"]
    archive = tmp_path / "control/archive"
    result = apply_source_page_plan(plan, archive_root=archive, db=db, layout=layout)
    assert result["archived"] == ["wiki/pages/clip-paper-123.md"]
    assert not page.exists()
    assert next((archive / "source-pages").glob("*.md")).read_bytes() == original
    assert source.read_bytes() == before
    exported = SourceRecordStore(db).export()
    assert exported["archives"][0]["document"].encode() == original
    assert exported["records"][0]["metadata"]["imported_page"]["unknown_metadata"] == "retain me"
    again = apply_source_page_plan(plan, archive_root=archive, db=db, layout=layout)
    assert not again["archived"]


@pytest.mark.parametrize("change", ["edited", "reference", "source_missing"])
def test_migration_preserves_changed_referenced_or_missing_source_records(tmp_path, change):
    obsidian, layout, source, page, db = setup(tmp_path)
    plan = plan_source_pages(layout, obsidian_root=obsidian)
    if change == "edited":
        page.write_text(page.read_text() + "Human addition\n")
    elif change == "reference":
        (obsidian / "My note.md").write_text("See [[clip-paper-123]]")
    else:
        source.unlink()
    result = apply_source_page_plan(plan, archive_root=tmp_path / "archive", db=db, layout=layout)
    assert not result["archived"]
    assert page.exists()


def test_archive_cannot_add_more_files_to_obsidian(tmp_path):
    obsidian, layout, _, page, db = setup(tmp_path)
    plan = plan_source_pages(layout, obsidian_root=obsidian)
    with pytest.raises(ValueError, match="outside"):
        apply_source_page_plan(plan, archive_root=obsidian / "hidden-control", db=db, layout=layout)
    assert page.exists()


def test_source_symlink_is_not_followed(tmp_path):
    obsidian, layout, source, page, db = setup(tmp_path)
    external = tmp_path / "private.pdf"
    external.write_bytes(b"private")
    source.unlink()
    source.symlink_to(external)
    plan = plan_source_pages(layout, obsidian_root=obsidian)
    assert plan["pages"][0]["blocked_by"]


def test_link_from_retained_source_record_protects_its_target(tmp_path):
    obsidian, layout, _, target, db = setup(tmp_path)
    other = target.with_name("clip-other-123.md")
    other.write_text(target.read_text().replace("clip-paper-123", "clip-other-123")
                     .replace("papers/paper.pdf", "papers/missing.pdf") + "\nSee [[clip-paper-123]]\n")
    plan = plan_source_pages(layout, obsidian_root=obsidian)
    result = apply_source_page_plan(plan, archive_root=tmp_path / "archive", db=db, layout=layout)
    assert not result["archived"]
    assert target.exists() and other.exists()
