import hashlib
from pathlib import Path
from types import SimpleNamespace

from core.metadata_db import MetadataDB
from core.wiki_metadata_migration import compact_topic_pages
from core.wiki_publication import WikiPublicationStore


def test_topic_metadata_compaction_preserves_prose_links_and_feedback(tmp_path):
    root = tmp_path / "obsidian"
    layout = SimpleNamespace(wiki_root=root / "wiki")
    page = layout.wiki_root / "pages/topic-example.md"
    page.parent.mkdir(parents=True)
    original = ("---\nthoth_kind: topic\nthoth_type: wiki_page\nthoth_id: topic-example\n"
        "title: Example\nthoth_updated_at: yesterday\nthoth_input_manifest:\n- sha256: example\n"
        "thoth_summary: metadata\n---\n\n# Example\n\nImportant human-readable prose.\n"
        "\n## Sources\n\n- [S1] [Original](../../knowledge_vault/paper.pdf)\n"
        "  - Path: paper.pdf\n  - Trust: 1.0\n\n> [!thoth-feedback]\n> More detail please.\n")
    page.write_text(original)
    db = MetadataDB(str(tmp_path / "state/meta.db"))
    expected = hashlib.sha256(original.encode()).hexdigest()
    result = compact_topic_pages(layout, db=db, obsidian_root=root, archive_root=tmp_path / "archives",
                                 expected_hashes={"wiki/pages/topic-example.md": expected})
    assert result["compacted"] == ["wiki/pages/topic-example.md"]
    after = page.read_text()
    assert "Important human-readable prose." in after
    assert "[Original](../../knowledge_vault/paper.pdf)" in after
    assert "> More detail please." in after
    assert "thoth_input_manifest" not in after
    assert "  - Trust:" not in after
    assert next((tmp_path / "archives/topic-pages").glob("*.md")).read_text() == original
    store = WikiPublicationStore(db, layout.wiki_root)
    assert store.metadata_for(page)["thoth_input_manifest"] == [{"sha256": "example"}]
    assert store.inspect(page).status == "clean"
    assert store.feedback_records(page)[0]["raw_text"].endswith("> More detail please.\n")
    page.write_text(after + "\nHuman edit\n")
    rerun = compact_topic_pages(layout, db=db, obsidian_root=root, archive_root=tmp_path / "archives",
                                expected_hashes={"wiki/pages/topic-example.md": expected})
    assert not rerun["compacted"]
    assert "Human edit" in page.read_text()
