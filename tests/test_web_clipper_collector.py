import asyncio
import os
from pathlib import Path

import pytest

from collectors.web_clipper_collector import WebClipperCollector
from core.config import Config
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "web_clipper"


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("sources.web_clipper.note_dirs", ["web-clipper/notes"])
    config.set("sources.web_clipper.attachment_dirs", ["web-clipper/assets"])
    return config


def _copy_fixture(name: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    source = FIXTURE_DIR / name
    destination.write_bytes(source.read_bytes())
    return destination


def make_collector(tmp_path: Path) -> tuple[WebClipperCollector, Path]:
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()

    note_dir = layout.raw_root / "web-clipper" / "notes"
    asset_dir = layout.raw_root / "web-clipper" / "assets"
    note_dir.mkdir(parents=True, exist_ok=True)
    asset_dir.mkdir(parents=True, exist_ok=True)

    db = MetadataDB(db_path=str(layout.database_path))
    collector = WebClipperCollector(config, layout=layout, db=db)
    return collector, layout.raw_root


def test_web_clipper_collector_indexes_allowlisted_roots_only(tmp_path: Path):
    collector, raw_root = make_collector(tmp_path)

    note_file = raw_root / "web-clipper" / "notes" / "capture.md"
    ignored_note_file = raw_root / "web-clipper" / "notes" / "capture.txt"
    attachment_file = raw_root / "web-clipper" / "assets" / "capture_attachment.pdf"
    ignored_attachment_file = raw_root / "web-clipper" / "assets" / "image.md"

    _copy_fixture("capture_note.md", note_file)
    ignored_note_file.write_text("skip me\n", encoding="utf-8")
    _copy_fixture("capture_attachment.pdf", attachment_file)
    ignored_attachment_file.write_text("skip me too\n", encoding="utf-8")

    discovered = collector.collect()

    assert {record.path for record in discovered} == {note_file, attachment_file}
    assert all(record.is_new_or_changed for record in discovered)
    note_record = next(record for record in discovered if record.path == note_file)
    assert note_record.artifact is not None
    assert note_record.artifact.raw_content == note_file.read_text(encoding="utf-8")
    assert note_record.artifact.title == "Web Clipper fixture note"
    assert note_record.artifact.source_url == "https://example.com/capture"
    assert note_record.artifact.frontmatter["title"] == "Web Clipper fixture note"
    attachment_record = next(record for record in discovered if record.path == attachment_file)
    assert attachment_record.artifact is not None
    assert attachment_record.artifact.file_type == "attachment"
    assert attachment_record.artifact.title == "capture_attachment"
    managed_attachment = (
        collector.layout.library_root / "web-clipper" / "assets" / "capture_attachment.pdf"
    )
    assert attachment_record.managed_path == managed_attachment
    assert attachment_record.artifact.output_paths["library"] == str(managed_attachment)
    assert managed_attachment.exists()
    assert managed_attachment.read_bytes() == attachment_file.read_bytes()
    assert attachment_file.exists()
    assert collector.db.get_file_entry(str(note_file)).file_type == "note"
    assert collector.db.get_file_entry(str(attachment_file)).file_type == "attachment"
    assert collector.db.get_file_entry(str(ignored_note_file)) is None
    assert collector.db.get_file_entry(str(ignored_attachment_file)) is None


def test_web_clipper_collector_reindexes_changed_files(tmp_path: Path):
    collector, raw_root = make_collector(tmp_path)

    note_file = raw_root / "web-clipper" / "notes" / "capture.md"
    note_file.write_text(
        "---\n"
        "title: first version\n"
        "---\n"
        "\n"
        "# first version\n",
        encoding="utf-8",
    )

    first_pass = collector.collect()
    assert len(first_pass) == 1
    assert first_pass[0].is_new_or_changed is True

    second_pass = collector.collect()
    assert len(second_pass) == 1
    assert second_pass[0].is_new_or_changed is False

    note_file.write_text(
        "---\n"
        "title: second version\n"
        "---\n"
        "\n"
        "# second version with a change\n",
        encoding="utf-8",
    )
    third_pass = collector.collect()
    assert len(third_pass) == 1
    assert third_pass[0].is_new_or_changed is True


def test_web_clipper_collector_queues_notes_for_shared_runtime(
    tmp_path: Path,
):
    collector, raw_root = make_collector(tmp_path)

    note_file = raw_root / "web-clipper" / "notes" / "capture.md"
    note_file.write_text(
        "---\n"
        "title: captured note\n"
        "url: https://example.com/capture\n"
        "lang: en\n"
        "---\n"
        "\n"
        "# captured note\n"
        "Body text.\n",
        encoding="utf-8",
    )

    discovered = collector.collect()

    assert len(discovered) == 1
    queue_entry = collector.db.get_ingestion_entry("webclip:web-clipper/notes/capture.md")
    assert queue_entry is not None
    assert queue_entry.artifact_type == "web_clipper"
    assert queue_entry.status == "pending"

    runtime = KnowledgeArtifactRuntime(layout=collector.layout, db=collector.db)
    results = asyncio.run(runtime.process_pending_ingestions_once())

    assert len(results) == 1
    assert results[0].artifact_type == "web_clipper"
    assert collector.db.get_ingestion_entry("webclip:web-clipper/notes/capture.md").status == "processed"

    wiki_page = collector.layout.wiki_root / "pages" / "clip-captured-note.md"
    assert wiki_page.exists()
    wiki_content = wiki_page.read_text(encoding="utf-8")
    assert "captured note" in wiki_content
    assert "web-clipper/notes/capture.md" in wiki_content


def test_web_clipper_collector_fails_closed_when_roots_missing(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()

    with pytest.raises(ValueError, match="do not exist"):
        WebClipperCollector(
            config,
            layout=layout,
            db=MetadataDB(db_path=str(layout.database_path)),
        )


def test_web_clipper_collector_rejects_notes_without_frontmatter(tmp_path: Path):
    collector, raw_root = make_collector(tmp_path)

    note_file = raw_root / "web-clipper" / "notes" / "capture.md"
    _copy_fixture("missing_frontmatter.md", note_file)

    with pytest.raises(ValueError, match="Missing frontmatter"):
        collector.collect()


def test_web_clipper_collector_rejects_attachment_symlink_escape(
    tmp_path: Path,
):
    collector, raw_root = make_collector(tmp_path)

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir(parents=True, exist_ok=True)
    outside_file = outside_dir / "escape.png"
    outside_file.write_bytes(b"binary-png")

    unsafe_link = raw_root / "web-clipper" / "assets" / "escape.png"
    os.symlink(outside_file, unsafe_link)

    with pytest.raises(ValueError, match="escapes the raw root"):
        collector.collect()
