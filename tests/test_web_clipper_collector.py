import asyncio
import json
import os
from pathlib import Path

import pytest

from collectors.web_clipper_collector import WebClipperCollector
from collectors.web_clipper_parser import WebClipperMarkdownError
from core.config import Config
from core.connector_budgets import ConnectorBudgetError
from core.ingestion_runtime import IngestionRuntimeError, KnowledgeArtifactRuntime
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.prompt_security import (
    PROMPT_SECURITY_POLICY_NEEDS_REVIEW,
    THOTH_SECURITY_PATTERN_IDS_KEY,
    THOTH_SECURITY_POLICY_KEY,
)
from tests.security_hostile_fixtures import hostile_text

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
    config.set("sources.web_clipper.note_dirs", ["Clippings"])
    config.set("sources.web_clipper.attachment_dirs", ["clipper-assets"])
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

    note_dir = layout.vault_root / "Clippings"
    asset_dir = layout.vault_root / "clipper-assets"
    note_dir.mkdir(parents=True, exist_ok=True)
    asset_dir.mkdir(parents=True, exist_ok=True)

    db = MetadataDB(db_path=str(layout.database_path))
    collector = WebClipperCollector(config, layout=layout, db=db)
    return collector, layout.vault_root


def test_pdf_opt_in_queues_previously_indexed_attachment(tmp_path):
    collector, vault = make_collector(tmp_path)
    pdf = vault / "clipper-assets" / "paper.pdf"
    pdf.write_bytes(b"%PDF-test")
    assert collector.collect()[0].would_queue is False
    assert collector.db.get_ingestion_entry("webclip:clipper-assets/paper.pdf") is None
    collector.config.set("sources.web_clipper.queue_pdfs", True)
    assert collector.plan()[0].would_queue is True
    assert collector.collect()[0].would_queue is True
    assert collector.db.get_ingestion_entry("webclip:clipper-assets/paper.pdf").status == "pending"
    assert collector.collect()[0].would_queue is False
    assert pdf.read_bytes() == b"%PDF-test"


@pytest.mark.parametrize("pdf", [False, True])
def test_failed_queue_does_not_mark_file_indexed_and_retries(tmp_path, monkeypatch, pdf):
    collector, vault = make_collector(tmp_path)
    collector.config.set("sources.web_clipper.queue_pdfs", True)
    path = vault / ("clipper-assets/paper.pdf" if pdf else "Clippings/note.md")
    path.write_bytes(b"%PDF-test" if pdf else b"---\ntitle: Useful note\n---\nUseful note\n")
    original = collector.capture_queue.queue_artifact
    def fail(*args, **kwargs):
        raise RuntimeError("queue unavailable")
    monkeypatch.setattr(collector.capture_queue, "queue_artifact", fail)
    with pytest.raises(RuntimeError, match="queue unavailable"):
        collector.collect()
    assert collector.db.get_file_entry(str(path)) is None
    monkeypatch.setattr(collector.capture_queue, "queue_artifact", original)
    assert collector.collect()[0].would_queue is True


def test_changed_processed_note_is_pending_again(tmp_path):
    collector, vault = make_collector(tmp_path)
    note = vault / "Clippings/note.md"
    note.write_text("---\ntitle: First version\n---\nFirst version\n")
    artifact_id = collector.collect()[0].artifact.id
    collector.db.mark_ingestion_processed(artifact_id)
    note.write_text("---\ntitle: New version\n---\nNew version\n")
    collector.collect()
    assert collector.db.get_ingestion_entry(artifact_id).status == "pending"


def test_bounded_collection_skips_unchanged_and_counts_pdf_backfill(tmp_path):
    collector, vault = make_collector(tmp_path)
    (vault / "Clippings/a.md").write_text("---\ntitle: Existing\n---\nExisting\n")
    collector.collect()
    (vault / "Clippings/b.md").write_text("---\ntitle: New\n---\nNew\n")
    (vault / "Clippings/c.md").write_text("---\ntitle: Also new\n---\nAlso new\n")
    records = collector.collect(limit=1)
    assert [r.path.name for r in records] == ["b.md"]
    assert records[0].would_queue is True
    assert [r.path.name for r in collector.collect(limit=1)] == ["c.md"]


def test_bad_source_does_not_starve_valid_source_in_bounded_scan(tmp_path):
    collector, vault = make_collector(tmp_path)
    (vault / "Clippings/a-invalid.md").write_text("Missing frontmatter")
    (vault / "Clippings/b-valid.md").write_text("---\ntitle: Valid\n---\nUseful evidence\n")
    with pytest.raises(ValueError, match="completed 1 files with 1 errors"):
        collector.collect(limit=1)
    assert collector.db.get_ingestion_entry("webclip:Clippings/b-valid.md") is not None
    assert collector.last_scan_errors[0]["path"].endswith("a-invalid.md")
    assert collector.db.get_file_entry(str(vault / "Clippings/a-invalid.md")) is None


@pytest.mark.parametrize("key", ["queue_pdfs", "summarize"])
@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, []])
def test_malformed_opt_ins_never_collect_or_plan(tmp_path, key, value):
    collector, vault = make_collector(tmp_path)
    collector.config.set(f"sources.web_clipper.{key}", value)
    path = vault / "Clippings/note.md"
    path.write_text("---\ntitle: Note\n---\nEvidence\n")
    with pytest.raises(ValueError, match="must be a boolean"):
        collector.plan()
    with pytest.raises(ValueError, match="must be a boolean"):
        collector.collect()
    assert collector.db.get_file_entry(str(path)) is None
    assert collector.db.get_ingestion_entry("webclip:Clippings/note.md") is None


@pytest.mark.parametrize("status", ["pending", "processing"])
def test_changed_active_source_keeps_old_queue_payload_and_file_hash(tmp_path, status):
    collector, vault = make_collector(tmp_path)
    path = vault / "Clippings/note.md"
    path.write_text("---\ntitle: Note\n---\nOld evidence\n")
    artifact_id = collector.collect()[0].artifact.id
    if status == "processing":
        collector.db.mark_ingestion_processing(artifact_id)
    before_entry = collector.db.get_ingestion_entry(artifact_id)
    before_hash = collector.db.get_file_entry(str(path)).hash
    path.write_text("---\ntitle: Note\n---\nNew evidence\n")
    new_path = vault / "Clippings/z-new.md"
    new_path.write_text("---\ntitle: New\n---\nIndependent evidence\n")
    records = collector.collect(limit=1)
    assert len(records) == 1
    assert records[0].path == new_path
    assert collector.last_deferred_sources == [str(path)]
    assert collector.db.get_ingestion_entry(artifact_id).payload_json == before_entry.payload_json
    assert collector.db.get_file_entry(str(path)).hash == before_hash
    collector.db.mark_ingestion_processed(artifact_id)
    collector.collect()
    assert collector.db.get_ingestion_entry(artifact_id).status == "pending"
    assert collector.db.get_file_entry(str(path)).hash != before_hash


def test_changed_terminal_failed_source_is_retryable(tmp_path):
    collector, vault = make_collector(tmp_path)
    path = vault / "Clippings/note.md"
    path.write_text("---\ntitle: Note\n---\nOld evidence\n")
    artifact_id = collector.collect()[0].artifact.id
    collector.db.mark_ingestion_processing(artifact_id)
    collector.db.mark_ingestion_failed(artifact_id, "exhausted retries", max_attempts=1)
    assert collector.db.get_ingestion_entry(artifact_id).status == "failed"
    path.write_text("---\ntitle: Note\n---\nCorrected evidence\n")
    collector.collect()
    refreshed = collector.db.get_ingestion_entry(artifact_id)
    assert refreshed.status == "pending"
    assert refreshed.attempts == 0
    assert refreshed.last_error is None


@pytest.mark.parametrize("reject", [False, True])
def test_changed_reviewed_source_preserves_operator_review_state(tmp_path, reject):
    collector, vault = make_collector(tmp_path)
    path = vault / "Clippings/note.md"
    path.write_text("---\ntitle: Note\n---\nOriginal evidence\n")
    artifact_id = collector.collect()[0].artifact.id
    collector.db.mark_ingestion_review_required(
        artifact_id, category="security_policy", reason="operator review required"
    )
    if reject:
        collector.db.reject_ingestion_review(artifact_id, actor="test", reason="rejected")
    expected = collector.db.get_ingestion_entry(artifact_id).status
    path.write_text("---\ntitle: Note\n---\nChanged evidence\n")
    collector.collect()
    assert collector.db.get_ingestion_entry(artifact_id).status == expected


def test_web_clipper_collector_indexes_allowlisted_roots_only(tmp_path: Path):
    collector, vault_root = make_collector(tmp_path)

    note_file = vault_root / "Clippings" / "capture.md"
    ignored_note_file = vault_root / "Clippings" / "capture.txt"
    attachment_file = vault_root / "clipper-assets" / "capture_attachment.pdf"
    ignored_attachment_file = vault_root / "clipper-assets" / "image.md"

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
        collector.layout.vault_root / "clipper-assets" / "capture_attachment.pdf"
    )
    assert attachment_record.managed_path == managed_attachment
    assert attachment_record.artifact.output_paths["vault"] == str(managed_attachment)
    assert managed_attachment.exists()
    assert managed_attachment.read_bytes() == attachment_file.read_bytes()
    assert attachment_file.exists()
    assert collector.db.get_file_entry(str(note_file)).file_type == "note"
    assert collector.db.get_file_entry(str(attachment_file)).file_type == "attachment"
    assert collector.db.get_file_entry(str(ignored_note_file)) is None
    assert collector.db.get_file_entry(str(ignored_attachment_file)) is None


def test_web_clipper_collector_reindexes_changed_files(tmp_path: Path):
    collector, vault_root = make_collector(tmp_path)

    note_file = vault_root / "Clippings" / "capture.md"
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
    collector.db.mark_ingestion_processed(first_pass[0].artifact.id)

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


def test_web_clipper_plan_and_collect_share_note_decode_errors(tmp_path: Path):
    collector, vault_root = make_collector(tmp_path)
    note_file = vault_root / "Clippings" / "invalid.md"
    note_file.write_bytes(b"\xff\xfe\x00")

    with pytest.raises(WebClipperMarkdownError, match="Failed to decode"):
        collector.plan()
    with pytest.raises(WebClipperMarkdownError, match="Failed to decode"):
        collector.collect()


def test_web_clipper_collector_queues_notes_for_shared_runtime(
    tmp_path: Path,
):
    collector, vault_root = make_collector(tmp_path)

    note_file = vault_root / "Clippings" / "capture.md"
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
    queue_entry = collector.db.get_ingestion_entry("webclip:Clippings/capture.md")
    assert queue_entry is not None
    assert queue_entry.artifact_type == "web_clipper"
    assert queue_entry.status == "pending"

    runtime = KnowledgeArtifactRuntime(layout=collector.layout, db=collector.db)
    results = asyncio.run(runtime.process_pending_ingestions_once())

    assert len(results) == 1
    assert results[0].artifact_type == "web_clipper"
    assert collector.db.get_ingestion_entry("webclip:Clippings/capture.md").status == "processed"

    wiki_page = next((collector.layout.wiki_root / "pages").glob("clip-captured-note-*.md"))
    assert wiki_page.exists()
    wiki_content = wiki_page.read_text(encoding="utf-8")
    assert "captured note" in wiki_content
    assert "Clippings/capture.md" in wiki_content


def test_web_clipper_collector_quarantines_hostile_fixture(tmp_path: Path):
    collector, vault_root = make_collector(tmp_path)

    note_file = vault_root / "Clippings" / "hostile-hidden-html.md"
    note_file.write_text(
        "---\n"
        "title: Hostile Hidden HTML\n"
        "url: https://example.com/hostile-hidden-html\n"
        "lang: en\n"
        "---\n"
        "\n"
        "# Hostile Hidden HTML\n\n"
        f"{hostile_text('hidden_html')}\n",
        encoding="utf-8",
    )

    discovered = collector.collect()

    assert len(discovered) == 1
    entry = collector.db.get_ingestion_entry(
        "webclip:Clippings/hostile-hidden-html.md"
    )
    assert entry is not None
    assert entry.status == "needs_review"

    payload = json.loads(entry.payload_json)
    metadata = payload["normalized_metadata"]
    assert "hidden_html_payload" in metadata[THOTH_SECURITY_PATTERN_IDS_KEY]
    assert metadata[THOTH_SECURITY_POLICY_KEY]["status"] == (
        PROMPT_SECURITY_POLICY_NEEDS_REVIEW
    )

    runtime = KnowledgeArtifactRuntime(layout=collector.layout, db=collector.db)
    with pytest.raises(IngestionRuntimeError, match="security review"):
        asyncio.run(runtime.process_ingestion_entry(entry))
    assert not list((collector.layout.wiki_root / "pages").glob("clip-hostile-hidden-html-*.md"))


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


def test_web_clipper_collector_stops_when_file_budget_exceeded(tmp_path: Path):
    collector, vault_root = make_collector(tmp_path)
    collector.config.set(
        "connectors.budgets.per_connector.web_clipper.max_files_per_run",
        1,
    )

    note_file = vault_root / "Clippings" / "capture.md"
    attachment_file = vault_root / "clipper-assets" / "capture_attachment.pdf"
    _copy_fixture("capture_note.md", note_file)
    _copy_fixture("capture_attachment.pdf", attachment_file)

    with pytest.raises(ConnectorBudgetError, match="max_files_per_run"):
        collector.collect()

    assert collector.db.list_ingestion_entries(limit=10) == []


def test_web_clipper_collector_rejects_notes_without_frontmatter(tmp_path: Path):
    collector, vault_root = make_collector(tmp_path)

    note_file = vault_root / "Clippings" / "capture.md"
    _copy_fixture("missing_frontmatter.md", note_file)

    with pytest.raises(ValueError, match="Missing frontmatter"):
        collector.collect()


def test_web_clipper_collector_rejects_attachment_symlink_escape(
    tmp_path: Path,
):
    collector, vault_root = make_collector(tmp_path)

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir(parents=True, exist_ok=True)
    outside_file = outside_dir / "escape.png"
    outside_file.write_bytes(b"binary-png")

    unsafe_link = vault_root / "clipper-assets" / "escape.png"
    os.symlink(outside_file, unsafe_link)

    with pytest.raises(ValueError, match="escapes the vault root"):
        collector.collect()
