import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import zipfile

import pytest

from collectors.inbox_connector import InboxConnector
from collectors.corpus_index_connector import CorpusIndexConnector
from collectors.web_clipper_collector import WebClipperCollector
from collectors.inbox_files import InboxFileError, immutable_write
from core.connector_registry import load_connector_registry
from core.connector_runners import ConnectorRunContext, connector_run_handler
from core.connector_scheduler import resolve_connector_schedules
from core.corpus_query import query_corpus
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.path_layout import build_path_layout
from tests.test_archivist_retrieval import make_config
from tests.security_hostile_fixtures import hostile_text


def setup_inbox(tmp_path, monkeypatch, *, consume=False):
    config, db = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    inbox = layout.vault_root / "inbox"
    inbox.mkdir()
    config.set("sources.inbox.enabled", True)
    config.set("sources.inbox.directory", str(inbox))
    config.set("sources.inbox.stable_seconds", 10)
    config.set("sources.inbox.consume", consume)
    clock = [2_000_000_000.0]
    monkeypatch.setattr("collectors.inbox_connector.time.time", lambda: clock[0])
    handler = connector_run_handler(load_connector_registry(config).get("inbox"),
                                    ConnectorRunContext(config, layout, db))
    return config, db, layout, inbox, clock, handler


def ready(path, content):
    path.write_bytes(content)
    os.utime(path, (1_999_999_000, 1_999_999_000))


def two_scans(handler, clock):
    first = handler({})
    clock[0] += 11
    return first, handler({})


def test_manifest_to_queue_to_runtime_and_no_own_write_loop(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    ready(inbox / "research.md", b"# Evidence\nUseful research about speech recognition.")
    first, second = two_scans(handler, clock)
    assert first["deferred_count"] == 1 and first["queued_count"] == 0
    assert second["queued_count"] == 1
    record = second["records"][0]
    destination = Path(record["destination"])
    assert destination.parent == layout.vault_root / "documents"
    assert destination.read_bytes() == (inbox / "research.md").read_bytes()
    row = db.get_ingestion_entry(record["artifact_id"])
    payload = json.loads(row.payload_json)
    assert payload["source_type"] == "inbox"
    assert payload["custom_metadata"]["document_text"].startswith("# Evidence")
    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    result = asyncio.run(runtime.process_ingestion_entry(row))
    assert result.status in {"skipped", "processed"}
    assert db.get_ingestion_entry(row.artifact_id).status in {"skipped", "processed"}
    assert handler({})["queued_count"] == 0
    assert len(list((layout.vault_root / "documents").iterdir())) == 1


def test_opt_in_consumption_archives_bytes_outside_vault_and_has_receipt(tmp_path, monkeypatch):
    _config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    content = b"# Useful source\nOriginal bytes stay recoverable."
    original = inbox / "source.md"
    ready(original, content)
    _, result = two_scans(handler, clock)
    assert result["consumed_count"] == 1
    assert not original.exists()
    archive = layout.system_root / "inbox" / "archive" / (hashlib.sha256(content).hexdigest() + ".md")
    assert archive.read_bytes() == content
    assert not archive.is_relative_to(layout.vault_root)
    key = "inbox:file:" + hashlib.sha256(str(original).encode()).hexdigest()
    receipt = db.get_automation_state(key)
    assert receipt["status"] == "consumed"
    assert Path(receipt["destination"]).read_bytes() == content
    assert handler({})["records"] == []


def test_copy_in_progress_needs_two_new_observations(tmp_path, monkeypatch):
    _config, _db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    path = inbox / "copy.txt"
    ready(path, b"first")
    assert handler({})["deferred_count"] == 1
    clock[0] += 11
    ready(path, b"still copying")
    assert handler({})["deferred_count"] == 1
    clock[0] += 11
    assert handler({})["queued_count"] == 1


@pytest.mark.parametrize("filename,content", [("unknown.exe", b"binary"), ("broken.pdf", b"not a PDF"),
                                                ("empty.txt", b""), ("broken.docx", b"not zip")])
def test_broken_or_unsupported_are_visible_review_and_never_consumed(tmp_path, monkeypatch, filename, content):
    _config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    path = inbox / filename
    ready(path, content)
    _, result = two_scans(handler, clock)
    assert result["review_count"] == 1
    assert path.read_bytes() == content
    row = db.get_ingestion_entry(result["records"][0]["artifact_id"])
    assert row.status == "needs_review"
    assert "inbox_input" in row.review_json
    assert not list((layout.system_root / "inbox").glob("archive/*"))


def test_docx_main_body_locally_extracted_without_markdown_sidecar(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("word/document.xml", '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>ASR evidence</w:t></w:r></w:p></w:body></w:document>')
    ready(inbox / "notes.docx", output.getvalue())
    _, result = two_scans(handler, clock)
    payload = json.loads(db.get_ingestion_entry(result["records"][0]["artifact_id"]).payload_json)
    assert payload["body"] == "ASR evidence"
    assert payload["custom_metadata"]["document_extraction"]["coverage"] == "document_body_only"
    assert Path(payload["source_path"]).parent == layout.vault_root / "documents"
    assert not list(layout.vault_root.rglob("*.md"))
    config.set("sources.corpus_index.enabled", True)
    config.set("sources.corpus_index.include_roots", ["documents"])
    config.set("sources.corpus_index.embeddings_enabled", False)
    indexed = asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())
    assert indexed["indexed_count"] == 1 and indexed["keyword_content_count"] == 1
    found = asyncio.run(query_corpus(config=config, layout=layout, db=db, query="ASR"))
    assert len(found["results"]) == 1
    assert found["results"][0]["path"] == payload["source_path"]


def test_duplicate_content_does_not_reset_processed_queue_state(tmp_path, monkeypatch):
    _config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    ready(inbox / "one.md", b"same")
    _, result = two_scans(handler, clock)
    row = db.get_ingestion_entry(result["records"][0]["artifact_id"])
    row.status = "processed"
    assert db.upsert_ingestion_entry(row)
    ready(inbox / "two.md", b"same")
    two_scans(handler, clock)
    assert db.get_ingestion_entry(row.artifact_id).status == "processed"
    assert len(list((layout.vault_root / "documents").iterdir())) == 1


def test_symlinks_and_same_destination_conflict_fail_closed(tmp_path, monkeypatch):
    _config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    outside = tmp_path / "private.md"
    outside.write_text("Never read me")
    (inbox / "link.md").symlink_to(outside)
    result = handler({})
    row = db.get_ingestion_entry(result["records"][0]["artifact_id"])
    assert row.status == "needs_review" and "Never read me" not in row.payload_json
    existing = layout.vault_root / "conflict.md"
    existing.write_bytes(b"user bytes")
    with pytest.raises(InboxFileError, match="different content"):
        immutable_write(existing, b"new bytes")
    assert existing.read_bytes() == b"user bytes"


def test_replacement_during_archive_receipt_is_retained(tmp_path, monkeypatch):
    _config, db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    original = inbox / "race.md"
    ready(original, b"old source")
    upsert = db.upsert_automation_state
    def replace_at_receipt(key, payload):
        upsert(key, payload)
        if payload.get("status") == "archived":
            original.write_bytes(b"new sync revision")
    monkeypatch.setattr(db, "upsert_automation_state", replace_at_receipt)
    _, result = two_scans(handler, clock)
    assert result["consumed_count"] == 0 and result["review_count"] == 1
    assert original.read_bytes() == b"new sync revision"


def test_default_disabled_and_schedule_uses_registry(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    config.set("sources.inbox.enabled", False)
    with pytest.raises(ValueError, match="disabled"):
        handler({})
    config.set("sources.inbox.enabled", True)
    config.set("sources.inbox.schedule", {"enabled": True, "interval_seconds": 60, "run_on_startup": True})
    assert any(item.connector_name == "inbox" for item in resolve_connector_schedules(config))


def test_configuration_cannot_watch_its_destinations(tmp_path, monkeypatch):
    config, _db, layout, _inbox, _clock, handler = setup_inbox(tmp_path, monkeypatch)
    config.set("sources.inbox.directory", str(layout.vault_root))
    with pytest.raises(ValueError, match="separate"):
        handler({})


def test_security_review_is_not_consumed(tmp_path, monkeypatch):
    _config, db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    source = inbox / "hostile.md"
    ready(source, hostile_text("fake_citations").encode())
    _, result = two_scans(handler, clock)
    assert result["consumed_count"] == 0
    assert source.exists()
    assert db.get_ingestion_entry(result["records"][0]["artifact_id"]).status == "needs_review"


def test_destination_symlink_does_not_write_outside_vault(tmp_path, monkeypatch):
    _config, _db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    outside = tmp_path / "outside"
    outside.mkdir()
    (layout.vault_root / "documents").symlink_to(outside, target_is_directory=True)
    ready(inbox / "input.md", b"A useful note")
    _, result = two_scans(handler, clock)
    assert result["review_count"] == 1
    assert list(outside.iterdir()) == []


def test_source_changed_during_extraction_not_published(tmp_path, monkeypatch):
    import collectors.inbox_connector as module
    _config, _db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    source = inbox / "changing.txt"
    ready(source, b"original")
    extract = module.extract_snapshot
    def change_source(*args, **kwargs):
        source.write_bytes(b"new revision")
        return extract(*args, **kwargs)
    monkeypatch.setattr(module, "extract_snapshot", change_source)
    _, result = two_scans(handler, clock)
    assert result["review_count"] == 1
    assert not (layout.vault_root / "documents").exists()


def test_queue_failure_preserves_inbox_original(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    original = inbox / "queue-failure.md"
    ready(original, b"useful source")
    two = InboxConnector(config, layout=layout, db=db)
    def fail(*args, **kwargs):
        raise RuntimeError("Queue unavailable")
    monkeypatch.setattr(two.queue, "queue_artifact", fail)
    two.collect()
    clock[0] += 11
    with pytest.raises(RuntimeError, match="Queue unavailable"):
        two.collect()
    assert original.read_bytes() == b"useful source"


def test_oversized_docx_xml_is_held_for_review(tmp_path, monkeypatch):
    config, db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", "x" * (6 * 1024 * 1024))
    ready(inbox / "compressed.docx", output.getvalue())
    _, result = two_scans(handler, clock)
    assert result["review_count"] == 1
    assert db.get_ingestion_entry(result["records"][0]["artifact_id"]).status == "needs_review"


def test_new_revision_at_original_name_after_claim_is_never_removed(tmp_path, monkeypatch):
    _config, _db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    source = inbox / "new-revision.txt"
    ready(source, b"old revision")
    rename = Path.rename
    def replace_after_claim(path, target):
        result = rename(path, target)
        if path == source:
            source.write_bytes(b"new sync revision")
        return result
    monkeypatch.setattr(Path, "rename", replace_after_claim)
    _, result = two_scans(handler, clock)
    assert result["consumed_count"] == 1
    assert source.read_bytes() == b"new sync revision"
    assert handler({})["deferred_count"] == 1


def test_connector_budget_failure_is_explicit_and_preserves_source(tmp_path, monkeypatch):
    from core.connector_budgets import ConnectorBudgetError
    config, _db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    source = inbox / "bounded.txt"
    ready(source, b"more than four bytes")
    config.set("sources.inbox.budgets", {"max_bytes_per_file": 4})
    handler({})
    clock[0] += 11
    with pytest.raises(ConnectorBudgetError):
        handler({})
    assert source.read_bytes() == b"more than four bytes"


def test_managed_reconciliation_preserves_inbox_identity_but_captures_user_edits(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    ready(inbox / "source.md", b"---\ntitle: Useful source\n---\n# Useful source\nEvidence to retain.")
    _, result = two_scans(handler, clock)
    artifact_id = result["records"][0]["artifact_id"]
    destination = Path(result["records"][0]["destination"])
    config.set("sources.web_clipper.note_dirs", ["documents"])
    config.set("sources.web_clipper.attachment_dirs", ["pdfs"])
    (layout.vault_root / "pdfs").mkdir()
    collector = WebClipperCollector(config, layout=layout, db=db)
    records = collector.collect()
    assert len(records) == 1
    assert not records[0].would_queue and not records[0].is_new_or_changed
    assert db.get_file_entry(str(destination)).source_id == artifact_id
    assert db.get_ingestion_entry(artifact_id).status == "pending"
    destination.write_text("---\ntitle: Human revision\n---\n# Human revision\nNew evidence I deliberately added.")
    changed = collector.collect()
    assert changed[0].is_new_or_changed and changed[0].would_queue
    row = db.get_ingestion_entry("webclip:" + destination.relative_to(layout.vault_root).as_posix())
    assert row is not None
    assert "Human revision" in row.payload_json


def test_managed_reconciliation_does_not_bypass_held_inbox_review(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    ready(inbox / "hostile.md", ("---\ntitle: Hostile\n---\n" + hostile_text("fake_citations")).encode())
    _, result = two_scans(handler, clock)
    artifact_id = result["records"][0]["artifact_id"]
    config.set("sources.web_clipper.note_dirs", ["documents"])
    config.set("sources.web_clipper.attachment_dirs", ["pdfs"])
    (layout.vault_root / "pdfs").mkdir()
    records = WebClipperCollector(config, layout=layout, db=db).collect()
    assert not records[0].would_queue
    assert db.get_ingestion_entry(artifact_id).status == "needs_review"


def test_pdf_inbox_uses_real_extraction_and_reconciliation_does_not_copy_again(tmp_path, monkeypatch):
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    stream = b"BT /F1 12 Tf 72 720 Td (Speech recognition evidence) Tj ET"
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>",
               b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
               b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"]
    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for number, content in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf += str(number).encode() + b" 0 obj\n" + content + b"\nendobj\n"
    xref_offset = len(pdf)
    pdf += b"xref\n0 6\n0000000000 65535 f \n"
    pdf += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    pdf += b"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n" + str(xref_offset).encode() + b"\n%%EOF\n"
    ready(inbox / "paper.pdf", pdf)
    _, result = two_scans(handler, clock)
    assert result["queued_count"] == 1
    artifact_id = result["records"][0]["artifact_id"]
    payload = json.loads(db.get_ingestion_entry(artifact_id).payload_json)
    assert payload["body"].strip()
    assert payload["custom_metadata"]["document_extraction"]["coverage"] == "bounded_pdf_excerpt"
    (layout.vault_root / "documents").mkdir()
    config.set("sources.web_clipper.note_dirs", ["documents"])
    config.set("sources.web_clipper.attachment_dirs", ["pdfs"])
    config.set("sources.web_clipper.queue_pdfs", True)
    records = WebClipperCollector(config, layout=layout, db=db).collect()
    assert len(records) == 1 and not records[0].would_queue and not records[0].would_stage
    assert records[0].managed_path == Path(payload["source_path"])


def test_docx_utf16_entity_declaration_rejected():
    from core.docx_text import DOCXTextExtractionError, extract_docx_text
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("word/document.xml", '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE x [<!ENTITY boom "unsafe">]><x>&boom;</x>'.encode("utf-16"))
    with pytest.raises(DOCXTextExtractionError, match="entity declarations"):
        extract_docx_text(output.getvalue())


def test_retained_accepted_inputs_do_not_spend_budget_or_starve_new_input(tmp_path, monkeypatch):
    config, _db, _layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch)
    ready(inbox / "aaa-old.txt", b"old retained input larger than new per-file budget")
    _, first = two_scans(handler, clock)
    assert first["queued_count"] == 1
    config.set("sources.inbox.budgets", {"max_bytes_per_file": 4, "max_bytes_per_run": 4})
    ready(inbox / "zzz-new.txt", b"new")
    _, new = two_scans(handler, clock)
    assert new["reused_count"] == 1 and new["queued_count"] == 1
