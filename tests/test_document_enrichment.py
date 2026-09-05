import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.document_enrichment import enrich_document, extract_document_abstract
from core.ingestion_runtime import KnowledgeArtifactRuntime
from tests.test_web_clipper_collector import make_collector
from tests.security_hostile_fixtures import hostile_text


def source(tmp_path, *, pdf=False):
    collector, vault = make_collector(tmp_path)
    collector.config.set("sources.web_clipper.queue_pdfs", True)
    collector.config.set("sources.web_clipper.summarize", True)
    path = vault / ("clipper-assets/paper.pdf" if pdf else "Clippings/note.md")
    path.write_bytes(b"%PDF-test" if pdf else b"---\ntitle: Evidence\n---\nUseful source evidence.")
    record = collector.collect()[0]
    return collector, path, record.artifact


def test_long_clipping_title_preserves_unique_valid_wiki_slug(tmp_path):
    collector, vault = make_collector(tmp_path)
    path = vault / 'Clippings/long.md'
    path.write_text('---\ntitle: ' + 'Long research title ' * 20 + '\n---\nEvidence.')
    artifact = collector.collect()[0].artifact
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == 'processed'
    page, = (collector.layout.wiki_root / 'pages').glob('clip-*.md')
    assert len(page.stem) <= 80


def mock_model(monkeypatch, *, content="A grounded summary.", error=None, route=True):
    generate = AsyncMock(return_value=SimpleNamespace(content=content, error=error))
    interface = SimpleNamespace(
        _resolve_task_route=lambda task: ("test", "model", {"max_tokens": 1000}) if route else None,
        generate=generate,
    )
    monkeypatch.setattr("core.document_enrichment.LLMInterface", lambda config: interface)
    return generate


def test_pdf_runtime_extracts_summarizes_persists_without_changing_source(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path, pdf=True)
    before = path.read_bytes()
    collector.config.set("sources.web_clipper.summary_max_chars", 25)
    monkeypatch.setattr("core.document_enrichment.extract_pdf_text", lambda path, max_pages: "A paper about useful evidence. " * 10)
    generate = mock_model(monkeypatch, content="A useful summary that describes the paper.")
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == "processed"
    assert generate.await_count == 1
    payload = json.loads(collector.db.get_ingestion_entry(artifact.id).payload_json)
    summary = payload["custom_metadata"]["document_summary"]
    assert summary["source_checksum"] == artifact.source_checksum
    assert summary["text_truncated"] is True
    assert summary["input_characters"] == 25
    assert summary["provider"] == "test"
    assert payload["body"] == ""
    pages = list((collector.layout.wiki_root / "pages").glob("clip-paper-*.md"))
    assert len(pages) == 1
    assert summary["text"] in pages[0].read_text()
    assert "Partial-source summary" in pages[0].read_text()
    assert path.read_bytes() == before


def test_note_summary_preserves_raw_body_and_reuses_derivative(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path)
    before, body, raw = path.read_bytes(), artifact.body, artifact.raw_content
    generate = mock_model(monkeypatch)
    first = asyncio.run(enrich_document(artifact, collector.config, collector.layout))
    second = asyncio.run(enrich_document(artifact, collector.config, collector.layout))
    assert first["summary_status"] == "generated"
    assert second["summary_status"] == "reused"
    assert generate.await_count == 1
    assert artifact.body == body and artifact.raw_content == raw
    assert path.read_bytes() == before


@pytest.mark.parametrize("mode", ["changed", "symlink", "outside", "oversize"])
def test_unsafe_or_changed_source_never_calls_model(tmp_path, monkeypatch, mode):
    collector, path, artifact = source(tmp_path)
    generate = mock_model(monkeypatch)
    if mode == "changed":
        path.write_text("Changed after collection")
    elif mode == "symlink":
        target = path.with_name("original.md")
        path.rename(target)
        path.symlink_to(target)
    elif mode == "outside":
        artifact.source_path = str(tmp_path / "outside.md")
    else:
        collector.config.set("sources.web_clipper.max_source_bytes", 1)
    with pytest.raises((ValueError, RuntimeError)):
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))
    generate.assert_not_awaited()


@pytest.mark.parametrize("text", ["", hostile_text("hidden_html")])
def test_empty_or_hostile_pdf_goes_to_review_without_model(tmp_path, monkeypatch, text):
    collector, path, artifact = source(tmp_path, pdf=True)
    monkeypatch.setattr("core.document_enrichment.extract_pdf_text", lambda path, max_pages: text)
    generate = mock_model(monkeypatch)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == "needs_review"
    generate.assert_not_awaited()
    assert collector.db.get_ingestion_entry(artifact.id).status == "needs_review"


@pytest.mark.parametrize("kwargs", [{"content": ""}, {"error": "provider down"}, {"route": False}])
def test_model_failures_are_not_successful_summaries(tmp_path, monkeypatch, kwargs):
    collector, path, artifact = source(tmp_path)
    mock_model(monkeypatch, **kwargs)
    with pytest.raises(RuntimeError):
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))
    assert "document_summary" not in artifact.custom_metadata


def test_identical_titles_have_separate_summary_pages(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path)
    path.with_name("other.md").write_bytes(path.read_bytes() + b"\nDistinct evidence.")
    collector.collect()
    mock_model(monkeypatch)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    results = asyncio.run(runtime.process_pending_ingestions_once())
    assert len(results) == 2
    assert all(item.status == "processed" for item in results)
    assert len(list((collector.layout.wiki_root / "pages").glob("clip-evidence-*.md"))) == 2


def test_identical_titles_have_separate_pages_without_summarization(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path)
    collector.config.set("sources.web_clipper.summarize", False)
    path.with_name("other.md").write_bytes(path.read_bytes() + b"\nDistinct evidence.")
    collector.collect()
    generate = mock_model(monkeypatch)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    results = asyncio.run(runtime.process_pending_ingestions_once())
    assert len(results) == 2
    assert len(list((collector.layout.wiki_root / "pages").glob("clip-evidence-*.md"))) == 2
    generate.assert_not_awaited()


def test_deterministic_pdf_abstract_and_full_text_without_model(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path, pdf=True)
    collector.config.set("sources.web_clipper.summarize", False)
    collector.config.set("sources.web_clipper.summary_max_chars", 10)
    text = "Title\nAbstract\nThis paper studies evidence.\n1 Introduction\nFull source text."
    monkeypatch.setattr("core.document_enrichment.extract_pdf_text", lambda path, max_pages: text)
    generate = mock_model(monkeypatch)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == "processed"
    payload = json.loads(collector.db.get_ingestion_entry(artifact.id).payload_json)
    assert payload["custom_metadata"]["document_text"] == text
    assert payload["custom_metadata"]["document_abstract"]["text"] == "This paper studies evidence."
    assert "document_summary" not in payload["custom_metadata"]
    generate.assert_not_awaited()
    page = next((collector.layout.wiki_root / "pages").glob("clip-paper-*.md"))
    assert "Source abstract (extracted, not AI-generated)" in page.read_text()


@pytest.mark.parametrize("text", ["First paragraph with no heading.", "Abstract\nUnbounded section."])
def test_missing_explicit_abstract_is_not_invented(text):
    assert extract_document_abstract(text) is None


@pytest.mark.parametrize("key", ["queue_pdfs", "summarize"])
@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, []])
def test_malformed_opt_ins_never_call_a_model(tmp_path, monkeypatch, key, value):
    collector, path, artifact = source(tmp_path)
    collector.config.set(f"sources.web_clipper.{key}", value)
    generate = mock_model(monkeypatch)
    with pytest.raises(ValueError, match="must be a boolean"):
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))
    generate.assert_not_awaited()


@pytest.mark.parametrize("summarize", [False, True])
def test_source_changed_during_pdf_extraction_never_publishes(tmp_path, monkeypatch, summarize):
    collector, path, artifact = source(tmp_path, pdf=True)
    collector.config.set("sources.web_clipper.summarize", summarize)
    def extract_changing_source(path, max_pages):
        path.write_bytes(b"%PDF-replaced-by-sync")
        return "Abstract\nOld source evidence.\n1 Introduction\nBody."
    monkeypatch.setattr("core.document_enrichment.extract_pdf_text", extract_changing_source)
    generate = mock_model(monkeypatch)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    with pytest.raises(RuntimeError, match="checksum changed"):
        asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    payload = json.loads(collector.db.get_ingestion_entry(artifact.id).payload_json)
    assert "document_extraction" not in payload["custom_metadata"]
    assert "document_summary" not in payload["custom_metadata"]
    assert not list((collector.layout.wiki_root / "pages").glob("clip-paper-*.md"))
    generate.assert_not_awaited()
