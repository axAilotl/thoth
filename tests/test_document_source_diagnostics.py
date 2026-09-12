"""Source integrity regressions through validation and real PDF extraction."""

import asyncio
from subprocess import CompletedProcess, TimeoutExpired
from unittest.mock import AsyncMock, Mock

import pytest

from core.document_enrichment import enrich_document
from core.pdf_text import PDFTextExtractionError, SourceIntegrityError, extract_pdf_text, extract_pdf_title
from tests.test_web_clipper_collector import make_collector


def captured_pdf(tmp_path, content):
    collector, vault = make_collector(tmp_path)
    collector.config.set("sources.web_clipper.queue_pdfs", True)
    collector.config.set("sources.web_clipper.summarize", True)
    path = vault / "clipper-assets/paper.pdf"
    path.write_bytes(content)
    artifact = collector.collect()[0].artifact
    return collector, path, artifact


def test_html_pdf_is_rejected_before_extraction_or_model(tmp_path, monkeypatch):
    content = b"<!DOCTYPE html><html><body>Download failed</body></html>"
    collector, path, artifact = captured_pdf(tmp_path, content)
    poppler = Mock(return_value=CompletedProcess([], 0, "", ""))
    model = AsyncMock(side_effect=AssertionError("Invalid source reached model"))
    monkeypatch.setattr("core.pdf_text.subprocess.run", poppler)
    monkeypatch.setattr("core.llm_interface.LLMInterface.generate", model)

    with pytest.raises(RuntimeError, match="HTML") as error:
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))

    assert error.value.source_status == "html"
    poppler.assert_not_called()
    model.assert_not_awaited()
    assert path.read_bytes() == content


def test_poppler_rejection_is_malformed_source_without_model(tmp_path, monkeypatch):
    content = b"%PDF-1.7\nTruncated download without a trailer or xref table\n"
    collector, path, artifact = captured_pdf(tmp_path, content)
    model = AsyncMock(side_effect=AssertionError("Malformed PDF reached model"))
    monkeypatch.setattr("core.llm_interface.LLMInterface.generate", model)

    with pytest.raises(SourceIntegrityError) as error:
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))

    assert error.value.source_status == "malformed_pdf"
    assert "xref" in str(error.value).lower()
    assert "rescan" in error.value.reason_summary
    model.assert_not_awaited()
    assert "document_text" not in artifact.custom_metadata
    assert path.read_bytes() == content


@pytest.mark.parametrize("failure,detail", [
    (FileNotFoundError("pdftotext"), "Missing required PDF utility"),
    (PermissionError("execution denied"), "execution denied"),
    (TimeoutExpired("pdftotext", 30), "timed out"),
    (CompletedProcess([], 2, "", "Couldn't open output file"), "Couldn't open output file"),
    (CompletedProcess([], 1, "", "Couldn't open file: Permission denied"), "Permission denied"),
    (CompletedProcess([], 3, "", "Incorrect password"), "Incorrect password"),
    (CompletedProcess([], -9, "", ""), "status -9"),
    (CompletedProcess([], 99, "", "unrecognized processing failure"), "unrecognized processing failure"),
])
def test_pdf_infrastructure_failures_keep_their_error(tmp_path, monkeypatch, failure, detail):
    path = tmp_path / "source.pdf"
    path.write_bytes(b"%PDF-1.7\n")
    poppler = Mock(side_effect=failure) if isinstance(failure, Exception) else Mock(return_value=failure)
    monkeypatch.setattr("core.pdf_text.subprocess.run", poppler)

    with pytest.raises(PDFTextExtractionError) as error:
        extract_pdf_text(path)

    assert not isinstance(error.value, SourceIntegrityError)
    assert detail in str(error.value)
    assert path.read_bytes() == b"%PDF-1.7\n"


@pytest.mark.parametrize("extract", [extract_pdf_text, extract_pdf_title])
@pytest.mark.parametrize("content,status", [
    (None, "missing"), (b"", "empty"),
    (b"\xef\xbb\xbf \n<!DOCTYPE html><html>Denied</html>", "html"),
    (b"<!-- proxy wrapper -->\n<!DOCTYPE html><html>Denied</html>", "html"),
    (b"<head><title>Download denied</title></head>", "html"),
    (b"<html lang='en'>Download page</html>", "html"),
    (b"Not a PDF document", "non_pdf"),
])
def test_invalid_pdf_bytes_never_reach_poppler(tmp_path, monkeypatch, extract, content, status):
    path = tmp_path / "source.pdf"
    if content is not None:
        path.write_bytes(content)
    poppler = Mock(return_value=CompletedProcess([], 0, "", ""))
    monkeypatch.setattr("core.pdf_text.subprocess.run", poppler)

    with pytest.raises(RuntimeError) as error:
        extract(path)

    assert error.value.source_status == status
    assert isinstance(error.value, PDFTextExtractionError)
    poppler.assert_not_called()
    if content is not None:
        assert path.read_bytes() == content


def test_pdf_header_allows_transport_whitespace_before_signature(tmp_path, monkeypatch):
    path = tmp_path / "source.pdf"
    path.write_bytes(b"\xef\xbb\xbf  \n%PDF-1.7\n")
    poppler = Mock(return_value=CompletedProcess([], 0, "text", ""))
    monkeypatch.setattr("core.pdf_text.subprocess.run", poppler)

    assert extract_pdf_text(path) == "text"
    poppler.assert_called_once()


@pytest.mark.parametrize("content,status", [(None, "missing"), (b"", "empty"),
                                          (b"Download unavailable", "non_pdf")])
def test_document_integrity_failure_precedes_extraction(tmp_path, monkeypatch, content, status):
    collector, path, artifact = captured_pdf(tmp_path, b"%PDF-test")
    if content is None:
        path.unlink()
    else:
        path.write_bytes(content)
    poppler = Mock(return_value=CompletedProcess([], 0, "", ""))
    model = AsyncMock(side_effect=AssertionError("Invalid source reached model"))
    monkeypatch.setattr("core.pdf_text.subprocess.run", poppler)
    monkeypatch.setattr("core.llm_interface.LLMInterface.generate", model)

    with pytest.raises(RuntimeError) as error:
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))

    assert error.value.source_status == status
    poppler.assert_not_called()
    model.assert_not_awaited()


@pytest.mark.parametrize('output,changed', [
    (CompletedProcess([], 0, '', ''), None),
    (CompletedProcess([], 1, '', "Syntax Error: Couldn't read xref table"), b'%PDF-new capture'),
])
def test_source_changed_during_failed_or_empty_extraction_is_revalidated(
    tmp_path, monkeypatch, output, changed,
):
    collector, path, artifact = captured_pdf(tmp_path, b'%PDF-1.7\n')

    def run(*args, **kwargs):
        if changed is None:
            path.unlink()
        else:
            path.write_bytes(changed)
        return output

    monkeypatch.setattr('core.pdf_text.subprocess.run', run)
    with pytest.raises(RuntimeError) as error:
        asyncio.run(enrich_document(artifact, collector.config, collector.layout))

    if changed is None:
        assert error.value.source_status == 'missing'
    else:
        assert 'checksum changed' in str(error.value)
        assert not isinstance(error.value, SourceIntegrityError)
    assert 'document_text' not in artifact.custom_metadata
