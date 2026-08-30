"""Tests for the paper-grade PDF processor."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from processors.pdf_processor import PDFDocument, PDFProcessor


@pytest.fixture
def processor(tmp_path):
    return PDFProcessor(output_dir=str(tmp_path))


def make_tweet(text: str, tweet_id: str = "t-1"):
    return SimpleNamespace(id=tweet_id, full_text=text, url_mappings=[])


def test_extract_urls_from_tweet_skips_arxiv(processor):
    tweet = make_tweet("Read https://arxiv.org/pdf/2604.12345.pdf and https://example.com/file.pdf")
    urls = processor.extract_urls_from_tweet(tweet)

    assert urls == ["https://example.com/file.pdf"]


def test_extract_urls_from_tweet_maps_expanded_urls(processor):
    tweet = make_tweet("Short link")
    tweet.url_mappings = [
        SimpleNamespace(expanded_url="https://example.com/paper.pdf"),
        SimpleNamespace(expanded_url="https://example.com/not-a-pdf"),
    ]
    urls = processor.extract_urls_from_tweet(tweet)

    assert urls == ["https://example.com/paper.pdf"]


def test_download_document_uses_title_override(processor, tmp_path):
    def fake_download(url, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4 fake")
        return True

    processor._download_file = fake_download
    doc = processor.download_document(
        "https://example.com/ugly-name.pdf",
        "tweet-1",
        title_override="A Beautiful Paper Title",
    )

    assert doc is not None
    assert doc.title == "A Beautiful Paper Title"
    assert doc.downloaded is True
    assert doc.filename == "A Beautiful Paper Title.pdf"
    assert (tmp_path / "pdfs" / "A Beautiful Paper Title.pdf").exists()


def test_download_document_adds_filename_prefix(processor, tmp_path):
    def fake_download(url, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4 fake")
        return True

    processor._download_file = fake_download
    doc = processor.download_document(
        "https://example.com/whitepaper.pdf",
        "tweet-2",
        filename_prefix="preprint",
    )

    assert doc is not None
    assert doc.filename.startswith("preprint-")
    assert doc.downloaded is True


def test_download_document_respects_existing_file(processor, tmp_path):
    # Title extraction capitalizes the stem, so the expected filename is "Existing.pdf".
    pdf_path = tmp_path / "pdfs" / "Existing.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path.write_bytes(b"%PDF-1.4 existing")

    with patch.object(processor, "_download_file") as mock_download:
        doc = processor.download_document("https://example.com/existing.pdf", "tweet-3", resume=True)

    assert doc is not None
    assert doc.downloaded is True
    assert doc.size_bytes == pdf_path.stat().st_size
    mock_download.assert_not_called()


def test_pdf_document_serializes_metadata():
    doc = PDFDocument("https://example.com/x.pdf", "My Paper", "my-paper.pdf", True)
    doc.size_bytes = 1234
    doc.source_domain = "example.com"

    assert doc.to_dict() == {
        "url": "https://example.com/x.pdf",
        "title": "My Paper",
        "filename": "my-paper.pdf",
        "downloaded": True,
        "size_bytes": 1234,
        "source_domain": "example.com",
    }


def test_processor_target_dir_name(tmp_path):
    proc = PDFProcessor(output_dir=str(tmp_path), target_dir_name="preprints")
    assert proc.pdfs_dir == tmp_path / "preprints"
