"""Tests for core.pdf_text extraction primitives."""

from pathlib import Path
from unittest.mock import patch

import pytest

from core.pdf_text import (
    PDFTextExtractionError,
    extract_pdf_text,
    extract_pdf_title,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "pdfs"
SAMPLE_PDF = FIXTURES / "sample_paper.pdf"


def _make_result(stdout: str = "", stderr: str = "", returncode: int = 0):
    return type("Result", (object,), {
        "stdout": stdout,
        "stderr": stderr,
        "returncode": returncode,
    })()


def test_extract_pdf_title_prefers_pdfinfo_title():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.side_effect = [
            _make_result("Title:   Paper-Grade PDF Retrieval\nAuthor: Ada\n"),
            _make_result("This is page one text."),
        ]
        title = extract_pdf_title(SAMPLE_PDF)

    assert title == "Paper-Grade PDF Retrieval"
    calls = mock_run.call_args_list
    assert calls[0][0][0] == ["pdfinfo", str(SAMPLE_PDF)]
    # A usable pdfinfo title short-circuits; pdftotext is not needed.
    assert len(calls) == 1


def test_extract_pdf_title_falls_back_to_first_page_line():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.side_effect = [
            _make_result("Title:        \n"),  # empty title
            _make_result("\n\nTowards a Science of Scaling Agent Systems\n\nAbstract\n"),
        ]
        title = extract_pdf_title(SAMPLE_PDF)

    assert title == "Towards a Science of Scaling Agent Systems"


def test_extract_pdf_title_skips_arxiv_header():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.side_effect = [
            _make_result("Title:        \n"),
            _make_result("arxiv:2604.12345 [cs.AI]\n\nReal Paper Title Here\n"),
        ]
        title = extract_pdf_title(SAMPLE_PDF)

    assert title == "Real Paper Title Here"


def test_extract_pdf_title_returns_empty_when_no_candidate():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.side_effect = [
            _make_result(""),
            _make_result("\n\n\n"),
        ]
        title = extract_pdf_title(SAMPLE_PDF)

    assert title == ""


def test_extract_pdf_text_runs_pdftotext():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.return_value = _make_result("Paragraph one.\n\nParagraph two.\n")
        text = extract_pdf_text(SAMPLE_PDF)

    assert text == "Paragraph one.\n\nParagraph two."
    mock_run.assert_called_once()
    args = mock_run.call_args[0][0]
    assert args[:2] == ["pdftotext", "-enc"]
    assert args[-2:] == [str(SAMPLE_PDF), "-"]


def test_extract_pdf_text_limits_pages():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.return_value = _make_result("Page one.\n")
        text = extract_pdf_text(SAMPLE_PDF, max_pages=1)

    assert text == "Page one."
    args = mock_run.call_args[0][0]
    assert "-f" in args
    assert "1" in args
    assert "-l" in args
    assert "1" in args


def test_extract_pdf_text_raises_on_missing_utility():
    with patch("core.pdf_text.subprocess.run", side_effect=FileNotFoundError("pdfinfo")):
        with pytest.raises(PDFTextExtractionError) as exc_info:
            extract_pdf_title(SAMPLE_PDF)

    assert "Missing required PDF utility" in str(exc_info.value)


def test_extract_pdf_text_raises_on_nonzero_exit():
    with patch("core.pdf_text.subprocess.run") as mock_run:
        mock_run.return_value = _make_result(stderr="Error: May not be a PDF file", returncode=1)
        with pytest.raises(PDFTextExtractionError) as exc_info:
            extract_pdf_text(SAMPLE_PDF)

    assert "May not be a PDF file" in str(exc_info.value)
