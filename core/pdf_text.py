"""PDF text and title extraction helpers."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


class PDFTextExtractionError(RuntimeError):
    """Raised when PDF text extraction cannot complete safely."""


def _run_pdf_command(args: list[str], *, timeout: int) -> str:
    """Run a Poppler PDF utility and return stdout or raise a domain error."""
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise PDFTextExtractionError(f"Missing required PDF utility: {args[0]}") from exc
    except Exception as exc:
        raise PDFTextExtractionError(f"Failed to execute PDF utility {args[0]}") from exc

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise PDFTextExtractionError(
            stderr or f"PDF utility {args[0]} exited with status {result.returncode}"
        )
    return result.stdout


def _normalize_line(value: str) -> str:
    """Collapse whitespace and strip a single title/text line."""
    cleaned = value.replace("\ufeff", "").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _normalize_text(value: str) -> str:
    """Normalize pdftotext output into clean paragraphs."""
    text = value.replace("\f", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_title(path: Path) -> str:
    """Extract a plausible title from PDF metadata or first-page text.

    Returns the first non-empty candidate found in this order:
    1. The ``Title`` field from ``pdfinfo``.
    2. The first meaningful line on the first page (skipping ``arxiv:`` headers).

    Raises :class:`PDFTextExtractionError` only when the underlying utilities
    report a hard failure (missing binary, non-zero exit). Callers that treat
    title extraction as optional should catch that exception explicitly.
    """
    try:
        metadata_output = _run_pdf_command(["pdfinfo", str(path)], timeout=10)
    except PDFTextExtractionError:
        metadata_output = ""

    for line in metadata_output.splitlines():
        if not line.startswith("Title:"):
            continue
        title = _normalize_line(line.split(":", 1)[1])
        if title:
            return title

    first_page_text = extract_pdf_text(path, max_pages=1)
    for raw_line in first_page_text.splitlines():
        candidate = _normalize_line(raw_line)
        if not candidate:
            continue
        if candidate.lower().startswith("arxiv:"):
            continue
        return candidate
    return ""


def extract_pdf_text(path: Path, *, max_pages: int | None = None) -> str:
    """Extract plain text from a PDF via ``pdftotext``.

    Args:
        path: PDF file path.
        max_pages: If set, extract only pages 1 through ``max_pages``.

    Returns:
        Normalized plain text extracted from the PDF.

    Raises:
        PDFTextExtractionError: If ``pdftotext`` is missing or exits with an
        error for the given file.
    """
    args = ["pdftotext", "-enc", "UTF-8", "-nopgbrk"]
    if max_pages is not None:
        args.extend(["-f", "1", "-l", str(max_pages)])
    args.extend([str(path), "-"])
    return _normalize_text(_run_pdf_command(args, timeout=30))
