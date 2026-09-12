"""PDF text and title extraction helpers."""

from __future__ import annotations

import hashlib
import re
import stat
import subprocess
from pathlib import Path


class PDFTextExtractionError(RuntimeError):
    """Raised when PDF text extraction cannot complete safely."""


class SourceIntegrityError(RuntimeError):
    """An observed source defect, distinct from a processing infrastructure error."""

    def __init__(self, source_status: str, reason: str, reason_summary: str):
        super().__init__(reason)
        self.source_status = source_status
        self.category = f"source_{source_status}"
        self.reason_summary = reason_summary


class PDFSourceIntegrityError(SourceIntegrityError, PDFTextExtractionError):
    """A source defect that prevents PDF extraction, with actionable diagnostics."""


def validate_source_integrity(
    path: Path, *, pdf: bool = False, max_bytes: int | None = None,
    expected_checksum: str | None = None,
) -> None:
    """Read-only byte checks shared by document validation and PDF utilities.

    A PDF signature is only a preflight check, never proof of a parseable PDF.
    Callers own path authorization; OS access failures remain infrastructure errors.
    """
    source_error = PDFSourceIntegrityError if pdf else SourceIntegrityError
    try:
        source_stat = path.stat()
        if not stat.S_ISREG(source_stat.st_mode):
            raise ValueError("Document source must be a regular file")
        if max_bytes is not None and source_stat.st_size > max_bytes:
            raise ValueError("Document source exceeds max_source_bytes")
        with path.open("rb") as source:
            header = source.read(1024)
            if not header:
                raise source_error(
                    "empty", "Document source is empty; restore or download it again, then rescan.",
                    "Empty source file; restore it and rescan.",
                )
            if pdf:
                _validate_pdf_header(header)
            if expected_checksum is not None:
                digest = hashlib.sha256(header)
                size = len(header)
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    size += len(chunk)
                    if max_bytes is not None and size > max_bytes:
                        raise ValueError("Document source exceeds max_source_bytes")
                    digest.update(chunk)
                if digest.hexdigest() != expected_checksum:
                    raise RuntimeError("Document source checksum changed since capture; rescan required")
    except FileNotFoundError as exc:
        raise source_error(
            "missing", "Document source file is missing; restore it at the captured path and rescan.",
            "Source file missing; restore it and rescan.",
        ) from exc


def _validate_pdf_header(header: bytes) -> None:
    prefix = header.lstrip().removeprefix(b"\xef\xbb\xbf").lstrip()
    if re.match(br"(?:<!doctype\s+html\b|<html\b)", prefix, re.IGNORECASE):
        raise PDFSourceIntegrityError(
            "html", "Document source contains HTML instead of PDF bytes; download the PDF and rescan.",
            "HTML saved as PDF; download the PDF and rescan.",
        )
    if not header.startswith(b"%PDF-"):
        raise PDFSourceIntegrityError(
            "non_pdf", "Document source does not contain a PDF header; download the PDF and rescan.",
            "Not a PDF file; download the PDF and rescan.",
        )


def _run_pdf_command(args: list[str], *, path: Path, timeout: int) -> str:
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
        raise PDFTextExtractionError(f"Failed to execute PDF utility {args[0]}: {exc}") from exc

    if result.returncode != 0:
        stderr = result.stderr.strip()
        # The source may have disappeared or been replaced while Poppler ran.
        validate_source_integrity(path, pdf=True)
        # Exit 1 includes OS open failures, 2 is output I/O, 3 is permissions,
        # and 99 is unspecified. Only explicit parser evidence proves damage.
        if result.returncode in (1, 99) and re.search(
            r"(?im)^Syntax Error:|may not be a PDF file", stderr
        ) and not re.search(r"(?i)permission denied|couldn't open|I/O error", stderr):
            raise PDFSourceIntegrityError(
                "malformed_pdf", f"Malformed PDF; restore or download the PDF and rescan. Poppler: {stderr}",
                "Malformed PDF; restore or download the PDF and rescan.",
            )
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

    Invalid source bytes raise :class:`SourceIntegrityError`. Utility failures
    raise :class:`PDFTextExtractionError`; neither is an absent metadata title.
    """
    validate_source_integrity(path, pdf=True)
    metadata_output = _run_pdf_command(["pdfinfo", str(path)], path=path, timeout=10)

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
        SourceIntegrityError: If the source is missing, empty, or not a PDF.
        PDFTextExtractionError: If ``pdftotext`` is missing or exits with an
        error for the given file.
    """
    validate_source_integrity(path, pdf=True)
    args = ["pdftotext", "-enc", "UTF-8", "-nopgbrk"]
    if max_pages is not None:
        args.extend(["-f", "1", "-l", str(max_pages)])
    args.extend([str(path), "-"])
    return _normalize_text(_run_pdf_command(args, path=path, timeout=30))
