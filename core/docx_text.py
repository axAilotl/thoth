"""Bounded local extraction of DOCX main-document text; no external resources."""

from __future__ import annotations

import io
import xml.etree.ElementTree as ET
import zipfile


class DOCXTextExtractionError(ValueError):
    """A DOCX cannot be safely extracted within the local bounds."""


def extract_docx_text(payload: bytes) -> str:
    """Extract body paragraphs only, rejecting entity declarations and ZIP bombs.

    Headers, footnotes, comments, embedded objects and linked resources are not
    interpreted. Callers must label this as document-body coverage, not the
    entire Word document including its attachments.
    """
    if len(payload) > 50 * 1024 * 1024:
        raise DOCXTextExtractionError("DOCX exceeds the 50 MiB source limit")
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            entry = archive.getinfo("word/document.xml")
            if entry.file_size > 5 * 1024 * 1024 or entry.file_size > max(1, entry.compress_size) * 200:
                raise DOCXTextExtractionError("DOCX document XML exceeds extraction safety limits")
            xml = archive.read(entry)
        declarations = xml.replace(b"\x00", b"").upper()
        if b"<!DOCTYPE" in declarations or b"<!ENTITY" in declarations:
            raise DOCXTextExtractionError("DOCX entity declarations are not allowed")
        root = ET.fromstring(xml)
    except (zipfile.BadZipFile, KeyError, ET.ParseError, RuntimeError) as exc:
        raise DOCXTextExtractionError(f"Cannot extract DOCX document body: {exc}") from exc
    namespace = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    text = "\n".join(
        "".join(part.text or "" for part in paragraph.iter(namespace + "t"))
        for paragraph in root.iter(namespace + "p")
    )
    if not text.strip():
        raise DOCXTextExtractionError("DOCX has no extractable main-document text")
    return text
