"""Bounded snapshot extraction and non-overwriting inbox file operations."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
import os
from pathlib import Path
import stat
import tempfile
import uuid

from core.docx_text import extract_docx_text
from core.pdf_text import extract_pdf_text


class InboxFileError(ValueError):
    """An input needs attention rather than automatic consumption."""


@contextmanager
def _parent_directory(path: Path, *, create=False):
    """Pin each directory component without following symlinks, even in a race."""
    if not path.is_absolute():
        raise InboxFileError("Inbox file operations require absolute paths")
    directory_fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parent.parts[1:]:
            if component == "..":
                raise InboxFileError("Inbox file operations reject parent traversal")
            if create:
                try:
                    os.mkdir(component, dir_fd=directory_fd)
                except FileExistsError:
                    pass
            next_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        yield directory_fd
    finally:
        os.close(directory_fd)


def fingerprint(path: Path) -> list[int]:
    for part in (path, *path.parents):
        if part.is_symlink():
            raise InboxFileError("Symlinks are not accepted by inbox intake")
    info = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise InboxFileError("Inbox input must be a regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def read_snapshot(path: Path, expected: list[int], max_bytes: int) -> bytes:
    """Read one bounded revision; never accept a file changing during the read."""
    if fingerprint(path) != expected:
        raise InboxFileError("Source changed since observation; waiting for a stable revision")
    with _parent_directory(path) as directory_fd:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=directory_fd)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        observed = [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]
        if observed != expected or not stat.S_ISREG(info.st_mode):
            raise InboxFileError("Source changed before snapshot")
        if info.st_size > max_bytes:
            raise InboxFileError("Source exceeds configured max_source_bytes")
        payload = source.read(max_bytes + 1)
        if len(payload) > max_bytes or fingerprint(path) != expected:
            raise InboxFileError("Source changed during snapshot")
    return payload


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def immutable_write(path: Path, payload: bytes) -> None:
    """Publish complete bytes atomically without ever replacing an existing file."""
    for parent in (path.parent, *path.parent.parents):
        if parent.is_symlink():
            raise InboxFileError("Destination symlinks are not allowed")
    if path.exists() or path.is_symlink():
        observed = fingerprint(path)
        if observed[2] != len(payload) or read_snapshot(path, observed, len(payload)) != payload:
            raise InboxFileError("Destination already exists with different content")
        return
    with _parent_directory(path, create=True) as directory_fd:
        temporary = ".thoth-inbox-" + uuid.uuid4().hex
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory_fd)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            try:
                os.link(temporary, path.name, src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd, follow_symlinks=False)
            except FileExistsError:
                observed = fingerprint(path)
                if observed[2] != len(payload) or read_snapshot(path, observed, len(payload)) != payload:
                    raise InboxFileError("Destination appeared with different content")
            os.fsync(directory_fd)
        finally:
            os.unlink(temporary, dir_fd=directory_fd)


def extract_snapshot(payload: bytes, suffix: str, *, temp_root: Path,
                     max_text_chars: int, pdf_max_pages: int) -> tuple[str, dict]:
    """Extract locally; record coverage and reject unreadable/oversized inputs."""
    metadata = {"coverage": "complete_source", "extractor": "utf8"}
    if suffix in {".md", ".markdown", ".txt"}:
        text = payload.decode("utf-8-sig")
    elif suffix == ".docx":
        text = extract_docx_text(payload)
        metadata = {"coverage": "document_body_only", "extractor": "docx-main-document-xml"}
    elif suffix == ".pdf":
        temp_root.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix=".pdf", dir=temp_root) as snapshot:
            snapshot.write(payload)
            snapshot.flush()
            text = extract_pdf_text(Path(snapshot.name), max_pages=pdf_max_pages)
        metadata = {"coverage": "bounded_pdf_excerpt", "extractor": "poppler-pdftotext",
                    "pdf_page_limit": pdf_max_pages}
    else:
        raise InboxFileError(f"Unsupported inbox file extension: {suffix or '(none)'}")
    if not text.strip():
        raise InboxFileError("No extractable text; document needs review or OCR")
    metadata.update(extracted_characters=len(text), text_truncated=len(text) > max_text_chars)
    return text[:max_text_chars], metadata
