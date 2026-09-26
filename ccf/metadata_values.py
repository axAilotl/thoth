"""CCF-owned conversions for native Thoth values and locations."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path


def portable(value):
    """Copy native metadata into integer-only JSON without altering native data."""
    if isinstance(value, float):
        return str(value)
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 2**53 - 1:
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): portable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [portable(item) for item in value]
    return value


def mapping(value):
    return dict(value) if isinstance(value, Mapping) else {}


def location(path: Path, vault_root: Path) -> str:
    path = path.resolve()
    try:
        return 'vault:' + path.relative_to(vault_root.resolve()).as_posix()
    except ValueError:
        return path.as_uri()


def content_path(value, vault_root: Path) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else vault_root / path


def artifact_type(value: str) -> str:
    return {
        'tweet': 'message', 'thread': 'conversation', 'markdown': 'note',
        'web_clipper': 'bookmark', 'video': 'recording', 'repository': 'document',
        'pdf': 'document', 'transcript': 'transcript', 'paper': 'paper',
        'note': 'note', 'document': 'document',
    }.get(value, 'thoth.artifact')
