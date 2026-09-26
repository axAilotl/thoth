"""Producer-generated UUIDv7 identifiers for CCF 0.3.0."""

from __future__ import annotations

import re
import secrets
import time
import uuid

_ID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)


class CcfIdError(ValueError):
    """An identifier is not a lowercase, hyphenated UUIDv7."""


def generate_id() -> str:
    """Generate an RFC 9562 UUIDv7 with a timestamp and 74 random bits."""
    milliseconds = time.time_ns() // 1_000_000
    if not 0 <= milliseconds < 2**48:
        raise CcfIdError("current time is outside the UUIDv7 timestamp range")
    random_bits = secrets.randbits(74)
    value = (
        (milliseconds << 80) | (7 << 76) | ((random_bits >> 62) << 64)
        | (2 << 62) | (random_bits & ((1 << 62) - 1))
    )
    return str(uuid.UUID(int=value))


def parse_id(value: str) -> uuid.UUID:
    """Validate the exact portable spelling and return its UUID value."""
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise CcfIdError(f"expected lowercase hyphenated UUIDv7: {value!r}")
    return uuid.UUID(value)


def is_valid_id(value: object) -> bool:
    return isinstance(value, str) and _ID_RE.fullmatch(value) is not None
