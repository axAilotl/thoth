"""CCF portable identifiers (spec section 2).

Canonical IDs are producer-generated before submission and have the form::

    urn:ccf:<kind>:<lowercase-hyphenated-uuidv4>

UUIDv7 and any other UUID variant/version are rejected: timestamp-bearing
IDs leak creation time after erasure, which is why CCF pins UUIDv4
(spec section 2.1). All validation fails closed.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass

#: Portable ID kinds defined by CCF 0.1.2 (spec section 2.1).
ID_KINDS: frozenset[str] = frozenset(
    {
        "record",
        "link",
        "blob",
        "archive",
        "lineage",
        "key",
        "credential",
        "batch",
        "pack",
    }
)

_URN_RE = re.compile(
    r"^urn:ccf:(?P<kind>[a-z]+):"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
)


class CcfIdError(ValueError):
    """Raised when a CCF identifier is malformed or violates the profile."""


@dataclass(frozen=True)
class CcfId:
    """A parsed, validated CCF portable identifier."""

    kind: str
    uuid: uuid.UUID

    def __str__(self) -> str:
        return f"urn:ccf:{self.kind}:{self.uuid}"


def generate_id(kind: str) -> str:
    """Generate a new canonical CCF ID of the given kind (UUIDv4)."""
    if kind not in ID_KINDS:
        raise CcfIdError(f"unsupported CCF id kind: {kind!r}")
    return str(CcfId(kind, uuid.uuid4()))


def derive_id(namespace: uuid.UUID, kind: str, material: list[str]) -> str:
    """Derive a deterministic canonical CCF ID from stable material.

    UUIDv5 digest of the JSON-canonical ``material`` under ``namespace``,
    with the version/variant bits forced to the UUIDv4 / RFC 4122 layout
    :func:`parse_id` demands — deterministic content with a spec-legal
    shape. Used ONLY where CCF offers no idempotency key of its own
    (bootstrap objects, origin-root sources); every origin-bearing object
    keeps a freshly generated URN and relies on the origin index.
    """
    if kind not in ID_KINDS:
        raise CcfIdError(f"unsupported CCF id kind: {kind!r}")
    if not material or not all(isinstance(part, str) and part for part in material):
        raise CcfIdError("deterministic id material must be non-empty strings")
    canonical = json.dumps(material, ensure_ascii=False, separators=(",", ":"))
    digest = bytearray(uuid.uuid5(namespace, canonical).bytes)
    digest[6] = (digest[6] & 0x0F) | 0x40  # version 4
    digest[8] = (digest[8] & 0x3F) | 0x80  # RFC 4122 variant
    return str(CcfId(kind, uuid.UUID(bytes=bytes(digest))))


def parse_id(urn: str) -> CcfId:
    """Parse and validate a CCF URN. Fails closed on any deviation.

    Rejects unknown kinds, non-lowercase text, malformed UUIDs, and any
    UUID that is not version 4 with the RFC 4122 variant (this excludes
    UUIDv7, nil/max UUIDs, and other layouts).
    """
    if not isinstance(urn, str):
        raise CcfIdError(f"CCF id must be a string, got {type(urn).__name__}")
    match = _URN_RE.match(urn)
    if match is None:
        raise CcfIdError(f"malformed CCF URN: {urn!r}")
    kind = match.group("kind")
    if kind not in ID_KINDS:
        raise CcfIdError(f"unsupported CCF id kind: {kind!r}")
    value = uuid.UUID(match.group("uuid"))
    if value.version != 4:
        raise CcfIdError(f"CCF id requires UUIDv4, got version {value.version}: {urn!r}")
    if value.variant != uuid.RFC_4122:
        raise CcfIdError(f"CCF id requires RFC 4122 variant: {urn!r}")
    # uuid.UUID() accepts several spellings; the regex already pinned the
    # exact lowercase hyphenated form, so a round-trip check is a cheap
    # belt-and-suspenders assertion of canonical text.
    if str(value) != match.group("uuid"):
        raise CcfIdError(f"non-canonical UUID text: {urn!r}")
    return CcfId(kind, value)


def is_valid_id(urn: str) -> bool:
    """Return True iff ``urn`` is a valid canonical CCF identifier."""
    try:
        parse_id(urn)
    except CcfIdError:
        return False
    return True
