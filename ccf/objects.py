"""CCF 0.3.0 validation, content hashes and deterministic object merge."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

from ccf import schema_id
from ccf.ids import parse_id
from ccf.jcs import canonical_bytes
from ccf.schemas import pinned_schemas

KINDS = frozenset({"record", "link", "blob"})
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z")
_NAME_RE = re.compile(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*")
_HASH_RE = re.compile(r"sha256:[0-9a-f]{64}")


class CcfObjectError(ValueError):
    """A CCF object or operation is invalid."""


def now_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def validate_timestamp(value: str) -> str:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        raise CcfObjectError(f"non-canonical timestamp: {value!r}")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as exc:
        raise CcfObjectError(f"invalid date/time: {value!r}") from exc
    return value


def hash_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def hash_data(value: object) -> str:
    return hash_bytes(canonical_bytes(value))


def validate_object(obj: object) -> list[str]:
    """Raise for malformed objects; return non-blocking content warnings.

    Locations and link endpoints are intentionally not resolved here. Hash
    mismatches never invalidate an otherwise well-formed object.
    """
    kind = obj.get("kind") if isinstance(obj, dict) else None
    if not isinstance(kind, str) or kind not in KINDS:
        raise CcfObjectError("kind must be record, link, or blob")
    pinned_schemas().validate(schema_id(kind), obj, what=f"{kind} object")
    parse_id(obj["id"])
    timestamp_fields = ("created", "updated", "deleted")
    if kind == "record":
        timestamp_fields += ("start", "end")
    for field in timestamp_fields:
        if field in obj:
            validate_timestamp(obj[field])
    for field in (("from", "to") if kind == "link" else ()):
        if field in obj:
            parse_id(obj[field])
    name_field = "type" if kind == "record" else "rel" if kind == "link" else None
    if name_field in obj and not _NAME_RE.fullmatch(obj[name_field]):
        raise CcfObjectError(f"invalid {name_field}: {obj[name_field]!r}")
    if kind in ("record", "blob") and "hash" in obj and not _HASH_RE.fullmatch(obj["hash"]):
        raise CcfObjectError(f"invalid hash: {obj['hash']!r}")
    warnings = []
    if kind == "record" and "deleted" not in obj and "loc" not in obj and "hash" in obj:
        if "data" not in obj:
            warnings.append(f"{obj['id']}: hash without loc or data")
        elif hash_data(obj["data"]) != obj["hash"]:
            warnings.append(f"{obj['id']}: hash does not match data")
    return warnings


def merge_key(obj: dict) -> tuple:
    return (
        obj["rev"], obj["updated"], obj["source"].get("device", "").encode("utf-8"),
        canonical_bytes(obj),
    )


def merge(a: dict, b: dict) -> dict:
    """Return the winning input object, independent of arrival order."""
    validate_object(a)
    validate_object(b)
    if a["id"] != b["id"]:
        raise CcfObjectError("cannot merge objects with different IDs")
    return a if merge_key(a) >= merge_key(b) else b
