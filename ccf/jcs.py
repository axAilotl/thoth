"""Strict integer-only canonical JSON from CCF 0.3.0 section 3.1."""

from __future__ import annotations

import json

MAX_SAFE_INTEGER = 2**53 - 1
_ESCAPES = {
    '"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f",
    "\n": "\\n", "\r": "\\r", "\t": "\\t",
}


class JcsError(ValueError):
    """A value is outside CCF's portable JSON profile."""


def _string(value: str) -> str:
    out = ['"']
    for char in value:
        code = ord(char)
        if 0xD800 <= code <= 0xDFFF:
            raise JcsError("unpaired surrogate in string")
        if char in _ESCAPES:
            out.append(_ESCAPES[char])
        elif code < 0x20:
            out.append(f"\\u{code:04x}")
        else:
            out.append(char)
    out.append('"')
    return "".join(out)


def canonicalize(value: object) -> str:
    """Serialize JSON, rejecting floats, unsafe integers and invalid strings."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise JcsError(f"integer outside CCF range: {value}")
        return str(value)
    if isinstance(value, str):
        return _string(value)
    if isinstance(value, list):
        return "[" + ",".join(canonicalize(item) for item in value) + "]"
    if isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                raise JcsError("object keys must be strings")
            _string(key)
        keys = sorted(value, key=lambda key: key.encode("utf-16-be"))
        return "{" + ",".join(
            _string(key) + ":" + canonicalize(value[key]) for key in keys
        ) + "}"
    raise JcsError(f"unsupported JSON value type: {type(value).__name__}")


def canonical_bytes(value: object) -> bytes:
    """Canonical JSON as UTF-8 bytes, without a byte-order mark."""
    return canonicalize(value).encode("utf-8")


def _reject_number(token: str) -> None:
    raise JcsError(f"CCF requires plain integer numbers: {token}")


def _strict_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise JcsError(f"duplicate object key: {key!r}")
        result[key] = value
    return result


def loads(text: str | bytes) -> object:
    """Parse and validate every value, including unknown fields, strictly."""
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    value = json.loads(
        text, object_pairs_hook=_strict_object,
        parse_float=_reject_number, parse_constant=_reject_number,
    )
    canonicalize(value)
    return value
