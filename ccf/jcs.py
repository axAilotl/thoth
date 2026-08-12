"""RFC 8785 (JCS) canonical JSON serialization per CCF spec section 4.1.

This is a semantic port of the reference implementation
``spec/ccf/0.1.1/tools/ccf-jcs.mjs``. The hard part is numbers: JSON has a
single number type and ECMAScript serializes it with
``Number::toString`` (shortest round-trip decimal, ES-specific exponent
rules). Python's ``repr`` of a float yields the same shortest round-trip
digits, so :func:`_ecmascript_number_to_string` re-derives the digit string
from ``repr`` and re-renders it under the ECMAScript rules.

Fail-closed rules (spec section 4.1):

- NaN, Infinity, and negative zero are rejected;
- unpaired surrogates (any surrogate code point in a Python ``str``) are
  rejected;
- JSON numbers with absolute integer value above 2**53 - 1 are rejected
  (spec section 4.2: such integers travel as canonical decimal strings);
- duplicate object keys are rejected at parse time by :func:`loads`;
- dict keys are sorted by UTF-16 code units, per RFC 8785.
"""

from __future__ import annotations

import json
import math
import re

#: Largest integer that survives the ECMAScript double pipeline exactly.
MAX_SAFE_INTEGER = 2**53 - 1


class JcsError(ValueError):
    """Raised when a value cannot be canonically serialized."""


def _serialize_string(value: str) -> str:
    """Serialize a string exactly as ECMAScript ``JSON.stringify`` does."""
    out = ['"']
    for ch in value:
        code = ord(ch)
        if 0xD800 <= code <= 0xDFFF:
            raise JcsError(f"unpaired surrogate U+{code:04X} in string")
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\b":
            out.append("\\b")
        elif ch == "\f":
            out.append("\\f")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif code < 0x20:
            out.append(f"\\u{code:04x}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


_FLOAT_REPR_RE = re.compile(
    r"^(?P<int>\d+)(?:\.(?P<frac>\d+))?(?:[eE](?P<exp>[+-]?\d+))?$"
)


def _ecmascript_number_to_string(value: float) -> str:
    """Port of ECMAScript ``Number::toString(x, 10)`` (ES2024 section 6.1.6.1.20).

    ``repr`` supplies the shortest round-trip decimal digits (same digit
    selection V8 makes); this function applies the ECMAScript layout rules,
    which differ from Python's (e.g. ``0.000001`` vs ``1e-06``).
    """
    if not math.isfinite(value):
        raise JcsError(f"non-finite number: {value!r}")
    if value == 0:
        if math.copysign(1.0, value) < 0:
            raise JcsError("negative zero")
        return "0"

    sign = "-" if value < 0 else ""
    match = _FLOAT_REPR_RE.match(repr(abs(value)))
    if match is None:  # pragma: no cover - repr of a finite float always matches
        raise JcsError(f"cannot decompose float repr: {value!r}")
    digits = (match.group("int") + (match.group("frac") or "")).lstrip("0")
    exp10 = int(match.group("exp") or 0) - len(match.group("frac") or "")
    if not digits:  # pragma: no cover - zero handled above
        raise JcsError(f"cannot decompose float repr: {value!r}")
    # n, k, e such that 10^(k-1) <= n < 10^k and n * 10^(e-k) == value.
    k = len(digits)
    e = k + exp10
    digits = digits.rstrip("0")  # minimal k; e is invariant under this strip
    k = len(digits)

    if k <= e <= 21:
        return sign + digits + "0" * (e - k)
    if 0 < e <= 21:
        return sign + digits[:e] + "." + digits[e:]
    if -6 < e <= 0:
        return sign + "0." + "0" * (-e) + digits
    exponent = e - 1
    mantissa = digits if k == 1 else digits[0] + "." + digits[1:]
    return f"{sign}{mantissa}e{'+' if exponent >= 0 else '-'}{abs(exponent)}"


def _serialize_number(value: int | float) -> str:
    if isinstance(value, bool):  # bool is an int subclass; never reach here
        raise JcsError("boolean reached number serializer")
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise JcsError(
                f"integer exceeds 2^53-1 and must travel as a decimal string: {value}"
            )
        return str(value)
    return _ecmascript_number_to_string(value)


def canonicalize(value: object) -> str:
    """Serialize ``value`` to RFC 8785 canonical JSON text."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return _serialize_number(value)
    if isinstance(value, str):
        return _serialize_string(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonicalize(item) for item in value) + "]"
    if isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                raise JcsError(f"object key must be a string: {key!r}")
            _serialize_string(key)  # reject unpaired surrogates before sorting
        ordered = sorted(value.keys(), key=lambda k: k.encode("utf-16-be"))
        parts = [
            f"{_serialize_string(key)}:{canonicalize(value[key])}" for key in ordered
        ]
        return "{" + ",".join(parts) + "}"
    raise JcsError(f"unsupported JSON value type: {type(value).__name__}")


def canonical_bytes(value: object) -> bytes:
    """Canonical JSON as UTF-8 bytes (no BOM, no normalization)."""
    return canonicalize(value).encode("utf-8")


def _reject_constant(token: str) -> None:
    raise JcsError(f"non-JSON constant in input: {token}")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, val in pairs:
        if key in result:
            raise JcsError(f"duplicate object key: {key!r}")
        result[key] = val
    return result


def loads(text: str | bytes) -> object:
    """Parse JSON strictly: duplicate keys and NaN/Infinity literals rejected."""
    if isinstance(text, bytes):
        text = text.decode("utf-8")  # fails closed on a BOM'd/invalid encoding
    return json.loads(
        text, object_pairs_hook=_strict_object, parse_constant=_reject_constant
    )
