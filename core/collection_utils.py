"""Small shared collection helpers."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


def first_present_value(payload: Mapping[str, Any], *keys: str) -> Any:
    """Return the first non-None value among the given keys, or None."""
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def group_by_optional_attr(
    items: Iterable[Any], attr_name: str
) -> dict[str, tuple[Any, ...]]:
    """Group items by a stripped string attribute, skipping empty keys."""
    grouped: dict[str, list[Any]] = {}
    for item in items:
        key = str(getattr(item, attr_name, "") or "").strip()
        if key:
            grouped.setdefault(key, []).append(item)
    return {key: tuple(values) for key, values in grouped.items()}
