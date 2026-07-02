"""Shared time helpers for durable metadata."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """Return the current UTC timestamp in Thoth's canonical ``Z`` form."""
    return utc_now().isoformat().replace("+00:00", "Z")
