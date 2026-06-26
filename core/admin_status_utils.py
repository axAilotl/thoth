"""Shared helpers for admin operational status modules."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping

from .sensitive_redaction import redact_sensitive_text

RECENT_LIMIT = 10
STUCK_LIMIT = 50
RUNNING_STUCK_AFTER = timedelta(hours=1)
OPEN_SESSION_STUCK_AFTER = timedelta(hours=24)
STALE_PAGE_CODES = frozenset({"stale-page", "stale-page-inputs"})


def safe_reason(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    redacted = redact_sensitive_text(text).redacted_text
    if len(redacted) > 500:
        return redacted[:497].rstrip() + "..."
    return redacted


def mapping_reason(*mappings: Any) -> str | None:
    keys = (
        "last_error",
        "error",
        "failure_reason",
        "reason",
        "status_reason",
        "message",
    )
    for mapping in mappings:
        if not isinstance(mapping, Mapping):
            continue
        for key in keys:
            text = safe_reason(mapping.get(key))
            if text:
                return text
    return None


def review_reason(review_json: str | None) -> str | None:
    if not review_json:
        return None
    try:
        payload = json.loads(review_json)
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    state = payload.get("state")
    if isinstance(state, Mapping):
        return mapping_reason(state)
    events = payload.get("events")
    if isinstance(events, list):
        for event in reversed(events):
            reason = mapping_reason(event)
            if reason:
                return reason
    return None


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value is None:
        return None
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def dt_text(value: Any) -> str | None:
    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed.isoformat().replace("+00:00", "Z")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def error_item(section: str, reason: Any) -> dict[str, str] | None:
    text = safe_reason(reason)
    if not text:
        return None
    return {"section": section, "reason": text}


def min_datetime() -> datetime:
    return datetime.min.replace(tzinfo=timezone.utc)
