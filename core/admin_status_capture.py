"""Capture source, session, and event health for the admin dashboard."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Mapping

from .admin_status_utils import (
    OPEN_SESSION_STUCK_AFTER,
    RECENT_LIMIT,
    dt_text,
    mapping_reason,
    min_datetime,
    parse_datetime,
    safe_reason,
)


def capture_status(
    event_store: Any | None,
    *,
    error: str | None,
    now: datetime,
) -> dict[str, Any]:
    empty = {
        "source_health": {
            "status": "unavailable" if error else "ok",
            "total": 0,
            "active": 0,
            "unhealthy": 0,
            "sources": [],
        },
        "recent_sessions": {
            "total": 0,
            "sessions": [],
            "limit": RECENT_LIMIT,
        },
        "event_counts": {
            "total": 0,
            "by_status": {},
            "by_type": {},
            "by_source": {},
        },
        "stuck_sessions": [],
    }
    if event_store is None:
        return {**empty, "error": safe_reason(error) if error else None}

    try:
        sources = list(event_store.list_sources())
        sessions = list(event_store.list_sessions())
        events = list(event_store.list_events())
    except Exception as exc:
        return {
            **empty,
            "source_health": {**empty["source_health"], "status": "error"},
            "error": safe_reason(exc),
        }

    source_by_id = {str(source.source_id): source for source in sources}
    events_by_source: Counter[str] = Counter()
    events_by_status: Counter[str] = Counter()
    events_by_type: Counter[str] = Counter()
    sessions_by_source: Counter[str] = Counter()
    open_sessions_by_source: Counter[str] = Counter()
    last_event_by_source: dict[str, datetime] = {}

    for event in events:
        source_id = str(getattr(event, "source_id", "") or "unknown")
        events_by_source[source_id] += 1
        events_by_status[str(getattr(event, "status", "") or "unknown")] += 1
        events_by_type[str(getattr(event, "event_type", "") or "unknown")] += 1
        event_at = _event_timestamp(event)
        if event_at and (
            source_id not in last_event_by_source
            or event_at > last_event_by_source[source_id]
        ):
            last_event_by_source[source_id] = event_at

    for session in sessions:
        source_id = str(getattr(session, "source_id", "") or "unknown")
        sessions_by_source[source_id] += 1
        if str(getattr(session, "status", "") or "").lower() in {"open", "running"}:
            open_sessions_by_source[source_id] += 1

    source_payloads = []
    for source in sorted(sources, key=lambda item: str(getattr(item, "source_name", ""))):
        source_id = str(source.source_id)
        status = str(getattr(source, "status", "") or "unknown")
        status_lower = status.lower()
        reason = mapping_reason(
            getattr(source, "metadata", None),
            getattr(source, "config", None),
        )
        unhealthy = status_lower not in {"active", "ok", "enabled"} or bool(reason)
        if unhealthy and not reason:
            reason = f"source status is {status}"
        source_payloads.append(
            {
                "source_id": source_id,
                "source_name": getattr(source, "source_name", None),
                "source_type": getattr(source, "source_type", None),
                "collector": getattr(source, "collector", None),
                "status": status,
                "event_count": events_by_source.get(source_id, 0),
                "session_count": sessions_by_source.get(source_id, 0),
                "open_session_count": open_sessions_by_source.get(source_id, 0),
                "last_event_at": dt_text(last_event_by_source.get(source_id)),
                "updated_at": dt_text(getattr(source, "updated_at", None)),
                "reason": reason,
            }
        )

    session_event_counts: Counter[str] = Counter(
        str(getattr(event, "session_id", "") or "")
        for event in events
        if getattr(event, "session_id", None)
    )
    session_payloads = [
        _session_payload(
            session,
            source_by_id=source_by_id,
            event_count=session_event_counts.get(str(session.session_id), 0),
        )
        for session in sessions
    ]
    session_payloads.sort(
        key=lambda item: parse_datetime(item.get("started_at")) or min_datetime(),
        reverse=True,
    )
    stuck_sessions = [
        item for item in session_payloads if _session_is_stuck(item, now=now)
    ]

    return {
        "source_health": {
            "status": (
                "degraded"
                if any(item.get("reason") for item in source_payloads)
                else "ok"
            ),
            "total": len(source_payloads),
            "active": sum(
                1 for item in source_payloads if item["status"].lower() == "active"
            ),
            "unhealthy": sum(1 for item in source_payloads if item.get("reason")),
            "sources": source_payloads,
        },
        "recent_sessions": {
            "total": len(session_payloads),
            "sessions": session_payloads[:RECENT_LIMIT],
            "limit": RECENT_LIMIT,
        },
        "event_counts": {
            "total": len(events),
            "by_status": dict(sorted(events_by_status.items())),
            "by_type": dict(sorted(events_by_type.items())),
            "by_source": {
                (getattr(source_by_id.get(source_id), "source_name", None) or source_id): count
                for source_id, count in sorted(events_by_source.items())
            },
        },
        "stuck_sessions": stuck_sessions[:RECENT_LIMIT],
    }


def _session_payload(
    session: Any,
    *,
    source_by_id: Mapping[str, Any],
    event_count: int,
) -> dict[str, Any]:
    source = source_by_id.get(str(getattr(session, "source_id", "")))
    status = str(getattr(session, "status", "") or "unknown")
    reason = None
    if status.lower() not in {"closed", "completed", "ok"}:
        reason = mapping_reason(
            getattr(session, "metadata", None),
            getattr(session, "provenance", None),
        )
    return {
        "session_id": getattr(session, "session_id", None),
        "source_id": getattr(session, "source_id", None),
        "source_name": getattr(source, "source_name", None) if source else None,
        "source_type": getattr(source, "source_type", None) if source else None,
        "session_type": getattr(session, "session_type", None),
        "native_session_id": getattr(session, "native_session_id", None),
        "status": status,
        "started_at": dt_text(getattr(session, "started_at", None)),
        "ended_at": dt_text(getattr(session, "ended_at", None)),
        "event_count": event_count,
        "reason": reason,
    }


def _session_is_stuck(session: Mapping[str, Any], *, now: datetime) -> bool:
    status = str(session.get("status") or "").lower()
    if status in {"failed", "error", "blocked"}:
        return True
    if status in {"open", "running"}:
        started_at = parse_datetime(session.get("started_at"))
        return started_at is None or now - started_at >= OPEN_SESSION_STUCK_AFTER
    return False


def _event_timestamp(event: Any) -> datetime | None:
    for attr in ("captured_at", "occurred_at", "updated_at", "created_at"):
        parsed = parse_datetime(getattr(event, attr, None))
        if parsed:
            return parsed
    return None
