"""Queue counts and stuck-work extraction for admin status."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from .admin_status_utils import (
    RUNNING_STUCK_AFTER,
    STUCK_LIMIT,
    min_datetime,
    parse_datetime,
    review_reason,
    safe_reason,
)
from .metadata_db import BookmarkQueueEntry, IngestionQueueEntry, MetadataDB


def queue_status(db: MetadataDB) -> dict[str, Any]:
    try:
        bookmark_counts = db.get_bookmark_queue_counts()
        ingestion_counts = db.get_ingestion_queue_counts()
    except Exception as exc:
        return {
            "bookmark_queue": {
                "by_status": {"pending": 0, "processing": 0, "processed": 0, "failed": 0},
                "total": 0,
            },
            "ingestion_queue": {
                "by_status": {},
                "by_artifact_type": {},
                "by_source": {},
                "total": 0,
            },
            "error": safe_reason(exc),
        }

    return {
        "bookmark_queue": {
            "by_status": bookmark_counts,
            "total": sum(int(value or 0) for value in bookmark_counts.values()),
        },
        "ingestion_queue": ingestion_counts,
    }


def stuck_work_status(
    *,
    db: MetadataDB,
    capture: dict[str, Any],
    stale_pages: dict[str, Any],
    compiler_runs: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    items.extend(_bookmark_stuck_items(db, now=now))
    items.extend(_ingestion_stuck_items(db, now=now))
    items.extend(_connector_stuck_items(db, now=now))

    for session in capture.get("stuck_sessions") or []:
        items.append(
            {
                "kind": "capture_session",
                "id": session.get("session_id"),
                "status": session.get("status"),
                "reason": session.get("reason") or "capture session is still open",
                "updated_at": session.get("ended_at") or session.get("started_at"),
                "source": session.get("source_name") or session.get("source_id"),
            }
        )

    for issue in stale_pages.get("items") or []:
        items.append(
            {
                "kind": "wiki_page",
                "id": issue.get("page_path"),
                "status": issue.get("code"),
                "reason": issue.get("message"),
                "updated_at": None,
            }
        )

    for run in (compiler_runs.get("archivist") or {}).get("forced") or []:
        items.append(
            {
                "kind": "archivist_topic",
                "id": run.get("topic_id"),
                "status": "force_queued",
                "reason": run.get("force_reason") or "manual force queued",
                "updated_at": run.get("force_requested_at"),
            }
        )

    for run in (compiler_runs.get("archivist") or {}).get("runs") or []:
        if run.get("status") == "error":
            items.append(
                {
                    "kind": "archivist_topic",
                    "id": run.get("topic_id"),
                    "status": "error",
                    "reason": run.get("reason"),
                    "updated_at": run.get("last_run_at"),
                }
            )

    items.sort(
        key=lambda item: parse_datetime(item.get("updated_at")) or min_datetime(),
        reverse=True,
    )
    return {
        "total": len(items),
        "items": items[:STUCK_LIMIT],
        "by_kind": dict(Counter(str(item.get("kind") or "unknown") for item in items)),
    }


def _bookmark_stuck_items(db: MetadataDB, *, now: datetime) -> list[dict[str, Any]]:
    items = []
    try:
        entries = db.get_unprocessed_bookmarks(limit=STUCK_LIMIT)
    except Exception as exc:
        return [
            {
                "kind": "bookmark_queue",
                "id": "bookmark_queue",
                "status": "error",
                "reason": safe_reason(exc),
                "updated_at": None,
            }
        ]
    for entry in entries:
        reason = _bookmark_stuck_reason(entry, now=now)
        if not reason:
            continue
        items.append(
            {
                "kind": "bookmark_queue",
                "id": entry.tweet_id,
                "status": entry.status,
                "reason": reason,
                "updated_at": entry.last_attempt_at or entry.captured_at,
                "source": entry.source,
                "attempts": entry.attempts,
                "next_attempt_at": entry.next_attempt_at,
            }
        )
    return items


def _ingestion_stuck_items(db: MetadataDB, *, now: datetime) -> list[dict[str, Any]]:
    items = []
    statuses = ("failed", "needs_review", "blocked", "rejected", "processing")
    try:
        entries = [
            entry
            for status in statuses
            for entry in db.list_ingestion_entries(status=status, limit=STUCK_LIMIT)
        ]
    except Exception as exc:
        return [
            {
                "kind": "ingestion_queue",
                "id": "ingestion_queue",
                "status": "error",
                "reason": safe_reason(exc),
                "updated_at": None,
            }
        ]
    for entry in entries[:STUCK_LIMIT]:
        reason = _ingestion_stuck_reason(entry, now=now)
        if not reason:
            continue
        items.append(
            {
                "kind": "ingestion_queue",
                "id": entry.artifact_id,
                "status": entry.status,
                "reason": reason,
                "updated_at": entry.processed_at or entry.created_at,
                "source": entry.source,
                "artifact_type": entry.artifact_type,
                "attempts": entry.attempts,
                "next_attempt_at": entry.next_attempt_at,
            }
        )
    return items


def _connector_stuck_items(db: MetadataDB, *, now: datetime) -> list[dict[str, Any]]:
    items = []
    try:
        runs = [
            *db.list_connector_runs(status="failed", limit=STUCK_LIMIT),
            *db.list_connector_runs(status="running", limit=STUCK_LIMIT),
        ]
    except Exception as exc:
        return [
            {
                "kind": "connector_run",
                "id": "connector_runs",
                "status": "error",
                "reason": safe_reason(exc),
                "updated_at": None,
            }
        ]
    for run in runs[:STUCK_LIMIT]:
        reason = _connector_stuck_reason(run, now=now)
        if not reason:
            continue
        items.append(
            {
                "kind": "connector_run",
                "id": run.run_id,
                "status": run.status,
                "reason": reason,
                "updated_at": run.finished_at or run.started_at,
                "source": run.connector_name,
                "attempt": run.attempt,
                "next_retry_at": run.next_retry_at,
            }
        )
    return items


def _bookmark_stuck_reason(entry: BookmarkQueueEntry, *, now: datetime) -> str | None:
    status = str(entry.status or "")
    if status == "failed":
        return safe_reason(entry.last_error) or "bookmark processing failed"
    if status == "processing":
        last_attempt = parse_datetime(entry.last_attempt_at or entry.captured_at)
        if last_attempt is None or now - last_attempt >= RUNNING_STUCK_AFTER:
            return safe_reason(entry.last_error) or "bookmark has been processing for too long"
    if status == "pending" and entry.attempts > 0:
        return safe_reason(entry.last_error) or "bookmark retry is pending after a failed attempt"
    return None


def _ingestion_stuck_reason(entry: IngestionQueueEntry, *, now: datetime) -> str | None:
    status = str(entry.status or "")
    if status in {"failed", "needs_review", "blocked", "rejected"}:
        return (
            safe_reason(entry.last_error)
            or review_reason(entry.review_json)
            or f"ingestion queue status is {status}"
        )
    if status == "processing":
        created_at = parse_datetime(entry.created_at)
        if created_at is None or now - created_at >= RUNNING_STUCK_AFTER:
            return safe_reason(entry.last_error) or "ingestion has been processing for too long"
    return None


def _connector_stuck_reason(run: Any, *, now: datetime) -> str | None:
    status = str(getattr(run, "status", "") or "")
    if status == "failed":
        return safe_reason(getattr(run, "failure_reason", None)) or "connector run failed"
    if status == "running":
        started_at = parse_datetime(getattr(run, "started_at", None))
        if started_at is None or now - started_at >= RUNNING_STUCK_AFTER:
            return "connector run has been running for too long"
    return None
