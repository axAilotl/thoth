"""
Bookmark ingest helpers.

These helpers keep bookmark payload canonicalization and realtime dedupe in one
place so browser captures and X API backfills converge on the same update
rules.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableSequence
from datetime import datetime
from typing import Any

from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id


def build_bookmark_queue_payload(
    bookmark_data: Mapping[str, Any],
    *,
    default_source: str = "browser_extension",
    default_timestamp: datetime | None = None,
    graphql_cache_file: str | None = None,
) -> dict[str, Any]:
    """Return the canonical payload that gets persisted to the queue."""
    normalized = normalize_bookmark_payload(
        bookmark_data,
        default_source=default_source,
        default_timestamp=default_timestamp,
    )

    if graphql_cache_file is not None:
        cache_filename = str(graphql_cache_file).strip()
        if not cache_filename:
            raise ValueError("bookmark payload missing graphql_cache_file")
        normalized["graphql_cache_file"] = cache_filename

    normalized.pop("graphql_response", None)
    return dict(normalized)


def build_realtime_bookmark_record(
    bookmark_data: Mapping[str, Any],
    *,
    default_source: str = "browser_extension",
    default_timestamp: datetime | None = None,
    graphql_cache_file: str | None = None,
) -> dict[str, Any]:
    """Return the bookmark record stored in the realtime JSON file."""
    queue_payload = build_bookmark_queue_payload(
        bookmark_data,
        default_source=default_source,
        default_timestamp=default_timestamp,
        graphql_cache_file=graphql_cache_file,
    )
    record: dict[str, Any] = {
        "tweet_id": queue_payload["tweet_id"],
        "timestamp": queue_payload["timestamp"],
        "source": queue_payload["source"],
        "processed": False,
    }
    if queue_payload.get("tweet_data") is not None:
        record["tweet_data"] = queue_payload["tweet_data"]
    if queue_payload.get("graphql_cache_file") is not None:
        record["graphql_cache_file"] = queue_payload["graphql_cache_file"]
    return record


def merge_realtime_bookmark_record(
    bookmarks: MutableSequence[dict[str, Any]],
    bookmark_record: Mapping[str, Any],
    *,
    reset_processed: bool = False,
) -> bool:
    """
    Merge a canonical bookmark record into the realtime JSON list.

    Returns True when the list changed.
    """
    canonical = build_realtime_bookmark_record(bookmark_record)
    tweet_id = validate_tweet_id(canonical.get("tweet_id"))

    existing = None
    for entry in bookmarks:
        if entry.get("tweet_id") == tweet_id:
            existing = entry
            break

    if existing is None:
        bookmarks.append(canonical)
        return True

    dirty = False
    for field in ("tweet_data", "graphql_cache_file"):
        value = canonical.get(field)
        if value is not None and existing.get(field) != value:
            existing[field] = value
            dirty = True

    for field in ("timestamp", "source"):
        value = canonical.get(field)
        if value is not None and not existing.get(field):
            existing[field] = value
            dirty = True

    if reset_processed and existing.get("processed") is not False:
        existing["processed"] = False
        dirty = True

    return dirty
