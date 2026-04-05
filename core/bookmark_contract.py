"""
Canonical bookmark capture contract for Thoth.

The live userscript, API ingress, durable queue, and future X API backfill
should all use this shape. The contract is intentionally small and fail-closed:

- tweet IDs must be numeric strings
- timestamps must be present before a payload is persisted
- source tags must be explicit and non-empty
- GraphQL payloads are optional, but if present they travel with the same
  canonical record through capture, queueing, and processing
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, MutableMapping

BOOKMARK_PAYLOAD_FIELDS = (
    "tweet_id",
    "tweet_data",
    "graphql_response",
    "graphql_cache_file",
    "timestamp",
    "source",
    "force",
)


def validate_tweet_id(tweet_id: Any) -> str:
    """Return a normalized tweet id or raise if the id is invalid."""
    if tweet_id is None:
        raise ValueError("bookmark payload missing tweet_id")

    normalized = str(tweet_id).strip()
    if not normalized or not normalized.isdigit():
        raise ValueError(f"invalid tweet_id format: {tweet_id!r}")
    return normalized


def normalize_source(source: Any, *, default: str = "browser_extension") -> str:
    """
    Normalize the bookmark source tag.

    The live userscript currently emits `userscript_*` hints, the API accepts
    `browser_extension` as the public capture source, and future X API sync will
    add explicit `x_api_*` tags. This helper keeps the contract explicit without
    guessing.
    """
    candidate = source if source is not None else default
    normalized = str(candidate).strip()
    if not normalized:
        raise ValueError("bookmark payload missing source")
    return normalized


def normalize_timestamp(timestamp: Any, *, default: datetime | None = None) -> str:
    """Return an ISO-8601 timestamp string, defaulting to now when absent."""
    candidate = timestamp if timestamp is not None else default or datetime.now()
    if isinstance(candidate, datetime):
        return candidate.isoformat()

    normalized = str(candidate).strip()
    if not normalized:
        raise ValueError("bookmark payload missing timestamp")
    return normalized


def build_graphql_cache_filename(
    tweet_id: Any, *, timestamp: datetime | None = None
) -> str:
    """Build the canonical cache filename for a GraphQL capture."""
    normalized_tweet_id = validate_tweet_id(tweet_id)
    stamp = (timestamp or datetime.now()).strftime("%Y%m%d_%H%M%S")
    return f"tweet_{normalized_tweet_id}_{stamp}.json"


def normalize_bookmark_payload(
    payload: Mapping[str, Any],
    *,
    default_source: str = "browser_extension",
    default_timestamp: datetime | None = None,
) -> dict[str, Any]:
    """
    Normalize a bookmark payload before it is persisted or queued.

    The output preserves the current live-capture keys while stripping transient
    flags that should not become part of durable storage.
    """
    normalized: MutableMapping[str, Any] = dict(payload)
    normalized["tweet_id"] = validate_tweet_id(normalized.get("tweet_id"))
    normalized["source"] = normalize_source(
        normalized.get("source"), default=default_source
    )
    normalized["timestamp"] = normalize_timestamp(
        normalized.get("timestamp"), default=default_timestamp
    )

    if "force" in normalized:
        normalized["force"] = bool(normalized["force"])

    return dict(normalized)


def bookmark_contract_summary() -> str:
    """Return a concise human-readable summary of the contract."""
    return (
        "Thoth bookmark payloads require tweet_id, source, and timestamp; "
        "GraphQL payloads are optional and, when present, are cached separately "
        "while the durable queue stores only canonical metadata."
    )
