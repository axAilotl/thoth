"""Cissa-managed continuous X bookmark sync.

Cissa owns the X OAuth token and is its only refresher; a managed run receives a
fresh access token as ``credentials.token``. This module never reads, writes or
refreshes the standalone token bundle. The durable incremental checkpoint stays
in the account root, so each run stops at the newest bookmark it already saw.
"""
from __future__ import annotations

import asyncio

import httpx

from .collection_result import CollectionCoverage, collection_limit
from .x_api_auth import X_API_ME_URL
from .x_api_bookmark_sync import (
    X_API_BOOKMARKS_MAX_RESULTS,
    XApiBookmarkSyncError,
    load_x_api_bookmark_sync_checkpoint,
    sync_x_api_bookmarks,
)


def run_managed_x_api_bookmarks(context) -> dict:
    config = context.config
    username = config.get("sources.x_api.username")
    resource = f"x_api:bookmarks:{username}"
    coverage = CollectionCoverage(config)
    entry = coverage.source(resource, time_basis="observation_time")
    token = context.credentials.get("token")
    if not isinstance(token, str) or not token.strip() or not isinstance(username, str):
        coverage.fail(resource, "x_api_credential_missing")
        return {**coverage.receipt(), "queued_count": 0}
    timeout = collection_limit(config, "request_timeout_seconds")
    try:
        user = asyncio.run(_current_user(token, timeout))
    except (httpx.HTTPError, ValueError):
        coverage.fail(resource, "x_api_user_unavailable")
        return {**coverage.receipt(), "queued_count": 0}
    if str(user.get("username", "")).lower() != username.lower() or not str(user.get("id", "")).isdigit():
        # The token belongs to another X account than the selected one.
        coverage.fail(resource, "x_api_account_mismatch")
        return {**coverage.receipt(), "queued_count": 0}
    try:
        result = asyncio.run(sync_x_api_bookmarks(
            config, layout=context.layout, db=context.db, capture_event_store=context.event_store,
            max_results=X_API_BOOKMARKS_MAX_RESULTS, max_pages=int(collection_limit(config, "max_pages")),
            resume_from_checkpoint=True, managed_identity=(token, str(user["id"]))))
    except XApiBookmarkSyncError as error:
        code = "x_api_rate_limited" if "rate limited" in str(error) else "x_api_bookmarks_unavailable"
        coverage.fail(resource, code, partial=True)
        return {**coverage.receipt(), "queued_count": 0}
    entry["observed_count"] = entry["queued_count"] = result["bookmarks_emitted"]
    checkpoint = load_x_api_bookmark_sync_checkpoint(context.layout)
    if checkpoint is not None and checkpoint.pagination_token and not result["stopped_at_known_id"]:
        # Older pages remain; the next run resumes from the stored checkpoint.
        coverage.limited(resource)
    return {**coverage.receipt(), "queued_count": result["bookmarks_emitted"]}


async def _current_user(token: str, timeout: float) -> dict:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(X_API_ME_URL, headers={"Authorization": f"Bearer {token.strip()}"})
        response.raise_for_status()
        data = response.json().get("data")
    if not isinstance(data, dict):
        raise ValueError("x_api_user_invalid")
    return data
