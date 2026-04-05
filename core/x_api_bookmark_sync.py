"""Strict X bookmark sync client.

This module pages the X bookmarks lookup endpoint, deduplicates against a
durable checkpoint, and emits canonical bookmark payloads for unseen posts
only. It is intentionally narrow and fail-closed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import httpx

from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id
from .config import Config
from .non_live_state import validate_non_live_interval_hours
from .path_layout import build_path_layout
from .x_api_auth import (
    XApiAuthConfig,
    XApiAuthStateError,
    XApiTokenError,
    fetch_current_x_user,
    load_x_api_token_bundle,
    refresh_x_api_tokens,
    resolve_x_api_auth_config,
    store_x_api_token_bundle,
)

X_API_BOOKMARKS_URL = "https://api.x.com/2/users/{user_id}/bookmarks"
X_API_BOOKMARKS_SOURCE = "x_api_backfill"
X_API_BOOKMARKS_CHECKPOINT_FILENAME = "x_api_bookmark_sync_checkpoint.json"
X_API_BOOKMARKS_MAX_RESULTS = 100
X_API_BOOKMARKS_MAX_SEEN_IDS = 2000
X_API_BOOKMARKS_REQUIRED_EXPANSIONS = (
    "author_id,attachments.media_keys,referenced_tweets.id,referenced_tweets.id.author_id"
)
X_API_BOOKMARKS_REQUIRED_TWEET_FIELDS = (
    "attachments,author_id,conversation_id,created_at,entities,lang,public_metrics,"
    "referenced_tweets,text"
)
X_API_BOOKMARKS_REQUIRED_USER_FIELDS = "id,name,profile_image_url,username,verified"
X_API_BOOKMARKS_REQUIRED_MEDIA_FIELDS = (
    "alt_text,duration_ms,height,media_key,preview_image_url,type,url,width"
)


class XApiBookmarkSyncError(RuntimeError):
    """Base class for bookmark sync failures."""


class XApiBookmarkSyncConfigError(XApiBookmarkSyncError, ValueError):
    """Raised when sync configuration is invalid or incomplete."""


class XApiBookmarkSyncStateError(XApiBookmarkSyncError, ValueError):
    """Raised when checkpoint or auth state is invalid."""


@dataclass(frozen=True)
class XApiBookmarkSyncConfig:
    """Resolved X bookmark backfill settings."""

    enabled: bool = False
    interval_hours: float = 8.0
    run_on_startup: bool = False
    max_results: int = X_API_BOOKMARKS_MAX_RESULTS
    max_pages: int | None = None
    resume_from_checkpoint: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "interval_hours": self.interval_hours,
            "run_on_startup": self.run_on_startup,
            "max_results": self.max_results,
            "max_pages": self.max_pages,
            "resume_from_checkpoint": self.resume_from_checkpoint,
        }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime | None = None) -> str:
    return (dt or _now_utc()).isoformat()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    tmp_path.replace(path)


def _read_json_file(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise XApiBookmarkSyncStateError(
            f"Invalid checkpoint payload in {path}: expected object"
        )
    return payload


def _normalize_string_list(values: Any) -> tuple[str, ...]:
    if values is None:
        return tuple()
    if isinstance(values, str):
        items = [part.strip() for part in values.split(",") if part.strip()]
    elif isinstance(values, (list, tuple)):
        items = [str(value).strip() for value in values if str(value).strip()]
    else:
        raise XApiBookmarkSyncStateError("Checkpoint list fields must be strings or arrays")
    deduped: dict[str, None] = {}
    for item in items:
        deduped[item] = None
    return tuple(deduped.keys())


def _parse_positive_int(
    value: Any,
    *,
    field_name: str,
    minimum: int = 1,
    maximum: int | None = None,
    allow_none: bool = False,
) -> int | None:
    if value is None:
        if allow_none:
            return None
        raise XApiBookmarkSyncConfigError(f"{field_name} is required")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise XApiBookmarkSyncConfigError(f"{field_name} must be an integer") from exc
    if parsed < minimum:
        raise XApiBookmarkSyncConfigError(f"{field_name} must be at least {minimum}")
    if maximum is not None and parsed > maximum:
        raise XApiBookmarkSyncConfigError(f"{field_name} must be at most {maximum}")
    return parsed


def resolve_x_api_bookmark_sync_config(config: Config) -> XApiBookmarkSyncConfig:
    """Validate and normalize X bookmark backfill settings."""
    automation_config = config.get("automation.x_api_sync", {}) or {}
    if not isinstance(automation_config, dict):
        raise XApiBookmarkSyncConfigError("automation.x_api_sync must be an object")

    enabled = bool(automation_config.get("enabled", False))
    try:
        interval_hours = validate_non_live_interval_hours(
            automation_config.get("interval_hours", 8),
            field_name="automation.x_api_sync.interval_hours",
        )
    except ValueError as exc:
        raise XApiBookmarkSyncConfigError(str(exc)) from exc
    run_on_startup = bool(automation_config.get("run_on_startup", False))
    max_results = _parse_positive_int(
        automation_config.get("max_results", X_API_BOOKMARKS_MAX_RESULTS),
        field_name="automation.x_api_sync.max_results",
        minimum=1,
        maximum=X_API_BOOKMARKS_MAX_RESULTS,
    )
    max_pages = _parse_positive_int(
        automation_config.get("max_pages"),
        field_name="automation.x_api_sync.max_pages",
        allow_none=True,
    )
    resume_from_checkpoint = bool(
        automation_config.get("resume_from_checkpoint", True)
    )

    if enabled:
        # Fail closed if auth is not configured correctly.
        resolve_x_api_auth_config(config)

    return XApiBookmarkSyncConfig(
        enabled=enabled,
        interval_hours=interval_hours,
        run_on_startup=run_on_startup,
        max_results=max_results,
        max_pages=max_pages,
        resume_from_checkpoint=resume_from_checkpoint,
    )


@dataclass(frozen=True)
class XApiBookmarkSyncCheckpoint:
    """Durable bookmark sync state."""

    user_id: str
    seen_bookmark_ids: tuple[str, ...] = ()
    pagination_token: str | None = None
    last_synced_at: str | None = None
    last_synced_bookmark_id: str | None = None
    last_result_count: int = 0
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "user_id": self.user_id,
            "seen_bookmark_ids": list(self.seen_bookmark_ids),
            "pagination_token": self.pagination_token,
            "last_synced_at": self.last_synced_at,
            "last_synced_bookmark_id": self.last_synced_bookmark_id,
            "last_result_count": self.last_result_count,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "XApiBookmarkSyncCheckpoint":
        if payload.get("version") not in (None, 1):
            raise XApiBookmarkSyncStateError(
                "Unsupported X API bookmark sync checkpoint version"
            )
        user_id = str(payload.get("user_id", "")).strip()
        if not user_id:
            raise XApiBookmarkSyncStateError(
                "X API bookmark sync checkpoint is missing user_id"
            )
        seen_ids = _normalize_string_list(payload.get("seen_bookmark_ids"))
        pagination_token = payload.get("pagination_token")
        if pagination_token is not None:
            pagination_token = str(pagination_token).strip() or None
        last_synced_at = payload.get("last_synced_at")
        if last_synced_at is not None:
            last_synced_at = str(last_synced_at).strip() or None
        last_synced_bookmark_id = payload.get("last_synced_bookmark_id")
        if last_synced_bookmark_id is not None:
            last_synced_bookmark_id = str(last_synced_bookmark_id).strip() or None
        last_result_count = payload.get("last_result_count", 0)
        try:
            last_result_count = int(last_result_count)
        except (TypeError, ValueError) as exc:
            raise XApiBookmarkSyncStateError(
                "Checkpoint last_result_count must be an integer"
            ) from exc
        return cls(
            user_id=user_id,
            seen_bookmark_ids=seen_ids,
            pagination_token=pagination_token,
            last_synced_at=last_synced_at,
            last_synced_bookmark_id=last_synced_bookmark_id,
            last_result_count=last_result_count,
        )


def _checkpoint_path(layout) -> Path:
    return layout.auth_root / X_API_BOOKMARKS_CHECKPOINT_FILENAME


def load_x_api_bookmark_sync_checkpoint(layout) -> XApiBookmarkSyncCheckpoint | None:
    """Load the stored checkpoint if it exists."""
    path = _checkpoint_path(layout)
    if not path.exists():
        return None
    return XApiBookmarkSyncCheckpoint.from_dict(_read_json_file(path))


def store_x_api_bookmark_sync_checkpoint(
    layout,
    checkpoint: XApiBookmarkSyncCheckpoint,
) -> dict[str, Any]:
    """Persist checkpoint state atomically."""
    payload = checkpoint.to_dict()
    _atomic_write_json(_checkpoint_path(layout), payload)
    return payload


def _build_request_params(
    *,
    pagination_token: str | None,
    max_results: int,
) -> dict[str, str]:
    params = {
        "max_results": str(max_results),
        "expansions": X_API_BOOKMARKS_REQUIRED_EXPANSIONS,
        "tweet.fields": X_API_BOOKMARKS_REQUIRED_TWEET_FIELDS,
        "user.fields": X_API_BOOKMARKS_REQUIRED_USER_FIELDS,
        "media.fields": X_API_BOOKMARKS_REQUIRED_MEDIA_FIELDS,
    }
    if pagination_token:
        params["pagination_token"] = pagination_token
    return params


def _bookmark_timestamp(bookmark: Mapping[str, Any]) -> str:
    for key in ("bookmarked_at", "bookmark_created_at", "saved_at", "timestamp"):
        value = bookmark.get(key)
        if value:
            return str(value).strip()
    return _iso_utc()


def _index_includes(includes: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    includes = includes or {}
    indexed: dict[str, dict[str, Any]] = {
        "users": {},
        "media": {},
        "tweets": {},
    }
    for item in includes.get("users", []) or []:
        if isinstance(item, dict):
            item_id = str(item.get("id", "")).strip()
            if item_id:
                indexed["users"][item_id] = item
    for item in includes.get("media", []) or []:
        if isinstance(item, dict):
            item_id = str(item.get("media_key", "")).strip()
            if item_id:
                indexed["media"][item_id] = item
    for item in includes.get("tweets", []) or []:
        if isinstance(item, dict):
            item_id = str(item.get("id", "")).strip()
            if item_id:
                indexed["tweets"][item_id] = item
    return indexed


def _build_tweet_data(
    tweet: Mapping[str, Any],
    *,
    includes: Mapping[str, Any] | None,
) -> dict[str, Any]:
    tweet_id = validate_tweet_id(tweet.get("id"))
    indexed_includes = _index_includes(includes)
    author_id = str(tweet.get("author_id", "")).strip()
    author = indexed_includes["users"].get(author_id, {}) if author_id else {}
    media_keys = []
    attachments = tweet.get("attachments")
    if isinstance(attachments, dict):
        media_keys = [
            str(media_key).strip()
            for media_key in attachments.get("media_keys", [])
            if str(media_key).strip()
        ]
    referenced_tweets = tweet.get("referenced_tweets")
    if not isinstance(referenced_tweets, list):
        referenced_tweets = []

    media_payloads: list[dict[str, Any]] = []
    for media_key in media_keys:
        media_item = indexed_includes["media"].get(media_key)
        if media_item:
            media_payloads.append(media_item)

    text = str(tweet.get("text", "")).strip()
    return {
        "id": tweet_id,
        "text": text,
        "full_text": text,
        "created_at": tweet.get("created_at"),
        "author_id": author_id or None,
        "author_username": author.get("username"),
        "author_name": author.get("name"),
        "author": author.get("username") or author.get("name") or author_id or None,
        "conversation_id": tweet.get("conversation_id"),
        "lang": tweet.get("lang"),
        "public_metrics": tweet.get("public_metrics"),
        "entities": tweet.get("entities"),
        "attachments": attachments,
        "referenced_tweets": referenced_tweets,
        "media": media_payloads,
    }


async def _fetch_bookmark_page(
    *,
    client: httpx.AsyncClient,
    access_token: str,
    user_id: str,
    pagination_token: str | None,
    max_results: int,
) -> dict[str, Any]:
    url = X_API_BOOKMARKS_URL.format(user_id=user_id)
    headers = {"Authorization": f"Bearer {access_token}"}
    params = _build_request_params(
        pagination_token=pagination_token,
        max_results=max_results,
    )
    response = await client.get(url, headers=headers, params=params)
    if response.status_code == 429:
        raise XApiBookmarkSyncError(
            "X API bookmark lookup was rate limited"
        )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise XApiBookmarkSyncError(
            f"X API bookmark lookup failed with status {response.status_code}"
        ) from exc
    payload = response.json()
    if not isinstance(payload, dict):
        raise XApiBookmarkSyncError("X API bookmark lookup response must be a JSON object")
    return payload


async def _resolve_bundle_and_user_id(
    config: Config,
    *,
    layout,
) -> tuple[XApiAuthConfig, dict[str, Any], str]:
    auth_config = resolve_x_api_auth_config(config)
    bundle = load_x_api_token_bundle(layout)
    if not bundle:
        raise XApiAuthStateError("No stored X API token bundle was found")

    expires_at = str(bundle.get("expires_at") or "").strip()
    refresh_token = str(bundle.get("refresh_token") or "").strip()
    if expires_at:
        try:
            is_expired = datetime.fromisoformat(expires_at)
            if is_expired.tzinfo is None:
                is_expired = is_expired.replace(tzinfo=timezone.utc)
            else:
                is_expired = is_expired.astimezone(timezone.utc)
            if is_expired <= _now_utc():
                if not refresh_token:
                    raise XApiTokenError(
                        "Stored X API token bundle is expired and lacks a refresh token"
                    )
                refreshed_bundle = await refresh_x_api_tokens(
                    auth_config,
                    refresh_token=refresh_token,
                )
                refreshed_payload = refreshed_bundle.to_dict()
                refreshed_payload["user"] = bundle.get("user")
                bundle = store_x_api_token_bundle(layout, refreshed_payload)
        except ValueError as exc:
            raise XApiTokenError("Stored X API token bundle has an invalid expires_at") from exc

    access_token = str(bundle.get("access_token") or "").strip()
    if not access_token:
        raise XApiTokenError("Stored X API token bundle is missing access_token")

    user = bundle.get("user")
    user_id = None
    if isinstance(user, dict):
        user_data = user.get("data") if isinstance(user.get("data"), dict) else user
        if isinstance(user_data, dict):
            user_id = str(user_data.get("id", "")).strip() or None

    if not user_id:
        user_payload = await fetch_current_x_user(auth_config, access_token=access_token)
        user_data = user_payload.get("data") if isinstance(user_payload, dict) else None
        if not isinstance(user_data, dict):
            raise XApiTokenError("X API /2/users/me response did not contain a user object")
        user_id = str(user_data.get("id", "")).strip()
        if not user_id:
            raise XApiTokenError("X API /2/users/me response did not contain id")
        bundle["user"] = user_payload
        bundle = store_x_api_token_bundle(layout, bundle)

    return auth_config, bundle, user_id


def _merge_seen_ids(existing: tuple[str, ...], new_ids: list[str]) -> tuple[str, ...]:
    merged = list(existing)
    seen = set(existing)
    for tweet_id in new_ids:
        if tweet_id not in seen:
            merged.append(tweet_id)
            seen.add(tweet_id)
    if len(merged) > X_API_BOOKMARKS_MAX_SEEN_IDS:
        merged = merged[-X_API_BOOKMARKS_MAX_SEEN_IDS:]
    return tuple(merged)


async def sync_x_api_bookmarks(
    config: Config,
    *,
    layout=None,
    max_results: int = X_API_BOOKMARKS_MAX_RESULTS,
    max_pages: int | None = None,
    resume_from_checkpoint: bool = True,
) -> dict[str, Any]:
    """Fetch bookmark pages, persist checkpoint state, and emit new payloads."""
    if not 1 <= max_results <= X_API_BOOKMARKS_MAX_RESULTS:
        raise XApiBookmarkSyncConfigError(
            f"max_results must be between 1 and {X_API_BOOKMARKS_MAX_RESULTS}"
        )
    if max_pages is not None and max_pages < 1:
        raise XApiBookmarkSyncConfigError("max_pages must be positive when provided")

    resolved_layout = layout or build_path_layout(config)
    resolved_layout.ensure_directories()
    auth_config, bundle, user_id = await _resolve_bundle_and_user_id(
        config, layout=resolved_layout
    )

    checkpoint = load_x_api_bookmark_sync_checkpoint(resolved_layout)
    if checkpoint and checkpoint.user_id != user_id:
        raise XApiBookmarkSyncStateError(
            "Stored bookmark sync checkpoint belongs to a different X user"
        )

    seen_ids = set(checkpoint.seen_bookmark_ids if checkpoint else ())
    seen_order = list(checkpoint.seen_bookmark_ids if checkpoint else ())
    pagination_token = (
        checkpoint.pagination_token if (resume_from_checkpoint and checkpoint) else None
    )

    payloads: list[dict[str, Any]] = []
    pages_fetched = 0
    stopped_at_known_id = False
    last_sync_timestamp = _iso_utc()

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            if max_pages is not None and pages_fetched >= max_pages:
                break

            page_payload = await _fetch_bookmark_page(
                client=client,
                access_token=str(bundle["access_token"]),
                user_id=user_id,
                pagination_token=pagination_token,
                max_results=max_results,
            )
            pages_fetched += 1

            page_data = page_payload.get("data") or []
            if not isinstance(page_data, list):
                raise XApiBookmarkSyncError("X API bookmark lookup data must be a list")
            includes = page_payload.get("includes")
            if includes is not None and not isinstance(includes, dict):
                raise XApiBookmarkSyncError("X API bookmark lookup includes must be an object")
            meta = page_payload.get("meta")
            if meta is not None and not isinstance(meta, dict):
                raise XApiBookmarkSyncError("X API bookmark lookup meta must be an object")

            unseen_ids: list[str] = []
            for tweet in page_data:
                if not isinstance(tweet, dict):
                    raise XApiBookmarkSyncError(
                        "X API bookmark lookup returned a non-object tweet"
                    )
                tweet_id = validate_tweet_id(tweet.get("id"))
                if tweet_id in seen_ids:
                    stopped_at_known_id = True
                    break
                payload = normalize_bookmark_payload(
                    {
                        "tweet_id": tweet_id,
                        "tweet_data": _build_tweet_data(tweet, includes=includes),
                        "timestamp": _bookmark_timestamp(tweet),
                        "source": X_API_BOOKMARKS_SOURCE,
                    },
                    default_source=X_API_BOOKMARKS_SOURCE,
                    default_timestamp=_now_utc(),
                )
                payloads.append(payload)
                seen_ids.add(tweet_id)
                unseen_ids.append(tweet_id)

            next_token = None
            if isinstance(meta, dict):
                raw_next_token = meta.get("next_token")
                if raw_next_token is not None:
                    next_token = str(raw_next_token).strip() or None

            seen_order = list(_merge_seen_ids(tuple(seen_order), unseen_ids))
            checkpoint_payload = XApiBookmarkSyncCheckpoint(
                user_id=user_id,
                seen_bookmark_ids=tuple(seen_order),
                pagination_token=None if stopped_at_known_id else next_token,
                last_synced_at=last_sync_timestamp,
                last_synced_bookmark_id=unseen_ids[-1] if unseen_ids else None,
                last_result_count=len(page_data),
            )
            store_x_api_bookmark_sync_checkpoint(resolved_layout, checkpoint_payload)

            if stopped_at_known_id or not next_token:
                break
            pagination_token = next_token

    return {
        "status": "ok",
        "user_id": user_id,
        "pages_fetched": pages_fetched,
        "bookmarks_emitted": len(payloads),
        "stopped_at_known_id": stopped_at_known_id,
        "checkpoint": load_x_api_bookmark_sync_checkpoint(resolved_layout).to_dict(),
        "payloads": payloads,
    }


async def run_x_api_bookmark_backfill(
    config: Config,
    *,
    layout=None,
    max_results: int | None = None,
    max_pages: int | None = None,
    resume_from_checkpoint: bool | None = None,
) -> dict[str, Any]:
    """Run a backfill using the resolved X sync settings."""
    sync_config = resolve_x_api_bookmark_sync_config(config)
    resolved_layout = layout or build_path_layout(config)
    resolved_layout.ensure_directories()

    return await sync_x_api_bookmarks(
        config,
        layout=resolved_layout,
        max_results=(
            sync_config.max_results if max_results is None else max_results
        ),
        max_pages=max_pages if max_pages is not None else sync_config.max_pages,
        resume_from_checkpoint=(
            sync_config.resume_from_checkpoint
            if resume_from_checkpoint is None
            else resume_from_checkpoint
        ),
    )
