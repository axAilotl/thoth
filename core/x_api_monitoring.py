"""Authenticated webhook capture for monitored X accounts."""

from __future__ import annotations

import hmac
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id
from .capture_event_store import CaptureEventStore
from .config import Config
from .connector_capture import ConnectorCaptureQueue, write_connector_raw_json
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout

X_API_MONITOR_SOURCE = "x_api_monitored_webhook"
X_API_MONITOR_SECRET_HEADER = "X-Thoth-Webhook-Secret"
X_API_MONITOR_REQUIRED_SCOPE = "bookmark.write"
DEFAULT_X_API_MONITOR_SECRET_ENV = "THOTH_X_MONITOR_WEBHOOK_SECRET"


class XApiMonitoringError(RuntimeError):
    """Base class for monitored-account webhook failures."""


class XApiMonitoringConfigError(XApiMonitoringError, ValueError):
    """Raised when monitored-account capture is not configured safely."""


class XApiMonitoringAuthError(XApiMonitoringError):
    """Raised when webhook authentication fails."""


class XApiMonitoringPayloadError(XApiMonitoringError, ValueError):
    """Raised when an X webhook payload cannot be normalized."""


@dataclass(frozen=True)
class XApiMonitoringConfig:
    """Validated monitored-account webhook settings."""

    accounts: tuple[str, ...]
    webhook_secret_env: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_account(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("@"):
        normalized = normalized[1:]
    return normalized


def resolve_x_api_monitoring_config(config: Config) -> XApiMonitoringConfig:
    """Resolve monitoring settings and fail closed on incomplete configuration."""
    x_api = config.get("sources.x_api", {}) or {}
    if not isinstance(x_api, dict):
        raise XApiMonitoringConfigError("sources.x_api must be an object")
    monitoring = x_api.get("monitoring", {}) or {}
    if not isinstance(monitoring, dict):
        raise XApiMonitoringConfigError("sources.x_api.monitoring must be an object")
    if not x_api.get("enabled", False) or not monitoring.get("enabled", False):
        raise XApiMonitoringConfigError("X API monitoring is not enabled")

    raw_accounts = monitoring.get("accounts")
    if not isinstance(raw_accounts, (list, tuple)):
        raise XApiMonitoringConfigError(
            "sources.x_api.monitoring.accounts must be an array"
        )
    accounts = tuple(
        dict.fromkeys(
            account
            for account in (_normalize_account(value) for value in raw_accounts)
            if account
        )
    )
    if not accounts:
        raise XApiMonitoringConfigError(
            "sources.x_api.monitoring.accounts must contain at least one account"
        )

    scopes = x_api.get("scopes") or []
    if isinstance(scopes, str):
        scopes = scopes.split()
    if not isinstance(scopes, (list, tuple)):
        raise XApiMonitoringConfigError("sources.x_api.scopes must be an array")
    normalized_scopes = {str(scope).strip() for scope in scopes}
    if X_API_MONITOR_REQUIRED_SCOPE not in normalized_scopes:
        raise XApiMonitoringConfigError(
            "sources.x_api.scopes must include bookmark.write for monitoring"
        )

    secret_env = str(
        monitoring.get("webhook_secret_env", DEFAULT_X_API_MONITOR_SECRET_ENV)
    ).strip()
    if not secret_env:
        raise XApiMonitoringConfigError(
            "sources.x_api.monitoring.webhook_secret_env is required"
        )
    if not str(os.getenv(secret_env) or "").strip():
        raise XApiMonitoringConfigError(
            f"{secret_env} must be set when X API monitoring is enabled"
        )
    return XApiMonitoringConfig(accounts=accounts, webhook_secret_env=secret_env)


def verify_x_api_monitoring_secret(
    monitoring_config: XApiMonitoringConfig,
    provided_secret: str | None,
) -> None:
    """Authenticate a webhook secret without exposing it in errors."""
    expected = str(os.getenv(monitoring_config.webhook_secret_env) or "").strip()
    provided = str(provided_secret or "")
    if not expected or not provided or not hmac.compare_digest(expected, provided):
        raise XApiMonitoringAuthError("Invalid X monitoring webhook credentials")


def normalize_x_api_monitoring_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize direct and X filtered-stream webhook shapes to bookmark form."""
    if not isinstance(payload, Mapping):
        raise XApiMonitoringPayloadError("X monitoring webhook payload must be an object")
    raw_tweet = payload.get("data", payload)
    if not isinstance(raw_tweet, Mapping):
        raise XApiMonitoringPayloadError("X monitoring webhook data must be an object")

    tweet_id = validate_tweet_id(raw_tweet.get("id", raw_tweet.get("tweet_id")))
    author_id = str(raw_tweet.get("author_id") or "").strip() or None
    author: Mapping[str, Any] = {}
    includes = payload.get("includes")
    if isinstance(includes, Mapping):
        users = includes.get("users") or []
        if isinstance(users, list):
            author = next(
                (
                    user
                    for user in users
                    if isinstance(user, Mapping)
                    and str(user.get("id") or "").strip() == author_id
                ),
                {},
            )

    username = _normalize_account(
        raw_tweet.get("author_username") or author.get("username")
    ) or None
    created_at = str(raw_tweet.get("created_at") or "").strip() or _now_iso()
    tweet_data = dict(raw_tweet)
    tweet_data["id"] = tweet_id
    if author_id:
        tweet_data["author_id"] = author_id
    if username:
        tweet_data["author_username"] = username
    if author:
        tweet_data["author"] = dict(author)

    return normalize_bookmark_payload(
        {
            "tweet_id": tweet_id,
            "tweet_data": tweet_data,
            "timestamp": created_at,
            "source": X_API_MONITOR_SOURCE,
        },
        default_source=X_API_MONITOR_SOURCE,
    )


def capture_x_api_monitoring_webhook(
    config: Config,
    payload: Mapping[str, Any],
    *,
    webhook_secret: str | None,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
    capture_event_store: CaptureEventStore | None = None,
) -> dict[str, Any]:
    """Authenticate, normalize, and queue one monitored X post."""
    monitoring = resolve_x_api_monitoring_config(config)
    verify_x_api_monitoring_secret(monitoring, webhook_secret)
    bookmark = normalize_x_api_monitoring_payload(payload)
    tweet_data = bookmark["tweet_data"]
    author_id = _normalize_account(tweet_data.get("author_id"))
    username = _normalize_account(tweet_data.get("author_username"))
    matched_account = next(
        (
            account
            for account in monitoring.accounts
            if account == author_id or account == username
        ),
        None,
    )
    if matched_account is None:
        return {
            "status": "ignored",
            "reason": "unmonitored_account",
            "tweet_id": bookmark["tweet_id"],
        }

    bookmark["normalized_metadata"] = {
        "monitored_account": matched_account,
        "provenance": {"collector": "x_api_monitoring"},
    }

    resolved_layout = layout or build_path_layout(config)
    resolved_layout.ensure_directories()
    capture_queue = ConnectorCaptureQueue(
        config,
        layout=resolved_layout,
        db=db or get_metadata_db(),
        capture_event_store=capture_event_store,
    )
    raw_path = write_connector_raw_json(
        resolved_layout,
        connector_name="x_api",
        subdir="monitoring",
        native_id=bookmark["tweet_id"],
        payload=bookmark,
        captured_at=bookmark["timestamp"],
    )
    with capture_queue.lifecycle() as lifecycle:
        result = capture_queue.queue_payload(
            lifecycle,
            artifact_type="tweet",
            payload=bookmark,
            source={
                "source_name": X_API_MONITOR_SOURCE,
                "source_type": "twitter",
                "collector": "x_api_monitoring",
                "account": matched_account,
                "native_source_id": matched_account,
                "base_uri": "https://api.x.com/2/tweets/search/stream",
            },
            session={
                "session_type": "x_api_monitoring_webhook",
                "native_session_id": f"x_api_monitoring:{matched_account}",
                "started_at": bookmark["timestamp"],
                "metadata": {"account": matched_account},
            },
            event={
                "event_type": "x_api_monitored_post",
                "native_event_id": bookmark["tweet_id"],
                "occurred_at": bookmark["timestamp"],
                "captured_at": bookmark["timestamp"],
                "privacy": {"classification": "personal"},
                "provenance": {"collector": "x_api_monitoring"},
            },
            raw_path=raw_path,
            queue_artifact_id=bookmark["tweet_id"],
            capabilities=(X_API_MONITOR_REQUIRED_SCOPE,),
        )
    return {
        "status": "accepted",
        "tweet_id": bookmark["tweet_id"],
        "matched_account": matched_account,
        "queue_status": result.queue_status,
        "artifact_id": result.queue_artifact_id,
        "capture_event_id": result.event_id,
    }


__all__ = [
    "DEFAULT_X_API_MONITOR_SECRET_ENV",
    "X_API_MONITOR_REQUIRED_SCOPE",
    "X_API_MONITOR_SECRET_HEADER",
    "X_API_MONITOR_SOURCE",
    "XApiMonitoringAuthError",
    "XApiMonitoringConfig",
    "XApiMonitoringConfigError",
    "XApiMonitoringError",
    "XApiMonitoringPayloadError",
    "capture_x_api_monitoring_webhook",
    "normalize_x_api_monitoring_payload",
    "resolve_x_api_monitoring_config",
    "verify_x_api_monitoring_secret",
]
