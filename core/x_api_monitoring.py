"""Webhook-driven monitored-account capture for X posts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from typing import Any, Mapping

import httpx

from .archivist_topics import load_archivist_topic_registry
from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id
from .config import Config
from .llm_interface import LLMInterface
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import build_path_layout
from .x_api_auth import (
    XApiTokenError,
    resolve_authenticated_x_api_context,
)

X_API_MONITOR_SOURCE = "x_api_monitored_webhook"
X_API_MONITOR_TASK = "x_monitor"
X_API_MONITOR_SECRET_HEADER = "X-Thoth-Webhook-Secret"
X_API_MONITOR_BOOKMARK_URL = "https://api.x.com/2/users/{user_id}/bookmarks"
X_API_MONITOR_REQUIRED_SCOPE = "bookmark.write"
DEFAULT_WEBHOOK_SECRET_ENV = "THOTH_X_MONITOR_WEBHOOK_SECRET"
DEFAULT_MONITOR_SYSTEM_PROMPT = (
    "You classify monitored X posts for capture into Thoth. "
    "Be conservative. Favor posts that add durable research, technical, or operator "
    "signal relative to the active archivist topics and ignore generic hype, personal "
    "banter, engagement bait, or low-information chatter. "
    "Return only valid JSON with keys useful, confidence, reason, and matched_topics. "
    "useful must be a boolean. confidence must be a number between 0 and 1. "
    "reason must be a short sentence. matched_topics must be an array of topic ids."
)


class XApiMonitoringError(RuntimeError):
    """Base class for monitored-account capture failures."""


class XApiMonitoringConfigError(XApiMonitoringError, ValueError):
    """Raised when monitored-account capture configuration is invalid."""


@dataclass(frozen=True)
class XApiMonitoredAccount:
    """Normalized monitored X account selector."""

    raw_value: str
    username: str | None = None
    user_id: str | None = None

    def matches(self, *, author_username: str | None, author_id: str | None) -> bool:
        normalized_username = _normalize_username(author_username)
        normalized_user_id = _normalize_user_id(author_id)
        return bool(
            (self.username and normalized_username and self.username == normalized_username)
            or (self.user_id and normalized_user_id and self.user_id == normalized_user_id)
        )

    def label(self) -> str:
        if self.username:
            return f"@{self.username}"
        if self.user_id:
            return self.user_id
        return self.raw_value


@dataclass(frozen=True)
class XApiMonitoringConfig:
    """Resolved monitored-account capture settings."""

    enabled: bool = False
    auto_bookmark: bool = True
    webhook_secret_env: str = DEFAULT_WEBHOOK_SECRET_ENV
    webhook_secret: str | None = None
    accounts: tuple[XApiMonitoredAccount, ...] = ()
    task_name: str = X_API_MONITOR_TASK

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "auto_bookmark": self.auto_bookmark,
            "webhook_secret_env": self.webhook_secret_env,
            "account_count": len(self.accounts),
            "accounts": [account.label() for account in self.accounts],
            "task_name": self.task_name,
        }


@dataclass(frozen=True)
class XApiMonitoredPost:
    """Normalized X post payload emitted by a webhook source."""

    tweet_id: str
    text: str
    created_at: str | None
    author_id: str | None
    author_username: str | None
    author_name: str | None
    conversation_id: str | None
    lang: str | None
    public_metrics: dict[str, Any] | None
    entities: dict[str, Any] | None
    attachments: dict[str, Any] | None
    referenced_tweets: list[dict[str, Any]]
    media: list[dict[str, Any]]
    matching_rules: tuple[str, ...] = ()

    def to_tweet_data(self) -> dict[str, Any]:
        return {
            "id": self.tweet_id,
            "text": self.text,
            "full_text": self.text,
            "created_at": self.created_at,
            "author_id": self.author_id,
            "author_username": self.author_username,
            "author_name": self.author_name,
            "author": self.author_username or self.author_name or self.author_id,
            "conversation_id": self.conversation_id,
            "lang": self.lang,
            "public_metrics": self.public_metrics,
            "entities": self.entities,
            "attachments": self.attachments,
            "referenced_tweets": self.referenced_tweets,
            "media": self.media,
        }


@dataclass(frozen=True)
class XApiMonitoringDecision:
    """Classifier output for a monitored X post."""

    useful: bool
    confidence: float
    reason: str
    matched_topics: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "useful": self.useful,
            "confidence": self.confidence,
            "reason": self.reason,
            "matched_topics": list(self.matched_topics),
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_username(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized.startswith("@"):
        normalized = normalized[1:]
    return normalized or None


def _normalize_user_id(value: Any) -> str | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return validate_tweet_id(normalized)


def _normalize_matching_rules(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    labels: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        label = str(item.get("tag") or item.get("id") or "").strip()
        if label:
            labels.append(label)
    return tuple(dict.fromkeys(labels))


def _normalize_monitored_account(value: Any) -> XApiMonitoredAccount:
    if isinstance(value, Mapping):
        raw_user_id = value.get("user_id") or value.get("id")
        raw_value = str(
            value.get("username") or raw_user_id or ""
        ).strip()
        username = _normalize_username(value.get("username"))
        user_id = _normalize_user_id(raw_user_id)
    else:
        raw_value = str(value or "").strip()
        username = None
        user_id = None
        if raw_value:
            if raw_value.isdigit():
                user_id = _normalize_user_id(raw_value)
            else:
                username = _normalize_username(raw_value)

    if not raw_value:
        raise XApiMonitoringConfigError("sources.x_api.monitoring.accounts cannot contain blanks")
    if not username and not user_id:
        raise XApiMonitoringConfigError(
            f"Unsupported monitored account identifier: {raw_value!r}"
        )
    return XApiMonitoredAccount(raw_value=raw_value, username=username, user_id=user_id)


def _normalize_monitored_accounts(value: Any) -> tuple[XApiMonitoredAccount, ...]:
    if value in (None, ""):
        return ()
    if not isinstance(value, (list, tuple)):
        raise XApiMonitoringConfigError("sources.x_api.monitoring.accounts must be a list")

    normalized: list[XApiMonitoredAccount] = []
    seen: set[tuple[str | None, str | None]] = set()
    for item in value:
        account = _normalize_monitored_account(item)
        key = (account.username, account.user_id)
        if key in seen:
            continue
        normalized.append(account)
        seen.add(key)
    return tuple(normalized)


def resolve_x_api_monitoring_config(config: Config) -> XApiMonitoringConfig:
    """Validate and normalize monitored-account capture settings."""
    x_api_config = config.get("sources.x_api", {}) or {}
    if not isinstance(x_api_config, dict):
        raise XApiMonitoringConfigError("sources.x_api must be an object")

    monitoring_config = x_api_config.get("monitoring", {}) or {}
    if not isinstance(monitoring_config, dict):
        raise XApiMonitoringConfigError("sources.x_api.monitoring must be an object")

    enabled = bool(monitoring_config.get("enabled", False))
    accounts = _normalize_monitored_accounts(monitoring_config.get("accounts"))
    auto_bookmark = bool(monitoring_config.get("auto_bookmark", True))
    webhook_secret_env = str(
        monitoring_config.get("webhook_secret_env") or DEFAULT_WEBHOOK_SECRET_ENV
    ).strip()
    task_name = str(monitoring_config.get("task_name") or X_API_MONITOR_TASK).strip()

    webhook_secret = None
    if webhook_secret_env:
        webhook_secret = os.getenv(webhook_secret_env)
        webhook_secret = webhook_secret.strip() if webhook_secret else None

    if enabled:
        if not x_api_config.get("enabled", False):
            raise XApiMonitoringConfigError(
                "sources.x_api.enabled must be true when monitored-account capture is enabled"
            )
        if not accounts:
            raise XApiMonitoringConfigError(
                "sources.x_api.monitoring.accounts must contain at least one account when enabled"
            )
        if not webhook_secret_env:
            raise XApiMonitoringConfigError(
                "sources.x_api.monitoring.webhook_secret_env is required when enabled"
            )
        if not webhook_secret:
            raise XApiMonitoringConfigError(
                f"{webhook_secret_env} is required when sources.x_api.monitoring.enabled is true"
            )
        if auto_bookmark:
            scopes = {
                str(scope).strip()
                for scope in x_api_config.get("scopes", []) or []
                if str(scope).strip()
            }
            if X_API_MONITOR_REQUIRED_SCOPE not in scopes:
                raise XApiMonitoringConfigError(
                    "sources.x_api.scopes must include bookmark.write when monitored-account auto-bookmarking is enabled"
                )

    return XApiMonitoringConfig(
        enabled=enabled,
        auto_bookmark=auto_bookmark,
        webhook_secret_env=webhook_secret_env,
        webhook_secret=webhook_secret,
        accounts=accounts,
        task_name=task_name or X_API_MONITOR_TASK,
    )


def verify_x_api_monitoring_webhook_secret(
    header_value: str | None,
    *,
    runtime_config: Config,
) -> XApiMonitoringConfig:
    """Return the normalized monitoring config after validating the shared secret."""
    monitoring_config = resolve_x_api_monitoring_config(runtime_config)
    if not monitoring_config.enabled:
        raise XApiMonitoringConfigError("Monitored-account capture is disabled")
    candidate = str(header_value or "").strip()
    if not candidate:
        raise XApiMonitoringConfigError(
            f"Missing required webhook header: {X_API_MONITOR_SECRET_HEADER}"
        )
    if candidate != monitoring_config.webhook_secret:
        raise XApiMonitoringConfigError("Invalid monitored-account webhook secret")
    return monitoring_config


def parse_x_api_monitored_post(payload: Mapping[str, Any]) -> XApiMonitoredPost:
    """Normalize either X-stream or simplified webhook payloads into a post object."""
    if not isinstance(payload, Mapping):
        raise XApiMonitoringError("Monitored-account webhook payload must be an object")

    if "tweet_id" in payload:
        tweet_id = validate_tweet_id(payload.get("tweet_id"))
        text = str(payload.get("text") or "").strip()
        tweet_data = payload.get("tweet_data")
        if isinstance(tweet_data, Mapping):
            text = text or str(tweet_data.get("text") or tweet_data.get("full_text") or "").strip()
            return XApiMonitoredPost(
                tweet_id=tweet_id,
                text=text,
                created_at=str(payload.get("created_at") or tweet_data.get("created_at") or "").strip() or None,
                author_id=_normalize_user_id(tweet_data.get("author_id")),
                author_username=_normalize_username(
                    payload.get("author_username") or tweet_data.get("author_username") or tweet_data.get("author")
                ),
                author_name=str(payload.get("author_name") or tweet_data.get("author_name") or "").strip() or None,
                conversation_id=str(tweet_data.get("conversation_id") or "").strip() or None,
                lang=str(tweet_data.get("lang") or "").strip() or None,
                public_metrics=tweet_data.get("public_metrics") if isinstance(tweet_data.get("public_metrics"), dict) else None,
                entities=tweet_data.get("entities") if isinstance(tweet_data.get("entities"), dict) else None,
                attachments=tweet_data.get("attachments") if isinstance(tweet_data.get("attachments"), dict) else None,
                referenced_tweets=list(tweet_data.get("referenced_tweets") or []),
                media=list(tweet_data.get("media") or []),
                matching_rules=_normalize_matching_rules(payload.get("matching_rules")),
            )

        return XApiMonitoredPost(
            tweet_id=tweet_id,
            text=text,
            created_at=str(payload.get("created_at") or "").strip() or None,
            author_id=_normalize_user_id(payload.get("author_id")),
            author_username=_normalize_username(payload.get("author_username")),
            author_name=str(payload.get("author_name") or "").strip() or None,
            conversation_id=str(payload.get("conversation_id") or "").strip() or None,
            lang=str(payload.get("lang") or "").strip() or None,
            public_metrics=payload.get("public_metrics") if isinstance(payload.get("public_metrics"), dict) else None,
            entities=payload.get("entities") if isinstance(payload.get("entities"), dict) else None,
            attachments=payload.get("attachments") if isinstance(payload.get("attachments"), dict) else None,
            referenced_tweets=list(payload.get("referenced_tweets") or []),
            media=list(payload.get("media") or []),
            matching_rules=_normalize_matching_rules(payload.get("matching_rules")),
        )

    tweet = payload.get("data")
    if not isinstance(tweet, Mapping):
        raise XApiMonitoringError("Monitored-account webhook payload is missing data.tweet")
    includes = payload.get("includes")
    if includes is not None and not isinstance(includes, Mapping):
        raise XApiMonitoringError("Monitored-account webhook includes payload must be an object")

    indexed_users: dict[str, Mapping[str, Any]] = {}
    indexed_media: dict[str, Mapping[str, Any]] = {}
    if isinstance(includes, Mapping):
        for user in includes.get("users", []) or []:
            if isinstance(user, Mapping):
                user_id = str(user.get("id", "")).strip()
                if user_id:
                    indexed_users[user_id] = user
        for media in includes.get("media", []) or []:
            if isinstance(media, Mapping):
                media_key = str(media.get("media_key", "")).strip()
                if media_key:
                    indexed_media[media_key] = media

    author_id = _normalize_user_id(tweet.get("author_id"))
    author = indexed_users.get(author_id or "", {})
    attachments = tweet.get("attachments") if isinstance(tweet.get("attachments"), dict) else None
    media: list[dict[str, Any]] = []
    if attachments:
        for media_key in attachments.get("media_keys", []) or []:
            normalized_key = str(media_key).strip()
            if normalized_key and normalized_key in indexed_media:
                media.append(dict(indexed_media[normalized_key]))

    referenced_tweets = tweet.get("referenced_tweets")
    if not isinstance(referenced_tweets, list):
        referenced_tweets = []

    return XApiMonitoredPost(
        tweet_id=validate_tweet_id(tweet.get("id")),
        text=str(tweet.get("text") or "").strip(),
        created_at=str(tweet.get("created_at") or "").strip() or None,
        author_id=author_id,
        author_username=_normalize_username(author.get("username")),
        author_name=str(author.get("name") or "").strip() or None,
        conversation_id=str(tweet.get("conversation_id") or "").strip() or None,
        lang=str(tweet.get("lang") or "").strip() or None,
        public_metrics=tweet.get("public_metrics") if isinstance(tweet.get("public_metrics"), dict) else None,
        entities=tweet.get("entities") if isinstance(tweet.get("entities"), dict) else None,
        attachments=attachments,
        referenced_tweets=list(referenced_tweets),
        media=media,
        matching_rules=_normalize_matching_rules(payload.get("matching_rules")),
    )


def match_monitored_account(
    post: XApiMonitoredPost,
    monitoring_config: XApiMonitoringConfig,
) -> XApiMonitoredAccount | None:
    """Return the configured monitored account that matched the webhook payload."""
    for account in monitoring_config.accounts:
        if account.matches(
            author_username=post.author_username,
            author_id=post.author_id,
        ):
            return account
    return None


def _strip_code_fences(value: str) -> str:
    text = value.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def _load_classifier_result(raw_content: str) -> XApiMonitoringDecision:
    try:
        payload = json.loads(_strip_code_fences(raw_content))
    except Exception as exc:
        raise XApiMonitoringError("X monitor classifier returned invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise XApiMonitoringError("X monitor classifier result must be an object")

    useful = bool(payload.get("useful", False))
    confidence = payload.get("confidence", 0.0)
    try:
        confidence_value = max(0.0, min(1.0, float(confidence)))
    except (TypeError, ValueError) as exc:
        raise XApiMonitoringError("X monitor classifier confidence must be numeric") from exc
    reason = str(payload.get("reason") or "").strip()
    matched_topics = payload.get("matched_topics") or []
    if not isinstance(matched_topics, list):
        raise XApiMonitoringError("X monitor classifier matched_topics must be an array")
    normalized_topics = tuple(
        str(topic).strip()
        for topic in matched_topics
        if str(topic).strip()
    )
    return XApiMonitoringDecision(
        useful=useful,
        confidence=confidence_value,
        reason=reason or "No reason provided",
        matched_topics=normalized_topics,
    )


def _active_archivist_context(runtime_config: Config) -> str:
    try:
        registry = load_archivist_topic_registry(runtime_config, required=False)
    except Exception:
        return "No archivist topic registry was available."
    if not registry.topics:
        return "No archivist topics are configured yet."

    topic_lines: list[str] = []
    for topic in registry.topics:
        term_preview = ", ".join(topic.include_terms[:8]) if topic.include_terms else "none"
        source_preview = ", ".join(topic.source_types[:6]) if topic.source_types else "all"
        topic_lines.append(
            f"- {topic.id}: {topic.title} | sources={source_preview} | terms={term_preview}"
        )
    return "\n".join(topic_lines)


async def classify_x_api_monitored_post(
    post: XApiMonitoredPost,
    *,
    runtime_config: Config,
    llm_interface: LLMInterface,
    task_name: str = X_API_MONITOR_TASK,
) -> XApiMonitoringDecision:
    """Classify a monitored post against the live archivist context."""
    route = llm_interface.resolve_task_route(task_name)
    if route is None:
        raise XApiMonitoringError(
            f"No LLM route is configured for llm.tasks.{task_name}"
        )
    provider, model_id, model_cfg = route
    system_prompt = runtime_config.get(
        f"llm.prompts.{task_name}",
        DEFAULT_MONITOR_SYSTEM_PROMPT,
    )
    prompt = (
        "Active archivist topics:\n"
        f"{_active_archivist_context(runtime_config)}\n\n"
        "Webhook post:\n"
        f"- author_username: {post.author_username or 'unknown'}\n"
        f"- author_name: {post.author_name or 'unknown'}\n"
        f"- author_id: {post.author_id or 'unknown'}\n"
        f"- created_at: {post.created_at or 'unknown'}\n"
        f"- lang: {post.lang or 'unknown'}\n"
        f"- matching_rules: {', '.join(post.matching_rules) if post.matching_rules else 'none'}\n"
        f"- text: {post.text or '(empty)'}\n\n"
        "Return JSON only."
    )
    response = await llm_interface.generate(
        prompt=prompt,
        system_prompt=system_prompt,
        provider=provider,
        model=model_id,
        max_tokens=model_cfg.get("max_tokens", 220),
        temperature=model_cfg.get("temperature", 0.1),
    )
    if response.error:
        raise XApiMonitoringError(
            f"X monitor classifier failed: {response.error}"
        )
    return _load_classifier_result(response.content)


async def create_x_api_bookmark(
    tweet_id: str,
    *,
    runtime_config: Config,
    layout=None,
) -> dict[str, Any]:
    """Add a bookmark through the authenticated X API user session."""
    resolved_layout = layout or build_path_layout(runtime_config)
    _, bundle, user_id = await resolve_authenticated_x_api_context(
        runtime_config,
        layout=resolved_layout,
        required_scopes=(X_API_MONITOR_REQUIRED_SCOPE,),
    )
    access_token = str(bundle.get("access_token") or "").strip()
    if not access_token:
        raise XApiTokenError("Stored X API token bundle is missing access_token")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            X_API_MONITOR_BOOKMARK_URL.format(user_id=user_id),
            headers=headers,
            json={"tweet_id": validate_tweet_id(tweet_id)},
        )
        if response.status_code == 429:
            raise XApiMonitoringError("X API bookmark write was rate limited")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise XApiMonitoringError(
                f"X API bookmark write failed with status {response.status_code}"
            ) from exc
        payload = response.json()

    if not isinstance(payload, Mapping):
        raise XApiMonitoringError("X API bookmark write response must be a JSON object")
    bookmark_data = payload.get("data")
    if not isinstance(bookmark_data, Mapping) or bookmark_data.get("bookmarked") is not True:
        raise XApiMonitoringError("X API bookmark write did not confirm bookmarked=true")
    return {
        "user_id": user_id,
        "bookmarked": True,
    }


def _build_monitoring_bookmark_payload(
    post: XApiMonitoredPost,
    *,
    decision: XApiMonitoringDecision,
    matched_account: XApiMonitoredAccount,
) -> dict[str, Any]:
    return normalize_bookmark_payload(
        {
            "tweet_id": post.tweet_id,
            "tweet_data": post.to_tweet_data(),
            "timestamp": post.created_at or _now_iso(),
            "source": X_API_MONITOR_SOURCE,
            "monitoring_decision": decision.to_dict(),
            "monitored_account": matched_account.label(),
            "matching_rules": list(post.matching_rules),
        },
        default_source=X_API_MONITOR_SOURCE,
        default_timestamp=datetime.now(timezone.utc),
    )


async def process_x_api_monitoring_webhook(
    payload: Mapping[str, Any],
    *,
    runtime_config: Config,
    llm_interface: LLMInterface | None = None,
    layout=None,
    db: MetadataDB | None = None,
) -> dict[str, Any]:
    """Evaluate a monitored-account webhook event and prepare bookmark ingestion."""
    monitoring_config = resolve_x_api_monitoring_config(runtime_config)
    if not monitoring_config.enabled:
        raise XApiMonitoringConfigError("Monitored-account capture is disabled")

    post = parse_x_api_monitored_post(payload)
    matched_account = match_monitored_account(post, monitoring_config)
    if matched_account is None:
        return {
            "status": "ignored",
            "reason": "unmonitored_account",
            "tweet_id": post.tweet_id,
            "author_username": post.author_username,
            "author_id": post.author_id,
        }

    metadata_db = db or get_metadata_db()
    existing = metadata_db.get_bookmark_entry(post.tweet_id)
    if existing is not None:
        return {
            "status": "ignored",
            "reason": "already_known",
            "tweet_id": post.tweet_id,
            "bookmark_status": existing.status,
            "monitored_account": matched_account.label(),
        }

    interface = llm_interface or LLMInterface(runtime_config.get("llm", {}) or {})
    decision = await classify_x_api_monitored_post(
        post,
        runtime_config=runtime_config,
        llm_interface=interface,
        task_name=monitoring_config.task_name,
    )
    result: dict[str, Any] = {
        "status": "ignored",
        "reason": "classifier_rejected",
        "tweet_id": post.tweet_id,
        "author_username": post.author_username,
        "author_id": post.author_id,
        "monitored_account": matched_account.label(),
        "decision": decision.to_dict(),
    }
    if not decision.useful:
        return result

    bookmark_write = None
    if monitoring_config.auto_bookmark:
        bookmark_write = await create_x_api_bookmark(
            post.tweet_id,
            runtime_config=runtime_config,
            layout=layout,
        )

    result.update(
        {
            "status": "accepted",
            "reason": "classifier_accepted",
            "bookmark_write": bookmark_write,
            "bookmark_payload": _build_monitoring_bookmark_payload(
                post,
                decision=decision,
                matched_account=matched_account,
            ),
        }
    )
    return result
