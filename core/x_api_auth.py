"""
X API OAuth 2.0 PKCE helpers.

This module owns the secure browser auth handshake and token persistence used
for X bookmark backfill and future API-driven sync.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode, quote

import httpx

from .config import Config
from .path_layout import build_path_layout

X_API_AUTHORIZE_URL = "https://x.com/i/oauth2/authorize"
X_API_TOKEN_URL = "https://api.x.com/2/oauth2/token"
X_API_ME_URL = "https://api.x.com/2/users/me"
X_API_REQUIRED_SCOPES = ("bookmark.read", "tweet.read", "users.read", "offline.access")
X_API_DEFAULT_SCOPES = X_API_REQUIRED_SCOPES
X_API_STATE_FILENAME = "x_api_pending_auth.json"
X_API_TOKEN_FILENAME = "x_api_tokens.json"
X_API_STATE_VERSION = 1
X_API_AUTH_WINDOW_SECONDS = 24 * 60 * 60


class XApiAuthError(RuntimeError):
    """Base class for X API auth failures."""


class XApiAuthConfigError(XApiAuthError, ValueError):
    """Raised when required auth configuration is missing or invalid."""


class XApiAuthStateError(XApiAuthError, ValueError):
    """Raised when the stored PKCE state is missing, stale, or mismatched."""


class XApiTokenError(XApiAuthError):
    """Raised when token exchange or refresh fails."""


@dataclass(frozen=True)
class XApiAuthConfig:
    """Resolved X auth configuration."""

    client_id: str
    redirect_uri: str
    scopes: tuple[str, ...]
    client_secret: str | None = None
    authorize_url: str = X_API_AUTHORIZE_URL
    token_url: str = X_API_TOKEN_URL
    me_url: str = X_API_ME_URL


@dataclass(frozen=True)
class XApiPendingAuth:
    """Stored PKCE auth state."""

    state: str
    code_verifier: str
    code_challenge: str
    client_id: str
    redirect_uri: str
    scopes: tuple[str, ...]
    created_at: str
    expires_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": X_API_STATE_VERSION,
            "state": self.state,
            "code_verifier": self.code_verifier,
            "code_challenge": self.code_challenge,
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scopes": list(self.scopes),
            "created_at": self.created_at,
            "expires_at": self.expires_at,
        }


@dataclass(frozen=True)
class XApiTokenBundle:
    """Persisted X OAuth tokens plus user metadata."""

    access_token: str
    refresh_token: str | None
    token_type: str
    scopes: tuple[str, ...]
    expires_at: str
    obtained_at: str
    client_id: str
    redirect_uri: str
    user: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": X_API_STATE_VERSION,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "scopes": list(self.scopes),
            "expires_at": self.expires_at,
            "obtained_at": self.obtained_at,
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
        }
        if self.user is not None:
            payload["user"] = self.user
        return payload


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime | None = None) -> str:
    return (dt or _now_utc()).isoformat()


def _parse_iso_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    os.replace(tmp_path, path)


def _read_json_file(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise XApiAuthError(f"Invalid JSON payload in {path}: expected object")
    return payload


def _normalize_scopes(raw_scopes: Any) -> tuple[str, ...]:
    if raw_scopes is None:
        scopes = list(X_API_DEFAULT_SCOPES)
    elif isinstance(raw_scopes, str):
        scopes = [scope for scope in raw_scopes.split() if scope.strip()]
    elif isinstance(raw_scopes, (list, tuple)):
        scopes = [str(scope).strip() for scope in raw_scopes if str(scope).strip()]
    else:
        raise XApiAuthConfigError("sources.x_api.scopes must be a string or list")

    if not scopes:
        raise XApiAuthConfigError("sources.x_api.scopes cannot be empty")

    deduped: dict[str, None] = {}
    for scope in scopes:
        deduped[scope] = None
    return tuple(deduped.keys())


def resolve_x_api_auth_config(config: Config) -> XApiAuthConfig:
    """Resolve and validate the X auth configuration."""
    x_api_config = config.get("sources.x_api", {}) or {}
    if not isinstance(x_api_config, dict):
        raise XApiAuthConfigError("sources.x_api must be an object")
    if not x_api_config.get("enabled", False):
        raise XApiAuthConfigError("sources.x_api.enabled must be true to use X auth")

    client_id = str(x_api_config.get("client_id", "")).strip()
    redirect_uri = str(x_api_config.get("redirect_uri", "")).strip()
    if not client_id:
        raise XApiAuthConfigError("sources.x_api.client_id is required")
    if not redirect_uri:
        raise XApiAuthConfigError("sources.x_api.redirect_uri is required")

    scopes = _normalize_scopes(x_api_config.get("scopes"))
    missing = [scope for scope in X_API_REQUIRED_SCOPES if scope not in scopes]
    if missing:
        raise XApiAuthConfigError(
            "sources.x_api.scopes must include: " + ", ".join(missing)
        )

    client_secret_env = str(x_api_config.get("client_secret_env", "")).strip()
    client_secret = None
    if client_secret_env:
        client_secret = os.getenv(client_secret_env)
        if not client_secret or not client_secret.strip():
            raise XApiAuthConfigError(
                f"{client_secret_env} is required when sources.x_api.client_secret_env is set"
            )
        client_secret = client_secret.strip()

    return XApiAuthConfig(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scopes=scopes,
        client_secret=client_secret,
    )


def _pending_auth_path(layout) -> Path:
    return layout.auth_root / X_API_STATE_FILENAME


def _token_bundle_path(layout) -> Path:
    return layout.auth_root / X_API_TOKEN_FILENAME


def generate_pkce_pair() -> tuple[str, str]:
    """Generate a PKCE verifier/challenge pair."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).decode("ascii").rstrip("=")
    return verifier, challenge


def build_authorize_url(
    auth_config: XApiAuthConfig,
    *,
    state: str,
    code_challenge: str,
) -> str:
    params = {
        "response_type": "code",
        "client_id": auth_config.client_id,
        "redirect_uri": auth_config.redirect_uri,
        "scope": " ".join(auth_config.scopes),
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return auth_config.authorize_url + "?" + urlencode(params, quote_via=quote)


def start_x_api_auth(config: Config, *, layout=None) -> dict[str, Any]:
    """Create and persist a new pending PKCE auth flow."""
    auth_config = resolve_x_api_auth_config(config)
    resolved_layout = layout or build_path_layout(config)
    resolved_layout.ensure_directories()

    state = secrets.token_urlsafe(32)
    code_verifier, code_challenge = generate_pkce_pair()
    created_at = _iso_utc()
    expires_at = _iso_utc(_now_utc() + timedelta(seconds=X_API_AUTH_WINDOW_SECONDS))
    pending = XApiPendingAuth(
        state=state,
        code_verifier=code_verifier,
        code_challenge=code_challenge,
        client_id=auth_config.client_id,
        redirect_uri=auth_config.redirect_uri,
        scopes=auth_config.scopes,
        created_at=created_at,
        expires_at=expires_at,
    )
    _atomic_write_json(_pending_auth_path(resolved_layout), pending.to_dict())
    return {
        "authorize_url": build_authorize_url(
            auth_config, state=state, code_challenge=code_challenge
        ),
        "state": state,
        "redirect_uri": auth_config.redirect_uri,
        "scopes": list(auth_config.scopes),
        "expires_at": expires_at,
    }


def load_pending_x_api_auth(layout) -> XApiPendingAuth | None:
    """Load the pending PKCE auth state if it exists."""
    path = _pending_auth_path(layout)
    if not path.exists():
        return None
    payload = _read_json_file(path)
    if payload.get("version") != X_API_STATE_VERSION:
        raise XApiAuthStateError("Unsupported X API pending auth state version")

    required_keys = (
        "state",
        "code_verifier",
        "code_challenge",
        "client_id",
        "redirect_uri",
        "scopes",
        "created_at",
        "expires_at",
    )
    missing = [key for key in required_keys if key not in payload]
    if missing:
        raise XApiAuthStateError(
            "X API pending auth state is missing keys: " + ", ".join(missing)
        )

    expires_at = _parse_iso_utc(str(payload["expires_at"]))
    if expires_at <= _now_utc():
        raise XApiAuthStateError("Stored X API pending auth state has expired")

    scopes = _normalize_scopes(payload["scopes"])
    return XApiPendingAuth(
        state=str(payload["state"]),
        code_verifier=str(payload["code_verifier"]),
        code_challenge=str(payload["code_challenge"]),
        client_id=str(payload["client_id"]),
        redirect_uri=str(payload["redirect_uri"]),
        scopes=scopes,
        created_at=str(payload["created_at"]),
        expires_at=str(payload["expires_at"]),
    )


def clear_pending_x_api_auth(layout) -> None:
    path = _pending_auth_path(layout)
    if path.exists():
        path.unlink()


def load_x_api_token_bundle(layout) -> dict[str, Any] | None:
    """Load the persisted token bundle if it exists."""
    path = _token_bundle_path(layout)
    if not path.exists():
        return None
    payload = _read_json_file(path)
    if payload.get("version") != X_API_STATE_VERSION:
        raise XApiTokenError("Unsupported X API token bundle version")
    return payload


def store_x_api_token_bundle(layout, bundle: XApiTokenBundle | dict[str, Any]) -> dict[str, Any]:
    """Persist the token bundle atomically."""
    payload = bundle.to_dict() if isinstance(bundle, XApiTokenBundle) else dict(bundle)
    if payload.get("version") != X_API_STATE_VERSION:
        payload["version"] = X_API_STATE_VERSION
    _atomic_write_json(_token_bundle_path(layout), payload)
    return payload


def clear_x_api_token_bundle(layout) -> None:
    path = _token_bundle_path(layout)
    if path.exists():
        path.unlink()


def _build_basic_auth_header(auth_config: XApiAuthConfig) -> dict[str, str]:
    if not auth_config.client_secret:
        return {}
    token = base64.b64encode(
        f"{auth_config.client_id}:{auth_config.client_secret}".encode("utf-8")
    ).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def _normalize_scope_response(value: Any) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        scopes = [scope for scope in value.split() if scope.strip()]
    elif isinstance(value, (list, tuple)):
        scopes = [str(scope).strip() for scope in value if str(scope).strip()]
    else:
        raise XApiTokenError("Unexpected scope response format from X API")
    return tuple(scopes)


def token_bundle_has_scopes(
    bundle: Mapping[str, Any] | None,
    required_scopes: tuple[str, ...] | list[str],
) -> bool:
    """Return True when a stored token bundle includes every required scope."""
    if not required_scopes:
        return True
    if not isinstance(bundle, Mapping):
        return False
    available = set(_normalize_scope_response(bundle.get("scopes")))
    return all(
        str(scope).strip() in available
        for scope in required_scopes
        if str(scope).strip()
    )


def _parse_token_response(
    auth_config: XApiAuthConfig,
    payload: Mapping[str, Any],
    *,
    refresh_token: str | None = None,
) -> XApiTokenBundle:
    access_token = str(payload.get("access_token", "")).strip()
    if not access_token:
        raise XApiTokenError("X API token response did not include access_token")

    token_type = str(payload.get("token_type", "bearer")).strip() or "bearer"
    expires_in_raw = payload.get("expires_in")
    if expires_in_raw is None:
        raise XApiTokenError("X API token response did not include expires_in")
    try:
        expires_in = int(expires_in_raw)
    except (TypeError, ValueError) as exc:
        raise XApiTokenError("X API token response returned invalid expires_in") from exc
    if expires_in <= 0:
        raise XApiTokenError("X API token response returned non-positive expires_in")

    scopes = _normalize_scope_response(payload.get("scope")) or auth_config.scopes
    return XApiTokenBundle(
        access_token=access_token,
        refresh_token=str(payload.get("refresh_token") or refresh_token).strip() or None,
        token_type=token_type,
        scopes=scopes,
        expires_at=_iso_utc(_now_utc() + timedelta(seconds=expires_in)),
        obtained_at=_iso_utc(),
        client_id=auth_config.client_id,
        redirect_uri=auth_config.redirect_uri,
    )


async def exchange_authorization_code(
    auth_config: XApiAuthConfig,
    *,
    code: str,
    code_verifier: str,
) -> XApiTokenBundle:
    """Exchange an authorization code for tokens."""
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": auth_config.client_id,
        "redirect_uri": auth_config.redirect_uri,
        "code_verifier": code_verifier,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        **_build_basic_auth_header(auth_config),
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(auth_config.token_url, data=data, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise XApiTokenError(
                f"X API token exchange failed with status {response.status_code}"
            ) from exc
        payload = response.json()

    if not isinstance(payload, dict):
        raise XApiTokenError("X API token response must be a JSON object")
    return _parse_token_response(auth_config, payload)


async def refresh_x_api_tokens(
    auth_config: XApiAuthConfig,
    *,
    refresh_token: str,
) -> XApiTokenBundle:
    """Refresh an existing token bundle."""
    if not refresh_token or not refresh_token.strip():
        raise XApiTokenError("refresh_token is required to refresh X API tokens")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": auth_config.client_id,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        **_build_basic_auth_header(auth_config),
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(auth_config.token_url, data=data, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise XApiTokenError(
                f"X API token refresh failed with status {response.status_code}"
            ) from exc
        payload = response.json()

    if not isinstance(payload, dict):
        raise XApiTokenError("X API refresh response must be a JSON object")
    return _parse_token_response(auth_config, payload, refresh_token=refresh_token)


async def fetch_current_x_user(auth_config: XApiAuthConfig, *, access_token: str) -> dict[str, Any]:
    """Fetch the current authenticated X user."""
    if not access_token or not access_token.strip():
        raise XApiTokenError("access_token is required to fetch the current user")

    headers = {"Authorization": f"Bearer {access_token.strip()}"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(auth_config.me_url, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise XApiTokenError(
                f"X API /2/users/me failed with status {response.status_code}"
            ) from exc
        payload = response.json()

    if not isinstance(payload, dict):
        raise XApiTokenError("X API user response must be a JSON object")
    return payload


async def resolve_authenticated_x_api_context(
    config: Config,
    *,
    layout=None,
    required_scopes: tuple[str, ...] = (),
) -> tuple[XApiAuthConfig, dict[str, Any], str]:
    """Resolve a valid token bundle and authenticated X user id for API calls."""
    auth_config = resolve_x_api_auth_config(config)
    resolved_layout = layout or build_path_layout(config)
    resolved_layout.ensure_directories()
    bundle = load_x_api_token_bundle(resolved_layout)
    if not bundle:
        raise XApiAuthStateError("No stored X API token bundle was found")

    expires_at = str(bundle.get("expires_at") or "").strip()
    refresh_token = str(bundle.get("refresh_token") or "").strip()
    if expires_at:
        try:
            is_expired = _parse_iso_utc(expires_at)
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
                bundle = store_x_api_token_bundle(resolved_layout, refreshed_payload)
        except ValueError as exc:
            raise XApiTokenError("Stored X API token bundle has an invalid expires_at") from exc

    access_token = str(bundle.get("access_token") or "").strip()
    if not access_token:
        raise XApiTokenError("Stored X API token bundle is missing access_token")

    if required_scopes and not token_bundle_has_scopes(bundle, required_scopes):
        available = set(_normalize_scope_response(bundle.get("scopes")))
        missing = [
            scope
            for scope in required_scopes
            if str(scope).strip() and str(scope).strip() not in available
        ]
        raise XApiTokenError(
            "Stored X API token bundle is missing scopes: " + ", ".join(missing)
        )

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
        bundle = store_x_api_token_bundle(resolved_layout, bundle)

    return auth_config, bundle, user_id


async def complete_x_api_auth(
    config: Config,
    *,
    code: str,
    state: str,
    layout=None,
) -> dict[str, Any]:
    """Validate the callback state, exchange code, and persist tokens."""
    auth_config = resolve_x_api_auth_config(config)
    resolved_layout = layout or build_path_layout(config)
    pending = load_pending_x_api_auth(resolved_layout)
    if pending is None:
        raise XApiAuthStateError("No pending X API auth flow exists")

    if pending.state != state:
        raise XApiAuthStateError("X API auth state did not match the stored state")
    if pending.client_id != auth_config.client_id:
        raise XApiAuthStateError("Stored X API auth state does not match client_id")
    if pending.redirect_uri != auth_config.redirect_uri:
        raise XApiAuthStateError("Stored X API auth state does not match redirect_uri")

    token_bundle = await exchange_authorization_code(
        auth_config,
        code=code,
        code_verifier=pending.code_verifier,
    )

    user_payload = await fetch_current_x_user(
        auth_config, access_token=token_bundle.access_token
    )
    token_bundle = XApiTokenBundle(
        access_token=token_bundle.access_token,
        refresh_token=token_bundle.refresh_token,
        token_type=token_bundle.token_type,
        scopes=token_bundle.scopes,
        expires_at=token_bundle.expires_at,
        obtained_at=token_bundle.obtained_at,
        client_id=token_bundle.client_id,
        redirect_uri=token_bundle.redirect_uri,
        user=user_payload,
    )
    stored_payload = store_x_api_token_bundle(resolved_layout, token_bundle)
    clear_pending_x_api_auth(resolved_layout)
    return {
        "token_bundle": stored_payload,
        "user": user_payload,
    }


def _token_is_expired(bundle: Mapping[str, Any]) -> bool:
    expires_at = bundle.get("expires_at")
    if not expires_at:
        return True
    try:
        return _parse_iso_utc(str(expires_at)) <= _now_utc()
    except ValueError:
        return True


def summarize_x_api_auth(layout) -> dict[str, Any]:
    """Return a concise auth status payload for the API surface."""
    pending = None
    token_bundle = None
    pending_error = None
    token_error = None

    try:
        pending = load_pending_x_api_auth(layout)
    except Exception as exc:  # noqa: BLE001
        pending_error = str(exc)

    try:
        token_bundle = load_x_api_token_bundle(layout)
    except Exception as exc:  # noqa: BLE001
        token_error = str(exc)

    status = {
        "pending": pending is not None,
        "pending_error": pending_error,
        "token_error": token_error,
        "has_token": token_bundle is not None,
        "token_expired": _token_is_expired(token_bundle or {}) if token_bundle else None,
        "user": (token_bundle or {}).get("user"),
        "expires_at": (token_bundle or {}).get("expires_at"),
        "obtained_at": (token_bundle or {}).get("obtained_at"),
        "scopes": (token_bundle or {}).get("scopes"),
    }
    return status
