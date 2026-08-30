"""Operator-safe diagnostics for X OAuth configuration and tokens."""

from __future__ import annotations

from typing import Any, Mapping

import httpx

from .config import Config
from .path_layout import build_path_layout
from .sensitive_redaction import redact_sensitive_text
from .x_api_auth import (
    XApiAuthConfig,
    XApiAuthConfigError,
    XApiAuthError,
    XApiTokenError,
    _token_is_expired,
    fetch_current_x_user,
    load_x_api_token_bundle,
    refresh_x_api_tokens,
    resolve_x_api_auth_config,
    store_x_api_token_bundle,
)

def _collect_x_api_secrets(
    auth_config: XApiAuthConfig | None,
    bundle: Mapping[str, Any] | None,
) -> list[str]:
    """Return sensitive values that must be redacted from output/logs."""
    secrets: list[str] = []
    if auth_config is not None and auth_config.client_secret:
        secrets.append(auth_config.client_secret)
    if bundle is not None:
        access_token = str(bundle.get("access_token") or "").strip()
        if access_token:
            secrets.append(access_token)
        refresh_token = str(bundle.get("refresh_token") or "").strip()
        if refresh_token:
            secrets.append(refresh_token)
    # Longest first so shorter secrets do not leave suffixes behind.
    secrets.sort(key=len, reverse=True)
    return secrets


def redact_x_api_secrets(
    text: str | None,
    *,
    auth_config: XApiAuthConfig | None = None,
    bundle: Mapping[str, Any] | None = None,
) -> str:
    """Redact access tokens, refresh tokens, client secrets, and auth headers."""
    original = text or ""
    redacted = redact_sensitive_text(original).redacted_text
    for secret in _collect_x_api_secrets(auth_config, bundle):
        redacted = redacted.replace(secret, "[[REDACTED]]")
    return redacted


_X_API_SECRET_KEYS = {
    "access_token",
    "refresh_token",
    "client_secret",
    "authorization",
    "authorization_header",
}


def _redact_x_api_structured_value(
    value: Any,
    *,
    auth_config: XApiAuthConfig | None,
    bundle: Mapping[str, Any] | None,
) -> Any:
    """Redact secret-bearing keys and known secret values in provider payloads."""
    provider_secrets: list[str] = []

    def collect(candidate: Any) -> None:
        if isinstance(candidate, Mapping):
            for key, nested in candidate.items():
                normalized_key = str(key).strip().lower().replace("-", "_")
                if normalized_key in _X_API_SECRET_KEYS and isinstance(nested, str):
                    if nested.strip():
                        provider_secrets.append(nested.strip())
                collect(nested)
        elif isinstance(candidate, (list, tuple)):
            for nested in candidate:
                collect(nested)

    collect(value)

    def redact(candidate: Any) -> Any:
        if isinstance(candidate, Mapping):
            result: dict[str, Any] = {}
            for key, nested in candidate.items():
                normalized_key = str(key).strip().lower().replace("-", "_")
                result[str(key)] = (
                    "[[REDACTED]]"
                    if normalized_key in _X_API_SECRET_KEYS
                    else redact(nested)
                )
            return result
        if isinstance(candidate, (list, tuple)):
            return [redact(nested) for nested in candidate]
        if isinstance(candidate, str):
            redacted = redact_x_api_secrets(
                candidate,
                auth_config=auth_config,
                bundle=bundle,
            )
            for secret in provider_secrets:
                redacted = redacted.replace(secret, "[[REDACTED]]")
            return redacted
        return candidate

    return redact(value)


def _inspect_client_metadata(config: Config) -> dict[str, Any]:
    """Inspect raw config for required X OAuth client metadata fields."""
    x_api_config = config.get("sources.x_api", {}) or {}
    if not isinstance(x_api_config, dict):
        return {
            "configured": False,
            "enabled": False,
            "required_fields": ["client_id", "redirect_uri", "scopes"],
            "configured_fields": {},
            "missing_fields": ["sources.x_api must be an object"],
        }

    required_fields = ["client_id", "redirect_uri", "scopes"]
    configured_fields: dict[str, bool] = {}
    missing_fields: list[str] = []

    for field in required_fields:
        value = x_api_config.get(field)
        if field == "scopes":
            if isinstance(value, str):
                has_value = bool(value.strip())
            elif isinstance(value, (list, tuple)):
                has_value = bool(value)
            else:
                has_value = False
        else:
            has_value = isinstance(value, str) and bool(value.strip())
        configured_fields[field] = has_value
        if not has_value:
            missing_fields.append(field)

    return {
        "configured": not missing_fields,
        "enabled": bool(x_api_config.get("enabled", False)),
        "required_fields": required_fields,
        "configured_fields": configured_fields,
        "missing_fields": missing_fields,
    }


def _build_refresh_behavior(auth_config: XApiAuthConfig) -> dict[str, Any]:
    """Describe refresh support and requirements."""
    offline_access_present = "offline.access" in auth_config.scopes
    return {
        "supported": offline_access_present,
        "requires": (
            "offline.access scope and a stored refresh token; client_secret_env "
            "is optional for the public-client PKCE flow"
        ),
        "client_secret_configured": auth_config.client_secret is not None,
        "client_type": (
            "confidential" if auth_config.client_secret is not None else "public_pkce"
        ),
        "offline_access_scope_required": "offline.access",
        "offline_access_present": offline_access_present,
    }


def _build_token_summary(bundle: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return non-secret token status summary."""
    if not bundle:
        return {
            "present": False,
            "expired": None,
            "refreshable": False,
        }
    refresh_token = str(bundle.get("refresh_token") or "").strip()
    return {
        "present": True,
        "expired": _token_is_expired(bundle),
        "refreshable": bool(refresh_token),
    }


def _build_diagnostic(
    *,
    status: str,
    error: str | None,
    config: Config,
    auth_config: XApiAuthConfig | None,
    bundle: Mapping[str, Any] | None,
    refreshed: bool = False,
    user: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a redacted, operator-safe diagnostic payload."""
    client_metadata = _inspect_client_metadata(config)
    if auth_config is not None:
        client_metadata["client_id"] = auth_config.client_id
        client_metadata["redirect_uri"] = auth_config.redirect_uri
        client_metadata["scopes"] = list(auth_config.scopes)
        refresh_behavior = _build_refresh_behavior(auth_config)
    else:
        refresh_behavior = {
            "supported": False,
            "requires": (
                "offline.access scope and a stored refresh token; client_secret_env "
                "is optional for the public-client PKCE flow"
            ),
            "client_secret_configured": False,
            "client_type": None,
            "offline_access_scope_required": "offline.access",
            "offline_access_present": False,
        }

    redacted_error = redact_x_api_secrets(error, auth_config=auth_config, bundle=bundle)
    safe_user = (
        _redact_x_api_structured_value(
            user,
            auth_config=auth_config,
            bundle=bundle,
        )
        if user is not None
        else None
    )
    redacted = redacted_error != (error or "") or safe_user != user

    return {
        "status": status,
        "connected": status == "ok",
        "client_metadata": client_metadata,
        "refresh_behavior": refresh_behavior,
        "token": _build_token_summary(bundle),
        "refreshed": refreshed,
        "user": safe_user,
        "error": redacted_error,
        "redacted": redacted,
    }


async def test_x_api_connection(
    config: Config,
    *,
    layout=None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Run an operator-safe X OAuth connection diagnostic.

    No secrets are written to the returned payload. All network calls use the
    optional injected ``client`` so tests can mock the X API without live traffic.
    """
    resolved_layout = layout or build_path_layout(config)

    # Phase 1: required client metadata.
    try:
        auth_config = resolve_x_api_auth_config(config)
    except XApiAuthConfigError as exc:
        return _build_diagnostic(
            status="config_error",
            error=str(exc),
            config=config,
            auth_config=None,
            bundle=None,
        )

    # Phase 2: stored user tokens.
    try:
        bundle = load_x_api_token_bundle(resolved_layout)
    except (XApiAuthError, OSError, ValueError) as exc:
        return _build_diagnostic(
            status="token_error",
            error=str(exc),
            config=config,
            auth_config=auth_config,
            bundle=None,
        )
    if not bundle:
        return _build_diagnostic(
            status="token_missing",
            error="No stored X API token bundle was found",
            config=config,
            auth_config=auth_config,
            bundle=None,
        )

    # Phase 3: expiration and refresh.
    refreshed = False
    if _token_is_expired(bundle):
        refresh_token = str(bundle.get("refresh_token") or "").strip()
        if not refresh_token:
            return _build_diagnostic(
                status="token_expired",
                error="Stored X API token bundle is expired and lacks a refresh token",
                config=config,
                auth_config=auth_config,
                bundle=bundle,
            )
        try:
            refreshed_bundle = await refresh_x_api_tokens(
                auth_config,
                refresh_token=refresh_token,
                client=client,
            )
            bundle = refreshed_bundle.to_dict()
            # Refresh-token rotation can invalidate the stored token. Persist the
            # replacement before the connectivity probe so diagnostics never leave
            # the operator with an unusable credential bundle.
            bundle = store_x_api_token_bundle(resolved_layout, bundle)
            refreshed = True
        except XApiTokenError as exc:
            return _build_diagnostic(
                status="refresh_failed",
                error=str(exc),
                config=config,
                auth_config=auth_config,
                bundle=bundle,
                refreshed=False,
            )

    # Phase 4: validate connectivity with /2/users/me.
    access_token = str(bundle.get("access_token") or "").strip()
    if not access_token:
        return _build_diagnostic(
            status="token_error",
            error="Stored X API token bundle is missing access_token",
            config=config,
            auth_config=auth_config,
            bundle=bundle,
            refreshed=refreshed,
        )

    try:
        user_payload = await fetch_current_x_user(
            auth_config,
            access_token=access_token,
            client=client,
        )
    except XApiTokenError as exc:
        return _build_diagnostic(
            status=(
                "token_invalid"
                if exc.status_code in {401, 403}
                else "connection_error"
            ),
            error=str(exc),
            config=config,
            auth_config=auth_config,
            bundle=bundle,
            refreshed=refreshed,
        )

    return _build_diagnostic(
        status="ok",
        error=None,
        config=config,
        auth_config=auth_config,
        bundle=bundle,
        refreshed=refreshed,
        user=user_payload,
    )



__all__ = ["redact_x_api_secrets", "test_x_api_connection"]

