"""Runtime bind options for the Thoth API server."""

from __future__ import annotations

import os
from typing import Mapping, TypedDict


class ApiServerOptions(TypedDict):
    host: str
    port: int
    reload: bool


def _parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"", "0", "false", "no", "off"}:
        return False
    if normalized in {"1", "true", "yes", "on"}:
        return True
    raise ValueError("THOTH_API_RELOAD must be a boolean-like value")


def resolve_api_server_options(
    env: Mapping[str, str] | None = None,
) -> ApiServerOptions:
    """Resolve host/port/reload flags for the local API server."""

    source = env or os.environ
    host = (source.get("THOTH_API_HOST") or source.get("HOST") or "0.0.0.0").strip()
    if not host:
        host = "0.0.0.0"

    raw_port = (source.get("THOTH_API_PORT") or source.get("PORT") or "8090").strip()
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ValueError("THOTH_API_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("THOTH_API_PORT must be between 1 and 65535")

    return {
        "host": host,
        "port": port,
        "reload": _parse_bool(source.get("THOTH_API_RELOAD"), default=True),
    }
