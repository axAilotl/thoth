"""Omi Developer API client for conversation transcript ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import httpx


OMI_DEFAULT_BASE_URL = "https://api.omi.me"
OMI_CONVERSATIONS_PATH = "/v1/dev/user/conversations"
OMI_DEFAULT_PAGE_SIZE = 25
OMI_MAX_PAGE_SIZE = 100


class OmiApiError(RuntimeError):
    """Raised when Omi API configuration or requests fail."""


@dataclass(frozen=True)
class OmiConversationQuery:
    """Validated query options for Omi conversation collection."""

    api_key: str
    base_url: str = OMI_DEFAULT_BASE_URL
    limit: int = OMI_DEFAULT_PAGE_SIZE
    page_size: int = OMI_DEFAULT_PAGE_SIZE
    include_transcript: bool = True
    start_date: str | None = None
    end_date: str | None = None
    categories: tuple[str, ...] = ()
    folder_id: str | None = None
    starred: bool | None = None
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise OmiApiError("Omi Developer API key is required")
        if not self.api_key.startswith("omi_dev_"):
            raise OmiApiError("Omi Developer API key must start with omi_dev_")
        if not self.base_url.startswith(("https://", "http://")):
            raise OmiApiError("Omi API base_url must start with http:// or https://")
        if self.limit < 1:
            raise OmiApiError("Omi API limit must be at least 1")
        if not 1 <= self.page_size <= OMI_MAX_PAGE_SIZE:
            raise OmiApiError(f"Omi API page_size must be between 1 and {OMI_MAX_PAGE_SIZE}")
        if self.timeout_seconds <= 0:
            raise OmiApiError("Omi API timeout_seconds must be positive")


async def fetch_omi_conversations(query: OmiConversationQuery) -> list[dict[str, Any]]:
    """Fetch Omi conversations, preserving each JSON object for artifact ingestion."""
    url = f"{query.base_url.rstrip('/')}{OMI_CONVERSATIONS_PATH}"
    headers = {"Authorization": f"Bearer {query.api_key}"}
    collected: list[dict[str, Any]] = []
    offset = 0

    async with httpx.AsyncClient(timeout=query.timeout_seconds) as client:
        while len(collected) < query.limit:
            page_limit = min(query.page_size, query.limit - len(collected))
            params = _build_params(query, limit=page_limit, offset=offset)
            response = await client.get(url, headers=headers, params=params)
            if response.status_code == 401:
                raise OmiApiError("Omi API authentication failed with status 401")
            if response.status_code == 403:
                raise OmiApiError("Omi API authentication failed with status 403")
            if response.status_code == 429:
                raise OmiApiError("Omi API conversation lookup was rate limited")
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise OmiApiError(
                    f"Omi API conversation lookup failed with status {response.status_code}"
                ) from exc

            payload = response.json()
            if not isinstance(payload, list):
                raise OmiApiError("Omi API conversation response must be a JSON array")
            page = [item for item in payload if isinstance(item, dict)]
            if len(page) != len(payload):
                raise OmiApiError("Omi API conversation response contained non-object items")
            if not page:
                break
            collected.extend(page)
            if len(page) < page_limit:
                break
            offset += page_limit

    return collected[: query.limit]


def normalize_categories(values: Any) -> tuple[str, ...]:
    """Normalize API category filters from comma strings or lists."""
    if values is None:
        return tuple()
    if isinstance(values, str):
        items = [part.strip() for part in values.split(",") if part.strip()]
    elif isinstance(values, Iterable) and not isinstance(values, Mapping):
        items = [str(value).strip() for value in values if str(value).strip()]
    else:
        items = [str(values).strip()] if str(values).strip() else []
    return tuple(dict.fromkeys(items))


def _build_params(
    query: OmiConversationQuery,
    *,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "limit": limit,
        "offset": offset,
        "include_transcript": str(query.include_transcript).lower(),
    }
    if query.start_date:
        params["start_date"] = query.start_date
    if query.end_date:
        params["end_date"] = query.end_date
    if query.categories:
        params["categories"] = ",".join(query.categories)
    if query.folder_id:
        params["folder_id"] = query.folder_id
    if query.starred is not None:
        params["starred"] = str(query.starred).lower()
    return params
