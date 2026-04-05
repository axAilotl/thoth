from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest

from core.config import Config
from core.path_layout import build_path_layout
from core.x_api_auth import store_x_api_token_bundle
from core.x_api_bookmark_sync import (
    XApiBookmarkSyncCheckpoint,
    load_x_api_bookmark_sync_checkpoint,
    sync_x_api_bookmarks,
)


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", ".thoth_system/meta.db")
    config.set("sources.x_api.enabled", True)
    config.set("sources.x_api.client_id", "client-123")
    config.set(
        "sources.x_api.redirect_uri",
        "http://127.0.0.1:8000/api/x-api/auth/callback",
    )
    config.set(
        "sources.x_api.scopes",
        ["bookmark.read", "tweet.read", "users.read", "offline.access"],
    )
    return config


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.request = httpx.Request("GET", "https://example.test")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"{self.status_code} error",
                request=self.request,
                response=httpx.Response(
                    self.status_code, request=self.request, json=self._payload
                ),
            )

    def json(self) -> dict:
        return self._payload


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, headers=None, params=None):
        self.requests.append((url, headers, params))
        return self.responses.pop(0)


def write_token_bundle(layout):
    store_x_api_token_bundle(
        layout,
        {
            "access_token": "access-123",
            "refresh_token": "refresh-123",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "obtained_at": datetime.now(timezone.utc).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
            "user": {
                "data": {
                    "id": "42",
                    "username": "thoth",
                    "name": "Thoth",
                }
            },
        },
    )


def test_sync_x_api_bookmarks_pages_and_persists_checkpoint(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    write_token_bundle(layout)

    responses = [
        FakeResponse(
            200,
            {
                "data": [
                    {
                        "id": "300",
                        "text": "new bookmark",
                        "created_at": "2026-04-04T12:00:00.000Z",
                        "author_id": "9",
                        "conversation_id": "300",
                    },
                    {
                        "id": "299",
                        "text": "second bookmark",
                        "created_at": "2026-04-04T11:55:00.000Z",
                        "author_id": "9",
                        "conversation_id": "299",
                    },
                    {
                        "id": "100",
                        "text": "already seen",
                        "created_at": "2026-04-04T10:00:00.000Z",
                        "author_id": "9",
                        "conversation_id": "100",
                    },
                ],
                "includes": {
                    "users": [
                        {"id": "9", "username": "alice", "name": "Alice"}
                    ]
                },
                "meta": {
                    "result_count": 3,
                    "next_token": "page-2",
                },
            },
        ),
        FakeResponse(
            200,
            {
                "data": [
                    {
                        "id": "90",
                        "text": "older bookmark",
                        "created_at": "2026-04-03T10:00:00.000Z",
                        "author_id": "9",
                        "conversation_id": "90",
                    }
                ],
                "includes": {
                    "users": [
                        {"id": "9", "username": "alice", "name": "Alice"}
                    ]
                },
                "meta": {
                    "result_count": 1,
                },
            },
        ),
    ]

    import core.x_api_bookmark_sync as x_api_bookmark_sync

    client = FakeAsyncClient(responses)
    x_api_bookmark_sync.httpx.AsyncClient = lambda *args, **kwargs: client

    checkpoint = XApiBookmarkSyncCheckpoint(
        user_id="42",
        seen_bookmark_ids=("100",),
    )
    x_api_bookmark_sync.store_x_api_bookmark_sync_checkpoint(layout, checkpoint)

    result = pytest.importorskip("asyncio").run(
        sync_x_api_bookmarks(config, layout=layout)
    )

    assert result["status"] == "ok"
    assert result["user_id"] == "42"
    assert result["pages_fetched"] == 1
    assert result["bookmarks_emitted"] == 2
    assert result["stopped_at_known_id"] is True

    assert len(client.requests) == 1
    request_url, headers, params = client.requests[0]
    parsed = urlparse(request_url)
    assert parsed.path == "/2/users/42/bookmarks"
    assert headers["Authorization"] == "Bearer access-123"
    assert params["max_results"] == "100"
    assert "pagination_token" not in params
    assert "attachments.media_keys" in params["expansions"]
    assert "tweet.fields" in params
    assert "user.fields" in params
    assert "media.fields" in params

    emitted_ids = [payload["tweet_id"] for payload in result["payloads"]]
    assert emitted_ids == ["300", "299"]
    assert result["payloads"][0]["source"] == "x_api_backfill"
    assert result["payloads"][0]["tweet_data"]["author_username"] == "alice"
    assert result["payloads"][0]["tweet_data"]["id"] == "300"

    stored = load_x_api_bookmark_sync_checkpoint(layout)
    assert stored is not None
    assert stored.user_id == "42"
    assert stored.pagination_token is None
    assert stored.seen_bookmark_ids[-2:] == ("300", "299")


def test_sync_x_api_bookmarks_uses_pagination_token_and_resumes_checkpoint(
    tmp_path: Path,
):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    write_token_bundle(layout)

    responses = [
        FakeResponse(
            200,
            {
                "data": [
                    {
                        "id": "500",
                        "text": "resume bookmark",
                        "created_at": "2026-04-04T12:30:00.000Z",
                        "author_id": "9",
                        "conversation_id": "500",
                    }
                ],
                "meta": {"next_token": "page-3", "result_count": 1},
            },
        )
    ]

    import core.x_api_bookmark_sync as x_api_bookmark_sync

    client = FakeAsyncClient(responses)
    x_api_bookmark_sync.httpx.AsyncClient = lambda *args, **kwargs: client

    x_api_bookmark_sync.store_x_api_bookmark_sync_checkpoint(
        layout,
        XApiBookmarkSyncCheckpoint(
            user_id="42",
            seen_bookmark_ids=("100", "299", "300"),
            pagination_token="page-2",
        ),
    )

    result = pytest.importorskip("asyncio").run(
        sync_x_api_bookmarks(config, layout=layout, max_pages=1)
    )

    assert result["pages_fetched"] == 1
    assert len(result["payloads"]) == 1
    assert client.requests[0][2]["pagination_token"] == "page-2"
    assert client.requests[0][2]["max_results"] == "100"
    assert result["checkpoint"]["pagination_token"] == "page-3"
    assert result["checkpoint"]["seen_bookmark_ids"][-1] == "500"
