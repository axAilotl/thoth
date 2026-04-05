from copy import deepcopy
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from fastapi.testclient import TestClient

import thoth_api
from core.config import Config
from core.path_layout import build_path_layout
from core.x_api_auth import (
    XApiAuthConfig,
    build_authorize_url,
    complete_x_api_auth,
    generate_pkce_pair,
    load_pending_x_api_auth,
    load_x_api_token_bundle,
    refresh_x_api_tokens,
    resolve_x_api_auth_config,
    start_x_api_auth,
    store_x_api_token_bundle,
    summarize_x_api_auth,
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
            response = httpx.Response(
                self.status_code,
                request=self.request,
                json=self._payload,
            )
            raise httpx.HTTPStatusError(
                f"{self.status_code} error",
                request=self.request,
                response=response,
            )

    def json(self) -> dict:
        return self._payload


class FakeAsyncClient:
    def __init__(self, *, post_response=None, get_response=None):
        self.post_response = post_response
        self.get_response = get_response
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, data=None, headers=None):
        self.requests.append(("post", url, data, headers))
        return self.post_response

    async def get(self, url, headers=None):
        self.requests.append(("get", url, headers))
        return self.get_response


@pytest.fixture
def restore_thoth_config():
    original = deepcopy(thoth_api.config.data)
    yield
    thoth_api.config.data = original


def test_generate_pkce_pair_and_authorize_url():
    verifier, challenge = generate_pkce_pair()
    auth_config = XApiAuthConfig(
        client_id="client-123",
        redirect_uri="http://127.0.0.1:8000/api/x-api/auth/callback",
        scopes=("bookmark.read", "tweet.read", "users.read", "offline.access"),
    )

    assert 43 <= len(verifier) <= 128
    assert challenge

    url = build_authorize_url(auth_config, state="state-123", code_challenge=challenge)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert parsed.netloc == "x.com"
    assert query["client_id"] == ["client-123"]
    assert query["state"] == ["state-123"]
    assert query["code_challenge"] == [challenge]
    assert query["code_challenge_method"] == ["S256"]
    assert query["scope"] == ["bookmark.read tweet.read users.read offline.access"]


def test_start_and_complete_x_api_auth_round_trip(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)

    start_payload = start_x_api_auth(config, layout=layout)
    pending = load_pending_x_api_auth(layout)
    assert pending is not None
    assert pending.state == start_payload["state"]
    assert pending.redirect_uri == config.get("sources.x_api.redirect_uri")

    clients = [
        FakeAsyncClient(
            post_response=FakeResponse(
                200,
                {
                    "access_token": "access-123",
                    "refresh_token": "refresh-123",
                    "token_type": "bearer",
                    "expires_in": 7200,
                    "scope": "bookmark.read tweet.read users.read offline.access",
                },
            )
        ),
        FakeAsyncClient(
            get_response=FakeResponse(
                200,
                {
                    "data": {
                        "id": "42",
                        "username": "thoth",
                        "name": "Thoth",
                    }
                },
            )
        ),
    ]

    import core.x_api_auth as x_api_auth

    x_api_auth.httpx.AsyncClient = lambda *args, **kwargs: clients.pop(0)
    completed = pytest.importorskip("asyncio").run(
        complete_x_api_auth(
            config,
            code="auth-code",
            state=start_payload["state"],
            layout=layout,
        )
    )

    assert completed["user"]["data"]["username"] == "thoth"
    bundle = load_x_api_token_bundle(layout)
    assert bundle is not None
    assert bundle["access_token"] == "access-123"
    assert bundle["refresh_token"] == "refresh-123"
    assert bundle["user"]["data"]["id"] == "42"
    assert summarize_x_api_auth(layout)["has_token"] is True
    assert not (layout.auth_root / "x_api_pending_auth.json").exists()


def test_refresh_x_api_tokens_updates_bundle(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)

    auth_config = resolve_x_api_auth_config(config)
    start_x_api_auth(config, layout=layout)

    token_clients = [
        FakeAsyncClient(
            post_response=FakeResponse(
                200,
                {
                    "access_token": "access-refreshed",
                    "refresh_token": "refresh-refreshed",
                    "token_type": "bearer",
                    "expires_in": 7200,
                    "scope": "bookmark.read tweet.read users.read offline.access",
                },
            )
        ),
        FakeAsyncClient(
            get_response=FakeResponse(
                200,
                {
                    "data": {
                        "id": "42",
                        "username": "thoth",
                        "name": "Thoth",
                    }
                },
            )
        ),
    ]

    import core.x_api_auth as x_api_auth

    x_api_auth.httpx.AsyncClient = lambda *args, **kwargs: token_clients.pop(0)

    refreshed_bundle = pytest.importorskip("asyncio").run(
        refresh_x_api_tokens(auth_config, refresh_token="refresh-original")
    )
    assert refreshed_bundle.access_token == "access-refreshed"
    user = pytest.importorskip("asyncio").run(
        x_api_auth.fetch_current_x_user(auth_config, access_token=refreshed_bundle.access_token)
    )
    assert user["data"]["username"] == "thoth"


def test_x_api_start_route_wires_into_fastapi(tmp_path: Path, restore_thoth_config):
    config = make_config(tmp_path)
    thoth_api.config.data = deepcopy(config.data)

    with TestClient(thoth_api.app) as client:
        response = client.post("/api/x-api/auth/start")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["auth_root"].endswith(".thoth_system/auth")
    assert payload["authorize_url"].startswith("https://x.com/i/oauth2/authorize")


def test_x_api_status_route_flattens_nested_user(
    tmp_path: Path,
    restore_thoth_config,
    monkeypatch,
):
    config = make_config(tmp_path)
    thoth_api.config.data = deepcopy(config.data)
    monkeypatch.setattr(
        thoth_api,
        "build_path_layout",
        lambda cfg: build_path_layout(cfg, project_root=tmp_path),
    )
    layout = build_path_layout(config, project_root=tmp_path)
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "access-123",
            "refresh_token": "refresh-123",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": "2026-04-05T00:00:00+00:00",
            "obtained_at": "2026-04-04T23:00:00+00:00",
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

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/x-api/auth/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["user"]["id"] == "42"
    assert payload["user"]["username"] == "thoth"


def test_x_api_callback_returns_html_for_browser_requests(
    tmp_path: Path,
    restore_thoth_config,
    monkeypatch,
):
    config = make_config(tmp_path)
    thoth_api.config.data = deepcopy(config.data)
    monkeypatch.setattr(
        thoth_api,
        "build_path_layout",
        lambda cfg: build_path_layout(cfg, project_root=tmp_path),
    )

    async def fake_complete_x_api_auth(*args, **kwargs):
        return {
            "token_bundle": {
                "version": 1,
                "access_token": "access-123",
                "refresh_token": "refresh-123",
                "token_type": "bearer",
                "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
                "expires_at": "2026-04-05T00:00:00+00:00",
                "obtained_at": "2026-04-04T23:00:00+00:00",
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
            "user": {
                "data": {
                    "id": "42",
                    "username": "thoth",
                    "name": "Thoth",
                }
            },
        }

    monkeypatch.setattr(thoth_api, "complete_x_api_auth", fake_complete_x_api_auth)

    with TestClient(thoth_api.app) as client:
        response = client.get(
            "/api/x-api/auth/callback?code=test-code&state=test-state",
            headers={"accept": "text/html"},
        )

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "window.opener.postMessage" in body
    assert "thoth:x-api-auth-complete" in body
    assert "@thoth" in body
    assert "x_api_auth" in body
