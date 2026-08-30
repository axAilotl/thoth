from copy import deepcopy
from datetime import datetime, timedelta, timezone
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
    XApiTokenError,
    build_authorize_url,
    complete_x_api_auth,
    generate_pkce_pair,
    load_pending_x_api_auth,
    load_x_api_token_bundle,
    redact_x_api_secrets,
    refresh_x_api_tokens,
    resolve_x_api_auth_config,
    start_x_api_auth,
    store_x_api_token_bundle,
    summarize_x_api_auth,
    test_x_api_connection as run_x_api_connection_test,
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


def test_x_api_test_connection_endpoint_returns_redacted_diagnostic(
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
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "obtained_at": datetime.now(timezone.utc).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )

    import core.x_api_auth as x_api_auth

    fake_http_client = FakeAsyncClient(
        get_response=FakeResponse(
            200,
            {"data": {"id": "42", "username": "thoth", "name": "Thoth"}},
        )
    )
    monkeypatch.setattr(
        x_api_auth.httpx,
        "AsyncClient",
        lambda *args, **kwargs: fake_http_client,
    )

    with TestClient(thoth_api.app) as client:
        response = client.post("/api/x-api/test-connection")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["connected"] is True
    assert payload["client_metadata"]["configured"] is True
    assert payload["token"]["present"] is True


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



def test_test_x_api_connection_valid_config_and_token(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "access-123",
            "refresh_token": "refresh-123",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "obtained_at": datetime.now(timezone.utc).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )

    client = FakeAsyncClient(
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
    )

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=layout, client=client)
    )

    assert result["status"] == "ok"
    assert result["connected"] is True
    assert result["refreshed"] is False
    assert result["token"]["present"] is True
    assert result["token"]["expired"] is False
    assert result["token"]["refreshable"] is True
    assert result["user"]["data"]["username"] == "thoth"
    assert result["client_metadata"]["configured"] is True
    assert result["client_metadata"]["missing_fields"] == []
    assert result["client_metadata"]["scopes"] == [
        "bookmark.read",
        "tweet.read",
        "users.read",
        "offline.access",
    ]
    assert result["refresh_behavior"]["offline_access_present"] is True
    assert result["refresh_behavior"]["supported"] is True
    assert result["refresh_behavior"]["client_type"] == "public_pkce"


def test_test_x_api_connection_missing_client_metadata(tmp_path: Path):
    config = make_config(tmp_path)
    config.set("sources.x_api.client_id", "")
    config.set("sources.x_api.redirect_uri", "")
    config.set("sources.x_api.scopes", [])

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=build_path_layout(config, project_root=tmp_path))
    )

    assert result["status"] == "config_error"
    assert result["connected"] is False
    assert result["client_metadata"]["configured"] is False
    assert set(result["client_metadata"]["missing_fields"]) == {
        "client_id",
        "redirect_uri",
        "scopes",
    }
    assert result["token"]["present"] is False
    assert result["refresh_behavior"]["supported"] is False


def test_test_x_api_connection_expired_token_without_refresh(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "access-123",
            "refresh_token": "",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "obtained_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=layout)
    )

    assert result["status"] == "token_expired"
    assert result["connected"] is False
    assert result["token"]["present"] is True
    assert result["token"]["expired"] is True
    assert result["token"]["refreshable"] is False


def test_test_x_api_connection_refresh_success(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "access-old",
            "refresh_token": "refresh-original",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "obtained_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )

    client = FakeAsyncClient(
        post_response=FakeResponse(
            200,
            {
                "access_token": "access-refreshed",
                "refresh_token": "refresh-refreshed",
                "token_type": "bearer",
                "expires_in": 7200,
                "scope": "bookmark.read tweet.read users.read offline.access",
            },
        ),
        get_response=FakeResponse(
            200,
            {
                "data": {
                    "id": "42",
                    "username": "thoth",
                    "name": "Thoth",
                }
            },
        ),
    )

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=layout, client=client)
    )

    assert result["status"] == "ok"
    assert result["connected"] is True
    assert result["refreshed"] is True
    assert result["token"]["present"] is True
    assert result["token"]["expired"] is False
    assert result["token"]["refreshable"] is True
    assert result["user"]["data"]["username"] == "thoth"
    assert len(client.requests) == 2
    assert client.requests[0][0] == "post"
    assert client.requests[1][0] == "get"
    stored = load_x_api_token_bundle(layout)
    assert stored["access_token"] == "access-refreshed"
    assert stored["refresh_token"] == "refresh-refreshed"


def test_test_x_api_connection_refresh_failure(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "access-old",
            "refresh_token": "refresh-original",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "obtained_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )

    client = FakeAsyncClient(
        post_response=FakeResponse(
            401,
            {"error": "invalid_request", "error_description": "refresh token revoked"},
        )
    )

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=layout, client=client)
    )

    assert result["status"] == "refresh_failed"
    assert result["connected"] is False
    assert result["refreshed"] is False
    assert "refresh failed" in result["error"].lower()


def test_test_x_api_connection_distinguishes_invalid_user_token(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "invalid-access",
            "refresh_token": "refresh-token",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "obtained_at": datetime.now(timezone.utc).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )
    client = FakeAsyncClient(get_response=FakeResponse(401, {"error": "unauthorized"}))

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=layout, client=client)
    )

    assert result["status"] == "token_invalid"
    assert result["connected"] is False
    assert result["token"]["present"] is True


def test_redact_x_api_secrets_removes_tokens_and_secrets():
    auth_config = XApiAuthConfig(
        client_id="client-123",
        redirect_uri="http://127.0.0.1/callback",
        scopes=("bookmark.read",),
        client_secret="super-secret-client-secret",
    )
    bundle = {
        "access_token": "access-token-value",
        "refresh_token": "refresh-token-value",
    }
    raw = (
        "Authorization: Bearer access-token-value, "
        "refresh=refresh-token-value, "
        "client_secret=super-secret-client-secret"
    )
    redacted = redact_x_api_secrets(raw, auth_config=auth_config, bundle=bundle)
    assert "access-token-value" not in redacted
    assert "refresh-token-value" not in redacted
    assert "super-secret-client-secret" not in redacted
    assert "[[REDACTED" in redacted or "[[REDACTED]]" in redacted


def test_test_x_api_connection_redacts_secrets_in_output(tmp_path: Path, monkeypatch):
    from core import x_api_auth as x_api_auth_module

    config = make_config(tmp_path)
    config.set("sources.x_api.client_secret_env", "X_API_CLIENT_SECRET")
    monkeypatch.setenv("X_API_CLIENT_SECRET", "leaked-client-secret")
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    store_x_api_token_bundle(
        layout,
        {
            "version": 1,
            "access_token": "leaked-access-token",
            "refresh_token": "leaked-refresh-token",
            "token_type": "bearer",
            "scopes": ["bookmark.read", "tweet.read", "users.read", "offline.access"],
            "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "obtained_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            "client_id": "client-123",
            "redirect_uri": "http://127.0.0.1:8000/api/x-api/auth/callback",
        },
    )

    async def fake_refresh_with_secret(*args, **kwargs):
        raise XApiTokenError(
            "refresh failed for token leaked-access-token "
            "with secret leaked-client-secret"
        )

    monkeypatch.setattr(x_api_auth_module, "refresh_x_api_tokens", fake_refresh_with_secret)

    result = pytest.importorskip("asyncio").run(
        run_x_api_connection_test(config, layout=layout)
    )

    assert result["status"] == "refresh_failed"
    result_text = str(result)
    assert "leaked-access-token" not in result_text
    assert "leaked-refresh-token" not in result_text
    assert "leaked-client-secret" not in result_text
    assert "[[REDACTED]]" in result["error"]
    assert result["redacted"] is True
