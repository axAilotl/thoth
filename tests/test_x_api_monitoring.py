from copy import deepcopy
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import thoth_api
from core.config import Config
from core.llm_interface import LLMResponse
from core.metadata_db import MetadataDB
from core.x_api_monitoring import (
    X_API_MONITOR_SECRET_HEADER,
    process_x_api_monitoring_webhook,
    resolve_x_api_monitoring_config,
    verify_x_api_monitoring_webhook_secret,
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
    config.set("database.path", str(tmp_path / ".thoth_system" / "meta.db"))
    config.set("sources.x_api.enabled", True)
    config.set("sources.x_api.client_id", "client-123")
    config.set(
        "sources.x_api.redirect_uri",
        "http://127.0.0.1:8090/api/x-api/auth/callback",
    )
    config.set(
        "sources.x_api.scopes",
        [
            "bookmark.read",
            "bookmark.write",
            "tweet.read",
            "users.read",
            "offline.access",
        ],
    )
    config.set("sources.x_api.monitoring.enabled", True)
    config.set("sources.x_api.monitoring.auto_bookmark", True)
    config.set(
        "sources.x_api.monitoring.webhook_secret_env",
        "THOTH_X_MONITOR_WEBHOOK_SECRET",
    )
    config.set("sources.x_api.monitoring.accounts", ["@OpenAI", {"user_id": "99"}])
    return config


class FakeLLMInterface:
    def __init__(self, content: str):
        self.content = content
        self.calls = []

    def resolve_task_route(self, task: str):
        assert task == "x_monitor"
        return ("openrouter", "fake-classifier", {"max_tokens": 220, "temperature": 0.1})

    async def generate(self, prompt: str, system_prompt: str | None = None, **kwargs):
        self.calls.append(
            {
                "prompt": prompt,
                "system_prompt": system_prompt,
                "kwargs": kwargs,
            }
        )
        return LLMResponse(
            content=self.content,
            model="fake-classifier",
            provider="openrouter",
        )


@pytest.fixture
def restore_thoth_config():
    original = deepcopy(thoth_api.config.data)
    yield
    thoth_api.config.data = original


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_resolve_x_api_monitoring_config_normalizes_accounts(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_X_MONITOR_WEBHOOK_SECRET", "monitor-secret")

    resolved = resolve_x_api_monitoring_config(config)

    assert resolved.enabled is True
    assert resolved.auto_bookmark is True
    assert [account.label() for account in resolved.accounts] == ["@openai", "99"]
    assert (
        verify_x_api_monitoring_webhook_secret(
            "monitor-secret",
            runtime_config=config,
        ).webhook_secret
        == "monitor-secret"
    )


@pytest.mark.anyio
async def test_process_x_api_monitoring_webhook_accepts_and_bookmarks(
    tmp_path: Path,
    monkeypatch,
):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_X_MONITOR_WEBHOOK_SECRET", "monitor-secret")
    db_path = tmp_path / ".thoth_system" / "meta.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = MetadataDB(str(db_path))

    async def fake_create_bookmark(tweet_id: str, *, runtime_config, layout=None):
        assert runtime_config is config
        assert tweet_id == "123"
        return {"user_id": "42", "bookmarked": True}

    monkeypatch.setattr(
        "core.x_api_monitoring.create_x_api_bookmark",
        fake_create_bookmark,
    )

    llm = FakeLLMInterface(
        '{"useful": true, "confidence": 0.93, "reason": "Matches active research topics.", "matched_topics": ["companion-ai-research"]}'
    )
    result = await process_x_api_monitoring_webhook(
        {
            "tweet_id": "123",
            "text": "Companion AI whitepaper on memory and alignment.",
            "author_username": "openai",
            "author_id": "42",
            "created_at": "2026-04-14T12:00:00Z",
        },
        runtime_config=config,
        llm_interface=llm,
        db=db,
    )

    assert result["status"] == "accepted"
    assert result["reason"] == "classifier_accepted"
    assert result["monitored_account"] == "@openai"
    assert result["bookmark_write"] == {"user_id": "42", "bookmarked": True}
    assert result["bookmark_payload"]["source"] == "x_api_monitored_webhook"
    assert result["bookmark_payload"]["monitoring_decision"]["matched_topics"] == [
        "companion-ai-research"
    ]
    assert llm.calls


@pytest.mark.anyio
async def test_process_x_api_monitoring_webhook_ignores_unmonitored_accounts(
    tmp_path: Path,
    monkeypatch,
):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_X_MONITOR_WEBHOOK_SECRET", "monitor-secret")
    db_path = tmp_path / ".thoth_system" / "meta.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = MetadataDB(str(db_path))

    result = await process_x_api_monitoring_webhook(
        {
            "tweet_id": "555",
            "text": "Unrelated post.",
            "author_username": "someone-else",
            "author_id": "77",
        },
        runtime_config=config,
        llm_interface=FakeLLMInterface(
            '{"useful": true, "confidence": 1, "reason": "unused", "matched_topics": []}'
        ),
        db=db,
    )

    assert result == {
        "status": "ignored",
        "reason": "unmonitored_account",
        "tweet_id": "555",
        "author_username": "someone-else",
        "author_id": "77",
    }


def test_x_api_monitoring_webhook_route_queues_bookmarks(
    tmp_path: Path,
    monkeypatch,
    restore_thoth_config,
):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_X_MONITOR_WEBHOOK_SECRET", "monitor-secret")
    thoth_api.config.data = deepcopy(config.data)

    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    queued = []

    async def fake_process(payload, *, runtime_config, llm_interface, layout):
        assert runtime_config is thoth_api.config
        return {
            "status": "accepted",
            "reason": "classifier_accepted",
            "tweet_id": "123",
            "bookmark_payload": {
                "tweet_id": "123",
                "tweet_data": {"id": "123", "text": "useful"},
                "timestamp": "2026-04-14T12:00:00Z",
                "source": "x_api_monitored_webhook",
            },
        }

    async def fake_ingest(payload, **kwargs):
        queued.append((payload, kwargs))

    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "archivist_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(
        thoth_api,
        "resolve_x_api_sync_config",
        lambda: {
            "enabled": False,
            "interval_hours": 6,
            "run_on_startup": False,
            "max_results": 100,
            "max_pages": 3,
            "resume_from_checkpoint": True,
        },
    )
    monkeypatch.setattr(thoth_api, "process_x_api_monitoring_webhook", fake_process)
    monkeypatch.setattr(thoth_api, "ingest_bookmark_capture", fake_ingest)

    with TestClient(thoth_api.app) as client:
        response = client.post(
            "/api/x-api/monitoring/webhook",
            headers={X_API_MONITOR_SECRET_HEADER: "monitor-secret"},
            json={"tweet_id": "123", "text": "useful"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["queued"] is True
    assert len(queued) == 1
    assert queued[0][0]["tweet_id"] == "123"
    assert queued[0][1] == {
        "process_immediately": False,
        "queue_bookmark": True,
        "reset_processed": True,
        "force": True,
    }
