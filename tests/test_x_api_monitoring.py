import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import thoth_api
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.x_api_monitoring import (
    XApiMonitoringAuthError,
    XApiMonitoringConfigError,
    capture_x_api_monitoring_webhook,
    normalize_x_api_monitoring_payload,
)


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / "system"))
    config.set("paths.cache_dir", "cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", str(tmp_path / "wiki"))
    config.set("database.path", "meta.db")
    config.set("sources.x_api.enabled", True)
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
    config.set("sources.x_api.monitoring.accounts", ["@AdaLovelace", "42"])
    config.set(
        "sources.x_api.monitoring.webhook_secret_env",
        "THOTH_TEST_X_MONITOR_SECRET",
    )
    return config


def test_normalize_x_api_monitoring_payload_uses_canonical_bookmark_contract():
    normalized = normalize_x_api_monitoring_payload(
        {
            "data": {
                "id": "123456",
                "author_id": "42",
                "text": "A monitored post",
                "created_at": "2026-08-30T12:00:00Z",
            },
            "includes": {
                "users": [
                    {"id": "42", "username": "AdaLovelace", "name": "Ada"}
                ]
            },
        }
    )

    assert normalized["tweet_id"] == "123456"
    assert normalized["source"] == "x_api_monitored_webhook"
    assert normalized["timestamp"] == "2026-08-30T12:00:00Z"
    assert normalized["tweet_data"]["author_username"] == "adalovelace"
    assert normalized["tweet_data"]["author"]["name"] == "Ada"


def test_monitoring_requires_bookmark_write_scope(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_TEST_X_MONITOR_SECRET", "secret")
    config.set(
        "sources.x_api.scopes",
        ["bookmark.read", "tweet.read", "users.read", "offline.access"],
    )

    with pytest.raises(XApiMonitoringConfigError, match="bookmark.write"):
        capture_x_api_monitoring_webhook(
            config,
            {"tweet_id": "1", "author_id": "42"},
            webhook_secret="secret",
        )


@pytest.mark.parametrize("provided", [None, "", "wrong-secret"])
def test_monitoring_webhook_auth_fails_closed(
    tmp_path: Path,
    monkeypatch,
    provided: str | None,
):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_TEST_X_MONITOR_SECRET", "correct-secret")

    with pytest.raises(XApiMonitoringAuthError, match="Invalid X monitoring"):
        capture_x_api_monitoring_webhook(
            config,
            {"tweet_id": "1", "author_id": "42"},
            webhook_secret=provided,
        )
    assert not (tmp_path / "meta.db").exists()


def test_monitoring_webhook_queues_monitored_post_with_provenance(
    tmp_path: Path,
    monkeypatch,
):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_TEST_X_MONITOR_SECRET", "correct-secret")
    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))

    result = capture_x_api_monitoring_webhook(
        config,
        {
            "data": {
                "id": "987654",
                "author_id": "42",
                "text": "Durable research note",
                "created_at": "2026-08-30T12:00:00Z",
            }
        },
        webhook_secret="correct-secret",
        layout=layout,
        db=db,
    )

    assert result == {
        "status": "accepted",
        "tweet_id": "987654",
        "matched_account": "42",
        "queue_status": "pending",
        "artifact_id": "987654",
        "capture_event_id": result["capture_event_id"],
    }
    assert result["capture_event_id"]
    entry = db.get_ingestion_entry("987654")
    assert entry is not None
    queued = json.loads(entry.payload_json)
    assert queued["source"] == "x_api_monitored_webhook"
    assert queued["normalized_metadata"]["provenance"]["collector"] == "x_api_monitoring"
    assert queued["normalized_metadata"]["monitored_account"] == "42"
    assert queued["normalized_metadata"]["capture_event_id"] == result["capture_event_id"]
    assert json.loads(entry.capabilities_json) == ["bookmark.write"]


def test_monitoring_webhook_ignores_unmonitored_account(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)
    monkeypatch.setenv("THOTH_TEST_X_MONITOR_SECRET", "correct-secret")
    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))

    result = capture_x_api_monitoring_webhook(
        config,
        {"tweet_id": "222", "author_id": "999", "text": "not monitored"},
        webhook_secret="correct-secret",
        layout=layout,
        db=db,
    )

    assert result == {
        "status": "ignored",
        "reason": "unmonitored_account",
        "tweet_id": "222",
    }
    assert db.get_ingestion_entry("222") is None


def test_monitoring_webhook_endpoint_passes_auth_header(monkeypatch):
    captured: dict[str, object] = {}

    def fake_capture(config, payload, *, webhook_secret):
        captured.update(payload=payload, webhook_secret=webhook_secret)
        return {"status": "accepted", "tweet_id": "123"}

    monkeypatch.setattr(thoth_api, "capture_x_api_monitoring_webhook", fake_capture)
    response = TestClient(thoth_api.app).post(
        "/api/x-api/monitoring/webhook",
        headers={"X-Thoth-Webhook-Secret": "route-secret"},
        json={"tweet_id": "123"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "tweet_id": "123"}
    assert captured == {
        "payload": {"tweet_id": "123"},
        "webhook_secret": "route-secret",
    }
