import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api
from core.admin_status import build_admin_status_dashboard
from core.archivist_state import archivist_topic_state_key
from core.capture_event_store import CaptureEvent, CaptureSession, CaptureSource
from core.config import Config
from core.metadata_db import BookmarkQueueEntry, IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.wiki_io import render_frontmatter


def _config_data(tmp_path: Path) -> dict:
    return {
        "paths": {
            "vault_dir": str(tmp_path / "vault"),
            "system_dir": ".thoth_system",
            "cache_dir": "cache",
            "raw_dir": "raw",
            "library_dir": "library",
            "wiki_dir": "wiki",
            "digests_dir": "_digests",
            "archivist_topics_file": "archivist_topics.yaml",
        },
        "database": {"path": "meta.db"},
        "sources": {},
    }


def _config(config_data: dict) -> Config:
    config = Config()
    config.data = config_data
    return config


class FakeCaptureStore:
    def __init__(self):
        self.sources = (
            CaptureSource(
                source_id="source-web",
                source_name="Web Clipper",
                source_type="web_clipper",
                status="error",
                metadata={"last_error": "token expired"},
                updated_at="2026-06-25T00:00:00Z",
            ),
            CaptureSource(
                source_id="source-x",
                source_name="X API",
                source_type="x_api",
                status="active",
                updated_at="2026-06-25T01:00:00Z",
            ),
        )
        self.sessions = (
            CaptureSession(
                session_id="session-failed",
                source_id="source-web",
                session_type="import",
                status="failed",
                started_at="2026-06-24T00:00:00Z",
                ended_at="2026-06-24T00:10:00Z",
                metadata={"error": "session import crashed"},
            ),
            CaptureSession(
                session_id="session-open",
                source_id="source-x",
                session_type="sync",
                status="open",
                started_at="2026-06-24T00:00:00Z",
            ),
        )
        self.events = (
            CaptureEvent(
                event_id="event-1",
                source_id="source-web",
                session_id="session-failed",
                event_type="web_clip",
                status="captured",
                captured_at="2026-06-24T00:05:00Z",
            ),
            CaptureEvent(
                event_id="event-2",
                source_id="source-x",
                session_id="session-open",
                event_type="x_bookmark",
                status="captured",
                captured_at="2026-06-24T00:06:00Z",
            ),
        )

    def list_sources(self):
        return self.sources

    def list_sessions(self):
        return self.sessions

    def list_events(self):
        return self.events


def test_admin_status_dashboard_uses_operational_stores(tmp_path: Path):
    config_data = _config_data(tmp_path)
    config = _config(config_data)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    (tmp_path / "archivist_topics.yaml").write_text(
        """
version: 1
topics:
  - id: test-topic
    title: Test Topic
    output_path: pages/topic-test-topic.md
    include_roots:
      - tweets
""".strip(),
        encoding="utf-8",
    )

    page_path = layout.wiki_root / "pages" / "capture-old.md"
    page_path.parent.mkdir(parents=True, exist_ok=True)
    page_path.write_text(
        render_frontmatter(
            {
                "title": "Old Capture Rollup",
                "thoth_slug": "capture-old",
                "slug": "capture-old",
                "record_type": "wiki_page",
                "kind": "capture",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "thoth_capture_page_type": "daily",
                "thoth_capture_page_key": "2026-01-01",
                "thoth_capture_event_count": 2,
                "thoth_input_hash": "old-hash",
                "thoth_input_manifest": [],
            }
        )
        + "\n# Old Capture Rollup\n",
        encoding="utf-8",
    )

    db = MetadataDB(str(layout.database_path))
    assert db.upsert_bookmark_entry(
        BookmarkQueueEntry(
            tweet_id="tweet-failed",
            source="userscript",
            captured_at="2026-06-24T00:00:00Z",
            status="failed",
            attempts=3,
            last_error="network down",
            last_attempt_at="2026-06-24T00:05:00Z",
        )
    )
    assert db.upsert_ingestion_entry(
        IngestionQueueEntry(
            artifact_id="artifact-failed",
            artifact_type="paper",
            source="arxiv",
            payload_json=json.dumps({"id": "artifact-failed", "raw_content": "queued"}),
            status="failed",
            attempts=2,
            last_error="compiler input invalid",
            created_at="2026-06-24T00:00:00Z",
        )
    )
    run = db.begin_connector_run("omi", inputs={"export_paths": ["omi.json"]})
    assert run is not None
    db.finish_connector_run(
        run.run_id,
        status="failed",
        failure_reason="temporary connector outage",
    )
    db.upsert_automation_state(
        archivist_topic_state_key("test-topic"),
        {
            "topic_id": "test-topic",
            "last_run_at": "2026-06-25T00:00:00Z",
            "last_success_at": "2026-06-25T00:00:00Z",
            "last_source_keys": ["tweet-1"],
            "last_source_hashes": {"tweet-1": "hash"},
            "last_source_fingerprint": "fingerprint",
            "last_candidate_count": 1,
            "last_model_provider": "openai",
            "last_model": "gpt-test",
            "force_requested_at": None,
            "force_reason": None,
        },
    )

    payload = build_admin_status_dashboard(
        config_data,
        project_root=tmp_path,
        db=db,
        event_store=FakeCaptureStore(),
        now=datetime(2026, 6, 26, tzinfo=timezone.utc),
        stale_after_days=1,
    )

    assert payload["status"] == "degraded"
    assert payload["source_health"]["total"] == 2
    assert payload["source_health"]["unhealthy"] == 1
    assert payload["source_health"]["sources"][0]["reason"] == "token expired"
    assert payload["recent_sessions"]["sessions"][0]["reason"] == "session import crashed"
    assert payload["event_counts"]["total"] == 2
    assert payload["event_counts"]["by_type"] == {"web_clip": 1, "x_bookmark": 1}
    assert payload["queue_counts"]["bookmark_queue"]["by_status"]["failed"] == 1
    assert payload["queue_counts"]["ingestion_queue"]["by_status"]["failed"] == 1
    assert payload["stale_pages"]["total"] >= 1
    assert payload["compiler_runs"]["archivist"]["topic_count"] == 1
    assert payload["compiler_runs"]["archivist"]["recent"][0]["last_candidate_count"] == 1
    assert payload["compiler_runs"]["capture_wiki"]["compiled_page_count"] == 1

    stuck_reasons = {item["reason"] for item in payload["stuck_work"]["items"]}
    assert "network down" in stuck_reasons
    assert "compiler input invalid" in stuck_reasons
    assert "temporary connector outage" in stuck_reasons
    assert "session import crashed" in stuck_reasons
    assert any("Page has not been updated" in reason for reason in stuck_reasons)


def test_admin_status_endpoint_returns_dashboard(monkeypatch):
    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "archivist_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(thoth_api, "resolve_x_api_sync_config", lambda: None)
    monkeypatch.setattr(thoth_api, "load_runtime_settings", lambda: {"paths": {}})
    monkeypatch.setattr(
        thoth_api,
        "build_admin_status_dashboard",
        lambda config_data, *, project_root: {
            "status": "ok",
            "source_health": {"total": 0},
            "errors": [],
        },
    )

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/admin/status")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
