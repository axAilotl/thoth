import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api
from core.admin_status import build_admin_status_dashboard
from core.archivist_state import archivist_topic_state_key
from core.capture_event_store import CaptureEvent, CaptureSession, CaptureSource
from core.config import Config
from core.llm_usage import record_llm_usage
from core.metadata_db import BookmarkQueueEntry, IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.semantic_memory import (
    SemanticMemoryCandidate,
    SemanticMemoryEvidence,
    SemanticMemoryStore,
)
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

    def get_event(self, event_id):
        return next(
            (event for event in self.events if event.event_id == event_id),
            None,
        )

    def list_raw_refs(self, *, event_id=None):
        return ()

    def list_artifact_links(self, *, event_id=None):
        return ()


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
    raw_source_path = layout.raw_root / "capture" / "event-1.json"
    raw_source_path.parent.mkdir(parents=True, exist_ok=True)
    raw_source_path.write_text('{"title":"Lineage source"}\n', encoding="utf-8")
    lineage_page_path = layout.wiki_root / "pages" / "lineage-page.md"
    lineage_page_path.write_text(
        render_frontmatter(
            {
                "type": "Topic",
                "id": "lineage-page",
                "title": "Lineage Page",
                "description": "Lineage Page",
                "thoth_type": "wiki_page",
                "thoth_id": "lineage-page",
                "thoth_slug": "lineage-page",
                "thoth_kind": "topic",
                "thoth_summary": "Lineage Page",
                "thoth_source_paths": ["raw/capture/event-1.json"],
                "thoth_event_ids": ["event-1"],
                "thoth_artifact_id": "artifact-failed",
                "thoth_source_type": "arxiv",
                "thoth_semantic_candidate_ids": ["candidate-lineage"],
                "thoth_updated_at": "2026-06-25T00:00:00Z",
                "updated_at": "2026-06-25T00:00:00Z",
                "thoth_input_hash": "new-hash",
                "thoth_input_manifest": [
                    {
                        "input_id": "capture_event:event-1",
                        "input_kind": "capture_event",
                        "event_id": "event-1",
                        "event_type": "web_clip",
                        "event_hash": "event-hash",
                        "sha256": "event-sha",
                    },
                    {
                        "input_id": "raw_ref:raw-1",
                        "input_kind": "raw_ref",
                        "raw_ref_id": "raw-1",
                        "event_id": "event-1",
                        "source_path": "raw/capture/event-1.json",
                        "sha256": "file-sha",
                        "size_bytes": 27,
                    },
                ],
                "thoth_change_provenance": {
                    "compiled_at": "2026-06-25T00:00:00Z",
                    "reason": "inputs_changed",
                    "input_hash_before": "old-hash",
                    "input_hash_after": "new-hash",
                    "changes": [
                        {
                            "change_type": "changed",
                            "input_id": "raw_ref:raw-1",
                            "input_kind": "raw_ref",
                            "reason": "Raw artifact raw-1 hash changed.",
                        }
                    ],
                },
            }
        )
        + "\n# Lineage Page\n",
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
    semantic_store = SemanticMemoryStore(db)
    semantic_store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-lineage",
            candidate_type="claim",
            status="confirmed",
            text="Lineage views should explain wiki changes.",
            entity_type="topic",
            entity_name="Lineage",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-lineage",
                evidence_id="evidence-lineage",
                artifact_id="artifact-failed",
                artifact_type="paper",
                capture_event_id="event-1",
                source_path="raw/capture/event-1.json",
                evidence_text="The local raw capture influenced this page.",
            ),
        ),
    )
    run = db.begin_connector_run("omi", inputs={"export_paths": ["omi.json"]})
    assert run is not None
    record_llm_usage(
        provider="openrouter",
        model="test/model",
        task="archivist",
        operation="generate",
        input_text="source material",
        output_text="compiled result",
        pricing={
            "input_cost_per_1k_tokens_usd": 1.0,
            "output_cost_per_1k_tokens_usd": 1.0,
        },
        source_connector="omi",
        run_id=run.run_id,
        db=db,
    )
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
    assert payload["llm_usage"]["call_count"] == 1
    assert payload["llm_usage"]["totals_by_source"][0]["source_connector"] == "omi"
    assert payload["llm_usage"]["totals_by_task"][0]["task"] == "archivist"
    assert payload["llm_usage"]["recent_expensive_runs"][0]["run_id"] == run.run_id
    assert payload["lineage"]["pages_with_lineage"] >= 1
    lineage_page = next(
        item
        for item in payload["lineage"]["recent_pages"]
        if item["slug"] == "lineage-page"
    )
    assert lineage_page["why_changed"] == "Raw artifact raw-1 hash changed."
    assert lineage_page["local_files"][0]["source_path"] == "raw/capture/event-1.json"
    assert lineage_page["capture_events"][0]["event_id"] == "event-1"
    assert lineage_page["raw_refs"][0]["raw_ref_id"] == "raw-1"
    assert lineage_page["artifacts"][0]["artifact_id"] == "artifact-failed"
    assert lineage_page["semantic_candidates"][0]["candidate_id"] == "candidate-lineage"
    assert lineage_page["semantic_candidates"][0]["evidence"][0]["source_path"] == (
        "raw/capture/event-1.json"
    )

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
