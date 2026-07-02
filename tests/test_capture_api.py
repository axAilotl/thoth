import asyncio

from fastapi.testclient import TestClient

import thoth_api


CAPTURE_SOURCE = {
    "source_id": "source-1",
    "source_name": "manual",
    "source_type": "manual",
    "collector": "cli",
    "status": "active",
}

CAPTURE_EVENT = {
    "event_id": "event-1",
    "source_id": "source-1",
    "session_id": "session-1",
    "event_type": "note",
    "status": "captured",
    "provenance": {"tool": "thoth.py"},
    "raw_ref_ids": ["raw-1"],
    "raw_refs": [{"raw_ref_id": "raw-1", "path": "/tmp/raw.json"}],
    "privacy": {"classification": "private"},
    "privacy_class": "private",
    "retention": {"policy": "default"},
    "retention_class": "default",
    "artifact_ids": ["artifact-1"],
    "artifacts": [{"artifact_id": "artifact-1", "artifact_type": "note"}],
    "security_state": {
        "state": "open",
        "finding_count": 1,
        "open_finding_count": 1,
        "max_severity": "high",
    },
    "security_findings": [{"finding_id": "finding-1", "severity": "high"}],
}


class FakeCaptureSurface:
    def list_sources(self):
        return {"sources": [CAPTURE_SOURCE], "total": 1}

    def list_events(self, *, source_id=None, session_id=None, limit=None):
        assert source_id in {None, "source-1"}
        assert session_id is None
        assert limit in {None, 10}
        return {"events": [CAPTURE_EVENT], "total": 1}

    def get_event(self, event_id):
        assert event_id == "event-1"
        return {**CAPTURE_EVENT, "payload": {"title": "Manual note"}}

    def inspect_retention(self, *, event_id=None, source_id=None, session_id=None, as_of=None):
        assert event_id == "event-1"
        assert source_id is None
        assert session_id is None
        assert as_of == "2026-01-01T00:00:00Z"
        return {
            "as_of": as_of,
            "targets": [
                {
                    "event_id": "event-1",
                    "target_type": "raw_ref",
                    "target_id": "raw-1",
                    "retention_scope": "raw_capture",
                    "retention_class": "raw-expire",
                    "privacy_class": "private",
                    "eligible": True,
                    "eligibility_reason": "eligible",
                }
            ],
            "total": 1,
            "eligible": 1,
            "by_scope": {"raw_capture": {"total": 1, "eligible": 1}},
        }

    def expire_retention(
        self,
        *,
        event_id,
        delete_raw=False,
        delete_distilled=False,
        dry_run=True,
        reason=None,
        actor=None,
        as_of=None,
    ):
        assert event_id == "event-1"
        assert delete_raw is False
        assert delete_distilled is True
        assert dry_run is False
        assert reason == "distilled expired"
        assert actor == "operator"
        assert as_of == "2026-01-01T00:00:00Z"
        return {
            "dry_run": False,
            "delete_raw": False,
            "delete_distilled": True,
            "operations": [{"status": "deleted", "retention_scope": "compiled_wiki"}],
            "audit_records": [{"operation": "retention.expired"}],
            "total": 1,
            "by_status": {"deleted": 1},
            "by_scope": {"compiled_wiki": {"deleted": 1}},
            "bytes_deleted": 20,
        }

    def ingest_manual(
        self,
        *,
        artifact_type,
        payload,
        source,
        session=None,
        event=None,
        raw_path=None,
        queue_artifact_id=None,
        priority=0,
        capabilities=None,
    ):
        return {
            "artifact_type": artifact_type,
            "payload": payload,
            "source": source,
            "session": session,
            "event": event,
            "raw_path": raw_path,
            "queue_artifact_id": queue_artifact_id,
            "priority": priority,
            "capabilities": capabilities,
        }


class FakeCaptureContext:
    def __enter__(self):
        return FakeCaptureSurface()

    def __exit__(self, exc_type, exc, traceback):
        return False


def _patch_background_tasks(monkeypatch):
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
    thoth_api._shutdown_event = asyncio.Event()


def test_capture_api_lists_sources_events_and_detail(monkeypatch):
    _patch_background_tasks(monkeypatch)
    monkeypatch.setattr(
        thoth_api,
        "open_api_capture_surface",
        lambda: FakeCaptureContext(),
    )

    with TestClient(thoth_api.app) as client:
        sources_response = client.get("/api/capture/sources")
        events_response = client.get("/api/capture/events", params={"limit": 10})
        detail_response = client.get("/api/capture/events/event-1")
        retention_response = client.get(
            "/api/capture/retention",
            params={"event_id": "event-1", "as_of": "2026-01-01T00:00:00Z"},
        )
        expire_response = client.post(
            "/api/capture/events/event-1/expire",
            json={
                "delete_raw": False,
                "delete_distilled": True,
                "execute": True,
                "reason": "distilled expired",
                "actor": "operator",
                "as_of": "2026-01-01T00:00:00Z",
            },
        )

    assert sources_response.status_code == 200
    assert sources_response.json()["sources"][0]["source_id"] == "source-1"

    assert events_response.status_code == 200
    event = events_response.json()["events"][0]
    assert event["provenance"] == {"tool": "thoth.py"}
    assert event["raw_ref_ids"] == ["raw-1"]
    assert event["privacy_class"] == "private"
    assert event["retention_class"] == "default"
    assert event["security_state"]["state"] == "open"

    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["event_id"] == "event-1"
    assert detail["payload"] == {"title": "Manual note"}
    assert detail["artifact_ids"] == ["artifact-1"]

    assert retention_response.status_code == 200
    assert retention_response.json()["targets"][0]["retention_class"] == "raw-expire"

    assert expire_response.status_code == 200
    expire_payload = expire_response.json()
    assert expire_payload["delete_raw"] is False
    assert expire_payload["delete_distilled"] is True
    assert expire_payload["by_status"] == {"deleted": 1}


def test_capture_api_omitted_capabilities_preserve_lifecycle_default(monkeypatch):
    _patch_background_tasks(monkeypatch)
    monkeypatch.setattr(
        thoth_api,
        "open_api_capture_surface",
        lambda: FakeCaptureContext(),
    )

    with TestClient(thoth_api.app) as client:
        omitted_response = client.post(
            "/api/capture/ingest",
            json={
                "artifact_type": "repository",
                "payload": {"id": "repo-1", "repo_name": "owner/repo"},
                "source": "github",
            },
        )
        explicit_empty_response = client.post(
            "/api/capture/ingest",
            json={
                "artifact_type": "repository",
                "payload": {"id": "repo-2", "repo_name": "owner/repo-2"},
                "source": "github",
                "capabilities": [],
            },
        )

    assert omitted_response.status_code == 200
    assert omitted_response.json()["capabilities"] is None
    assert explicit_empty_response.status_code == 200
    assert explicit_empty_response.json()["capabilities"] == []
