from fastapi.testclient import TestClient

import thoth_api


def test_stats_api_separates_realtime_bookmarks_from_queue_history(monkeypatch):
    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(thoth_api, "resolve_x_api_sync_config", lambda: None)
    monkeypatch.setattr(
        thoth_api,
        "load_realtime_bookmarks",
        lambda: [
            {
                "tweet_id": "1",
                "timestamp": "2026-04-04T10:00:00Z",
                "source": "userscript",
                "processed": True,
            },
            {
                "tweet_id": "2",
                "timestamp": "2026-04-04T11:00:00Z",
                "source": "userscript",
                "processed": False,
            },
        ],
    )
    monkeypatch.setattr(
        thoth_api,
        "get_metadata_db",
        lambda: type(
            "FakeDB",
            (),
            {
                "get_bookmark_queue_counts": lambda self: {
                    "pending": 3,
                    "processing": 1,
                    "processed": 20,
                    "failed": 2,
                }
            },
        )(),
    )

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/stats")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_bookmarks"] == 2
    assert payload["realtime_counts"] == {
        "total": 2,
        "processed": 1,
        "pending": 1,
    }
    assert payload["queue_counts"]["processed"] == 20
    assert payload["queue_total"] == 26
