from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import thoth
import thoth_api
from core.bookmark_ingest import (
    build_realtime_bookmark_record,
    merge_realtime_bookmark_record,
)
from core import XApiBookmarkSyncConfigError
from core.config import Config
from core.non_live_state import MIN_NON_LIVE_INTERVAL_HOURS


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
    config.set("automation.x_api_sync.enabled", True)
    config.set("automation.x_api_sync.interval_hours", 6)
    config.set("automation.x_api_sync.run_on_startup", False)
    config.set("automation.x_api_sync.max_results", 100)
    config.set("automation.x_api_sync.max_pages", 3)
    config.set("automation.x_api_sync.resume_from_checkpoint", True)
    return config


@pytest.fixture
def restore_thoth_config():
    original = deepcopy(thoth_api.config.data)
    yield
    thoth_api.config.data = original


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_run_x_api_bookmark_sync_enqueues_or_processes(
    tmp_path: Path,
    restore_thoth_config,
    monkeypatch,
):
    config = make_config(tmp_path)
    thoth_api.config.data = deepcopy(config.data)

    calls: list[tuple[str, str]] = []

    async def fake_backfill(*args, **kwargs):
        assert args[0] is thoth_api.config
        return {
            "status": "ok",
            "user_id": "42",
            "pages_fetched": 1,
            "bookmarks_emitted": 2,
            "stopped_at_known_id": False,
            "checkpoint": {"user_id": "42"},
            "payloads": [
                {"tweet_id": "100", "source": "x_api_backfill"},
                {"tweet_id": "101", "source": "x_api_backfill"},
            ],
        }

    def record_upsert(payload):
        calls.append(("upsert", payload["tweet_id"]))

    async def record_enqueue(payload, delay_seconds: float = 0.0):
        calls.append(("enqueue", payload["tweet_id"]))

    async def record_process(payload):
        calls.append(("process", payload["tweet_id"]))

    async def record_realtime_mutation(mutator):
        return None

    monkeypatch.setattr(thoth_api, "run_x_api_bookmark_backfill", fake_backfill)
    monkeypatch.setattr(thoth_api, "upsert_bookmark_queue_entry", record_upsert)
    monkeypatch.setattr(thoth_api, "enqueue_bookmark_payload", record_enqueue)
    monkeypatch.setattr(thoth_api, "process_bookmark_async", record_process)
    monkeypatch.setattr(thoth_api, "mutate_realtime_bookmarks", record_realtime_mutation)
    monkeypatch.setattr(
        thoth_api,
        "resolve_x_api_sync_config",
        lambda: {
            "enabled": True,
            "interval_hours": 6,
            "run_on_startup": False,
            "max_results": 100,
            "max_pages": 3,
            "resume_from_checkpoint": True,
        },
    )

    queued_result = await thoth_api.run_x_api_bookmark_sync(
        max_results=50,
        max_pages=2,
        resume_from_checkpoint=False,
        process_immediately=False,
    )
    assert queued_result["queued"] == 2
    assert queued_result["processed_immediately"] == 0
    assert calls == [
        ("upsert", "100"),
        ("enqueue", "100"),
        ("upsert", "101"),
        ("enqueue", "101"),
    ]

    calls.clear()
    processed_result = await thoth_api.run_x_api_bookmark_sync(
        max_results=50,
        max_pages=2,
        resume_from_checkpoint=False,
        process_immediately=True,
    )
    assert processed_result["queued"] == 2
    assert processed_result["processed_immediately"] == 2
    assert calls == [
        ("upsert", "100"),
        ("process", "100"),
        ("upsert", "101"),
        ("process", "101"),
    ]


def test_resolve_x_api_sync_config_requires_valid_config(tmp_path: Path, restore_thoth_config):
    config = make_config(tmp_path)
    thoth_api.config.data = deepcopy(config.data)

    resolved = thoth_api.resolve_x_api_sync_config()
    assert resolved["enabled"] is True
    assert resolved["interval_hours"] == 6.0
    assert resolved["max_results"] == 100
    assert resolved["max_pages"] == 3

    thoth_api.config.set("automation.x_api_sync.interval_hours", 0)
    with pytest.raises(XApiBookmarkSyncConfigError):
        thoth_api.resolve_x_api_sync_config()


def test_resolve_x_api_sync_config_defaults_run_on_startup_false(
    tmp_path: Path,
    restore_thoth_config,
):
    config = make_config(tmp_path)
    config.data["automation"]["x_api_sync"].pop("run_on_startup", None)
    thoth_api.config.data = deepcopy(config.data)

    resolved = thoth_api.resolve_x_api_sync_config()

    assert resolved["run_on_startup"] is False


def test_resolve_social_sync_config_enforces_non_live_minimum(
    tmp_path: Path,
    restore_thoth_config,
):
    config = make_config(tmp_path)
    config.set("automation.social_sync.enabled", True)
    config.set("automation.social_sync.interval_hours", MIN_NON_LIVE_INTERVAL_HOURS)
    thoth_api.config.data = deepcopy(config.data)

    resolved = thoth_api.resolve_social_sync_config()
    assert resolved["interval_hours"] == MIN_NON_LIVE_INTERVAL_HOURS
    assert resolved["run_on_startup"] is False

    thoth_api.config.set(
        "automation.social_sync.interval_hours",
        MIN_NON_LIVE_INTERVAL_HOURS - 1,
    )
    with pytest.raises(ValueError):
        thoth_api.resolve_social_sync_config()


def test_x_api_bookmark_sync_route_wires_into_fastapi(restore_thoth_config, monkeypatch):
    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
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

    async def fake_run_x_api_bookmark_sync(*args, **kwargs):
        return {
            "status": "ok",
            "sync_config": {
                "enabled": False,
                "interval_hours": 6,
                "run_on_startup": False,
                "max_results": 100,
                "max_pages": 3,
                "resume_from_checkpoint": True,
            },
            "queued": 2,
            "processed_immediately": 0,
            "user_id": "42",
            "pages_fetched": 1,
            "bookmarks_emitted": 2,
            "stopped_at_known_id": False,
            "checkpoint": {"user_id": "42"},
        }

    monkeypatch.setattr(thoth_api, "run_x_api_bookmark_sync", fake_run_x_api_bookmark_sync)

    with TestClient(thoth_api.app) as client:
        response = client.post(
            "/api/x-api/bookmarks/sync",
            json={
                "max_results": 25,
                "max_pages": 2,
                "resume_from_checkpoint": False,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["queued"] == 2
    assert payload["backfill"]["user_id"] == "42"


def test_merge_realtime_bookmark_record_dedupes_and_preserves_existing_metadata():
    bookmarks = [
        {
            "tweet_id": "100",
            "tweet_data": {"text": "old"},
            "source": "browser_extension",
            "timestamp": "2026-04-04T12:00:00",
            "processed": True,
        }
    ]
    record = build_realtime_bookmark_record(
        {
            "tweet_id": "100",
            "tweet_data": {"text": "new"},
            "source": "x_api_backfill",
            "timestamp": "2026-04-04T12:30:45",
            "graphql_cache_file": "tweet_100.json",
        }
    )

    dirty = merge_realtime_bookmark_record(bookmarks, record, reset_processed=True)

    assert dirty is True
    assert bookmarks == [
        {
            "tweet_id": "100",
            "tweet_data": {"text": "new"},
            "source": "browser_extension",
            "timestamp": "2026-04-04T12:00:00",
            "processed": False,
            "graphql_cache_file": "tweet_100.json",
        }
    ]


@pytest.mark.anyio
async def test_receive_bookmark_uses_shared_ingest_helper(monkeypatch):
    calls = []

    async def fake_ingest(bookmark_data, **kwargs):
        calls.append({"bookmark_data": bookmark_data, "kwargs": kwargs})
        return {"tweet_id": bookmark_data["tweet_id"]}

    monkeypatch.setattr(thoth_api, "ingest_bookmark_capture", fake_ingest)

    response = await thoth_api.receive_bookmark(
        thoth_api.BookmarkCapture(
            tweet_id="100",
            graphql_response={"data": {"id": "100"}},
            source="userscript_fetch",
            timestamp="2026-04-04T12:30:45",
        )
    )

    assert response.status == "accepted"
    assert calls == [
        {
            "bookmark_data": {
                "tweet_id": "100",
                "tweet_data": None,
                "graphql_response": {"data": {"id": "100"}},
                "timestamp": "2026-04-04T12:30:45",
                "source": "userscript_fetch",
                "force": False,
            },
            "kwargs": {
                "graphql_response": {"data": {"id": "100"}},
                "process_immediately": False,
                "queue_bookmark": True,
                "reset_processed": True,
                "force": True,
            },
        }
    ]


@pytest.mark.anyio
async def test_process_bookmark_async_keeps_resume_guards_enabled(monkeypatch):
    calls = []

    class FakeDB:
        def mark_bookmark_processing(self, tweet_id):
            calls.append(("processing", tweet_id))
            return True

        def mark_bookmark_processed(self, tweet_id, with_graphql=False):
            calls.append(("processed", tweet_id, with_graphql))
            return True

        def mark_bookmark_failed(self, tweet_id, error):
            calls.append(("failed", tweet_id, error))
            return None

    class FakeRuntime:
        async def process_bookmark_payload(self, payload, **kwargs):
            calls.append(("runtime", payload["tweet_id"], kwargs))
            return SimpleNamespace(
                pipeline_result=SimpleNamespace(processed_tweets=1),
                tweet_count=1,
            )

    async def fake_mutate_realtime_bookmarks(mutator):
        return None

    monkeypatch.setattr(thoth_api, "get_metadata_db", lambda: FakeDB())
    monkeypatch.setattr(thoth_api, "get_knowledge_artifact_runtime", lambda *args, **kwargs: FakeRuntime())
    monkeypatch.setattr(thoth_api, "mutate_realtime_bookmarks", fake_mutate_realtime_bookmarks)

    await thoth_api.process_bookmark_async(
        {
            "tweet_id": "100",
            "source": "userscript_fetch",
            "force": True,
        }
    )

    assert ("runtime", "100", {"resume": True}) in calls


@pytest.mark.anyio
async def test_run_x_api_bookmark_sync_uses_shared_ingest_helper(
    tmp_path: Path,
    restore_thoth_config,
    monkeypatch,
):
    config = make_config(tmp_path)
    thoth_api.config.data = deepcopy(config.data)

    calls = []

    async def fake_backfill(*args, **kwargs):
        return {
            "status": "ok",
            "user_id": "42",
            "pages_fetched": 1,
            "bookmarks_emitted": 1,
            "stopped_at_known_id": False,
            "checkpoint": {"user_id": "42"},
            "payloads": [
                {"tweet_id": "100", "source": "x_api_backfill"},
            ],
        }

    async def fake_ingest(bookmark_data, **kwargs):
        calls.append({"bookmark_data": bookmark_data, "kwargs": kwargs})
        return bookmark_data

    monkeypatch.setattr(thoth_api, "run_x_api_bookmark_backfill", fake_backfill)
    monkeypatch.setattr(thoth_api, "ingest_bookmark_capture", fake_ingest)
    monkeypatch.setattr(
        thoth_api,
        "resolve_x_api_sync_config",
        lambda: {
            "enabled": True,
            "interval_hours": 6,
            "run_on_startup": False,
            "max_results": 100,
            "max_pages": 3,
            "resume_from_checkpoint": True,
        },
    )

    result = await thoth_api.run_x_api_bookmark_sync(
        max_results=50,
        max_pages=2,
        resume_from_checkpoint=False,
        process_immediately=True,
    )

    assert result["queued"] == 1
    assert result["processed_immediately"] == 1
    assert calls == [
        {
            "bookmark_data": {"tweet_id": "100", "source": "x_api_backfill"},
            "kwargs": {
                "process_immediately": True,
                "queue_bookmark": True,
                "reset_processed": True,
            },
        }
    ]


@pytest.mark.anyio
async def test_x_api_cli_command_wires_shared_runner(monkeypatch):
    calls = []

    async def fake_run_x_api_bookmark_sync(*, max_results, max_pages, resume_from_checkpoint, process_immediately):
        calls.append(
            {
                "max_results": max_results,
                "max_pages": max_pages,
                "resume_from_checkpoint": resume_from_checkpoint,
                "process_immediately": process_immediately,
            }
        )
        return {
            "user_id": "42",
            "pages_fetched": 1,
            "bookmarks_emitted": 1,
            "queued": 1,
            "processed_immediately": 1,
            "stopped_at_known_id": False,
        }

    monkeypatch.setattr("thoth_api.run_x_api_bookmark_sync", fake_run_x_api_bookmark_sync)

    args = SimpleNamespace(max_results=50, max_pages=4, no_resume=True)
    await thoth.cmd_x_api_sync(args)

    assert calls == [
        {
            "max_results": 50,
            "max_pages": 4,
            "resume_from_checkpoint": False,
            "process_immediately": True,
        }
    ]
