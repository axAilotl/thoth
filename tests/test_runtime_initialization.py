"""Isolated regression tests for runtime initialization and queue health.

These tests use temporary state and never touch the operator vault or start
connectors, providers, or background processing.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

import thoth_api
from core import config
from core.config import Config
from core.ingestion_runtime import (
    IngestionDispatchResult,
    KnowledgeArtifactRuntime,
    clear_knowledge_artifact_runtime,
    get_knowledge_artifact_runtime,
    get_knowledge_artifact_runtime_health,
)
from core.metadata_db import MetadataDB, clear_metadata_db, get_metadata_db
from core.path_layout import build_path_layout
from core.runtime_composition import (
    resolve_runtime_database,
    reset_runtime_database,
)


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


@pytest.fixture(autouse=True)
def reset_global_db():
    reset_runtime_database()
    clear_knowledge_artifact_runtime()
    yield
    reset_runtime_database()
    clear_knowledge_artifact_runtime()


def _configure_runtime_paths(tmp_path: Path) -> None:
    """Point runtime paths at a temporary tree, mirroring other ingestion tests."""
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")


def _patch_background_tasks(monkeypatch, module: Any) -> None:
    """Prevent the API from starting background processing during tests."""

    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(module, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(module, "background_processor", noop_async)
    monkeypatch.setattr(module, "ingestion_worker", noop_async)
    monkeypatch.setattr(module, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(module, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(module, "archivist_scheduler", noop_async)
    monkeypatch.setattr(module, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(module, "resolve_x_api_sync_config", lambda: None)
    module._shutdown_event = asyncio.Event()


def test_importing_metadata_db_does_not_create_state(tmp_path: Path, monkeypatch):
    """Importing the metadata database module must not create files or dirs."""
    monkeypatch.chdir(tmp_path)
    # Run the import in a subprocess so a fresh module load does not mutate
    # the test process's cached runtime state.
    import subprocess

    project_root = Path(__file__).parent.parent
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import core.metadata_db",
        ],
        cwd=str(tmp_path),
        check=False,
        capture_output=True,
        text=True,
        env={**dict(__import__("os").environ), "PYTHONPATH": str(project_root)},
    )
    assert result.returncode == 0, result.stderr

    assert not (tmp_path / ".thoth_system").exists()
    assert not list(tmp_path.glob("**/meta.db"))


def test_importing_metadata_db_and_thoth_api_does_not_create_state(
    tmp_path: Path, monkeypatch
):
    """Importing the runtime modules must not create files, dirs, or caches."""
    monkeypatch.chdir(tmp_path)
    import subprocess

    project_root = Path(__file__).parent.parent
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import core.metadata_db; import thoth_api",
        ],
        cwd=str(tmp_path),
        check=False,
        capture_output=True,
        text=True,
        env={**dict(__import__("os").environ), "PYTHONPATH": str(project_root)},
    )
    assert result.returncode == 0, result.stderr

    assert not (tmp_path / ".thoth_system").exists()
    assert not list(tmp_path.glob("**/meta.db"))
    assert not list(tmp_path.glob("**/llm_cache"))


def test_default_database_path_resolves_to_single_system_dir(
    tmp_path: Path, monkeypatch
):
    """Tracked defaults must resolve to exactly one .thoth_system directory."""
    monkeypatch.chdir(tmp_path)
    cfg = Config()
    cfg.reload([str(Path(__file__).parent.parent / "config.example.json")])

    layout = build_path_layout(cfg)

    assert layout.database_path == tmp_path / ".thoth_system" / "meta.db"
    assert not (tmp_path / ".thoth_system" / ".thoth_system").exists()


def test_composition_root_injects_same_db_into_runtime(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """The composition root must construct one DB used by runtime and helpers."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    db = resolve_runtime_database(config)
    from_db = get_metadata_db()
    runtime = get_knowledge_artifact_runtime(config, db=from_db)

    assert db is from_db
    assert runtime.db is db
    assert db.db_path == build_path_layout(config).database_path
    assert db.db_path == tmp_path / ".thoth_system" / "meta.db"


def test_resolve_runtime_database_rejects_mismatched_existing_path(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A registered DB at a different canonical path must fail closed."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    first_db = resolve_runtime_database(config)

    other_config = Config()
    other_config.data = deepcopy(config.data)
    other_config.set("database.path", "other_meta.db")
    other_layout = build_path_layout(other_config)

    with pytest.raises(RuntimeError, match="Refusing to reuse metadata database"):
        resolve_runtime_database(other_config, layout=other_layout)

    # The originally registered DB is unchanged.
    assert get_metadata_db() is first_db


@pytest.mark.anyio
async def test_queue_read_error_raises_and_worker_records_unhealthy(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A pending-ingestion read error must not look like an idle queue."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))
    runtime = get_knowledge_artifact_runtime(config, layout=layout, db=db)

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("simulated queue read failure")

    monkeypatch.setattr(db, "get_pending_ingestions", boom)

    with pytest.raises(sqlite3.OperationalError):
        await runtime.process_pending_ingestions_once()

    shutdown = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.05)
        shutdown.set()

    asyncio.create_task(stop_soon())
    await runtime.run_background(shutdown, poll_interval_seconds=0.1)

    health = runtime.worker_health
    assert health["healthy"] is False
    assert health["consecutive_failures"] >= 1
    assert "simulated queue read failure" in (health["last_error"] or "")
    global_health = get_knowledge_artifact_runtime_health()
    assert global_health["healthy"] is False


@pytest.mark.anyio
async def test_worker_health_recovers_after_successful_empty_poll(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A successful empty queue poll must reset consecutive failures."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))
    runtime = get_knowledge_artifact_runtime(config, layout=layout, db=db)

    calls: list[str] = []

    async def fake_poll(*args, **kwargs):
        calls.append("poll")
        if len(calls) == 1:
            raise sqlite3.OperationalError("simulated queue read failure")
        # Successful empty poll recovers health; stop deterministically.
        shutdown.set()
        return []

    monkeypatch.setattr(runtime, "process_pending_ingestions_once", fake_poll)

    shutdown = asyncio.Event()
    await runtime.run_background(shutdown, poll_interval_seconds=0.1)

    health = runtime.worker_health
    assert health["healthy"] is True
    assert health["consecutive_failures"] == 0
    assert health["last_error"] is None


@pytest.mark.anyio
async def test_worker_health_recovers_after_successful_nonempty_poll(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A successful nonempty queue poll must reset consecutive failures."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))
    runtime = get_knowledge_artifact_runtime(config, layout=layout, db=db)

    calls: list[str] = []

    async def fake_poll(*args, **kwargs):
        calls.append("poll")
        if len(calls) == 1:
            raise sqlite3.OperationalError("simulated queue read failure")
        # Successful nonempty poll recovers health; stop deterministically.
        shutdown.set()
        return [
            IngestionDispatchResult(
                artifact_id="a-1",
                artifact_type="markdown",
                source="test",
                status="processed",
                processed_at="now",
            )
        ]

    monkeypatch.setattr(runtime, "process_pending_ingestions_once", fake_poll)

    shutdown = asyncio.Event()
    await runtime.run_background(shutdown, poll_interval_seconds=0.1)

    health = runtime.worker_health
    assert health["healthy"] is True
    assert health["consecutive_failures"] == 0
    assert health["last_error"] is None


def test_cli_uses_composition_root(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """The CLI composition root must register the same DB the ingest-queue path uses."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    # Resolve the canonical database through the composition root, as main() does.
    composed_db = resolve_runtime_database(config)

    # Simulate the CLI ingest-queue handler constructing a runtime with the global DB.
    runtime = get_knowledge_artifact_runtime(config, db=get_metadata_db())

    assert runtime.db is composed_db
    assert runtime.db.db_path == tmp_path / ".thoth_system" / "meta.db"


def test_get_knowledge_artifact_runtime_is_stable_and_rejects_mismatches(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """The runtime singleton must be stable and reject mismatched construction args."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))
    runtime = get_knowledge_artifact_runtime(config, layout=layout, db=db)

    # No-args lookup returns the same instance.
    assert get_knowledge_artifact_runtime() is runtime

    # Equivalent explicit args return the same instance.
    assert get_knowledge_artifact_runtime(config, layout=layout, db=db) is runtime

    # A deep-copied equivalent config also returns the same instance.
    equivalent_config = Config()
    equivalent_config.data = deepcopy(config.data)
    assert get_knowledge_artifact_runtime(equivalent_config, layout=layout, db=db) is runtime

    # A mismatched database must fail closed.
    other_db = MetadataDB(str(tmp_path / "other_meta.db"))
    with pytest.raises(RuntimeError, match="Mismatched knowledge artifact runtime"):
        get_knowledge_artifact_runtime(config, layout=layout, db=other_db)

    # A mismatched layout (different vault root) must fail closed.
    other_config = Config()
    other_config.data = deepcopy(config.data)
    other_config.set("paths.vault_dir", str(tmp_path / "other_vault"))
    other_layout = build_path_layout(other_config)
    with pytest.raises(RuntimeError, match="Mismatched knowledge artifact runtime"):
        get_knowledge_artifact_runtime(config, layout=other_layout, db=db)


def test_uninitialized_runtime_health_is_explicitly_unhealthy(
    tmp_path: Path, monkeypatch, reset_global_db
):
    """Before initialization the runtime must report unhealthy, not fabricated healthy."""
    monkeypatch.chdir(tmp_path)
    health = get_knowledge_artifact_runtime_health()
    assert health["healthy"] is False
    assert health["state"] == "uninitialized"
    assert "not been initialized" in health["reason"]


@pytest.mark.anyio
async def test_api_health_endpoint_cannot_mask_unhealthy_worker(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """Bookmark/request health lookup must surface an unhealthy worker, not hide it."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    import thoth_api

    _patch_background_tasks(monkeypatch, thoth_api)
    await thoth_api.startup_event()

    runtime = get_knowledge_artifact_runtime()
    runtime._record_worker_failure(RuntimeError("simulated worker failure"))

    # The worker and bookmark paths use the same singleton instance.
    assert thoth_api.get_knowledge_artifact_runtime() is runtime

    # The health endpoint must report unhealthy.
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        await thoth_api.health_check()
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["ingestion_worker"]["healthy"] is False
    assert "simulated worker failure" in (
        exc_info.value.detail["ingestion_worker"].get("last_error") or ""
    )

    if thoth_api._shutdown_event is not None:
        thoth_api._shutdown_event.set()


@pytest.mark.anyio
async def test_api_startup_registers_database(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """API startup must register the runtime database before starting workers."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    import thoth_api

    _patch_background_tasks(monkeypatch, thoth_api)

    # Simulate FastAPI startup. It calls resolve_runtime_database(config).
    await thoth_api.startup_event()

    db = get_metadata_db()
    assert db.db_path == tmp_path / ".thoth_system" / "meta.db"

    # Startup must also create the shared runtime instance.
    assert get_knowledge_artifact_runtime() is not None

    # Cleanup event state left by the startup handler.
    if thoth_api._shutdown_event is not None:
        thoth_api._shutdown_event.set()


def test_knowledge_artifact_runtime_rejects_mismatched_db_path_on_first_construction(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """First construction must fail closed when an explicit DB points elsewhere."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    other_db = MetadataDB(str(tmp_path / "other_meta.db"))

    with pytest.raises(RuntimeError, match="does not match"):
        KnowledgeArtifactRuntime(config, layout=layout, db=other_db)


@pytest.mark.anyio
async def test_api_lifecycle_binds_fresh_database_per_startup(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """Shutdown must clear singleton state so a second startup binds its own DB."""
    import thoth_api

    _patch_background_tasks(monkeypatch, thoth_api)

    # First lifecycle against tmp_path / "a".
    path_a = tmp_path / "a"
    path_a.mkdir()
    monkeypatch.chdir(path_a)
    config.data = {}
    config.set("paths.vault_dir", str(path_a / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")

    await thoth_api.startup_event()
    db_a = get_metadata_db()
    runtime_a = get_knowledge_artifact_runtime()
    assert Path(db_a.db_path).resolve() == (path_a / ".thoth_system" / "meta.db").resolve()
    assert runtime_a.layout.database_path == path_a / ".thoth_system" / "meta.db"
    await thoth_api.shutdown_event()

    # Second lifecycle against tmp_path / "b".
    path_b = tmp_path / "b"
    path_b.mkdir()
    monkeypatch.chdir(path_b)
    config.data = {}
    config.set("paths.vault_dir", str(path_b / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")

    await thoth_api.startup_event()
    db_b = get_metadata_db()
    runtime_b = get_knowledge_artifact_runtime()
    assert Path(db_b.db_path).resolve() == (path_b / ".thoth_system" / "meta.db").resolve()
    assert runtime_b.layout.database_path == path_b / ".thoth_system" / "meta.db"
    assert db_b is not db_a
    assert runtime_b is not runtime_a
    await thoth_api.shutdown_event()


@pytest.mark.anyio
async def test_api_shutdown_tears_down_runtime_when_background_task_failed(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A failed background task must surface without leaking runtime singletons."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    import thoth_api

    layout = build_path_layout(config)
    runtime_db = resolve_runtime_database(config, layout=layout)
    get_knowledge_artifact_runtime(config, layout=layout, db=runtime_db)

    async def failed_task():
        raise RuntimeError("simulated background failure")

    thoth_api._shutdown_event = asyncio.Event()
    thoth_api._background_task = asyncio.create_task(failed_task())
    thoth_api._ingestion_task = None
    thoth_api._social_sync_task = None
    thoth_api._x_api_sync_task = None
    thoth_api._archivist_task = None
    await asyncio.sleep(0)

    with pytest.raises(ExceptionGroup, match="API background task shutdown failed"):
        await thoth_api.shutdown_event()

    with pytest.raises(RuntimeError, match="No metadata database"):
        get_metadata_db()
    assert get_knowledge_artifact_runtime_health()["state"] == "uninitialized"
    assert thoth_api._background_task is None


def test_save_bookmark_writes_to_temp_path(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """save_bookmark must write to the configured realtime bookmarks file."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    is_new, saved = thoth_api.save_bookmark(
        {"tweet_id": "1234567890", "source": "test"}
    )

    assert is_new is True
    bookmarks_file = thoth_api.get_realtime_bookmarks_file()
    assert bookmarks_file.exists()
    assert bookmarks_file == tmp_path / ".thoth_system" / "realtime_bookmarks.json"
    data = json.loads(bookmarks_file.read_text(encoding="utf-8"))
    assert len(data) == 1
    assert data[0]["tweet_id"] == "1234567890"


@pytest.mark.anyio
async def test_mutate_realtime_bookmarks_mark_processed(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """mutate_realtime_bookmarks must persist processed flag changes."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    thoth_api.save_bookmark({"tweet_id": "1234567890", "source": "test"})

    def mark_processed(bookmarks):
        for entry in bookmarks:
            if entry.get("tweet_id") == "1234567890":
                if not entry.get("processed"):
                    entry["processed"] = True
                    return True, None
        return False, None

    await thoth_api.mutate_realtime_bookmarks(mark_processed)

    bookmarks = thoth_api.load_realtime_bookmarks()
    assert bookmarks[0]["processed"] is True


@pytest.mark.anyio
async def test_ingest_bookmark_capture_persists_to_temp_paths(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """ingest_bookmark_capture must write the queue entry and realtime bookmark."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    db = resolve_runtime_database(config, layout=layout)
    get_knowledge_artifact_runtime(config, layout=layout, db=db)

    await thoth_api.ingest_bookmark_capture(
        {"tweet_id": "1234567890", "source": "test"},
        process_immediately=False,
        queue_bookmark=True,
        reset_processed=True,
    )

    entry = db.get_bookmark_entry("1234567890")
    assert entry is not None
    assert entry.status == "pending"
    bookmarks = thoth_api.load_realtime_bookmarks()
    assert any(b["tweet_id"] == "1234567890" for b in bookmarks)


def test_resolve_runtime_database_rejects_legacy_doubled_database(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A legacy doubled-path database must not be silently reused or abandoned."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    legacy_path = layout.system_root / layout.system_root.name / "meta.db"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_path.write_bytes(b"")

    assert not layout.database_path.exists()
    with pytest.raises(RuntimeError, match="Legacy metadata database found"):
        resolve_runtime_database(config, layout=layout)


def test_resolve_runtime_database_allows_absent_legacy(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """When no legacy database exists, the composition root creates the canonical DB."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    legacy_path = layout.system_root / layout.system_root.name / "meta.db"
    assert not legacy_path.exists()
    assert not layout.database_path.exists()

    db = resolve_runtime_database(config, layout=layout)

    assert db is get_metadata_db()
    assert layout.database_path.exists()
    assert not legacy_path.exists()


def test_validate_metadata_db_matches_layout_fails_before_directory_creation(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """A mismatched DB/layout must fail before any directories are created."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    other_layout = build_path_layout(config, project_root=tmp_path / "other")
    other_db = MetadataDB(str(other_layout.database_path))

    with pytest.raises(RuntimeError, match="does not match"):
        KnowledgeArtifactRuntime(config, layout=layout, db=other_db)

    assert not layout.system_root.exists()


def test_open_api_capture_surface_uses_registered_runtime(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """open_api_capture_surface must pass the exact registered runtime layout/db."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)
    db = resolve_runtime_database(config, layout=layout)
    runtime = get_knowledge_artifact_runtime(config, layout=layout, db=db)

    captured = {}

    @contextmanager
    def fake_open_capture_surface(runtime_config, *, layout, db):
        captured["layout"] = layout
        captured["db"] = db
        yield type("FakeSurface", (), {"list_sources": lambda self: []})()

    monkeypatch.setattr(thoth_api, "open_capture_surface", fake_open_capture_surface)

    with thoth_api.open_api_capture_surface() as surface:
        surface.list_sources()

    assert captured["layout"] is runtime.layout
    assert captured["db"] is runtime.db


def test_agent_surface_endpoints_use_registered_runtime(
    tmp_path: Path, monkeypatch, restore_runtime_config, reset_global_db
):
    """AgentSurfaceService endpoints must receive the registered runtime layout/db."""
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    import thoth_api

    _patch_background_tasks(monkeypatch, thoth_api)
    awaitable = thoth_api.startup_event()
    if asyncio.iscoroutine(awaitable):
        asyncio.run(awaitable)

    runtime = get_knowledge_artifact_runtime()
    captured = {}

    class FakeAgentSurfaceService:
        def __init__(self, runtime_config, *, layout, db, event_store=None):
            captured["layout"] = layout
            captured["db"] = db

        def list_connector_runs(self, **kwargs):
            return {"runs": [], "checkpoints": [], "total": 0}

    monkeypatch.setattr(thoth_api, "AgentSurfaceService", FakeAgentSurfaceService)

    from fastapi.testclient import TestClient

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/connectors/runs")

    assert response.status_code == 200
    assert captured["layout"] is runtime.layout
    assert captured["db"] is runtime.db

    if thoth_api._shutdown_event is not None:
        thoth_api._shutdown_event.set()
