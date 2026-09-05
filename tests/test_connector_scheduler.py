import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from jsonschema import Draft7Validator

from core.agent_surface import AgentSurfaceService
from core.config import Config
from core.connector_runners import _RUN_ADAPTERS
from core.connector_scheduler import ConnectorScheduler, resolve_connector_schedules
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def environment(tmp_path):
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / "system"))
    for name in ("cache", "raw", "library", "wiki", "digests"):
        config.set(f"paths.{name}_dir", name)
    config.set("database.path", "meta.db")
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)
    now = [datetime(2026, 9, 4, tzinfo=timezone.utc)]
    return SimpleNamespace(
        config=config, db=db, service=service, now=now,
        scheduler=ConnectorScheduler(service, clock=lambda: now[0]),
        stop=asyncio.Event(),
    )


def enable(env, **overrides):
    env.config.set("sources.web_clipper.schedule", {
        "enabled": True, "interval_seconds": 60, "run_on_startup": True,
        "options": {"limit": 20}, **overrides,
    })


def state(env):
    return env.db.get_automation_state("non_live_scheduler:connector:web_clipper")


def adapter(monkeypatch, handler):
    monkeypatch.setitem(_RUN_ADAPTERS, "collectors.web_clipper_collector:WebClipperCollector", ("class", handler))


@pytest.mark.anyio
async def test_disabled_default_does_not_collect_or_create_state(environment, monkeypatch):
    env = environment
    adapter(monkeypatch, lambda *_: pytest.fail("disabled source executed"))
    await env.scheduler.tick(env.stop)
    assert state(env) is None
    enable(env, enabled=False)
    await env.scheduler.tick(env.stop)
    assert state(env) is None
    enable(env)
    env.config.set("sources.web_clipper.enabled", False)
    await env.scheduler.tick(env.stop)
    assert state(env) is None


@pytest.mark.anyio
async def test_due_run_offloads_preserves_options_history_and_restart_cadence(environment, monkeypatch):
    env = environment
    enable(env)
    calls = []
    def collect(context, entrypoint, options):
        calls.append((dict(options), threading.get_ident()))
        return {"queued_count": 0}
    adapter(monkeypatch, collect)
    await env.scheduler.tick(env.stop)
    assert calls == [({"limit": 20}, calls[0][1])]
    assert calls[0][1] != threading.get_ident()
    assert state(env)["last_success"] is True
    assert env.service.list_connector_runs(connector_name="web_clipper")["runs"][0]["status"] == "completed"
    # A reconstructed scheduler must respect the previous attempt, even with
    # run_on_startup enabled.
    restarted = ConnectorScheduler(env.service, clock=lambda: env.now[0])
    await restarted.tick(env.stop)
    assert len(calls) == 1
    env.now[0] += timedelta(seconds=60)
    await restarted.tick(env.stop)
    assert len(calls) == 2


@pytest.mark.anyio
async def test_first_delayed_run_is_persisted_across_restart(environment, monkeypatch):
    env = environment
    enable(env, run_on_startup=False)
    calls = []
    adapter(monkeypatch, lambda *_: calls.append(True) or {"queued_count": 0})
    await env.scheduler.tick(env.stop)
    assert "last_attempt_at" not in state(env)
    due = state(env)["next_run_at"]
    env.now[0] += timedelta(seconds=30)
    restarted = ConnectorScheduler(env.service, clock=lambda: env.now[0])
    await restarted.tick(env.stop)
    assert state(env)["next_run_at"] == due
    assert not calls
    env.now[0] += timedelta(seconds=30)
    await restarted.tick(env.stop)
    assert calls == [True]


@pytest.mark.anyio
async def test_failed_run_is_recorded_and_retries_only_after_interval(environment, monkeypatch, caplog):
    env = environment
    enable(env)
    calls = []
    def fail(*_):
        calls.append(True)
        env.now[0] += timedelta(seconds=90)
        raise ValueError("temporary failure")
    adapter(monkeypatch, fail)
    await env.scheduler.tick(env.stop)
    assert state(env)["last_success"] is False
    assert state(env)["last_error"] == "temporary failure"
    assert "Scheduled connector web_clipper failed" in caplog.text
    assert env.service.list_connector_runs(connector_name="web_clipper")["runs"][0]["status"] == "failed"
    await env.scheduler.tick(env.stop)
    assert len(calls) == 1
    env.now[0] += timedelta(seconds=60)
    await env.scheduler.tick(env.stop)
    assert len(calls) == 2


@pytest.mark.anyio
async def test_policy_rejection_records_scheduler_failure_without_connector_side_effects(environment, monkeypatch):
    env = environment
    enable(env)
    env.config.set("connectors.allowlist", ["arxiv"])
    adapter(monkeypatch, lambda *_: pytest.fail("policy bypass"))
    await env.scheduler.tick(env.stop)
    assert state(env)["last_success"] is False
    assert "not allowlisted" in state(env)["last_error"]


@pytest.mark.parametrize("schedule", [
    {"enabled": "true"}, {"enabled": True}, {"enabled": True, "interval_seconds": 0},
    {"enabled": True, "interval_seconds": True},
    {"interval_seconds": 60, "interval_hours": 1},
    {"interval_seconds": float("nan")}, {"interval_seconds": float("inf")},
    {"options": []}, {"run_on_startup": 1}, {"enabeld": True},
])
def test_invalid_schedule_fails_closed(environment, schedule):
    environment.config.set("sources.web_clipper.schedule", schedule)
    with pytest.raises(ValueError):
        resolve_connector_schedules(environment.config)


def test_unknown_scheduled_source_fails_closed(environment):
    environment.config.set("sources.typo.schedule", {"enabled": True, "interval_seconds": 60})
    with pytest.raises(ValueError, match="no registered connector"):
        resolve_connector_schedules(environment.config)


def test_hours_schedule_and_incomplete_attempt_preserve_cadence(environment):
    env = environment
    env.config.set("sources.web_clipper.schedule", {
        "enabled": True, "interval_hours": 1, "run_on_startup": True,
    })
    schedule = resolve_connector_schedules(env.config)[0]
    assert schedule.interval_seconds == 3600
    env.db.upsert_automation_state(schedule.state_key, {
        "last_attempt_at": env.now[0].isoformat(),
        "next_run_at": env.now[0].isoformat(),
    })
    assert env.scheduler._next_due(schedule, env.now[0]) == env.now[0] + timedelta(hours=1)


@pytest.mark.anyio
async def test_concurrent_ticks_do_not_overlap(environment, monkeypatch):
    env = environment
    enable(env)
    calls = []
    adapter(monkeypatch, lambda *_: calls.append(True) or {"queued_count": 0})
    await asyncio.gather(env.scheduler.tick(env.stop), env.scheduler.tick(env.stop))
    assert calls == [True]


@pytest.mark.anyio
async def test_cancellation_drains_inflight_collector_before_database_teardown(environment, monkeypatch):
    env = environment
    enable(env)
    started = threading.Event()
    release = threading.Event()
    def collect(*_):
        started.set()
        assert release.wait(timeout=5)
        return {"queued_count": 0}
    adapter(monkeypatch, collect)
    task = asyncio.create_task(env.scheduler.run(env.stop))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        env.stop.set()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert state(env)["last_success"] is True


@pytest.mark.anyio
async def test_disabled_loop_wakes_immediately_on_shutdown(environment):
    env = environment
    task = asyncio.create_task(env.scheduler.run(env.stop))
    await asyncio.sleep(0)
    env.stop.set()
    await asyncio.wait_for(task, timeout=1)


def test_schedule_schema_for_builtin_and_plugin_namespaces():
    from pathlib import Path
    schema = json.loads((Path(__file__).parents[1] / "config.schema.json").read_text())
    validator = Draft7Validator(schema["definitions"]["connectorSchedule"])
    assert not list(validator.iter_errors({"enabled": False}))
    assert not list(validator.iter_errors({"enabled": True, "interval_seconds": 60, "options": {"limit": 20}}))
    assert list(validator.iter_errors({"enabled": True}))
    assert list(validator.iter_errors({"interval_seconds": 60, "interval_hours": 1}))
    assert "patternProperties" in schema["properties"]["sources"]
