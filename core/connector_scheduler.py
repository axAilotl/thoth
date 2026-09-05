"""Opt-in, serial scheduling over the existing connector execution surface.

Cadence lives in automation_state; outputs, budgets, policy checks and run
history remain owned by AgentSurfaceService. One API process owns this loop.
Shutdown drains any blocking collector before the runtime database is closed.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from .agent_surface import AgentSurfaceService
from .connector_registry import load_connector_registry
from .non_live_state import (
    get_non_live_next_run_at,
    mark_non_live_run_finished,
    mark_non_live_run_started,
)

logger = logging.getLogger(__name__)
MIN_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class ConnectorSchedule:
    connector_name: str
    interval_seconds: float
    run_on_startup: bool
    options: Mapping[str, Any]

    @property
    def job_name(self) -> str:
        return f"connector:{self.connector_name}"

    @property
    def state_key(self) -> str:
        return f"non_live_scheduler:{self.job_name}"


def resolve_connector_schedules(config: Any) -> list[ConnectorSchedule]:
    """Validate every declared schedule, then return explicitly enabled jobs."""
    registry = load_connector_registry(config)
    manifests = registry.list()
    namespaces = {manifest.config_namespace for manifest in manifests}
    sources = config.get("sources", {})
    if not isinstance(sources, Mapping):
        raise ValueError("sources must be an object")
    for source, settings in sources.items():
        if isinstance(settings, Mapping) and "schedule" in settings:
            if f"sources.{source}" not in namespaces:
                raise ValueError(f"sources.{source}.schedule has no registered connector")

    schedules = []
    for manifest in manifests:
        if not manifest.config_namespace:
            continue
        settings = config.get(manifest.config_namespace, {})
        if not isinstance(settings, Mapping):
            raise ValueError(f"{manifest.config_namespace} must be an object")
        if "schedule" not in settings:
            continue
        origin = f"{manifest.config_namespace}.schedule"
        raw = settings["schedule"]
        if not isinstance(raw, Mapping):
            raise ValueError(f"{origin} must be an object")
        unknown = set(raw) - {
            "enabled", "interval_seconds", "interval_hours", "run_on_startup", "options"
        }
        if unknown:
            raise ValueError(f"{origin} has unknown fields: {sorted(unknown)}")
        enabled = raw.get("enabled", False)
        startup = raw.get("run_on_startup", False)
        source_enabled = settings.get("enabled", manifest.default_enabled)
        if not all(isinstance(value, bool) for value in (enabled, startup, source_enabled)):
            raise ValueError(f"{origin} enabled and run_on_startup must be booleans")
        options = raw.get("options", {})
        if not isinstance(options, Mapping):
            raise ValueError(f"{origin}.options must be an object")
        intervals = [key for key in ("interval_seconds", "interval_hours") if key in raw]
        if len(intervals) > 1 or (enabled and not intervals):
            raise ValueError(f"{origin} requires exactly one interval when enabled")
        seconds = MIN_INTERVAL_SECONDS
        if intervals:
            value = raw[intervals[0]]
            if isinstance(value, bool) or not isinstance(value, (float, int)):
                raise ValueError(f"{origin}.{intervals[0]} must be a number")
            seconds = value * (3600 if intervals[0] == "interval_hours" else 1)
            if not math.isfinite(seconds) or seconds < MIN_INTERVAL_SECONDS:
                raise ValueError(f"{origin} interval must be finite and at least 60 seconds")
        if enabled and source_enabled:
            schedules.append(ConnectorSchedule(manifest.name, seconds, startup, dict(options)))
    return schedules


class ConnectorScheduler:
    """A single serialized loop; never cancels a live collector thread."""

    def __init__(
        self,
        service: AgentSurfaceService,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.service = service
        self.clock = clock
        self._lock = asyncio.Lock()

    def _next_due(self, schedule: ConnectorSchedule, now: datetime) -> datetime:
        db = self.service.db
        state = db.get_automation_state(schedule.state_key) or {}
        if state.get("next_run_at"):
            due = datetime.fromisoformat(state["next_run_at"])
            if due.tzinfo is None:
                raise ValueError(f"{schedule.state_key}: next_run_at must have a timezone")
            if state.get("last_attempt_at"):
                # A process can exit after recording the attempt but before
                # reserving next_run_at. Never bypass the durable attempt.
                due = max(due, get_non_live_next_run_at(
                    db, job_name=schedule.job_name,
                    interval_hours=schedule.interval_seconds / 3600,
                    run_on_startup=False, now=now,
                ))
            return due
        due = get_non_live_next_run_at(
            db, job_name=schedule.job_name,
            interval_hours=schedule.interval_seconds / 3600,
            run_on_startup=schedule.run_on_startup, now=now,
        )
        # Persist the first deferred run without claiming an attempt occurred.
        state.update(job_name=schedule.job_name, next_run_at=due.isoformat())
        db.upsert_automation_state(schedule.state_key, state)
        return due

    def _execute(self, schedule: ConnectorSchedule) -> None:
        db = self.service.db
        started = self.clock()
        mark_non_live_run_started(
            db, job_name=schedule.job_name,
            interval_hours=schedule.interval_seconds / 3600, now=started,
        )
        # Reserve cadence before any connector side effects, including crashes.
        state = db.get_automation_state(schedule.state_key)
        state["next_run_at"] = (started + timedelta(seconds=schedule.interval_seconds)).isoformat()
        db.upsert_automation_state(schedule.state_key, state)
        try:
            self.service.run_connector(
                schedule.connector_name, execute=True, options=dict(schedule.options),
            )
        except Exception as exc:
            mark_non_live_run_finished(
                db, job_name=schedule.job_name, success=False,
                error=str(exc) or type(exc).__name__, now=self.clock(),
            )
            logger.exception("Scheduled connector %s failed", schedule.connector_name)
        else:
            mark_non_live_run_finished(
                db, job_name=schedule.job_name, success=True, now=self.clock(),
            )
            logger.info("Scheduled connector %s completed", schedule.connector_name)
        # An expensive failure must not immediately run again when it finishes.
        state = db.get_automation_state(schedule.state_key)
        state["next_run_at"] = (
            self.clock() + timedelta(seconds=schedule.interval_seconds)
        ).isoformat()
        db.upsert_automation_state(schedule.state_key, state)

    async def tick(self, shutdown: asyncio.Event) -> None:
        async with self._lock:
            schedules = resolve_connector_schedules(self.service.config)
            for schedule in schedules:
                if shutdown.is_set():
                    break
                if self._next_due(schedule, self.clock()) > self.clock():
                    continue
                worker = asyncio.create_task(asyncio.to_thread(self._execute, schedule))
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    # Cancelling to_thread doesn't stop its thread. Drain it before
                    # shutdown_event tears down the shared database and services.
                    await worker
                    raise

    async def run(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await self.tick(shutdown)
            except Exception:
                logger.exception("Connector scheduler failed; retrying in 60 seconds")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=MIN_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
