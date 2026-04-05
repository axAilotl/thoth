"""Durable state helpers for non-live automation jobs and repository probes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Protocol


class AutomationStateStore(Protocol):
    def get_automation_state(self, state_key: str) -> dict[str, Any] | None:
        ...

    def upsert_automation_state(self, state_key: str, payload: dict[str, Any]) -> None:
        ...

MIN_NON_LIVE_INTERVAL_HOURS = 5.0
README_MISS_COOLDOWN_HOURS = 24.0


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if hasattr(value, "isoformat"):
        try:
            text = value.isoformat()
        except Exception:
            text = str(value)
    else:
        text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_revision(value: Any) -> str | None:
    parsed = _coerce_datetime(value)
    if parsed is not None:
        return parsed.isoformat()
    if value in (None, ""):
        return None
    return str(value).strip() or None


def validate_non_live_interval_hours(
    value: Any,
    *,
    field_name: str,
    default: float = 8.0,
) -> float:
    raw_value = default if value in (None, "") else value
    try:
        interval_hours = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number") from exc
    if interval_hours < MIN_NON_LIVE_INTERVAL_HOURS:
        raise ValueError(
            f"{field_name} must be at least {MIN_NON_LIVE_INTERVAL_HOURS:g} hours"
        )
    return interval_hours


def _scheduler_state_key(job_name: str) -> str:
    return f"non_live_scheduler:{job_name}"


def _readme_probe_state_key(provider: str, repo_name: str) -> str:
    return f"readme_probe:{provider}:{repo_name}"


def get_non_live_next_run_at(
    metadata_db: AutomationStateStore,
    *,
    job_name: str,
    interval_hours: float,
    run_on_startup: bool,
    now: datetime | None = None,
) -> datetime:
    current_time = now or _now_utc()
    state = metadata_db.get_automation_state(_scheduler_state_key(job_name)) or {}
    last_attempt_at = _coerce_datetime(state.get("last_attempt_at"))
    if last_attempt_at is None:
        if run_on_startup:
            return current_time
        return current_time + timedelta(hours=interval_hours)

    next_due_at = last_attempt_at + timedelta(hours=interval_hours)
    if next_due_at <= current_time:
        return current_time
    return next_due_at


def mark_non_live_run_started(
    metadata_db: AutomationStateStore,
    *,
    job_name: str,
    interval_hours: float,
    now: datetime | None = None,
) -> None:
    current_time = now or _now_utc()
    state_key = _scheduler_state_key(job_name)
    state = metadata_db.get_automation_state(state_key) or {}
    state.update(
        {
            "job_name": job_name,
            "interval_hours": interval_hours,
            "last_attempt_at": current_time.isoformat(),
        }
    )
    metadata_db.upsert_automation_state(state_key, state)


def mark_non_live_run_finished(
    metadata_db: AutomationStateStore,
    *,
    job_name: str,
    success: bool,
    error: str | None = None,
    now: datetime | None = None,
) -> None:
    current_time = now or _now_utc()
    state_key = _scheduler_state_key(job_name)
    state = metadata_db.get_automation_state(state_key) or {"job_name": job_name}
    state["last_finished_at"] = current_time.isoformat()
    state["last_success"] = bool(success)
    state["last_error"] = None if success else (str(error).strip() or "unknown error")
    metadata_db.upsert_automation_state(state_key, state)


def should_skip_readme_probe(
    metadata_db: AutomationStateStore,
    *,
    provider: str,
    repo_name: str,
    repo_revision: Any,
    cooldown_hours: float = README_MISS_COOLDOWN_HOURS,
    now: datetime | None = None,
) -> bool:
    state = metadata_db.get_automation_state(
        _readme_probe_state_key(provider, repo_name)
    )
    if not state or state.get("status") != "missing":
        return False

    current_time = now or _now_utc()
    normalized_revision = _normalize_revision(repo_revision)
    cached_revision = _normalize_revision(state.get("repo_revision"))
    if normalized_revision and cached_revision and normalized_revision == cached_revision:
        return True
    if normalized_revision and cached_revision and normalized_revision != cached_revision:
        return False

    checked_at = _coerce_datetime(state.get("checked_at"))
    if checked_at is None:
        return False
    return checked_at + timedelta(hours=cooldown_hours) > current_time


def get_known_readme_filename(
    metadata_db: AutomationStateStore,
    *,
    provider: str,
    repo_name: str,
    repo_revision: Any,
) -> str | None:
    state = metadata_db.get_automation_state(
        _readme_probe_state_key(provider, repo_name)
    )
    if not state or state.get("status") != "found":
        return None

    cached_revision = _normalize_revision(state.get("repo_revision"))
    normalized_revision = _normalize_revision(repo_revision)
    if cached_revision and normalized_revision and cached_revision != normalized_revision:
        return None

    filename = str(state.get("filename") or "").strip()
    return filename or None


def record_readme_probe_outcome(
    metadata_db: AutomationStateStore,
    *,
    provider: str,
    repo_name: str,
    repo_revision: Any,
    found: bool,
    filename: str | None = None,
    now: datetime | None = None,
) -> None:
    current_time = now or _now_utc()
    payload = {
        "provider": provider,
        "repo_name": repo_name,
        "repo_revision": _normalize_revision(repo_revision),
        "checked_at": current_time.isoformat(),
        "status": "found" if found else "missing",
    }
    if found and filename:
        payload["filename"] = filename

    metadata_db.upsert_automation_state(
        _readme_probe_state_key(provider, repo_name),
        payload,
    )
