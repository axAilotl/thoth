"""LLM usage and cost observability.

This module records only operational metadata. It intentionally never stores
prompts, completions, embedding text, API keys, or other raw provider payloads.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from typing import Any

from .metadata_db import MetadataDB, get_metadata_db
from .sensitive_redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 8
_MAX_LABEL_LENGTH = 160
_MAX_ERROR_LENGTH = 500
_LLM_USAGE_SCHEMA_READY: set[str] = set()
_LLM_USAGE_SCHEMA_LOCK = threading.Lock()


@dataclass(frozen=True)
class LLMUsageEvent:
    """Sanitized metadata for one LLM usage event."""

    event_id: str
    recorded_at: str
    provider: str
    model: str
    task: str
    operation: str
    status: str
    cache_hit: bool
    source_connector: str | None
    run_id: str | None
    input_tokens_estimate: int
    output_tokens_estimate: int
    total_tokens_estimate: int
    provider_tokens: int | None
    cost_estimate_usd: float
    cost_estimate_source: str
    duration_ms: int | None = None
    redaction_json: str = "{}"
    error: str | None = None

    def to_row(self) -> tuple[Any, ...]:
        return (
            self.event_id,
            self.recorded_at,
            self.provider,
            self.model,
            self.task,
            self.operation,
            self.status,
            1 if self.cache_hit else 0,
            self.source_connector,
            self.run_id,
            self.input_tokens_estimate,
            self.output_tokens_estimate,
            self.total_tokens_estimate,
            self.provider_tokens,
            self.cost_estimate_usd,
            self.cost_estimate_source,
            self.duration_ms,
            self.redaction_json,
            self.error,
        )


def record_llm_usage(
    *,
    provider: str | None,
    model: str | None,
    task: str | None,
    operation: str,
    input_text: str | None = None,
    output_text: str | None = None,
    input_bytes: int | None = None,
    output_bytes: int | None = None,
    provider_tokens: int | None = None,
    provider_cost: float | None = None,
    pricing: Mapping[str, Any] | None = None,
    cache_hit: bool = False,
    source_connector: str | None = None,
    run_id: str | None = None,
    status: str | None = None,
    error: str | None = None,
    redaction_metadata: Mapping[str, Any] | None = None,
    duration_ms: int | None = None,
    db: MetadataDB | None = None,
) -> LLMUsageEvent | None:
    """Persist one sanitized LLM usage event.

    Observability failures are logged and do not interrupt the caller. The row
    contains lengths, estimates, identifiers, and sanitized errors only.
    """

    try:
        metadata_db = db or get_metadata_db()
        ensure_llm_usage_schema_once(metadata_db)
        source_connector, run_id = _resolve_run_context(
            metadata_db,
            source_connector=source_connector,
            run_id=run_id,
        )
        input_tokens = _estimate_tokens(input_text, input_bytes)
        output_tokens = _estimate_tokens(output_text, output_bytes)
        total_tokens = provider_tokens if provider_tokens is not None else input_tokens + output_tokens
        cost, cost_source = _estimate_cost(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            provider_tokens=provider_tokens,
            provider_cost=provider_cost,
            pricing=pricing,
            cache_hit=cache_hit,
        )
        event = LLMUsageEvent(
            event_id=f"llm_usage_{uuid.uuid4().hex}",
            recorded_at=_now_text(),
            provider=_clean_label(provider, default="unknown"),
            model=_clean_label(model, default="unknown"),
            task=_clean_label(task, default="generic"),
            operation=_clean_label(operation, default="generate"),
            status=_clean_label(status or ("cache_hit" if cache_hit else "ok"), default="ok"),
            cache_hit=bool(cache_hit),
            source_connector=_optional_label(source_connector),
            run_id=_optional_label(run_id, max_length=120),
            input_tokens_estimate=input_tokens,
            output_tokens_estimate=output_tokens,
            total_tokens_estimate=max(0, int(total_tokens or 0)),
            provider_tokens=provider_tokens if provider_tokens is not None else None,
            cost_estimate_usd=cost,
            cost_estimate_source=cost_source,
            duration_ms=_optional_nonnegative_int(duration_ms),
            redaction_json=json.dumps(
                dict(redaction_metadata or {}),
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ),
            error=_clean_error(error),
        )
        with metadata_db._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO llm_usage_events (
                    event_id, recorded_at, provider, model, task, operation,
                    status, cache_hit, source_connector, run_id,
                    input_tokens_estimate, output_tokens_estimate,
                    total_tokens_estimate, provider_tokens,
                    cost_estimate_usd, cost_estimate_source, duration_ms,
                    redaction_json, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                event.to_row(),
            )
        return event
    except Exception as exc:
        logger.warning("Failed to record LLM usage metadata: %s", _clean_error(str(exc)))
        return None


def record_llm_cache_hit(
    *,
    task_type: str,
    model_provider: str | None,
    content: str,
    result: Any,
    db: MetadataDB | None = None,
) -> LLMUsageEvent | None:
    """Record an LLM cache hit as a zero-cost usage event."""

    provider, model = split_model_provider(model_provider)
    result_text = json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)
    return record_llm_usage(
        provider=provider,
        model=model,
        task=task_type,
        operation="cache",
        input_text=content,
        output_text=result_text,
        cache_hit=True,
        status="cache_hit",
        db=db,
    )


def build_llm_usage_status(
    db: MetadataDB,
    *,
    recent_limit: int = _DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Return usage totals and recent expensive run aggregates for admin status."""

    try:
        ensure_llm_usage_schema_once(db)
        limit = max(1, int(recent_limit or _DEFAULT_LIMIT))
        with db._get_connection() as conn:
            summary = _summary(conn)
            return {
                "status": "ok",
                **summary,
                "recent_expensive_runs": _recent_expensive_runs(conn, limit=limit),
                "totals_by_source": _totals_by_dimension(conn, "source_connector", limit=limit),
                "totals_by_task": _totals_by_dimension(conn, "task", limit=limit),
            }
    except Exception as exc:
        return {
            "status": "error",
            "error": _clean_error(str(exc)),
            "call_count": 0,
            "cache_hits": 0,
            "total_tokens_estimate": 0,
            "total_cost_estimate_usd": 0.0,
            "recent_expensive_runs": [],
            "totals_by_source": [],
            "totals_by_task": [],
        }


def ensure_llm_usage_schema(db: MetadataDB) -> None:
    """Create the LLM usage event table if needed."""

    with db._get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_usage_events (
                event_id TEXT PRIMARY KEY,
                recorded_at TEXT NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                task TEXT NOT NULL,
                operation TEXT NOT NULL,
                status TEXT NOT NULL,
                cache_hit INTEGER NOT NULL DEFAULT 0,
                source_connector TEXT,
                run_id TEXT,
                input_tokens_estimate INTEGER NOT NULL DEFAULT 0,
                output_tokens_estimate INTEGER NOT NULL DEFAULT 0,
                total_tokens_estimate INTEGER NOT NULL DEFAULT 0,
                provider_tokens INTEGER,
                cost_estimate_usd REAL NOT NULL DEFAULT 0.0,
                cost_estimate_source TEXT NOT NULL DEFAULT 'unconfigured',
                duration_ms INTEGER,
                redaction_json TEXT NOT NULL DEFAULT '{}',
                error TEXT
            )
            """
        )
        for statement in (
            "CREATE INDEX IF NOT EXISTS idx_llm_usage_recorded_at ON llm_usage_events (recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_llm_usage_cost ON llm_usage_events (cost_estimate_usd DESC)",
            "CREATE INDEX IF NOT EXISTS idx_llm_usage_source_task ON llm_usage_events (source_connector, task)",
            "CREATE INDEX IF NOT EXISTS idx_llm_usage_run ON llm_usage_events (run_id)",
        ):
            conn.execute(statement)


def ensure_llm_usage_schema_once(db: MetadataDB) -> None:
    """Create the LLM usage schema once per process for each database."""
    cache_key = _llm_usage_schema_cache_key(db)
    with _LLM_USAGE_SCHEMA_LOCK:
        if cache_key in _LLM_USAGE_SCHEMA_READY:
            return
        ensure_llm_usage_schema(db)
        _LLM_USAGE_SCHEMA_READY.add(cache_key)


def _llm_usage_schema_cache_key(db: MetadataDB) -> str:
    db_path = getattr(db, "db_path", None)
    if db_path is None:
        return f"object:{id(db)}"
    return str(Path(db_path).expanduser().resolve(strict=False))


def split_model_provider(model_provider: str | None) -> tuple[str, str]:
    """Split the historical ``provider:model`` cache label."""

    text = str(model_provider or "").strip()
    if ":" in text:
        provider, model = text.split(":", 1)
        return (
            _clean_label(provider, default="unknown"),
            _clean_label(model, default="unknown"),
        )
    return "unknown", _clean_label(text, default="unknown")


def usage_timer_started() -> float:
    return time.perf_counter()


def elapsed_ms(started_at: float | None) -> int | None:
    if started_at is None:
        return None
    return max(0, int((time.perf_counter() - started_at) * 1000))


def _summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS call_count,
            COALESCE(SUM(cache_hit), 0) AS cache_hits,
            COALESCE(SUM(total_tokens_estimate), 0) AS total_tokens,
            COALESCE(SUM(cost_estimate_usd), 0.0) AS total_cost
        FROM llm_usage_events
        """
    ).fetchone()
    return {
        "call_count": int(row["call_count"] or 0),
        "cache_hits": int(row["cache_hits"] or 0),
        "total_tokens_estimate": int(row["total_tokens"] or 0),
        "total_cost_estimate_usd": round(float(row["total_cost"] or 0.0), 8),
    }


def _recent_expensive_runs(conn: sqlite3.Connection, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH recent AS (
            SELECT *
            FROM llm_usage_events
            ORDER BY recorded_at DESC
            LIMIT 500
        )
        SELECT
            COALESCE(run_id, event_id) AS usage_run_id,
            COALESCE(source_connector, 'direct') AS source_connector,
            GROUP_CONCAT(DISTINCT task) AS tasks,
            COUNT(*) AS call_count,
            COALESCE(SUM(cache_hit), 0) AS cache_hits,
            COALESCE(SUM(total_tokens_estimate), 0) AS total_tokens,
            COALESCE(SUM(cost_estimate_usd), 0.0) AS total_cost,
            MIN(recorded_at) AS first_recorded_at,
            MAX(recorded_at) AS last_recorded_at
        FROM recent
        GROUP BY usage_run_id, source_connector
        ORDER BY total_cost DESC, last_recorded_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "run_id": row["usage_run_id"],
            "source_connector": row["source_connector"],
            "tasks": _split_group_concat(row["tasks"]),
            "call_count": int(row["call_count"] or 0),
            "cache_hits": int(row["cache_hits"] or 0),
            "total_tokens_estimate": int(row["total_tokens"] or 0),
            "cost_estimate_usd": round(float(row["total_cost"] or 0.0), 8),
            "first_recorded_at": row["first_recorded_at"],
            "last_recorded_at": row["last_recorded_at"],
        }
        for row in rows
    ]


def _totals_by_dimension(
    conn: sqlite3.Connection,
    column: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if column not in {"source_connector", "task"}:
        raise ValueError("unsupported LLM usage dimension")
    label = "source_connector" if column == "source_connector" else "task"
    rows = conn.execute(
        f"""
        SELECT
            COALESCE({column}, 'direct') AS label,
            COUNT(*) AS call_count,
            COALESCE(SUM(cache_hit), 0) AS cache_hits,
            COALESCE(SUM(total_tokens_estimate), 0) AS total_tokens,
            COALESCE(SUM(cost_estimate_usd), 0.0) AS total_cost,
            MAX(recorded_at) AS last_recorded_at
        FROM llm_usage_events
        GROUP BY COALESCE({column}, 'direct')
        ORDER BY total_cost DESC, call_count DESC, label ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            label: row["label"],
            "call_count": int(row["call_count"] or 0),
            "cache_hits": int(row["cache_hits"] or 0),
            "total_tokens_estimate": int(row["total_tokens"] or 0),
            "cost_estimate_usd": round(float(row["total_cost"] or 0.0), 8),
            "last_recorded_at": row["last_recorded_at"],
        }
        for row in rows
    ]


def _resolve_run_context(
    db: MetadataDB,
    *,
    source_connector: str | None,
    run_id: str | None,
) -> tuple[str | None, str | None]:
    if source_connector and run_id:
        return source_connector, run_id
    try:
        from .connector_capture import current_connector_run_context

        context = current_connector_run_context()
    except Exception:
        context = None
    if context is None:
        return source_connector, run_id
    resolved_run_id = run_id or context.run_id
    if source_connector:
        return source_connector, resolved_run_id
    run = db.get_connector_run(resolved_run_id)
    return (run.connector_name if run else None), resolved_run_id


def _estimate_tokens(text: str | None, byte_count: int | None = None) -> int:
    if byte_count is not None:
        size = max(0, int(byte_count or 0))
    else:
        size = len((text or "").encode("utf-8"))
    if size <= 0:
        return 0
    return int(math.ceil(size / 4))


def _estimate_cost(
    *,
    input_tokens: int,
    output_tokens: int,
    provider_tokens: int | None,
    provider_cost: float | None,
    pricing: Mapping[str, Any] | None,
    cache_hit: bool,
) -> tuple[float, str]:
    if cache_hit:
        return 0.0, "cache_hit"
    if provider_cost is not None:
        try:
            return round(max(0.0, float(provider_cost)), 8), "provider"
        except (TypeError, ValueError):
            pass
    rates = dict(pricing or {})
    input_rate = _nonnegative_float(rates.get("input_cost_per_1k_tokens_usd"))
    output_rate = _nonnegative_float(rates.get("output_cost_per_1k_tokens_usd"))
    if input_rate == 0.0 and output_rate == 0.0:
        fallback_rate = _nonnegative_float(rates.get("cost_per_1k_tokens_usd"))
        if fallback_rate > 0.0 and provider_tokens is not None:
            return round(provider_tokens * fallback_rate / 1000, 8), "configured"
    total = (input_tokens * input_rate / 1000) + (output_tokens * output_rate / 1000)
    return round(max(0.0, total), 8), "configured" if total > 0 else "unconfigured"


def _nonnegative_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if number > 0 else 0.0


def _optional_nonnegative_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _clean_label(
    value: Any,
    *,
    default: str,
    max_length: int = _MAX_LABEL_LENGTH,
) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    text = text.replace("\n", " ").replace("\r", " ")
    return text[:max_length]


def _optional_label(value: Any, *, max_length: int = _MAX_LABEL_LENGTH) -> str | None:
    text = _clean_label(value, default="", max_length=max_length)
    return text or None


def _clean_error(value: str | None) -> str | None:
    if not value:
        return None
    result = redact_sensitive_text(str(value))
    text = result.redacted_text.replace("\n", " ").replace("\r", " ").strip()
    return text[:_MAX_ERROR_LENGTH] if text else None


def _split_group_concat(value: Any) -> list[str]:
    if not value:
        return []
    return [item for item in str(value).split(",") if item]


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()
