"""Postgres configuration and connection helpers for Thoth."""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Mapping

from .postgres_migrations import (
    DEFAULT_CAPTURE_SCHEMA,
    DEFAULT_MIGRATION_LOCK_ID,
    PostgresMigrationReport,
    apply_postgres_migrations,
    quote_identifier,
)


DEFAULT_POSTGRES_DSN_ENV = "THOTH_POSTGRES_DSN"
DEFAULT_POSTGRES_APPLICATION_NAME = "thoth-capture-event-store"


class PostgresConfigError(RuntimeError):
    """Raised when Postgres mode is enabled but cannot be configured safely."""


@dataclass(frozen=True)
class PostgresSettings:
    """Resolved Postgres settings for the capture event store."""

    enabled: bool
    dsn: str | None = None
    dsn_env: str = DEFAULT_POSTGRES_DSN_ENV
    schema: str = DEFAULT_CAPTURE_SCHEMA
    connect_timeout_seconds: int = 10
    application_name: str = DEFAULT_POSTGRES_APPLICATION_NAME
    migration_lock_id: int = DEFAULT_MIGRATION_LOCK_ID


def _capture_event_store_config(config_obj) -> dict:
    event_store = config_obj.get("database.capture_event_store", {})
    if event_store is None:
        return {}
    if not isinstance(event_store, dict):
        raise PostgresConfigError("database.capture_event_store must be an object")
    return event_store


def resolve_postgres_settings(
    config_obj,
    *,
    environ: Mapping[str, str] | None = None,
) -> PostgresSettings:
    """Resolve capture event-store Postgres settings from config and environment."""

    event_store = _capture_event_store_config(config_obj)
    enabled = bool(event_store.get("enabled", False))
    backend = str(event_store.get("backend", "postgres") or "").strip()
    dsn_env = str(event_store.get("dsn_env", DEFAULT_POSTGRES_DSN_ENV) or "").strip()
    schema = str(event_store.get("schema", DEFAULT_CAPTURE_SCHEMA) or "").strip()
    application_name = str(
        event_store.get("application_name", DEFAULT_POSTGRES_APPLICATION_NAME) or ""
    ).strip()
    lock_id = event_store.get("migration_lock_id", DEFAULT_MIGRATION_LOCK_ID)
    connect_timeout = event_store.get("connect_timeout_seconds", 10)

    if backend and backend != "postgres":
        raise PostgresConfigError(
            "database.capture_event_store.backend must be 'postgres'"
        )
    if not dsn_env:
        raise PostgresConfigError("database.capture_event_store.dsn_env is required")
    if not application_name:
        raise PostgresConfigError(
            "database.capture_event_store.application_name is required"
        )

    try:
        quote_identifier(schema)
    except Exception as exc:
        raise PostgresConfigError(str(exc)) from exc

    try:
        parsed_lock_id = int(lock_id)
    except (TypeError, ValueError) as exc:
        raise PostgresConfigError(
            "database.capture_event_store.migration_lock_id must be an integer"
        ) from exc

    try:
        parsed_connect_timeout = int(connect_timeout)
    except (TypeError, ValueError) as exc:
        raise PostgresConfigError(
            "database.capture_event_store.connect_timeout_seconds must be an integer"
        ) from exc
    if parsed_connect_timeout <= 0:
        raise PostgresConfigError(
            "database.capture_event_store.connect_timeout_seconds must be positive"
        )

    env = os.environ if environ is None else environ
    dsn = env.get(dsn_env)
    if enabled and (not dsn or not dsn.strip()):
        raise PostgresConfigError(
            "database.capture_event_store is enabled with backend 'postgres', "
            f"but {dsn_env} is not set"
        )

    return PostgresSettings(
        enabled=enabled,
        dsn=dsn.strip() if dsn else None,
        dsn_env=dsn_env,
        schema=schema,
        connect_timeout_seconds=parsed_connect_timeout,
        application_name=application_name,
        migration_lock_id=parsed_lock_id,
    )


def validate_capture_event_store_config(
    config_obj,
    *,
    environ: Mapping[str, str] | None = None,
) -> list[str]:
    """Return config validation errors for the Postgres capture event store."""

    try:
        resolve_postgres_settings(config_obj, environ=environ)
    except PostgresConfigError as exc:
        return [str(exc)]
    return []


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise PostgresConfigError(
            "psycopg is required for Postgres capture event-store connections. "
            "Install requirements.txt before enabling database.capture_event_store."
        ) from exc
    return psycopg


@contextmanager
def open_postgres_connection(settings: PostgresSettings):
    """Open a psycopg connection for enabled capture event-store settings."""

    if not settings.enabled:
        raise PostgresConfigError("Postgres capture event store is not enabled")
    if not settings.dsn:
        raise PostgresConfigError(
            f"{settings.dsn_env} is required for Postgres capture event-store connections"
        )

    psycopg = _import_psycopg()
    with psycopg.connect(
        settings.dsn,
        autocommit=False,
        connect_timeout=settings.connect_timeout_seconds,
        application_name=settings.application_name,
    ) as conn:
        yield conn


def migrate_capture_event_store(settings: PostgresSettings) -> PostgresMigrationReport:
    """Run capture event-store migrations for resolved Postgres settings."""

    with open_postgres_connection(settings) as conn:
        return apply_postgres_migrations(
            conn,
            schema=settings.schema,
            lock_id=settings.migration_lock_id,
        )
