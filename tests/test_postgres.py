import os
import uuid

import pytest

from core.config import Config
from core.postgres import (
    DEFAULT_POSTGRES_DSN_ENV,
    PostgresConfigError,
    resolve_postgres_settings,
    validate_capture_event_store_config,
)
from core.postgres_migrations import (
    CAPTURE_EVENT_STORE_MIGRATIONS,
    INSERT_MIGRATION_SQL,
    SELECT_MIGRATION_SQL,
    apply_postgres_migrations,
    migration_statements,
    quote_identifier,
)


class FakeCursor:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class FakeTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self):
        self.applied_versions = set()
        self.executed = []

    def transaction(self):
        return FakeTransaction()

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if sql == SELECT_MIGRATION_SQL:
            version = params[0]
            row = (version,) if version in self.applied_versions else None
            return FakeCursor(row)
        if sql == INSERT_MIGRATION_SQL:
            self.applied_versions.add(params[0])
        return FakeCursor()


def make_config(*, enabled: bool = False) -> Config:
    config = Config()
    config.set("database.capture_event_store.enabled", enabled)
    config.set("database.capture_event_store.backend", "postgres")
    config.set("database.capture_event_store.dsn_env", DEFAULT_POSTGRES_DSN_ENV)
    config.set("database.capture_event_store.schema", "thoth_capture")
    return config


def test_disabled_capture_event_store_does_not_require_dsn():
    settings = resolve_postgres_settings(make_config(), environ={})

    assert not settings.enabled
    assert settings.dsn is None
    assert settings.dsn_env == DEFAULT_POSTGRES_DSN_ENV


def test_enabled_capture_event_store_fails_closed_without_dsn():
    config = make_config(enabled=True)

    with pytest.raises(PostgresConfigError, match=DEFAULT_POSTGRES_DSN_ENV):
        resolve_postgres_settings(config, environ={})

    assert validate_capture_event_store_config(config, environ={}) == [
        "database.capture_event_store is enabled with backend 'postgres', "
        f"but {DEFAULT_POSTGRES_DSN_ENV} is not set"
    ]


def test_config_validate_reports_missing_postgres_dsn(monkeypatch, tmp_path):
    monkeypatch.delenv(DEFAULT_POSTGRES_DSN_ENV, raising=False)
    config = make_config(enabled=True)
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.images_dir", "images")
    config.set("paths.videos_dir", "videos")
    config.set("paths.media_dir", "media")
    config.set("paths.bookmarks_file", str(tmp_path / "bookmarks.json"))
    config.set("paths.cookies_file", str(tmp_path / "cookies.txt"))
    config.set("database.path", "meta.db")
    (tmp_path / "bookmarks.json").write_text("[]\n", encoding="utf-8")
    (tmp_path / "cookies.txt").write_text("", encoding="utf-8")

    errors = config.validate()

    assert any(DEFAULT_POSTGRES_DSN_ENV in error for error in errors)


def test_enabled_capture_event_store_reads_configured_env():
    config = make_config(enabled=True)
    config.set("database.capture_event_store.dsn_env", "CUSTOM_THOTH_DSN")
    config.set("database.capture_event_store.schema", "capture_test")

    settings = resolve_postgres_settings(
        config,
        environ={"CUSTOM_THOTH_DSN": "postgresql://example/db"},
    )

    assert settings.enabled
    assert settings.dsn == "postgresql://example/db"
    assert settings.dsn_env == "CUSTOM_THOTH_DSN"
    assert settings.schema == "capture_test"


def test_invalid_capture_event_store_schema_fails_closed():
    config = make_config(enabled=True)
    config.set("database.capture_event_store.schema", "bad-schema")

    with pytest.raises(PostgresConfigError, match="Invalid Postgres identifier"):
        resolve_postgres_settings(
            config,
            environ={DEFAULT_POSTGRES_DSN_ENV: "postgresql://example/db"},
        )


def test_capture_event_store_migrations_are_explicit_and_idempotent():
    conn = FakeConnection()

    first = apply_postgres_migrations(conn, schema="capture_unit")
    second = apply_postgres_migrations(conn, schema="capture_unit")

    assert first.applied_versions == (1,)
    assert first.skipped_versions == ()
    assert second.applied_versions == ()
    assert second.skipped_versions == (1,)
    assert CAPTURE_EVENT_STORE_MIGRATIONS[0].name == "0001_capture_event_store_metadata"
    assert any(
        "CREATE TABLE IF NOT EXISTS capture_event_store_metadata" in statement
        for statement in migration_statements()
    )


def test_quote_identifier_rejects_unsafe_identifiers():
    assert quote_identifier("capture_unit_1") == '"capture_unit_1"'

    with pytest.raises(Exception, match="Invalid Postgres identifier"):
        quote_identifier("capture-unit")


def test_live_postgres_migrations_are_idempotent():
    dsn = os.getenv("THOTH_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip(
            "THOTH_TEST_POSTGRES_DSN is not set; skipping live Postgres migration test"
        )
    psycopg = pytest.importorskip(
        "psycopg",
        reason="psycopg is required for live Postgres migration tests",
    )
    schema = f"thoth_test_{uuid.uuid4().hex}"
    quoted_schema = quote_identifier(schema)

    with psycopg.connect(dsn, autocommit=True) as admin_conn:
        admin_conn.execute(f"CREATE SCHEMA {quoted_schema}")
        try:
            with psycopg.connect(dsn) as conn:
                first = apply_postgres_migrations(conn, schema=schema)
            with psycopg.connect(dsn) as conn:
                second = apply_postgres_migrations(conn, schema=schema)
            with psycopg.connect(dsn) as conn:
                row = conn.execute(
                    f"SELECT count(*) FROM {quoted_schema}.thoth_schema_migrations"
                ).fetchone()

            assert first.applied_versions == (1,)
            assert second.skipped_versions == (1,)
            assert row[0] == 1
        finally:
            admin_conn.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
