"""Postgres migrations for the capture event store."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Sequence


DEFAULT_CAPTURE_SCHEMA = "thoth_capture"
DEFAULT_MIGRATION_LOCK_ID = 840729145

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresMigrationError(RuntimeError):
    """Raised when Postgres migrations cannot be built or applied safely."""


@dataclass(frozen=True)
class PostgresMigration:
    """A single explicit Postgres migration."""

    version: int
    name: str
    statements: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.version <= 0:
            raise ValueError("Migration version must be positive")
        if not self.name.strip():
            raise ValueError("Migration name must be non-empty")
        if not self.statements:
            raise ValueError("Migration statements must be non-empty")


@dataclass(frozen=True)
class PostgresMigrationReport:
    """Summary of a migration run."""

    schema: str
    applied_versions: tuple[int, ...]
    skipped_versions: tuple[int, ...]


CAPTURE_EVENT_STORE_MIGRATIONS: tuple[PostgresMigration, ...] = (
    PostgresMigration(
        version=1,
        name="0001_capture_event_store_metadata",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS capture_event_store_metadata (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            COMMENT ON TABLE capture_event_store_metadata IS
            'Metadata for the Thoth Postgres capture event store'
            """,
        ),
    ),
    PostgresMigration(
        version=2,
        name="0002_capture_source_session_event_raw_refs",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS capture_sources (
                source_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                collector TEXT,
                account TEXT,
                native_source_id TEXT,
                base_uri TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                config JSONB NOT NULL DEFAULT '{}'::jsonb,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (source_name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS capture_sessions (
                session_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES capture_sources(source_id) ON DELETE RESTRICT,
                native_session_id TEXT,
                session_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                ended_at TIMESTAMPTZ,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS capture_sessions_source_native_session_idx
            ON capture_sessions(source_id, native_session_id)
            WHERE native_session_id IS NOT NULL
            """,
            """
            CREATE TABLE IF NOT EXISTS capture_events (
                event_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES capture_sources(source_id) ON DELETE RESTRICT,
                session_id TEXT REFERENCES capture_sessions(session_id) ON DELETE SET NULL,
                native_event_id TEXT,
                event_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'captured',
                occurred_at TIMESTAMPTZ,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                event_hash TEXT,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                privacy JSONB NOT NULL DEFAULT '{}'::jsonb,
                retention JSONB NOT NULL DEFAULT '{}'::jsonb,
                provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS capture_events_source_native_event_idx
            ON capture_events(source_id, native_event_id)
            WHERE native_event_id IS NOT NULL
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS capture_events_source_event_hash_idx
            ON capture_events(source_id, event_hash)
            WHERE event_hash IS NOT NULL
            """,
            """
            CREATE INDEX IF NOT EXISTS capture_events_session_idx
            ON capture_events(session_id, captured_at)
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_artifact_refs (
                raw_ref_id TEXT PRIMARY KEY,
                event_id TEXT REFERENCES capture_events(event_id) ON DELETE SET NULL,
                source_id TEXT NOT NULL REFERENCES capture_sources(source_id) ON DELETE RESTRICT,
                session_id TEXT REFERENCES capture_sessions(session_id) ON DELETE SET NULL,
                raw_root TEXT,
                path TEXT NOT NULL,
                sha256 TEXT,
                size_bytes BIGINT,
                mime_type TEXT,
                immutable BOOLEAN NOT NULL DEFAULT TRUE,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CHECK (immutable IS TRUE)
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS raw_artifact_refs_sha256_idx
            ON raw_artifact_refs(sha256)
            WHERE sha256 IS NOT NULL
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS raw_artifact_refs_path_idx
            ON raw_artifact_refs(path)
            """,
            """
            CREATE TABLE IF NOT EXISTS artifact_links (
                artifact_link_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES capture_events(event_id) ON DELETE CASCADE,
                raw_ref_id TEXT REFERENCES raw_artifact_refs(raw_ref_id) ON DELETE SET NULL,
                artifact_id TEXT NOT NULL,
                artifact_type TEXT NOT NULL,
                link_type TEXT NOT NULL DEFAULT 'source',
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (event_id, artifact_id, artifact_type, link_type)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS security_findings (
                finding_id TEXT PRIMARY KEY,
                event_id TEXT REFERENCES capture_events(event_id) ON DELETE CASCADE,
                raw_ref_id TEXT REFERENCES raw_artifact_refs(raw_ref_id) ON DELETE CASCADE,
                finding_type TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                status TEXT NOT NULL DEFAULT 'open',
                scanner TEXT,
                fingerprint TEXT,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS security_findings_event_fingerprint_idx
            ON security_findings(event_id, fingerprint)
            WHERE event_id IS NOT NULL AND fingerprint IS NOT NULL
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS security_findings_raw_ref_fingerprint_idx
            ON security_findings(raw_ref_id, fingerprint)
            WHERE raw_ref_id IS NOT NULL AND fingerprint IS NOT NULL
            """,
            """
            CREATE TABLE IF NOT EXISTS privacy_annotations (
                privacy_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES capture_events(event_id) ON DELETE CASCADE,
                raw_ref_id TEXT REFERENCES raw_artifact_refs(raw_ref_id) ON DELETE SET NULL,
                scope TEXT NOT NULL DEFAULT 'event',
                classification TEXT NOT NULL,
                policy TEXT,
                subject_ref TEXT NOT NULL DEFAULT '',
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (event_id, scope, classification, subject_ref)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS retention_policies (
                retention_id TEXT PRIMARY KEY,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                policy_name TEXT NOT NULL,
                action TEXT NOT NULL DEFAULT 'retain',
                retain_until TIMESTAMPTZ,
                delete_after TIMESTAMPTZ,
                legal_hold BOOLEAN NOT NULL DEFAULT FALSE,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (target_type, target_id, policy_name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS provenance_records (
                provenance_id TEXT PRIMARY KEY,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                actor TEXT,
                tool TEXT,
                fingerprint TEXT,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS provenance_records_target_fingerprint_idx
            ON provenance_records(target_type, target_id, operation, fingerprint)
            WHERE fingerprint IS NOT NULL
            """,
        ),
    ),
)

CREATE_SCHEMA_TEMPLATE = "CREATE SCHEMA IF NOT EXISTS {schema}"
SET_SEARCH_PATH_TEMPLATE = "SET LOCAL search_path TO {schema}, public"
CREATE_MIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS thoth_schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""
SELECT_MIGRATION_SQL = "SELECT version FROM thoth_schema_migrations WHERE version = %s"
INSERT_MIGRATION_SQL = """
INSERT INTO thoth_schema_migrations (version, name)
VALUES (%s, %s)
ON CONFLICT (version) DO NOTHING
"""
ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock(%s)"


def quote_identifier(identifier: str) -> str:
    """Quote a trusted Postgres identifier after rejecting unsafe input."""

    if not isinstance(identifier, str) or not _IDENTIFIER_RE.fullmatch(identifier):
        raise PostgresMigrationError(
            f"Invalid Postgres identifier: {identifier!r}. "
            "Use letters, digits, and underscores, starting with a letter or underscore."
        )
    return '"' + identifier.replace('"', '""') + '"'


def validate_migration_sequence(migrations: Sequence[PostgresMigration]) -> None:
    """Validate migration ordering and uniqueness before applying SQL."""

    seen_versions: set[int] = set()
    expected_version = 1
    for migration in migrations:
        if migration.version in seen_versions:
            raise PostgresMigrationError(
                f"Duplicate Postgres migration version: {migration.version}"
            )
        if migration.version != expected_version:
            raise PostgresMigrationError(
                "Postgres migration versions must be contiguous starting at 1; "
                f"expected {expected_version}, found {migration.version}"
            )
        seen_versions.add(migration.version)
        expected_version += 1


def apply_postgres_migrations(
    conn,
    *,
    schema: str = DEFAULT_CAPTURE_SCHEMA,
    lock_id: int = DEFAULT_MIGRATION_LOCK_ID,
    migrations: Sequence[PostgresMigration] = CAPTURE_EVENT_STORE_MIGRATIONS,
) -> PostgresMigrationReport:
    """Apply capture event-store migrations to a schema-scoped Postgres connection."""

    validate_migration_sequence(migrations)
    quoted_schema = quote_identifier(schema)
    applied: list[int] = []
    skipped: list[int] = []

    with conn.transaction():
        conn.execute(CREATE_SCHEMA_TEMPLATE.format(schema=quoted_schema))
        conn.execute(SET_SEARCH_PATH_TEMPLATE.format(schema=quoted_schema))
        conn.execute(ADVISORY_LOCK_SQL, (lock_id,))
        conn.execute(CREATE_MIGRATION_TABLE_SQL)

        for migration in migrations:
            existing = conn.execute(
                SELECT_MIGRATION_SQL,
                (migration.version,),
            ).fetchone()
            if existing:
                skipped.append(migration.version)
                continue

            for statement in migration.statements:
                conn.execute(statement)

            conn.execute(
                INSERT_MIGRATION_SQL,
                (migration.version, migration.name),
            )
            applied.append(migration.version)

    return PostgresMigrationReport(
        schema=schema,
        applied_versions=tuple(applied),
        skipped_versions=tuple(skipped),
    )


def migration_statements(
    migrations: Iterable[PostgresMigration] = CAPTURE_EVENT_STORE_MIGRATIONS,
) -> tuple[str, ...]:
    """Return migration SQL statements for tests and review tooling."""

    return tuple(statement for migration in migrations for statement in migration.statements)
