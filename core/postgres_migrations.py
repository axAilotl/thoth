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
