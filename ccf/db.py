"""Postgres settings and schema migrations for the CCF archive envelope.

The CCF operational envelope lives in its own Postgres schema (default
``ccf``) and follows the vendored reference envelope
(``spec/ccf/0.1.1/sql/postgres-reference.sql``): object headers, compartment
and Blob content storage, admissions, commit journal + members, the
origin/idempotency index, lineage heads, and the signed producer-batch spool
with receipts.

Canonical timestamps that feed hashes and signatures (commit ``committed_at``,
member ``admitted_at``) are stored as their canonical text form so chain
verification can rebuild byte-exact members; operational timestamps use
``timestamptz``.

Settings resolve from the Thoth config object (``database.ccf_archive``)
and environment, mirroring ``core/postgres.py``. There is no fallback DSN:
an enabled CCF store without an explicit DSN or explicit key paths fails
closed.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Mapping

from core.postgres_migrations import (
    PostgresMigration,
    PostgresMigrationReport,
    apply_postgres_migrations,
    quote_identifier,
)


DEFAULT_CCF_SCHEMA = "ccf"
DEFAULT_CCF_DSN_ENV = "THOTH_CCF_POSTGRES_DSN"
DEFAULT_CCF_DEVICE_KEY_ENV = "THOTH_CCF_DEVICE_KEY"
DEFAULT_CCF_ARCHIVE_KEY_ENV = "THOTH_CCF_ARCHIVE_KEY"
DEFAULT_CCF_APPLICATION_NAME = "thoth-ccf-archive"
DEFAULT_CCF_MIGRATION_LOCK_ID = 840729146


class CcfConfigError(RuntimeError):
    """Raised when CCF Postgres configuration is missing or unsafe."""


@dataclass(frozen=True)
class CcfPostgresSettings:
    """Resolved Postgres settings for the CCF archive envelope."""

    enabled: bool
    dsn: str | None = None
    dsn_env: str = DEFAULT_CCF_DSN_ENV
    schema: str = DEFAULT_CCF_SCHEMA
    connect_timeout_seconds: int = 10
    application_name: str = DEFAULT_CCF_APPLICATION_NAME
    migration_lock_id: int = DEFAULT_CCF_MIGRATION_LOCK_ID
    device_key_path: str | None = None
    archive_key_path: str | None = None


def _ccf_store_config(config_obj) -> dict:
    store = config_obj.get("database.ccf_archive", {})
    if store is None:
        return {}
    if not isinstance(store, dict):
        raise CcfConfigError("database.ccf_archive must be an object")
    return store


def resolve_ccf_postgres_settings(
    config_obj,
    *,
    environ: Mapping[str, str] | None = None,
) -> CcfPostgresSettings:
    """Resolve CCF archive Postgres settings from config and environment."""

    store = _ccf_store_config(config_obj)
    enabled = bool(store.get("enabled", False))
    backend = str(store.get("backend", "postgres") or "").strip()
    dsn_env = str(store.get("dsn_env", DEFAULT_CCF_DSN_ENV) or "").strip()
    schema = str(store.get("schema", DEFAULT_CCF_SCHEMA) or "").strip()
    application_name = str(
        store.get("application_name", DEFAULT_CCF_APPLICATION_NAME) or ""
    ).strip()
    lock_id = store.get("migration_lock_id", DEFAULT_CCF_MIGRATION_LOCK_ID)
    connect_timeout = store.get("connect_timeout_seconds", 10)

    if backend and backend != "postgres":
        raise CcfConfigError("database.ccf_archive.backend must be 'postgres'")
    if not dsn_env:
        raise CcfConfigError("database.ccf_archive.dsn_env is required")
    if not application_name:
        raise CcfConfigError("database.ccf_archive.application_name is required")

    try:
        quote_identifier(schema)
    except Exception as exc:
        raise CcfConfigError(str(exc)) from exc

    try:
        parsed_lock_id = int(lock_id)
        parsed_connect_timeout = int(connect_timeout)
    except (TypeError, ValueError) as exc:
        raise CcfConfigError(
            "database.ccf_archive migration_lock_id and connect_timeout_seconds "
            "must be integers"
        ) from exc
    if parsed_connect_timeout <= 0:
        raise CcfConfigError(
            "database.ccf_archive.connect_timeout_seconds must be positive"
        )

    env = os.environ if environ is None else environ
    dsn = env.get(dsn_env)
    if enabled and (not dsn or not dsn.strip()):
        raise CcfConfigError(
            "database.ccf_archive is enabled with backend 'postgres', "
            f"but {dsn_env} is not set"
        )

    device_key_path = store.get("device_key_path") or env.get(
        DEFAULT_CCF_DEVICE_KEY_ENV
    )
    archive_key_path = store.get("archive_key_path") or env.get(
        DEFAULT_CCF_ARCHIVE_KEY_ENV
    )

    return CcfPostgresSettings(
        enabled=enabled,
        dsn=dsn.strip() if dsn else None,
        dsn_env=dsn_env,
        schema=schema,
        connect_timeout_seconds=parsed_connect_timeout,
        application_name=application_name,
        migration_lock_id=parsed_lock_id,
        device_key_path=device_key_path or None,
        archive_key_path=archive_key_path or None,
    )


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise CcfConfigError(
            "psycopg is required for CCF archive connections. "
            "Install requirements.txt before enabling database.ccf_archive."
        ) from exc
    return psycopg


@contextmanager
def open_ccf_connection(settings: CcfPostgresSettings):
    """Open a psycopg connection scoped to the CCF schema."""

    if not settings.enabled:
        raise CcfConfigError("CCF archive store is not enabled")
    if not settings.dsn:
        raise CcfConfigError(
            f"{settings.dsn_env} is required for CCF archive connections"
        )

    psycopg = _import_psycopg()
    with psycopg.connect(
        settings.dsn,
        autocommit=False,
        connect_timeout=settings.connect_timeout_seconds,
        application_name=settings.application_name,
    ) as conn:
        conn.execute(
            "SET search_path TO " + quote_identifier(settings.schema) + ", public"
        )
        yield conn


CCF_MIGRATIONS: tuple[PostgresMigration, ...] = (
    PostgresMigration(
        version=1,
        name="0001_ccf_operational_envelope",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS archive (
                archive_id              text PRIMARY KEY,
                epoch_id                text NOT NULL,
                genesis_commit_hash     text NOT NULL,
                hash_profile            text NOT NULL CHECK (hash_profile = 'ccf-jcs-sha256-v2'),
                signature_profile       text NOT NULL CHECK (signature_profile = 'ed25519-jcs-v1'),
                semantic_catalog_root   text NOT NULL,
                active_profiles         jsonb NOT NULL,
                signer_key_id           text NOT NULL,
                erasure_domain_id       text NOT NULL,
                created_at              text NOT NULL,
                CHECK (archive_id ~ '^urn:ccf:archive:'),
                CHECK (epoch_id ~ '^urn:ccf:lineage:')
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS object_header (
                id                      text PRIMARY KEY,
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                object_kind             text NOT NULL CHECK (object_kind IN ('record','link','blob')),
                spec                    text NOT NULL CHECK (spec = 'ccf/0.1.1'),
                hash_profile            text NOT NULL CHECK (hash_profile = 'ccf-jcs-sha256-v2'),
                structural_commitment   text NOT NULL,
                semantic_commitment     text,
                object_hash             text NOT NULL UNIQUE,
                submission_hash         text,
                CHECK (substring(id from 1 for 9 + length(object_kind)) = 'urn:ccf:' || object_kind || ':')
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS compartment (
                object_id               text NOT NULL REFERENCES object_header(id),
                compartment             text NOT NULL CHECK (compartment IN ('structural','semantic')),
                state                   text NOT NULL CHECK (state IN ('plaintext','encrypted','withheld','erased')),
                format                  text,
                salt                    bytea,
                plaintext_json          jsonb,
                ciphertext              bytea,
                ciphertext_digest       text,
                storage_ref             text,
                updated_at              text NOT NULL,
                PRIMARY KEY (object_id, compartment),
                CHECK (
                  (state IN ('plaintext','encrypted') AND salt IS NOT NULL)
                  OR (state IN ('withheld','erased') AND salt IS NULL)
                ),
                CHECK (
                  (state = 'plaintext' AND plaintext_json IS NOT NULL)
                  OR (state <> 'plaintext' AND plaintext_json IS NULL)
                )
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS blob_content (
                blob_id                 text PRIMARY KEY REFERENCES object_header(id),
                state                   text NOT NULL CHECK (state IN ('plaintext','encrypted','withheld','erased')),
                byte_length             numeric(20,0),
                plaintext_bytes         bytea,
                storage_ref             text,
                content_salt            bytea,
                updated_at              text NOT NULL,
                CHECK (
                  (state = 'plaintext' AND plaintext_bytes IS NOT NULL)
                  OR (state <> 'plaintext' AND plaintext_bytes IS NULL)
                ),
                CHECK (state <> 'erased' OR content_salt IS NULL)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS admission (
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                commit_sequence         numeric(20,0) NOT NULL,
                commit_position         integer NOT NULL CHECK (commit_position >= 0),
                object_kind             text NOT NULL,
                object_id               text NOT NULL REFERENCES object_header(id),
                object_hash             text NOT NULL,
                admitted_at             text NOT NULL,
                PRIMARY KEY (archive_id, commit_sequence, commit_position),
                UNIQUE (archive_id, object_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS origin_index (
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                source_id               text NOT NULL,
                native_id               text NOT NULL,
                revision                text NOT NULL,
                submission_hash         text NOT NULL,
                object_kind             text NOT NULL,
                object_id               text NOT NULL,
                lifecycle               text NOT NULL DEFAULT 'active',
                -- The vendored thoth-capture example admits an artifact Record
                -- and a Blob under one origin tuple, so the idempotency key is
                -- the origin tuple plus the object kind.
                PRIMARY KEY (archive_id, source_id, native_id, revision, object_kind)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS producer_batch (
                batch_id                text PRIMARY KEY,
                producer_id             text NOT NULL,
                producer_sequence       numeric(20,0) NOT NULL,
                previous_batch_hash     text,
                credential_id           text NOT NULL,
                created_at              text NOT NULL,
                semantic_catalog_root   text NOT NULL,
                batch_hash              text NOT NULL UNIQUE,
                signature               bytea NOT NULL,
                batch_json              jsonb NOT NULL,
                status                  text NOT NULL CHECK (status IN
                    ('queued','verifying','committed','partial','rejected','conflict')),
                spooled_at              text NOT NULL,
                committed_sequence      numeric(20,0),
                result_json             jsonb,
                UNIQUE (producer_id, producer_sequence)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS producer_head (
                producer_id             text PRIMARY KEY,
                producer_sequence       numeric(20,0) NOT NULL,
                batch_hash              text NOT NULL,
                credential_id           text NOT NULL,
                updated_at              text NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS commit_journal (
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                sequence                numeric(20,0) NOT NULL,
                commit_record_id        text NOT NULL REFERENCES object_header(id),
                parent_commit_hash      text,
                commit_hash             text NOT NULL UNIQUE,
                batch_merkle_root       text NOT NULL,
                member_count            numeric(20,0) NOT NULL,
                signer_key_id           text NOT NULL,
                semantic_catalog_root   text NOT NULL,
                committed_at            text NOT NULL,
                PRIMARY KEY (archive_id, sequence),
                UNIQUE (archive_id, commit_record_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS commit_member (
                archive_id              text NOT NULL,
                commit_sequence         numeric(20,0) NOT NULL,
                commit_position         integer NOT NULL,
                object_kind             text NOT NULL,
                object_id               text NOT NULL,
                object_hash             text NOT NULL,
                admitted_at             text NOT NULL,
                leaf_hash               text NOT NULL,
                PRIMARY KEY (archive_id, commit_sequence, commit_position),
                FOREIGN KEY (archive_id, commit_sequence)
                    REFERENCES commit_journal(archive_id, sequence)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS archive_head (
                archive_id              text PRIMARY KEY REFERENCES archive(archive_id),
                sequence                numeric(20,0) NOT NULL,
                commit_record_id        text NOT NULL,
                commit_hash             text NOT NULL,
                semantic_catalog_root   text NOT NULL,
                signer_key_id           text NOT NULL,
                updated_at              text NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lineage_head (
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                lineage_id              text NOT NULL,
                head_record_id          text NOT NULL REFERENCES object_header(id),
                head_record_hash        text NOT NULL,
                head_commit_sequence    numeric(20,0) NOT NULL,
                state                   text NOT NULL,
                valid_from              text NOT NULL,
                expires_at              text,
                PRIMARY KEY (archive_id, lineage_id),
                CHECK (lineage_id ~ '^urn:ccf:lineage:')
            )
            """,
        ),
    ),
    PostgresMigration(
        version=2,
        name="0002_governance_baseline",
        statements=(
            # Generation fences (spec section 9.5). Governance mutations bump
            # the matching ``governance.*`` fence in the admission transaction;
            # cached decisions record the generation vector they were computed
            # against and are only served while every generation matches.
            # ``projection.*`` fences live in the same table (other stream).
            """
            CREATE TABLE IF NOT EXISTS generation_fence (
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                fence                   text NOT NULL,
                generation              numeric(20,0) NOT NULL,
                last_change_sequence    numeric(20,0) NOT NULL,
                updated_at              text NOT NULL,
                PRIMARY KEY (archive_id, fence)
            )
            """,
            # Local decision cache keyed by decision-context hash.
            """
            CREATE TABLE IF NOT EXISTS governance_decision (
                decision_context_hash   text PRIMARY KEY,
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                decision_json           jsonb NOT NULL,
                generation_vector       jsonb NOT NULL,
                head_sequence           numeric(20,0) NOT NULL,
                valid_until             text,
                created_at              text NOT NULL
            )
            """,
            # Short-expiry, use-counted egress capabilities (spec section 9.7).
            """
            CREATE TABLE IF NOT EXISTS egress_capability (
                capability_id           text PRIMARY KEY,
                archive_id              text NOT NULL REFERENCES archive(archive_id),
                capability_hash         text NOT NULL UNIQUE,
                decision_context_hash   text NOT NULL,
                generation_vector       jsonb NOT NULL,
                head_sequence           numeric(20,0) NOT NULL,
                object_ids              jsonb NOT NULL,
                availability            jsonb NOT NULL,
                remaining_uses          integer NOT NULL CHECK (remaining_uses >= 0),
                expires_at              text NOT NULL,
                created_at              text NOT NULL,
                updated_at              text NOT NULL
            )
            """,
        ),
    ),
)


def migrate_ccf_store(settings: CcfPostgresSettings) -> PostgresMigrationReport:
    """Apply CCF envelope migrations (advisory-locked, versioned)."""

    with open_ccf_connection(settings) as conn:
        return apply_postgres_migrations(
            conn,
            schema=settings.schema,
            lock_id=settings.migration_lock_id,
            migrations=CCF_MIGRATIONS,
        )
