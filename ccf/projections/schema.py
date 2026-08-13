"""Projection schema: additive migration 0002 for the CCF envelope.

Every table here is a rebuildable projection (or projection machinery:
generation fences, the invalidation queue, checkpoints). No human decision
is stored only in these tables — canonical state lives in the phase 1-3
tables (``object_header``, ``compartment``, ``admission``, ``lineage_head``,
``commit_journal``). Dropping every ``projection_*`` table plus the fence
and invalidation machinery and rebuilding from canonical state must lose
nothing (spec sections 1.4 and 8.7).

Table shapes follow the vendored reference envelope
(``spec/ccf/0.1.2-rc1/sql/postgres-reference.sql``), trimmed to what phase 5
needs: single-current-generation rows (older generations are not retained),
no policy/authorization projections (those belong to the governance
stream).

The pgvector-backed embedding table is created only when the ``vector``
extension is available; ``ccf.projections.vectors`` fails closed at use
time when it is not. Everything else in this migration is plain Postgres.
"""

from __future__ import annotations

from core.postgres_migrations import PostgresMigration


CCF_PROJECTION_MIGRATION = PostgresMigration(
    version=2,
    name="0002_ccf_projections",
    statements=(
        # -- Projection machinery: coarse generation fences (spec 10.4). --
        """
        CREATE TABLE IF NOT EXISTS generation_fence (
            archive_id          text NOT NULL REFERENCES archive(archive_id),
            namespace           text NOT NULL,
            subject_key         text NOT NULL,
            generation          numeric(20,0) NOT NULL,
            changed_at_sequence numeric(20,0) NOT NULL,
            direction           text NOT NULL CHECK (direction IN ('tighten','widen','unknown')),
            cause_object_id     text NOT NULL,
            PRIMARY KEY (archive_id, namespace, subject_key)
        )
        """,
        # -- Projection machinery: invalidation queue (spec 10.4). --
        """
        CREATE TABLE IF NOT EXISTS projection_invalidation (
            id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            archive_id            text NOT NULL REFERENCES archive(archive_id),
            projection_name       text NOT NULL,
            target_key            text,
            cause_object_kind     text,
            cause_object_id       text,
            cause_commit_sequence numeric(20,0) NOT NULL,
            priority              integer NOT NULL DEFAULT 0,
            status                text NOT NULL CHECK (status IN ('queued','running','done','failed')),
            created_at            timestamptz NOT NULL DEFAULT now(),
            resolved_at           timestamptz,
            last_error            text
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS projection_invalidation_work_idx
            ON projection_invalidation (projection_name, status, priority DESC, cause_commit_sequence)
        """,
        # -- Current Link state from lineage.link_disposition heads (8.4). --
        """
        CREATE TABLE IF NOT EXISTS projection_link_state (
            archive_id                text NOT NULL REFERENCES archive(archive_id),
            link_id                   text NOT NULL REFERENCES object_header(id),
            type                      text,
            from_id                   text,
            to_id                     text,
            state                     text NOT NULL CHECK (state IN
                ('active','retracted','superseded','tombstoned')),
            selector_available        boolean NOT NULL,
            disposition_record_id     text,
            replacement_link_id       text,
            computed_through_sequence numeric(20,0) NOT NULL,
            generation                numeric(20,0) NOT NULL,
            PRIMARY KEY (archive_id, link_id)
        )
        """,
        # -- Active derived_from closure, rebuildable acceleration (10.3). --
        """
        CREATE TABLE IF NOT EXISTS projection_derivation_closure (
            archive_id                text NOT NULL REFERENCES archive(archive_id),
            ancestor_id               text NOT NULL,
            descendant_id             text NOT NULL,
            minimum_depth             integer NOT NULL CHECK (minimum_depth > 0),
            active_path_count         numeric(20,0) NOT NULL CHECK (active_path_count > 0),
            computed_through_sequence numeric(20,0) NOT NULL,
            generation                numeric(20,0) NOT NULL,
            PRIMARY KEY (archive_id, ancestor_id, descendant_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS projection_derivation_descendant_idx
            ON projection_derivation_closure (archive_id, descendant_id, ancestor_id)
        """,
        # -- Entity clusters from resolution records + same_as links (8.5). --
        """
        CREATE TABLE IF NOT EXISTS projection_entity_cluster (
            archive_id                text NOT NULL REFERENCES archive(archive_id),
            member_id                 text NOT NULL,
            cluster_id                text NOT NULL,
            canonical_member_id       text,
            computed_through_sequence numeric(20,0) NOT NULL,
            generation                numeric(20,0) NOT NULL,
            PRIMARY KEY (archive_id, member_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS projection_entity_cluster_idx
            ON projection_entity_cluster (archive_id, cluster_id)
        """,
        # -- Full-text search over available semantic text (10.1). --
        """
        CREATE TABLE IF NOT EXISTS projection_full_text (
            archive_id                text NOT NULL REFERENCES archive(archive_id),
            object_id                 text NOT NULL REFERENCES object_header(id),
            document                  tsvector NOT NULL,
            computed_through_sequence numeric(20,0) NOT NULL,
            generation                numeric(20,0) NOT NULL,
            PRIMARY KEY (archive_id, object_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS projection_full_text_document_idx
            ON projection_full_text USING gin (document)
        """,
        # -- Caller-supplied embedding storage (10.1); pgvector optional. --
        # The extension may be absent (plain postgres images) or the role may
        # lack privilege. That is not an error here: the vectors projection
        # fails closed when used without support. Anything else (a real SQL
        # error) still aborts the migration.
        """
        DO $$
        BEGIN
            CREATE EXTENSION IF NOT EXISTS vector;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pgvector extension unavailable; projection_embedding not created';
        END $$
        """,
        """
        DO $$
        BEGIN
            -- to_regtype resolves through the current search_path: when the
            -- extension already exists but its type lives in another schema
            -- (a second CCF schema in the same database), the type is not
            -- visible here and the optional table is skipped, exactly as
            -- when the extension is absent entirely.
            IF to_regtype('vector') IS NOT NULL THEN
                CREATE TABLE IF NOT EXISTS projection_embedding (
                    archive_id                text NOT NULL REFERENCES archive(archive_id),
                    object_id                 text NOT NULL REFERENCES object_header(id),
                    model_id                  text NOT NULL,
                    embedding                 vector NOT NULL,
                    computed_through_sequence numeric(20,0) NOT NULL,
                    generation                numeric(20,0) NOT NULL,
                    PRIMARY KEY (archive_id, object_id, model_id)
                );
            END IF;
        END $$
        """,
        # -- Checkpoints: accelerations, never authority (10.5). --
        """
        CREATE TABLE IF NOT EXISTS projection_checkpoint (
            archive_id              text NOT NULL REFERENCES archive(archive_id),
            projection_name         text NOT NULL,
            generation              numeric(20,0) NOT NULL,
            through_commit_sequence numeric(20,0) NOT NULL,
            source_head_hash        text NOT NULL,
            dependency_generations  jsonb NOT NULL,
            snapshot_digest         text NOT NULL,
            snapshot_payload        jsonb NOT NULL,
            storage_ref             text NOT NULL,
            created_at              timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (archive_id, projection_name, generation)
        )
        """,
    ),
)
