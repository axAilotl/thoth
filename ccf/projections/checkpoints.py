"""Projection checkpoints and recovery (spec 10.5).

A checkpoint pins one projection's rows at a fence generation: projection
name, generation, source head (commit sequence + hash), dependency
generations, and a snapshot digest over the rows. Checkpoints are
accelerations, never authority — recovery validates the newest checkpoint
against its stored snapshot and replays later commits; a corrupt
checkpoint falls back to an older one, and with no valid checkpoint
recovery rebuilds from genesis (canonical state).

Phase 5 keeps snapshots in the ``snapshot_payload`` column
(``storage_ref = "table:<name>"``) and "replays" by full recompute from
canonical state, which is equivalent for derived projections and always
available as the final fallback. The checkpoint document returned by
:func:`save_checkpoint` conforms to
``schemas/operational/projection-checkpoint.schema.json``.
"""

from __future__ import annotations

from decimal import Decimal

from psycopg.types.json import Jsonb

from ccf.hashing import canonical_digest
from ccf.objects import now_timestamp
from ccf.projections import DERIVATION, EMBEDDING, ENTITY_CLUSTER, FULL_TEXT, LINK_STATE

SNAPSHOT_DIGEST_DOMAIN = "ccf-projection-snapshot-v1"


class CheckpointError(RuntimeError):
    """Raised when checkpoint save or recovery cannot proceed safely."""


#: projection_name -> (table, primary-key columns for deterministic order)
_SNAPSHOT_TABLES = {
    LINK_STATE: ("projection_link_state", ("link_id",)),
    DERIVATION: ("projection_derivation_closure", ("ancestor_id", "descendant_id")),
    ENTITY_CLUSTER: ("projection_entity_cluster", ("member_id",)),
    FULL_TEXT: ("projection_full_text", ("object_id",)),
    EMBEDDING: ("projection_embedding", ("object_id", "model_id")),
}


def _normalize(value: object) -> object:
    """Normalize DB driver values for canonical serialization."""
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    return value


def _table_name(conn, projection_name: str) -> tuple[str, tuple[str, ...]]:
    entry = _SNAPSHOT_TABLES.get(projection_name)
    if entry is None:
        raise CheckpointError(f"projection {projection_name!r} has no snapshot table")
    table, _pk = entry
    exists = conn.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname = current_schema() AND tablename = %s",
        (table,),
    ).fetchone()
    if exists is None:
        raise CheckpointError(f"snapshot table {table} does not exist in this schema")
    return entry


def _dump_rows(conn, archive_id: str, table: str, pk: tuple[str, ...]) -> list[dict]:
    cursor = conn.execute(
        f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY "  # noqa: S608 - table is registry-fixed
        + ", ".join(pk),
        (archive_id,),
    )
    columns = [col.name for col in cursor.description]
    return [
        {column: _normalize(value) for column, value in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def _restore_rows(conn, archive_id: str, table: str, rows: list[dict]) -> None:
    conn.execute(f"DELETE FROM {table} WHERE archive_id = %s", (archive_id,))  # noqa: S608
    for row in rows:
        columns = list(row)
        conn.execute(
            f"INSERT INTO {table} ("  # noqa: S608 - table is registry-fixed
            + ", ".join(columns)
            + ") VALUES ("
            + ", ".join(["%s"] * len(columns))
            + ")",
            [row[column] for column in columns],
        )


def _snapshot_digest(rows: list[dict]) -> str:
    return canonical_digest(SNAPSHOT_DIGEST_DOMAIN, rows)


def save_checkpoint(
    conn,
    *,
    archive_id: str,
    projection_name: str,
    clock=now_timestamp,
) -> dict:
    """Snapshot one projection's rows into a new checkpoint row.

    Returns the checkpoint document (conformant with the operational
    ``projection-checkpoint`` schema).
    """
    from ccf.projections.invalidation import fence_state

    table, pk = _table_name(conn, projection_name)
    rows = _dump_rows(conn, archive_id, table, pk)
    fence = fence_state(conn, archive_id=archive_id, projection_name=projection_name)
    head = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if head is None:
        raise CheckpointError(f"archive {archive_id} has no head")
    digest = _snapshot_digest(rows)
    created_at = clock()
    dependency_generations = {projection_name: str(fence.generation)}

    conn.execute(
        """
        INSERT INTO projection_checkpoint (
            archive_id, projection_name, generation, through_commit_sequence,
            source_head_hash, dependency_generations, snapshot_digest,
            snapshot_payload, storage_ref, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (archive_id, projection_name, generation) DO UPDATE SET
            through_commit_sequence = EXCLUDED.through_commit_sequence,
            source_head_hash = EXCLUDED.source_head_hash,
            dependency_generations = EXCLUDED.dependency_generations,
            snapshot_digest = EXCLUDED.snapshot_digest,
            snapshot_payload = EXCLUDED.snapshot_payload,
            storage_ref = EXCLUDED.storage_ref,
            created_at = EXCLUDED.created_at
        """,
        (
            archive_id,
            projection_name,
            fence.generation,
            int(head[0]),
            head[1],
            Jsonb(dependency_generations),
            digest,
            Jsonb(rows),
            f"table:{table}",
            created_at,
        ),
    )
    return {
        "projection_name": projection_name,
        "generation": str(fence.generation),
        "through_commit_sequence": str(int(head[0])),
        "dependency_generations": dependency_generations,
        "snapshot_digest": digest,
        "created_at": created_at,
        "storage_ref": f"table:{table}",
    }


def validate_checkpoint(
    conn, *, archive_id: str, projection_name: str, generation: int
) -> bool:
    """True when the checkpoint's stored snapshot matches its digest."""
    row = conn.execute(
        """
        SELECT snapshot_digest, snapshot_payload FROM projection_checkpoint
        WHERE archive_id = %s AND projection_name = %s AND generation = %s
        """,
        (archive_id, projection_name, generation),
    ).fetchone()
    if row is None:
        raise CheckpointError(
            f"no checkpoint for {projection_name!r} at generation {generation}"
        )
    payload = _normalize(row[1])
    return _snapshot_digest(payload) == row[0]


def recover(conn, *, archive_id: str, projection_name: str) -> dict:
    """Restore a projection from its newest valid checkpoint, then replay.

    Tries checkpoints newest-first; the first whose snapshot validates is
    restored into the projection table, then later commits are replayed by
    a full rebuild from canonical state (equivalent for derived
    projections). With no valid checkpoint, recovery falls back to
    genesis: a full rebuild with no restored rows.
    """
    table, _pk = _table_name(conn, projection_name)
    checkpoints = conn.execute(
        """
        SELECT generation, snapshot_digest, snapshot_payload
        FROM projection_checkpoint
        WHERE archive_id = %s AND projection_name = %s
        ORDER BY generation DESC
        """,
        (archive_id, projection_name),
    ).fetchall()

    restored_from: int | None = None
    for generation, digest, payload in checkpoints:
        normalized = _normalize(payload)
        if _snapshot_digest(normalized) != digest:
            continue  # corrupt checkpoint: fall back to an older one
        _restore_rows(conn, archive_id, table, normalized)
        restored_from = int(generation)
        break

    replayable = projection_name != EMBEDDING
    rebuilt_rows: int | None = None
    if replayable:
        # "Replay later commits": full recompute from canonical state,
        # equivalent for derived projections and always available.
        from ccf.projections.rebuild import rebuild_projection

        rebuilt_rows = rebuild_projection(
            conn, archive_id=archive_id, name=projection_name
        )
    elif restored_from is None:
        raise CheckpointError(
            "no valid checkpoint for 'embedding' and it cannot be rebuilt "
            "from canonical state; caller must re-supply vectors"
        )
    return {
        "projection_name": projection_name,
        "restored_from_generation": restored_from,
        "replayed_to_completion": replayable,
        "rebuilt_rows": rebuilt_rows,
    }
