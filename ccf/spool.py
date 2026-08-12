"""Durable signed producer-batch spool (spec sections 6.2-6.3).

The spool is the producer's durable local state: every signed batch is
written to Postgres in the same transaction that advances the producer
head, so batch chain state survives restarts and is replay-safe. A batch
row carries the archive's result once admission completes; replaying an
already-answered batch returns the stored result instead of re-admitting.

Provisional objects (spec section 6.3) are submissions in spooled batches
that have no archive admission order yet. They keep stable IDs and
submission hashes and are always reported with ``status: "provisional"`` —
never as canonically committed.
"""

from __future__ import annotations

from ccf.hashing import decode_b64url


class SpoolError(RuntimeError):
    """Raised when spool state is inconsistent with the producer chain."""


TERMINAL_STATUSES: frozenset[str] = frozenset({"committed", "partial", "rejected", "conflict"})


def spool_batch(conn, batch: dict, *, spooled_at: str) -> None:
    """Insert a signed batch and advance the producer head atomically.

    The caller holds an open transaction; both writes commit or neither
    does. Re-spooling an identical batch ID is a no-op only when the stored
    hash matches (idempotent retry after a crash between spool and send).
    """
    existing = conn.execute(
        "SELECT batch_hash FROM producer_batch WHERE batch_id = %s",
        (batch["batch_id"],),
    ).fetchone()
    if existing is not None:
        if existing[0] != batch["batch_hash"]:
            raise SpoolError(
                f"batch {batch['batch_id']} already spooled with a different hash"
            )
        return

    conn.execute(
        """
        INSERT INTO producer_batch (
            batch_id, producer_id, producer_sequence, previous_batch_hash,
            credential_id, created_at, semantic_catalog_root, batch_hash,
            signature, batch_json, status, spooled_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'queued', %s)
        """,
        (
            batch["batch_id"],
            batch["producer_id"],
            int(batch["producer_sequence"]),
            batch["previous_batch_hash"],
            batch["credential_id"],
            batch["created_at"],
            batch["semantic_catalog_root"],
            batch["batch_hash"],
            decode_b64url(batch["signature"]),
            _jsonb(batch),
            spooled_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO producer_head (producer_id, producer_sequence, batch_hash, credential_id, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (producer_id) DO UPDATE SET
            producer_sequence = EXCLUDED.producer_sequence,
            batch_hash = EXCLUDED.batch_hash,
            credential_id = EXCLUDED.credential_id,
            updated_at = EXCLUDED.updated_at
        """,
        (
            batch["producer_id"],
            int(batch["producer_sequence"]),
            batch["batch_hash"],
            batch["credential_id"],
            spooled_at,
        ),
    )


def lock_producer_head(conn, producer_id: str) -> dict | None:
    """Read and row-lock the producer head inside the current transaction."""
    row = conn.execute(
        """
        SELECT producer_sequence, batch_hash, credential_id FROM producer_head
        WHERE producer_id = %s FOR UPDATE
        """,
        (producer_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "producer_sequence": int(row[0]),
        "batch_hash": row[1],
        "credential_id": row[2],
    }


def load_batch(conn, batch_id: str) -> dict | None:
    """Load a spooled batch document by ID."""
    row = conn.execute(
        "SELECT batch_json FROM producer_batch WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    return row[0] if row else None


def stored_batch_result(conn, batch_id: str) -> dict | None:
    """Return the stored terminal admission result for a batch, if any."""
    row = conn.execute(
        "SELECT status, result_json FROM producer_batch WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    if row is None or row[0] not in TERMINAL_STATUSES or row[1] is None:
        return None
    return row[1]


def record_batch_result(
    conn,
    batch_id: str,
    *,
    status: str,
    committed_sequence: int | None,
    result: dict,
) -> None:
    """Attach the archive's outcome to a spooled batch (idempotent)."""
    if status not in TERMINAL_STATUSES and status not in ("queued", "verifying"):
        raise SpoolError(f"invalid batch status: {status!r}")
    conn.execute(
        """
        UPDATE producer_batch
        SET status = %s, committed_sequence = %s, result_json = %s
        WHERE batch_id = %s
        """,
        (status, committed_sequence, _jsonb(result), batch_id),
    )


def pending_batches(conn, producer_id: str | None = None) -> list[dict]:
    """Spooled batches without a terminal archive result, in chain order."""
    if producer_id is None:
        rows = conn.execute(
            """
            SELECT batch_json FROM producer_batch
            WHERE status IN ('queued', 'verifying')
            ORDER BY producer_id, producer_sequence ASC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT batch_json FROM producer_batch
            WHERE producer_id = %s AND status IN ('queued', 'verifying')
            ORDER BY producer_sequence ASC
            """,
            (producer_id,),
        ).fetchall()
    return [row[0] for row in rows]


def provisional_objects(conn, producer_id: str) -> list[dict]:
    """Submissions in uncommitted batches, visibly marked provisional.

    Each entry carries the stable producer-generated object ID and the
    producer submission hash — never admission coordinates, which do not
    exist until canonical admission (spec section 6.3).
    """
    from ccf.hashing import submission_hash

    objects: list[dict] = []
    for batch in pending_batches(conn, producer_id):
        for kind in ("records", "links", "blobs"):
            for submission in batch[kind]:
                objects.append(
                    {
                        "object_id": submission["id"],
                        "object_kind": submission["submission_kind"],
                        "submission_hash": submission_hash(submission),
                        "batch_id": batch["batch_id"],
                        "producer_sequence": batch["producer_sequence"],
                        "status": "provisional",
                    }
                )
    return objects


def _jsonb(value):
    from psycopg.types.json import Jsonb

    return Jsonb(value)
