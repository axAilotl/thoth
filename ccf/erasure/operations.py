"""Durable erasure operation state (spec 3.8).

One ``erasure_operation`` row per erasure saga, mirroring
``schemas/operational/erasure-operation.schema.json``. Stage order::

    request -> decision -> block -> destroy -> verify -> receipt
                         +-> failed (from any stage)

Every stage transition commits in the same database transaction as that
stage's effects, so the row is always the honest lower bound of what has
happened: a crash after the ``block`` stage (which destroys plaintext and
salts) but before the receipt resumes from durable state and finishes the
saga — and never reports the content recoverable.
"""

from __future__ import annotations

from psycopg.types.json import Jsonb

from ccf.erasure.errors import ErasureError

SCHEMA_OPERATION = "urn:ccf:schema:0.1.2:operational.erasure-operation"

#: Forward stage order; ``failed`` is reachable from any stage.
STAGE_ORDER = ("decision", "block", "destroy", "verify", "receipt")

TERMINAL_STAGES = frozenset({"receipt", "failed"})

#: The CCF profile the erasure saga runs under (the operational schema's
#: ``profile`` is a profile name, not the assurance level).
OPERATION_PROFILE = "ccf-core-0.1.2"


def next_stage(stage: str) -> str:
    """The stage after ``stage``; fail closed on terminal/unknown stages."""
    if stage not in STAGE_ORDER:
        raise ErasureError(f"erasure stage {stage!r} has no successor")
    following = STAGE_ORDER[STAGE_ORDER.index(stage) + 1]
    return following


def _document(row: dict) -> dict:
    """The operational-schema view of a row."""
    return {
        "operation_id": row["operation_id"],
        "decision_id": row["decision_id"],
        "stage": row["stage"],
        "targets": row["targets"],
        "profile": row["profile"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "last_error": row["last_error"],
    }


def create_operation(
    conn,
    *,
    schemas,
    archive_id: str,
    operation_id: str,
    request_id: str | None,
    decision_id: str,
    plans: list[dict],
    assurance: str,
    lineage_id: str,
    decision: str,
    actor: dict,
    authorized_producers: list[str],
    now: str,
) -> None:
    """Insert the durable row at stage ``decision`` (schema-validated)."""
    if assurance not in ("logical", "storage_verified"):
        raise ErasureError(f"unsupported erasure assurance {assurance!r}")
    targets = [plan["object_id"] for plan in plans]
    row = {
        "operation_id": operation_id,
        "decision_id": decision_id,
        "stage": "decision",
        "targets": targets,
        "profile": OPERATION_PROFILE,
        "started_at": now,
        "updated_at": now,
        "last_error": None,
    }
    schemas.validate(SCHEMA_OPERATION, _document(row), what="erasure operation")
    conn.execute(
        """
        INSERT INTO erasure_operation (
            operation_id, archive_id, request_id, decision_id, stage,
            targets, profile, assurance, plans, purged, receipt_id,
            lineage_id, stage_head_id, decision, actor, authorized_producers,
            started_at, updated_at, last_error
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '[]', NULL, %s, %s, %s,
                  %s, %s, %s, %s, NULL)
        """,
        (
            operation_id,
            archive_id,
            request_id,
            decision_id,
            "decision",
            Jsonb(targets),
            OPERATION_PROFILE,
            assurance,
            Jsonb(plans),
            lineage_id,
            decision_id,
            decision,
            Jsonb(actor),
            Jsonb(list(authorized_producers)),
            now,
            now,
        ),
    )


def load_operation(conn, operation_id: str) -> dict:
    """One operation row; fail closed when unknown."""
    row = conn.execute(
        """
        SELECT operation_id, archive_id, request_id, decision_id, stage,
               targets, profile, assurance, plans, purged, receipt_id,
               lineage_id, stage_head_id, decision, actor,
               authorized_producers, started_at, updated_at, last_error,
               suppression_set
        FROM erasure_operation WHERE operation_id = %s
        """,
        (operation_id,),
    ).fetchone()
    if row is None:
        raise ErasureError(f"unknown erasure operation {operation_id!r}")
    return {
        "operation_id": row[0],
        "archive_id": row[1],
        "request_id": row[2],
        "decision_id": row[3],
        "stage": row[4],
        "targets": list(row[5]),
        "profile": row[6],
        "assurance": row[7],
        "plans": list(row[8]),
        "purged": list(row[9]),
        "receipt_id": row[10],
        "lineage_id": row[11],
        "stage_head_id": row[12],
        "decision": row[13],
        "actor": dict(row[14]),
        "authorized_producers": list(row[15]),
        "started_at": row[16],
        "updated_at": row[17],
        "last_error": row[18],
        "suppression_set": dict(row[19]) if row[19] is not None else None,
    }


def record_suppression_set(
    conn, *, operation_id: str, descriptor: dict, now: str
) -> None:
    """Pin the canonical suppression-set descriptor committed at block."""
    conn.execute(
        """
        UPDATE erasure_operation
        SET suppression_set = %s, updated_at = %s
        WHERE operation_id = %s
        """,
        (Jsonb(descriptor), now, operation_id),
    )


def record_stage_head(conn, *, operation_id: str, stage_head_id: str, now: str) -> None:
    """Advance the decision lineage head after a stage marker commits."""
    conn.execute(
        """
        UPDATE erasure_operation
        SET stage_head_id = %s, updated_at = %s
        WHERE operation_id = %s
        """,
        (stage_head_id, now, operation_id),
    )


def advance_stage(conn, *, schemas, operation_id: str, stage: str, now: str) -> None:
    """Move the durable row to ``stage`` (validated against the schema)."""
    row = load_operation(conn, operation_id)
    updated = {**row, "stage": stage, "updated_at": now, "last_error": None}
    schemas.validate(SCHEMA_OPERATION, _document(updated), what="erasure operation")
    conn.execute(
        """
        UPDATE erasure_operation
        SET stage = %s, updated_at = %s, last_error = NULL
        WHERE operation_id = %s
        """,
        (stage, now, operation_id),
    )


def record_purged(conn, *, operation_id: str, purged: list[dict], now: str) -> None:
    """Merge newly purged controlled stores into the durable row."""
    conn.execute(
        """
        UPDATE erasure_operation
        SET purged = purged || %s, updated_at = %s
        WHERE operation_id = %s
        """,
        (Jsonb(purged), now, operation_id),
    )


def record_receipt(conn, *, operation_id: str, receipt_id: str, now: str) -> None:
    conn.execute(
        """
        UPDATE erasure_operation
        SET receipt_id = %s, updated_at = %s
        WHERE operation_id = %s
        """,
        (receipt_id, now, operation_id),
    )


def record_failure(conn, *, operation_id: str, error: str, now: str) -> None:
    """Mark the operation failed with its error; content stays unrecoverable
    whenever the failure happened at or past the block stage."""
    conn.execute(
        """
        UPDATE erasure_operation
        SET stage = 'failed', last_error = %s, updated_at = %s
        WHERE operation_id = %s
        """,
        (error, now, operation_id),
    )


def lock_operation(conn, operation_id: str) -> dict:
    """Row-lock and load one operation, serializing concurrent workers."""
    conn.execute(
        "SELECT 1 FROM erasure_operation WHERE operation_id = %s FOR UPDATE",
        (operation_id,),
    )
    return load_operation(conn, operation_id)


def pending_operations(conn, archive_id: str) -> list[dict]:
    """Operations not yet at a terminal stage, oldest first (for resume)."""
    rows = conn.execute(
        """
        SELECT operation_id FROM erasure_operation
        WHERE archive_id = %s AND stage NOT IN ('receipt', 'failed')
        ORDER BY started_at ASC, operation_id ASC
        """,
        (archive_id,),
    ).fetchall()
    return [load_operation(conn, row[0]) for row in rows]


def status_of(row: dict) -> dict:
    """Honest status view: content is recoverable only before ``block``.

    The block stage destroys plaintext and salts in the envelope, so any
    stage at or past ``block`` — including a failure — means the archive
    can no longer serve the content (spec 3.8).
    """
    content_recoverable = row["stage"] == "decision"
    return {
        "operation_id": row["operation_id"],
        "decision_id": row["decision_id"],
        "stage": row["stage"],
        "profile": row["assurance"],
        "targets": list(row["targets"]),
        "purged": row["purged"],
        "receipt_id": row["receipt_id"],
        "content_recoverable": content_recoverable,
        "last_error": row["last_error"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
    }
