"""Vector projection: caller-supplied embeddings via pgvector (spec 10.1).

Embedding *generation* is out of scope for phase 5: callers compute
vectors elsewhere and store them here. The projection owns storage,
nearest-neighbour query, and metadata (the fence generation and commit
sequence the row was written under).

Records are immutable once admitted, so a stored embedding does not drift
with later admissions; erasure-driven purge is a phase 7 concern.

pgvector is an optional dependency of the deployment: when the ``vector``
extension is unavailable, migration 0002 skips the table and every
function here fails closed with :class:`VectorSupportError`.
"""

from __future__ import annotations

import math

from ccf.projections import EMBEDDING


class VectorSupportError(RuntimeError):
    """Raised when pgvector support is missing or a vector is malformed."""


def _require_support(conn) -> None:
    row = conn.execute(
        "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
    ).fetchone()
    if row is None:
        raise VectorSupportError(
            "pgvector extension is not installed; the embedding projection "
            "is unavailable in this deployment"
        )
    row = conn.execute(
        "SELECT 1 FROM pg_tables WHERE tablename = 'projection_embedding' "
        "AND schemaname = current_schema()"
    ).fetchone()
    if row is None:
        raise VectorSupportError(
            "projection_embedding table is missing; re-run CCF migrations"
        )


def _format_vector(vector: list[float] | tuple[float, ...]) -> str:
    if not vector:
        raise VectorSupportError("embedding vector must be non-empty")
    values = []
    for component in vector:
        value = float(component)
        if not math.isfinite(value):
            raise VectorSupportError("embedding components must be finite")
        values.append(repr(value))
    return "[" + ",".join(values) + "]"


def put_embedding(
    conn,
    *,
    archive_id: str,
    object_id: str,
    model_id: str,
    vector: list[float] | tuple[float, ...],
) -> None:
    """Store a caller-supplied embedding for an admitted object.

    Stamped with the current archive head and fence generation so the
    fast-path metadata check applies unchanged.
    """
    _require_support(conn)
    literal = _format_vector(vector)
    exists = conn.execute(
        "SELECT 1 FROM object_header WHERE id = %s AND archive_id = %s",
        (object_id, archive_id),
    ).fetchone()
    if exists is None:
        raise VectorSupportError(
            f"cannot embed unknown object {object_id}: not admitted to {archive_id}"
        )
    row = conn.execute(
        "SELECT sequence FROM archive_head WHERE archive_id = %s", (archive_id,)
    ).fetchone()
    through_sequence = int(row[0])
    conn.execute(
        """
        INSERT INTO projection_embedding (
            archive_id, object_id, model_id, embedding,
            computed_through_sequence, generation
        ) VALUES (%s, %s, %s, %s::vector, %s,
                  (SELECT COALESCE((SELECT generation FROM generation_fence
                   WHERE archive_id = %s AND namespace = 'projection.embedding'
                     AND subject_key = '*'), 0)))
        ON CONFLICT (archive_id, object_id, model_id) DO UPDATE SET
            embedding = EXCLUDED.embedding,
            computed_through_sequence = EXCLUDED.computed_through_sequence,
            generation = EXCLUDED.generation
        """,
        (archive_id, object_id, model_id, literal, through_sequence, archive_id),
    )


def get_embedding(
    conn, *, archive_id: str, object_id: str, model_id: str
) -> list[float] | None:
    """Round-trip one stored embedding."""
    _require_support(conn)
    row = conn.execute(
        """
        SELECT embedding::text FROM projection_embedding
        WHERE archive_id = %s AND object_id = %s AND model_id = %s
        """,
        (archive_id, object_id, model_id),
    ).fetchone()
    if row is None:
        return None
    return [float(component) for component in row[0].strip("[]").split(",")]


def nearest(
    conn,
    *,
    archive_id: str,
    model_id: str,
    query_vector: list[float] | tuple[float, ...],
    limit: int = 10,
) -> list[dict]:
    """Nearest neighbours by L2 distance for one model."""
    _require_support(conn)
    if limit <= 0:
        raise ValueError("limit must be positive")
    literal = _format_vector(query_vector)
    rows = conn.execute(
        """
        SELECT object_id, embedding <-> %s::vector AS distance
        FROM projection_embedding
        WHERE archive_id = %s AND model_id = %s
        ORDER BY embedding <-> %s::vector
        LIMIT %s
        """,
        (literal, archive_id, model_id, literal, limit),
    ).fetchall()
    return [
        {"object_id": object_id, "distance": float(distance)}
        for object_id, distance in rows
    ]
