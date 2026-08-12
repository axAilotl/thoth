"""Full-text search projection (spec 10.1).

A ``tsvector`` document per Record, built from the available (plaintext)
semantic compartment text: ``payload.text`` when present (e.g.
``experience.utterance``), otherwise every string value in the payload,
recursively. Records without available semantic text get no row.

The index is a projection: rebuild recomputes every document from
canonical compartments. The ``'simple'`` tsvector config keeps documents
deterministic across deployments (no dictionary stemming).
"""

from __future__ import annotations

from ccf.projections import FULL_TEXT
from ccf.projections.invalidation import ProjectionStaleError, has_pending
from ccf.projections.rebuild import begin_rebuild, finish_rebuild


def _payload_text(payload: object) -> str:
    """All human-readable text in a payload, deterministically ordered."""
    parts: list[str] = []

    def walk(value: object) -> None:
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, dict):
            for key in sorted(value):
                walk(value[key])
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(payload)
    return "\n".join(part for part in parts if part.strip())


def rebuild(conn, archive_id: str) -> int:
    """Rewrite ``projection_full_text`` from canonical compartments."""
    stamp = begin_rebuild(conn, archive_id=archive_id, projection_name=FULL_TEXT)
    conn.execute(
        "DELETE FROM projection_full_text WHERE archive_id = %s", (archive_id,)
    )
    rows = conn.execute(
        """
        SELECT oh.id, c.plaintext_json -> 'payload' AS payload
        FROM object_header oh
        JOIN compartment c
          ON c.object_id = oh.id AND c.compartment = 'semantic'
        WHERE oh.archive_id = %s AND oh.object_kind = 'record'
          AND c.state = 'plaintext'
        ORDER BY oh.id
        """,
        (archive_id,),
    ).fetchall()
    count = 0
    for object_id, payload in rows:
        text = _payload_text(payload or {})
        if not text:
            continue
        conn.execute(
            """
            INSERT INTO projection_full_text (
                archive_id, object_id, document,
                computed_through_sequence, generation
            ) VALUES (%s, %s, to_tsvector('simple', %s), %s, %s)
            """,
            (
                archive_id,
                object_id,
                text,
                stamp.computed_through_sequence,
                stamp.generation,
            ),
        )
        count += 1
    finish_rebuild(conn, archive_id=archive_id, projection_name=FULL_TEXT)
    return count


def search(
    conn, archive_id: str, query: str, *, limit: int = 50
) -> list[dict]:
    """Full-text search; fails closed while the index has pending work.

    Results are complete only when every queued invalidation has been
    drained, so a search against a dirty index raises
    :class:`ProjectionStaleError` instead of silently missing rows.
    """
    if not query or not query.strip():
        raise ValueError("full-text query must be non-empty")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if has_pending(conn, archive_id=archive_id, projection_name=FULL_TEXT):
        raise ProjectionStaleError(
            "projection 'full_text' has unresolved invalidations; rebuild first"
        )
    rows = conn.execute(
        """
        SELECT object_id,
               ts_rank(document, plainto_tsquery('simple', %s)) AS rank
        FROM projection_full_text
        WHERE archive_id = %s
          AND document @@ plainto_tsquery('simple', %s)
        ORDER BY rank DESC, object_id
        LIMIT %s
        """,
        (query, archive_id, query, limit),
    ).fetchall()
    return [{"object_id": object_id, "rank": float(rank)} for object_id, rank in rows]
