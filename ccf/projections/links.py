"""Current Link state projection (spec 8.4).

Links are immutable; their current use is governed by
``lineage.link_disposition`` Records admitted under compare-and-swap
lineages. This projection folds each Link together with the latest
disposition head targeting it into one current-state row:

- no disposition (or a ``restore`` head) → ``active``;
- ``retract`` / ``supersede`` / ``tombstone`` → the matching deactivated
  state (``tombstone`` is terminal);
- ``invalidate_selector`` → still ``active`` with
  ``selector_available = false``.

Rebuild reads only canonical state: object headers, plaintext structural
compartments, and ``lineage_head``. Rebuilding after destroying the table
loses nothing (spec 8.7).
"""

from __future__ import annotations

from ccf.projections import LINK_STATE
from ccf.projections.invalidation import require_usable
from ccf.projections.rebuild import begin_rebuild, finish_rebuild

#: Disposition action -> (state, selector_available).
_ACTION_STATES = {
    "retract": ("retracted", True),
    "restore": ("active", True),
    "supersede": ("superseded", True),
    "invalidate_selector": ("active", False),
    "tombstone": ("tombstoned", True),
}


def _disposition_heads(conn, archive_id: str) -> dict[str, dict]:
    """Latest admitted disposition per target Link.

    Mirrors ``ccf.lineage.current_link_actions`` but also carries the
    disposition Record and replacement Link so the projection can point at
    its canonical evidence.
    """
    rows = conn.execute(
        """
        SELECT c.plaintext_json -> 'structural_payload' ->> 'target_link_id',
               c.plaintext_json -> 'structural_payload' ->> 'action',
               c.plaintext_json -> 'structural_payload' ->> 'replacement_link_id',
               lh.head_record_id
        FROM lineage_head lh
        JOIN compartment c
          ON c.object_id = lh.head_record_id AND c.compartment = 'structural'
        WHERE lh.archive_id = %s
          AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = 'lineage.link_disposition'
        ORDER BY lh.head_commit_sequence ASC
        """,
        (archive_id,),
    ).fetchall()
    dispositions: dict[str, dict] = {}
    for target, action, replacement, head_record_id in rows:
        if target:
            dispositions[target] = {
                "action": action,
                "replacement_link_id": replacement,
                "disposition_record_id": head_record_id,
            }
    return dispositions


def rebuild(conn, archive_id: str) -> int:
    """Rewrite ``projection_link_state`` from canonical state."""
    stamp = begin_rebuild(conn, archive_id=archive_id, projection_name=LINK_STATE)
    conn.execute(
        "DELETE FROM projection_link_state WHERE archive_id = %s", (archive_id,)
    )
    links = conn.execute(
        """
        SELECT oh.id,
               c.plaintext_json ->> 'type',
               c.plaintext_json ->> 'from_id',
               c.plaintext_json ->> 'to_id'
        FROM object_header oh
        JOIN compartment c
          ON c.object_id = oh.id AND c.compartment = 'structural'
        WHERE oh.archive_id = %s AND oh.object_kind = 'link'
          AND c.state = 'plaintext'
        ORDER BY oh.id
        """,
        (archive_id,),
    ).fetchall()
    dispositions = _disposition_heads(conn, archive_id)
    for link_id, type_name, from_id, to_id in links:
        disposition = dispositions.get(link_id)
        if disposition is None:
            state, selector_available = "active", True
            disposition_record_id = replacement_link_id = None
        else:
            action = disposition["action"]
            if action not in _ACTION_STATES:
                raise ValueError(f"unknown link disposition action {action!r}")
            state, selector_available = _ACTION_STATES[action]
            disposition_record_id = disposition["disposition_record_id"]
            replacement_link_id = disposition["replacement_link_id"]
        conn.execute(
            """
            INSERT INTO projection_link_state (
                archive_id, link_id, type, from_id, to_id, state,
                selector_available, disposition_record_id, replacement_link_id,
                computed_through_sequence, generation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                archive_id,
                link_id,
                type_name,
                from_id,
                to_id,
                state,
                selector_available,
                disposition_record_id,
                replacement_link_id,
                stamp.computed_through_sequence,
                stamp.generation,
            ),
        )
    finish_rebuild(conn, archive_id=archive_id, projection_name=LINK_STATE)
    return len(links)


def get_link_state(conn, archive_id: str, link_id: str, *, require_fresh: bool = True) -> dict | None:
    """One Link's current state; fail closed on staleness by default."""
    row = conn.execute(
        """
        SELECT link_id, type, from_id, to_id, state, selector_available,
               disposition_record_id, replacement_link_id,
               computed_through_sequence, generation
        FROM projection_link_state
        WHERE archive_id = %s AND link_id = %s
        """,
        (archive_id, link_id),
    ).fetchone()
    if row is None:
        return None
    if require_fresh:
        require_usable(
            conn,
            archive_id=archive_id,
            projection_name=LINK_STATE,
            computed_through_sequence=int(row[8]),
            generation=int(row[9]),
            target_key=link_id,
        )
    return {
        "link_id": row[0],
        "type": row[1],
        "from_id": row[2],
        "to_id": row[3],
        "state": row[4],
        "selector_available": row[5],
        "disposition_record_id": row[6],
        "replacement_link_id": row[7],
        "computed_through_sequence": int(row[8]),
        "generation": int(row[9]),
    }


def active_links(conn, archive_id: str, *, link_type: str | None = None) -> list[dict]:
    """All currently active Links, optionally of one type.

    Raises :class:`ProjectionStaleError` when the projection has pending
    invalidations — a listing is only meaningful when fully drained.
    """
    from ccf.projections.invalidation import ProjectionStaleError, has_pending

    if has_pending(conn, archive_id=archive_id, projection_name=LINK_STATE):
        raise ProjectionStaleError(
            "projection 'link_state' has unresolved invalidations; rebuild first"
        )
    query = """
        SELECT link_id, type, from_id, to_id FROM projection_link_state
        WHERE archive_id = %s AND state = 'active'
    """
    params: list = [archive_id]
    if link_type is not None:
        query += " AND type = %s"
        params.append(link_type)
    query += " ORDER BY link_id"
    rows = conn.execute(query, params).fetchall()
    return [
        {"link_id": r[0], "type": r[1], "from_id": r[2], "to_id": r[3]} for r in rows
    ]
