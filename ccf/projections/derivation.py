"""Active ``derived_from`` graph: CTE baseline + closure (spec 8.6, 10.3).

A ``ccf.derived_from`` Link points from the derived object (``from_id``,
the descendant) to its source evidence (``to_id``, the ancestor). Active
Links — those not deactivated by a current disposition head — form a DAG
(admission cycle-checks every new or restored edge).

:func:`ancestors_of` / :func:`descendants_of` are the correctness
baseline: plain recursive CTEs over canonical state. The
``projection_derivation_closure`` table is a rebuildable acceleration
storing every active (ancestor, descendant) pair with its minimum depth
and active path count, stamped with computation generation and commit
sequence. If the closure ever exceeds deployment bounds, depth-capped
materialization plus on-demand CTE traversal degrades latency, never
correctness (spec 10.3).
"""

from __future__ import annotations

from ccf.projections import DERIVATION
from ccf.projections.invalidation import (
    ProjectionStaleError,
    has_pending,
    require_usable,
)
from ccf.projections.rebuild import begin_rebuild, finish_rebuild

#: Active derived_from edges (from_id = descendant, to_id = ancestor) as
#: seen by both the CTE baseline and the closure rebuild. Disposition
#: heads deactivate retracted/superseded/tombstoned Links.
_ACTIVE_EDGES_SQL = """
    SELECT c.plaintext_json ->> 'from_id' AS from_id,
           c.plaintext_json ->> 'to_id'   AS to_id,
           oh.id                          AS link_id
    FROM object_header oh
    JOIN compartment c
      ON c.object_id = oh.id AND c.compartment = 'structural'
    WHERE oh.archive_id = %s
      AND oh.object_kind = 'link'
      AND c.state = 'plaintext'
      AND c.plaintext_json ->> 'type' = 'ccf.derived_from'
"""


def _active_edges(conn, archive_id: str) -> list[tuple[str, str]]:
    from ccf.lineage import DEACTIVATING_ACTIONS, current_link_actions

    actions = current_link_actions(conn, archive_id)
    edges = []
    for from_id, to_id, link_id in conn.execute(_ACTIVE_EDGES_SQL, (archive_id,)):
        if actions.get(link_id) in DEACTIVATING_ACTIONS:
            continue
        if from_id and to_id:
            edges.append((from_id, to_id))
    return edges


def ancestors_of(conn, archive_id: str, object_id: str) -> dict[str, int]:
    """All active derivation ancestors of an object: id -> minimum depth.

    Correctness baseline (recursive CTE over canonical state).
    """
    rows = conn.execute(
        """
        WITH RECURSIVE active_edge AS (
            SELECT c.plaintext_json ->> 'from_id' AS from_id,
                   c.plaintext_json ->> 'to_id'   AS to_id,
                   oh.id AS link_id
            FROM object_header oh
            JOIN compartment c
              ON c.object_id = oh.id AND c.compartment = 'structural'
            WHERE oh.archive_id = %s AND oh.object_kind = 'link'
              AND c.state = 'plaintext'
              AND c.plaintext_json ->> 'type' = 'ccf.derived_from'
        ),
        deactivated AS (
            SELECT target FROM (
                SELECT DISTINCT ON (c.plaintext_json -> 'structural_payload' ->> 'target_link_id')
                       c.plaintext_json -> 'structural_payload' ->> 'target_link_id' AS target,
                       c.plaintext_json -> 'structural_payload' ->> 'action' AS action
                FROM lineage_head lh
                JOIN compartment c
                  ON c.object_id = lh.head_record_id AND c.compartment = 'structural'
                WHERE lh.archive_id = %s
                  AND c.state = 'plaintext'
                  AND c.plaintext_json ->> 'type' = 'lineage.link_disposition'
                ORDER BY target, lh.head_commit_sequence DESC
            ) latest
            WHERE latest.action IN ('retract', 'supersede', 'tombstone')
        ),
        reach AS (
            SELECT e.to_id AS ancestor, 1 AS depth, ARRAY[e.from_id, e.to_id] AS seen
            FROM active_edge e
            WHERE e.from_id = %s
              AND e.link_id NOT IN (SELECT target FROM deactivated)
            UNION ALL
            SELECT e.to_id, r.depth + 1, r.seen || e.to_id
            FROM reach r
            JOIN active_edge e ON e.from_id = r.ancestor
            WHERE NOT e.to_id = ANY(r.seen)
              AND e.link_id NOT IN (SELECT target FROM deactivated)
        )
        SELECT ancestor, min(depth) FROM reach GROUP BY ancestor
        """,
        (archive_id, archive_id, object_id),
    ).fetchall()
    return {ancestor: int(depth) for ancestor, depth in rows}


def descendants_of(conn, archive_id: str, object_id: str) -> dict[str, int]:
    """All active derivation descendants of an object: id -> minimum depth."""
    rows = conn.execute(
        """
        WITH RECURSIVE active_edge AS (
            SELECT c.plaintext_json ->> 'from_id' AS from_id,
                   c.plaintext_json ->> 'to_id'   AS to_id,
                   oh.id AS link_id
            FROM object_header oh
            JOIN compartment c
              ON c.object_id = oh.id AND c.compartment = 'structural'
            WHERE oh.archive_id = %s AND oh.object_kind = 'link'
              AND c.state = 'plaintext'
              AND c.plaintext_json ->> 'type' = 'ccf.derived_from'
        ),
        deactivated AS (
            SELECT target FROM (
                SELECT DISTINCT ON (c.plaintext_json -> 'structural_payload' ->> 'target_link_id')
                       c.plaintext_json -> 'structural_payload' ->> 'target_link_id' AS target,
                       c.plaintext_json -> 'structural_payload' ->> 'action' AS action
                FROM lineage_head lh
                JOIN compartment c
                  ON c.object_id = lh.head_record_id AND c.compartment = 'structural'
                WHERE lh.archive_id = %s
                  AND c.state = 'plaintext'
                  AND c.plaintext_json ->> 'type' = 'lineage.link_disposition'
                ORDER BY target, lh.head_commit_sequence DESC
            ) latest
            WHERE latest.action IN ('retract', 'supersede', 'tombstone')
        ),
        reach AS (
            SELECT e.from_id AS descendant, 1 AS depth, ARRAY[e.to_id, e.from_id] AS seen
            FROM active_edge e
            WHERE e.to_id = %s
              AND e.link_id NOT IN (SELECT target FROM deactivated)
            UNION ALL
            SELECT e.from_id, r.depth + 1, r.seen || e.from_id
            FROM reach r
            JOIN active_edge e ON e.to_id = r.descendant
            WHERE NOT e.from_id = ANY(r.seen)
              AND e.link_id NOT IN (SELECT target FROM deactivated)
        )
        SELECT descendant, min(depth) FROM reach GROUP BY descendant
        """,
        (archive_id, archive_id, object_id),
    ).fetchall()
    return {descendant: int(depth) for descendant, depth in rows}


def rebuild(conn, archive_id: str) -> int:
    """Rewrite the derivation closure from the active edge set.

    Full recursive computation over active edges; the per-edge cycle guard
    (``seen``) makes the traversal total even if canonical state were ever
    corrupted into a cycle.
    """
    stamp = begin_rebuild(conn, archive_id=archive_id, projection_name=DERIVATION)
    conn.execute(
        "DELETE FROM projection_derivation_closure WHERE archive_id = %s",
        (archive_id,),
    )
    edges = _active_edges(conn, archive_id)
    count = 0
    if edges:
        conn.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS _ccf_closure_edges (
                from_id text NOT NULL,
                to_id   text NOT NULL
            ) ON COMMIT DROP
            """
        )
        conn.execute("DELETE FROM _ccf_closure_edges")
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO _ccf_closure_edges (from_id, to_id) VALUES (%s, %s)",
                edges,
            )
        rows = conn.execute(
            """
            WITH RECURSIVE paths AS (
                SELECT from_id AS descendant, to_id AS ancestor, 1 AS depth,
                       ARRAY[from_id, to_id] AS seen
                FROM _ccf_closure_edges
                UNION ALL
                SELECT p.descendant, e.to_id, p.depth + 1, p.seen || e.to_id
                FROM paths p
                JOIN _ccf_closure_edges e ON e.from_id = p.ancestor
                WHERE NOT e.to_id = ANY(p.seen)
            )
            SELECT descendant, ancestor, min(depth), count(*)
            FROM paths
            GROUP BY descendant, ancestor
            """
        ).fetchall()
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO projection_derivation_closure (
                    archive_id, ancestor_id, descendant_id, minimum_depth,
                    active_path_count, computed_through_sequence, generation
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        archive_id,
                        ancestor,
                        descendant,
                        int(min_depth),
                        int(path_count),
                        stamp.computed_through_sequence,
                        stamp.generation,
                    )
                    for descendant, ancestor, min_depth, path_count in rows
                ],
            )
        count = len(rows)
    finish_rebuild(conn, archive_id=archive_id, projection_name=DERIVATION)
    return count


def closure_pairs(conn, archive_id: str) -> dict[tuple[str, str], dict]:
    """Every closure pair: ``(ancestor, descendant) -> metadata``.

    Raises :class:`ProjectionStaleError` when the closure has unresolved
    invalidations.
    """
    if has_pending(conn, archive_id=archive_id, projection_name=DERIVATION):
        raise ProjectionStaleError(
            "projection 'derivation' has unresolved invalidations; rebuild first"
        )
    rows = conn.execute(
        """
        SELECT ancestor_id, descendant_id, minimum_depth, active_path_count
        FROM projection_derivation_closure
        WHERE archive_id = %s
        """,
        (archive_id,),
    ).fetchall()
    return {
        (ancestor, descendant): {
            "minimum_depth": int(min_depth),
            "active_path_count": int(path_count),
        }
        for ancestor, descendant, min_depth, path_count in rows
    }


def closure_ancestors_of(conn, archive_id: str, object_id: str) -> dict[str, int]:
    """Acceleration-path ancestors from the closure, fast-path checked."""
    rows = conn.execute(
        """
        SELECT ancestor_id, minimum_depth, computed_through_sequence, generation
        FROM projection_derivation_closure
        WHERE archive_id = %s AND descendant_id = %s
        """,
        (archive_id, object_id),
    ).fetchall()
    result: dict[str, int] = {}
    for ancestor, depth, through_seq, generation in rows:
        require_usable(
            conn,
            archive_id=archive_id,
            projection_name=DERIVATION,
            computed_through_sequence=int(through_seq),
            generation=int(generation),
        )
        result[ancestor] = int(depth)
    return result
