"""Projection invalidation: fences, queue, and the fast path (spec 10.4).

Canonical mutations do two things synchronously inside the serialized
admission transaction:

1. record invalidation causes in ``projection_invalidation`` (with a
   fine-grained ``target_key`` where one is known), and
2. advance the coarse ``projection.<name>`` generation fence
   (``subject_key = '*'``) — so no stale projection row can be served
   after the mutating commit lands.

Background workers drain the queue by recomputing the projection (phase 5
recomputes projections wholesale; the recorded ``target_key`` is the hook
for future fine-grained affected-descendant computation) and stamping rows
with the fence ``generation`` and the archive head sequence they computed
through.

A projection row is usable only if (spec 10.4)::

    computed_through_sequence >= latest_affecting_sequence
    all dependency generations match
    no unresolved invalidation covers the row

:func:`row_usable` decides this from projection metadata alone — the
fence row plus the invalidation queue — never by replaying history.

This module's fence namespaces are ``projection.*``; the governance stream
owns ``governance.*``. Neither touches the other's rows.
"""

from __future__ import annotations

from dataclasses import dataclass

from ccf.projections import (
    DERIVATION,
    ENTITY_CLUSTER,
    FULL_TEXT,
    LINK_STATE,
    WIKI,
)

FENCE_PREFIX = "projection."
COARSE_SUBJECT = "*"

#: Link types whose admission or disposition affects the entity clusters.
_ENTITY_LINK_TYPES = frozenset({"ccf.same_as", "ccf.distinct_from"})


class ProjectionStaleError(RuntimeError):
    """Raised when a projection row fails the fast-path usability check."""


@dataclass(frozen=True)
class FenceState:
    """Current coarse fence for one projection."""

    generation: int
    changed_at_sequence: int


def fence_namespace(projection_name: str) -> str:
    return FENCE_PREFIX + projection_name


def _effects_for(object_kind: str, type_name: str | None, structural_payload: dict) -> dict[str, str | None]:
    """Map one admitted object to ``{projection_name: target_key}``."""
    effects: dict[str, str | None] = {}
    if object_kind == "link":
        effects[LINK_STATE] = None  # filled by caller with the link id
        if type_name == "ccf.derived_from":
            effects[DERIVATION] = None
        elif type_name in _ENTITY_LINK_TYPES:
            effects[ENTITY_CLUSTER] = None
            effects[WIKI] = None
    elif object_kind == "record":
        if type_name == "lineage.link_disposition":
            target = structural_payload.get("target_link_id")
            effects[LINK_STATE] = target
            effects[DERIVATION] = target
            effects[ENTITY_CLUSTER] = target
            effects[WIKI] = target
        else:
            # Records with semantic text feed the full-text index.
            effects[FULL_TEXT] = None
            if type_name == "semantic.entity_resolution":
                effects[ENTITY_CLUSTER] = None
                effects[WIKI] = None
            elif type_name == "semantic.entity":
                effects[WIKI] = None
    return effects


def record_commit_effects(conn, *, archive_id: str, objects: list, sequence: int) -> None:
    """Synchronous admission hook: queue invalidations and advance fences.

    Called inside the serialized admission transaction (from
    ``ccf.admission.commit_objects``) after object writes and before the
    head advances. ``objects`` are the admitted :class:`ResolvedObject`
    entries (the commit Record itself is excluded — it carries no semantic
    content and feeds no projection).
    """
    causes: dict[str, list[tuple[str | None, str, str]]] = {}
    for obj in objects:
        type_name = obj.structural["content"].get("type")
        payload = obj.structural["content"].get("structural_payload") or {}
        for name, target in _effects_for(obj.object_kind, type_name, payload).items():
            if target is None and obj.object_kind == "link":
                target = obj.object_id
            if target is None and name == FULL_TEXT:
                target = obj.object_id
            causes.setdefault(name, []).append((target, obj.object_kind, obj.object_id))

    for name, entries in causes.items():
        # One queue row per cause object keeps the fine-grained trail.
        for target, kind, object_id in entries:
            conn.execute(
                """
                INSERT INTO projection_invalidation (
                    archive_id, projection_name, target_key, cause_object_kind,
                    cause_object_id, cause_commit_sequence, status
                ) VALUES (%s, %s, %s, %s, %s, %s, 'queued')
                """,
                (archive_id, name, target, kind, object_id, sequence),
            )
        advance_fence(
            conn,
            archive_id=archive_id,
            projection_name=name,
            sequence=sequence,
            cause_object_id=entries[-1][2],
        )


def advance_fence(
    conn, *, archive_id: str, projection_name: str, sequence: int, cause_object_id: str
) -> int:
    """Advance the coarse fence for a projection; returns the new generation."""
    row = conn.execute(
        """
        INSERT INTO generation_fence (
            archive_id, namespace, subject_key, generation,
            changed_at_sequence, direction, cause_object_id
        ) VALUES (%s, %s, %s, 1, %s, 'unknown', %s)
        ON CONFLICT (archive_id, namespace, subject_key) DO UPDATE SET
            generation = generation_fence.generation + 1,
            changed_at_sequence = EXCLUDED.changed_at_sequence,
            cause_object_id = EXCLUDED.cause_object_id
        RETURNING generation
        """,
        (archive_id, fence_namespace(projection_name), COARSE_SUBJECT, sequence, cause_object_id),
    ).fetchone()
    return int(row[0])


def fence_state(conn, *, archive_id: str, projection_name: str) -> FenceState:
    """Current coarse fence; generation 0 / sequence 0 before any mutation."""
    row = conn.execute(
        """
        SELECT generation, changed_at_sequence FROM generation_fence
        WHERE archive_id = %s AND namespace = %s AND subject_key = %s
        """,
        (archive_id, fence_namespace(projection_name), COARSE_SUBJECT),
    ).fetchone()
    if row is None:
        return FenceState(generation=0, changed_at_sequence=0)
    return FenceState(generation=int(row[0]), changed_at_sequence=int(row[1]))


def pending_invalidations(conn, *, archive_id: str, projection_name: str) -> list[dict]:
    """Unresolved invalidation causes for one projection, oldest first."""
    rows = conn.execute(
        """
        SELECT id, target_key, cause_object_kind, cause_object_id,
               cause_commit_sequence
        FROM projection_invalidation
        WHERE archive_id = %s AND projection_name = %s AND status = 'queued'
        ORDER BY cause_commit_sequence ASC
        """,
        (archive_id, projection_name),
    ).fetchall()
    return [
        {
            "id": row[0],
            "target_key": row[1],
            "cause_object_kind": row[2],
            "cause_object_id": row[3],
            "cause_commit_sequence": int(row[4]),
        }
        for row in rows
    ]


def resolve_invalidations(conn, *, archive_id: str, projection_name: str) -> int:
    """Mark every queued invalidation for a projection done; returns count."""
    row = conn.execute(
        """
        UPDATE projection_invalidation
        SET status = 'done', resolved_at = now()
        WHERE archive_id = %s AND projection_name = %s AND status = 'queued'
        """,
        (archive_id, projection_name),
    )
    return row.rowcount


def has_pending(conn, *, archive_id: str, projection_name: str) -> bool:
    """True when any unresolved invalidation exists for the projection."""
    row = conn.execute(
        """
        SELECT 1 FROM projection_invalidation
        WHERE archive_id = %s AND projection_name = %s
          AND status IN ('queued', 'running')
        LIMIT 1
        """,
        (archive_id, projection_name),
    ).fetchone()
    return row is not None


def row_usable(
    conn,
    *,
    archive_id: str,
    projection_name: str,
    computed_through_sequence: int,
    generation: int,
    target_key: str | None = None,
) -> bool:
    """Fast-path usability check (spec 10.4) from projection metadata only.

    A row is usable iff its computation covers the latest affecting commit
    (``computed_through_sequence >= latest_affecting_sequence``), its
    dependency generation still matches the fence, and no unresolved
    invalidation covers it (projection-wide rows, or the row's own
    ``target_key`` when given).
    """
    fence = fence_state(conn, archive_id=archive_id, projection_name=projection_name)
    if computed_through_sequence < fence.changed_at_sequence:
        return False
    if generation != fence.generation:
        return False
    row = conn.execute(
        """
        SELECT 1 FROM projection_invalidation
        WHERE archive_id = %s AND projection_name = %s
          AND status IN ('queued', 'running')
          AND (target_key IS NULL OR target_key = %s)
        LIMIT 1
        """,
        (archive_id, projection_name, target_key),
    ).fetchone()
    return row is None


def require_usable(conn, *, archive_id: str, projection_name: str, **row_metadata) -> None:
    """Fail closed when a projection row is stale."""
    if not row_usable(
        conn, archive_id=archive_id, projection_name=projection_name, **row_metadata
    ):
        raise ProjectionStaleError(
            f"projection {projection_name!r} is stale: rebuild or drain "
            "invalidations before serving this row"
        )
