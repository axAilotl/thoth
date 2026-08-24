"""Shared rebuild protocol for table projections.

Every table projection follows the same rebuild contract:

1. read the archive head sequence and the projection's coarse fence
   generation *inside the rebuild transaction*;
2. rewrite the projection table from canonical state, stamping every row
   with ``computed_through_sequence = head sequence`` and
   ``generation = fence generation``;
3. resolve the projection's queued invalidations.

Because rows carry the fence generation they were computed under, the
fast-path check (:func:`ccf.projections.invalidation.row_usable`) refuses
them as soon as admission advances the fence again — a rebuild is a
full-table rewrite, so either every row matches the fence or none do.
"""

from __future__ import annotations

from dataclasses import dataclass

from ccf import projections
from ccf.projections import invalidation


class RebuildError(RuntimeError):
    """Raised when a projection rebuild cannot proceed."""


@dataclass(frozen=True)
class ProjectionStamp:
    """Metadata every rebuilt row carries."""

    generation: int
    computed_through_sequence: int


def begin_rebuild(conn, *, archive_id: str, projection_name: str) -> ProjectionStamp:
    """Read the head sequence and fence generation for row stamping."""
    row = conn.execute(
        "SELECT sequence FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if row is None:
        raise RebuildError(f"archive {archive_id} has no head")
    fence = invalidation.fence_state(
        conn, archive_id=archive_id, projection_name=projection_name
    )
    return ProjectionStamp(
        generation=fence.generation,
        computed_through_sequence=int(row[0]),
    )


def finish_rebuild(conn, *, archive_id: str, projection_name: str) -> int:
    """Resolve queued invalidations after a successful table rewrite."""
    return invalidation.resolve_invalidations(
        conn, archive_id=archive_id, projection_name=projection_name
    )


def rebuild_projection(conn, *, archive_id: str, name: str) -> int:
    """Rebuild one table projection from canonical state; returns row count.

    The embedding projection is not rebuildable from canonical state —
    vectors are caller-supplied (spec 10.1) — so it is refused here rather
    than silently zeroed.
    """
    if name == projections.EMBEDDING:
        raise RebuildError(
            "projection 'embedding' stores caller-supplied vectors; it cannot "
            "be rebuilt from canonical state"
        )
    if name == projections.LINK_STATE:
        from ccf.projections import links

        return links.rebuild(conn, archive_id)
    if name == projections.DERIVATION:
        from ccf.projections import derivation

        return derivation.rebuild(conn, archive_id)
    if name == projections.ENTITY_CLUSTER:
        from ccf.projections import entities

        return entities.rebuild(conn, archive_id)
    if name == projections.FULL_TEXT:
        from ccf.projections import fulltext

        return fulltext.rebuild(conn, archive_id)
    raise RebuildError(f"unknown projection {name!r}")


def rebuild_all(conn, *, archive_id: str) -> dict[str, int]:
    """Rebuild every canonically rebuildable table projection."""
    return {
        name: rebuild_projection(conn, archive_id=archive_id, name=name)
        for name in (
            projections.LINK_STATE,
            projections.DERIVATION,
            projections.ENTITY_CLUSTER,
            projections.FULL_TEXT,
        )
    }
