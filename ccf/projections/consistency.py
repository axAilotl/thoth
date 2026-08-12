"""Cross-projection consistency (spec 10.6).

A request that reads multiple projections MUST pin one archive head and
one compatible dependency-generation vector; mixing independently current
projections without a snapshot contract is nonconformant for
consequential results.

:func:`pin_snapshot` captures that contract: the archive head (sequence +
commit hash) plus the coarse fence generation of every table projection.
:func:`require_current` then refuses a consequential read when the head
moved, a fence advanced, or invalidations remain unresolved — the caller
rebuilds and re-pins instead of serving a mixed view.
"""

from __future__ import annotations

from dataclasses import dataclass

from ccf.projections import TABLE_PROJECTIONS
from ccf.projections.invalidation import (
    ProjectionStaleError,
    fence_state,
    has_pending,
)


class SnapshotError(RuntimeError):
    """Raised when a pinned snapshot cannot be established."""


@dataclass(frozen=True)
class SnapshotPin:
    """One archive head plus one dependency-generation vector."""

    archive_id: str
    head_sequence: int
    head_commit_hash: str
    generations: dict[str, int]


def pin_snapshot(
    conn, archive_id: str, *, projections: tuple[str, ...] = TABLE_PROJECTIONS
) -> SnapshotPin:
    """Pin the current archive head and projection generation vector."""
    row = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if row is None:
        raise SnapshotError(f"archive {archive_id} has no head")
    generations = {
        name: fence_state(conn, archive_id=archive_id, projection_name=name).generation
        for name in projections
    }
    return SnapshotPin(
        archive_id=archive_id,
        head_sequence=int(row[0]),
        head_commit_hash=row[1],
        generations=generations,
    )


def require_current(conn, pin: SnapshotPin, projection_name: str) -> None:
    """Fail closed unless the pinned snapshot still covers this projection.

    Checks metadata only: archive head unchanged, the projection's fence
    generation unchanged since the pin, and no unresolved invalidations.
    """
    row = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (pin.archive_id,),
    ).fetchone()
    if row is None or int(row[0]) != pin.head_sequence or row[1] != pin.head_commit_hash:
        raise ProjectionStaleError(
            "archive head advanced past the pinned snapshot; re-pin before "
            "serving consequential results"
        )
    fence = fence_state(
        conn, archive_id=pin.archive_id, projection_name=projection_name
    )
    pinned = pin.generations.get(projection_name)
    if pinned is None:
        raise ProjectionStaleError(
            f"projection {projection_name!r} is not part of the pinned "
            "generation vector"
        )
    if fence.generation != pinned:
        raise ProjectionStaleError(
            f"projection {projection_name!r} fence advanced from {pinned} to "
            f"{fence.generation} since the snapshot was pinned"
        )
    if has_pending(conn, archive_id=pin.archive_id, projection_name=projection_name):
        raise ProjectionStaleError(
            f"projection {projection_name!r} has unresolved invalidations"
        )
