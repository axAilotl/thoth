"""Governance generation fences (spec section 9.5).

A governance mutation atomically advances the relevant ``governance.*``
fences in the same transaction as admission. Cached decisions record the
generation vector they were computed against and are served only while
every generation matches, so a tightening (or unknown-direction) change
blocks stale allows immediately; widening changes are conservatively
recomputed instead of served from cache.

Fence names are namespaced ``governance.*``; projection fences live in the
same table under ``projection.*`` and are owned by another stream.
"""

from __future__ import annotations

FENCE_POLICY = "governance.policy"
FENCE_CONSENT = "governance.consent"
FENCE_RESTRICTION = "governance.restriction"
FENCE_OBJECTION = "governance.objection"
FENCE_LEGAL_BASIS = "governance.legal_basis"
FENCE_LEGAL_HOLD = "governance.legal_hold"
#: ``ccf.governed_by`` policy bindings.
FENCE_LINKS = "governance.links"
#: ``ccf.same_as`` / ``ccf.distinct_from`` identity resolution.
FENCE_IDENTITY = "governance.identity"

ALL_FENCES: tuple[str, ...] = (
    FENCE_POLICY,
    FENCE_CONSENT,
    FENCE_RESTRICTION,
    FENCE_OBJECTION,
    FENCE_LEGAL_BASIS,
    FENCE_LEGAL_HOLD,
    FENCE_LINKS,
    FENCE_IDENTITY,
)

#: Governance Record types and the fence their head transitions advance.
RECORD_TYPE_FENCES: dict[str, str] = {
    "governance.policy": FENCE_POLICY,
    "governance.consent": FENCE_CONSENT,
    "governance.restriction": FENCE_RESTRICTION,
    "governance.objection": FENCE_OBJECTION,
    "governance.legal_basis": FENCE_LEGAL_BASIS,
    "governance.legal_hold": FENCE_LEGAL_HOLD,
}

#: Governance-relevant Link types and the fence their (de)activation advances.
LINK_TYPE_FENCES: dict[str, str] = {
    "ccf.governed_by": FENCE_LINKS,
    "ccf.same_as": FENCE_IDENTITY,
    "ccf.distinct_from": FENCE_IDENTITY,
}


def snapshot_fences(conn, archive_id: str) -> dict[str, str]:
    """Current generation of every governance fence (missing fences read 0).

    The snapshot always covers :data:`ALL_FENCES` so generation vectors are
    structurally stable whether or not a fence has ever been bumped.
    """
    rows = conn.execute(
        """
        SELECT fence, generation FROM governance_fence
        WHERE archive_id = %s AND fence = ANY(%s)
        """,
        (archive_id, list(ALL_FENCES)),
    ).fetchall()
    vector = {fence: "0" for fence in ALL_FENCES}
    for fence, generation in rows:
        vector[fence] = str(int(generation))
    return vector


def fence_last_change(conn, archive_id: str, fence: str) -> int | None:
    """Commit sequence of the fence's last advance, or None if never bumped."""
    row = conn.execute(
        """
        SELECT last_change_sequence FROM governance_fence
        WHERE archive_id = %s AND fence = %s
        """,
        (archive_id, fence),
    ).fetchone()
    return int(row[0]) if row else None


def advance_fences(
    conn, archive_id: str, fences: set[str], sequence: int, updated_at: str
) -> None:
    """Advance each fence by one inside the admission transaction.

    Unknown (non-governance) fence names are rejected: callers must not
    silently invent fence namespaces.
    """
    unknown = set(fences) - set(ALL_FENCES)
    if unknown:
        raise ValueError(f"unknown governance fences: {sorted(unknown)}")
    for fence in sorted(fences):
        conn.execute(
            """
            INSERT INTO governance_fence (
                archive_id, fence, generation, last_change_sequence, updated_at
            ) VALUES (%s, %s, 1, %s, %s)
            ON CONFLICT (archive_id, fence) DO UPDATE SET
                generation = governance_fence.generation + 1,
                last_change_sequence = EXCLUDED.last_change_sequence,
                updated_at = EXCLUDED.updated_at
            """,
            (archive_id, fence, sequence, updated_at),
        )


def classify_governance_mutations(conn, objects) -> set[str]:
    """Fences a committed object set must advance (spec section 9.5).

    Governance Record head transitions advance their type's fence; direct
    ``ccf.governed_by`` / ``ccf.same_as`` / ``ccf.distinct_from`` Links and
    Link dispositions targeting them advance the link/identity fences.
    Sealed objects hide their type, so they conservatively advance every
    governance fence rather than risk a stale allow.
    """
    fences: set[str] = set()
    disposition_targets: list[str] = []
    for obj in objects:
        structural_type = obj.structural["content"]["type"]
        if obj.object_kind == "record":
            if structural_type in RECORD_TYPE_FENCES:
                fences.add(RECORD_TYPE_FENCES[structural_type])
            elif structural_type == "lineage.link_disposition":
                target = obj.structural["content"]["structural_payload"].get(
                    "target_link_id"
                )
                if target:
                    disposition_targets.append(target)
            elif structural_type == "sealed.record":
                return set(ALL_FENCES)
        elif obj.object_kind == "link":
            if structural_type in LINK_TYPE_FENCES:
                fences.add(LINK_TYPE_FENCES[structural_type])
            elif structural_type == "sealed.link":
                return set(ALL_FENCES)
    for target in disposition_targets:
        target_type = _link_type(conn, target)
        if target_type is None or target_type == "sealed.link":
            return set(ALL_FENCES)
        if target_type in LINK_TYPE_FENCES:
            fences.add(LINK_TYPE_FENCES[target_type])
    return fences


def _link_type(conn, link_id: str) -> str | None:
    row = conn.execute(
        """
        SELECT plaintext_json ->> 'type' FROM compartment
        WHERE object_id = %s AND compartment = 'structural' AND state = 'plaintext'
        """,
        (link_id,),
    ).fetchone()
    return row[0] if row else None
