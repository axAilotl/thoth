"""Entity cluster projection (spec 8.5).

Entity decisions are canonical: a ``semantic.entity_resolution`` Record
plus authoritative membership Links admitted atomically. Clusters are a
projection — originals keep their IDs.

Rebuild folds three canonical inputs:

- every ``semantic.entity`` Record is a member (singleton if unlinked);
- active ``ccf.same_as`` Links union members into clusters
  (retracted/superseded/tombstoned Links do not);
- the latest active resolution head covering a member names the cluster's
  ``canonical_member_id`` via ``payload.canonical_entity_id``; otherwise
  the lexicographically smallest member ID stands in.

``ccf.distinct_from`` Links do not split clusters on their own: a human
adjudication splits by retracting the ``same_as`` Links. (The spec's
record/Link agreement check is an admission concern — phase 6 territory —
not recomputed here.)
"""

from __future__ import annotations

from ccf.lineage import DEACTIVATING_ACTIONS, current_link_actions
from ccf.projections import ENTITY_CLUSTER
from ccf.projections.invalidation import ProjectionStaleError, has_pending
from ccf.projections.rebuild import begin_rebuild, finish_rebuild

#: Lineage head states in which a resolution Record is in effect
#: (ccf.state.reviewable-v1 minus retract/tombstone).
_ACTIVE_HEAD_STATES = frozenset({"create", "supersede", "restore", "release"})


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, node: str) -> str:
        self.parent.setdefault(node, node)
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[node] != root:
            self.parent[node], node = root, self.parent[node]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def _active_resolution_heads(conn, archive_id: str) -> list[dict]:
    """Active entity-resolution heads, newest admitted last."""
    rows = conn.execute(
        """
        SELECT lh.head_commit_sequence, lh.state,
               sem.plaintext_json -> 'payload' AS payload
        FROM lineage_head lh
        JOIN compartment s
          ON s.object_id = lh.head_record_id AND s.compartment = 'structural'
        LEFT JOIN compartment sem
          ON sem.object_id = lh.head_record_id AND sem.compartment = 'semantic'
             AND sem.state = 'plaintext'
        WHERE lh.archive_id = %s
          AND s.state = 'plaintext'
          AND s.plaintext_json ->> 'type' = 'semantic.entity_resolution'
        ORDER BY lh.head_commit_sequence ASC
        """,
        (archive_id,),
    ).fetchall()
    heads = []
    for _seq, state, payload in rows:
        if state in _ACTIVE_HEAD_STATES and payload:
            heads.append(payload)
    return heads


def rebuild(conn, archive_id: str) -> int:
    """Rewrite ``projection_entity_cluster`` from canonical state."""
    stamp = begin_rebuild(conn, archive_id=archive_id, projection_name=ENTITY_CLUSTER)
    conn.execute(
        "DELETE FROM projection_entity_cluster WHERE archive_id = %s", (archive_id,)
    )

    uf = _UnionFind()

    entities = conn.execute(
        """
        SELECT oh.id FROM object_header oh
        JOIN compartment c
          ON c.object_id = oh.id AND c.compartment = 'structural'
        WHERE oh.archive_id = %s AND oh.object_kind = 'record'
          AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = 'semantic.entity'
        ORDER BY oh.id
        """,
        (archive_id,),
    ).fetchall()
    for (entity_id,) in entities:
        uf.find(entity_id)

    actions = current_link_actions(conn, archive_id)
    same_as_links = conn.execute(
        """
        SELECT oh.id,
               c.plaintext_json ->> 'from_id',
               c.plaintext_json ->> 'to_id'
        FROM object_header oh
        JOIN compartment c
          ON c.object_id = oh.id AND c.compartment = 'structural'
        WHERE oh.archive_id = %s AND oh.object_kind = 'link'
          AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = 'ccf.same_as'
        """,
        (archive_id,),
    ).fetchall()
    for link_id, from_id, to_id in same_as_links:
        if actions.get(link_id) in DEACTIVATING_ACTIONS:
            continue
        if from_id and to_id:
            uf.union(from_id, to_id)

    canonical_choice: dict[str, str] = {}
    for payload in _active_resolution_heads(conn, archive_id):
        entity_ids = payload.get("entity_ids") or []
        canonical = payload.get("canonical_entity_id")
        if canonical:
            for member in entity_ids:
                # Heads arrive oldest-first; later heads override.
                canonical_choice[member] = canonical

    clusters: dict[str, list[str]] = {}
    for member in list(uf.parent):
        clusters.setdefault(uf.find(member), []).append(member)

    count = 0
    for members in clusters.values():
        members.sort()
        cluster_id = members[0]
        canonical = next(
            (canonical_choice[m] for m in members if m in canonical_choice),
            None,
        )
        if canonical is not None and canonical not in members:
            canonical = None  # canonical member must belong to the cluster
        for member in members:
            conn.execute(
                """
                INSERT INTO projection_entity_cluster (
                    archive_id, member_id, cluster_id, canonical_member_id,
                    computed_through_sequence, generation
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    member,
                    cluster_id,
                    canonical,
                    stamp.computed_through_sequence,
                    stamp.generation,
                ),
            )
            count += 1
    finish_rebuild(conn, archive_id=archive_id, projection_name=ENTITY_CLUSTER)
    return count


def clusters(conn, archive_id: str) -> dict[str, list[str]]:
    """All clusters as ``cluster_id -> sorted member IDs``."""
    if has_pending(conn, archive_id=archive_id, projection_name=ENTITY_CLUSTER):
        raise ProjectionStaleError(
            "projection 'entity_cluster' has unresolved invalidations; rebuild first"
        )
    rows = conn.execute(
        """
        SELECT cluster_id, member_id FROM projection_entity_cluster
        WHERE archive_id = %s ORDER BY cluster_id, member_id
        """,
        (archive_id,),
    ).fetchall()
    result: dict[str, list[str]] = {}
    for cluster_id, member_id in rows:
        result.setdefault(cluster_id, []).append(member_id)
    return result


def cluster_of(conn, archive_id: str, member_id: str) -> dict | None:
    """One member's cluster assignment."""
    row = conn.execute(
        """
        SELECT cluster_id, canonical_member_id FROM projection_entity_cluster
        WHERE archive_id = %s AND member_id = %s
        """,
        (archive_id, member_id),
    ).fetchone()
    if row is None:
        return None
    return {"cluster_id": row[0], "canonical_member_id": row[1]}
