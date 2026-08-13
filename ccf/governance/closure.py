"""Policy closure collection (spec section 9.2).

Deterministically collects every input the baseline evaluator may use:

- the direct object policy (the Record's resolved ``policy_ref`` lineage);
- active ``ccf.governed_by`` Links;
- active derivation ancestors over Link types the registry marks
  ``policy_propagates`` (none in the 0.1.2 registry, so the traversal is
  registry-driven and currently vacuous rather than hard-coded);
- current resolved data-subject identities (active ``ccf.same_as`` merges,
  vetoed pairwise by active ``ccf.distinct_from``);
- consent, restriction, objection, legal-basis, and hold lineage heads for
  those subjects;
- archive governance (the policy chain rooted at each policy's own
  ``policy_ref``, followed to a self-rooted policy);
- destination-local tightening overlays (policies bound to the recipient).

The collection is a pure function of canonical state; the closure hash
binds the exact inputs the evaluator consumed.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ccf.hashing import canonical_digest
from ccf.lineage import DEACTIVATING_ACTIONS, current_link_actions

GOVERNANCE_LINEAGE_TYPES: tuple[str, ...] = (
    "governance.consent",
    "governance.restriction",
    "governance.objection",
    "governance.legal_basis",
    "governance.legal_hold",
)


@dataclass
class PolicyDoc:
    """One applicable policy document and how it was reached."""

    lineage_id: str | None
    head_record_id: str
    head_record_hash: str | None
    payload: dict
    overlay: bool
    via: str  # direct | governed_by | derived | archive | recipient_overlay


@dataclass
class ObjectClosure:
    """Governance inputs collected for one requested object."""

    object_id: str
    object_kind: str
    available: bool
    structural_type: str | None
    data_classes: list[str] = field(default_factory=list)
    subject_coverage: str = "unknown"
    subjects: list[str] = field(default_factory=list)
    consent_refs: list[str] = field(default_factory=list)
    legal_basis_refs: list[str] = field(default_factory=list)
    policies: list[PolicyDoc] = field(default_factory=list)
    unresolved_policy_lineages: list[str] = field(default_factory=list)


@dataclass
class GovernanceRecord:
    """Current head of one governance lineage relevant to a data subject."""

    type: str
    lineage_id: str
    head_record_id: str
    head_record_hash: str
    state: str
    payload: dict


@dataclass
class PolicyClosure:
    """The complete deterministic input set for one evaluation."""

    objects: list[ObjectClosure]
    overlays: list[PolicyDoc]
    governance: list[GovernanceRecord]
    generation_vector: dict[str, str]
    head_sequence: str
    closure_hash: str


def collect_policy_closure(
    conn,
    *,
    archive_id: str,
    registries,
    object_ids: list[str],
    recipient: str | None,
    generation_vector: dict[str, str],
    head_sequence: str,
) -> PolicyClosure:
    """Collect the policy closure for ``object_ids`` at the current head."""
    actions = current_link_actions(conn, archive_id)
    governed_by = _active_links(conn, archive_id, "ccf.governed_by", actions)
    same_as = _active_links(conn, archive_id, "ccf.same_as", actions)
    distinct_from = _active_links(conn, archive_id, "ccf.distinct_from", actions)
    propagating = _propagating_ancestors(conn, archive_id, registries, actions)

    lineage_heads = _lineage_heads(conn, archive_id)
    objects: list[ObjectClosure] = []
    all_subjects: set[str] = set()
    for object_id in object_ids:
        info = _object_info(conn, archive_id, object_id)
        if info is None:
            objects.append(
                ObjectClosure(
                    object_id=object_id,
                    object_kind="unknown",
                    available=False,
                    structural_type=None,
                )
            )
            continue
        privacy = info["privacy"]
        closure = ObjectClosure(
            object_id=object_id,
            object_kind=info["object_kind"],
            available=info["available"],
            structural_type=info["structural_type"],
            data_classes=sorted(privacy.get("data_classes") or []),
            subject_coverage=privacy.get("subject_coverage") or "unknown",
            consent_refs=sorted(privacy.get("consent_refs") or []),
            legal_basis_refs=sorted(privacy.get("legal_basis_refs") or []),
        )
        declared_subjects = sorted(
            entry["person_id"]
            for entry in (privacy.get("data_subjects") or [])
            if isinstance(entry, dict) and entry.get("person_id")
        )
        closure.subjects = _resolve_subjects(
            declared_subjects, same_as, distinct_from
        )
        all_subjects.update(closure.subjects)

        seen_policies: set[str] = set()
        # Direct object policy, then the archive-governance chain above it.
        policy_ref = info["policy_ref"]
        if policy_ref is not None:
            _collect_policy_chain(
                conn,
                lineage_heads,
                policy_ref["lineage_id"],
                closure.policies,
                closure.unresolved_policy_lineages,
                seen_policies,
                via="direct",
            )
        # Active governed_by bindings on the object itself.
        for target in sorted(governed_by.get(object_id, ())):
            _collect_bound_policy(
                conn,
                lineage_heads,
                target,
                closure.policies,
                closure.unresolved_policy_lineages,
                seen_policies,
                via="governed_by",
            )
        # Derivation ancestors where the registry says policy propagates.
        for ancestor in sorted(propagating.get(object_id, ())):
            ancestor_ref = _policy_ref_of(conn, ancestor)
            if ancestor_ref is not None:
                _collect_policy_chain(
                    conn,
                    lineage_heads,
                    ancestor_ref["lineage_id"],
                    closure.policies,
                    closure.unresolved_policy_lineages,
                    seen_policies,
                    via="derived",
                )
            for target in sorted(governed_by.get(ancestor, ())):
                _collect_bound_policy(
                    conn,
                    lineage_heads,
                    target,
                    closure.policies,
                    closure.unresolved_policy_lineages,
                    seen_policies,
                    via="derived",
                )
        objects.append(closure)

    # Destination-local tightening overlays: policies bound to the recipient.
    overlays: list[PolicyDoc] = []
    if recipient is not None:
        overlay_seen: set[str] = set()
        for target in sorted(governed_by.get(recipient, ())):
            _collect_bound_policy(
                conn,
                lineage_heads,
                target,
                overlays,
                [],
                overlay_seen,
                via="recipient_overlay",
            )
        for doc in overlays:
            doc.overlay = True

    governance = _subject_governance(
        conn, archive_id, sorted(all_subjects)
    )
    result = PolicyClosure(
        objects=objects,
        overlays=overlays,
        governance=governance,
        generation_vector=dict(generation_vector),
        head_sequence=str(head_sequence),
        closure_hash="",
    )
    result.closure_hash = _closure_hash(result)
    return result


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------


def _object_info(conn, archive_id: str, object_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT oh.object_kind,
               sc.plaintext_json AS structural,
               mc.state AS semantic_state,
               mc.plaintext_json AS semantic
        FROM object_header oh
        LEFT JOIN compartment sc
          ON sc.object_id = oh.id AND sc.compartment = 'structural'
         AND sc.state = 'plaintext'
        LEFT JOIN compartment mc
          ON mc.object_id = oh.id AND mc.compartment = 'semantic'
        WHERE oh.archive_id = %s AND oh.id = %s
        """,
        (archive_id, object_id),
    ).fetchone()
    if row is None:
        return None
    object_kind, structural, semantic_state, semantic = row
    available = True
    if object_kind == "blob":
        blob_row = conn.execute(
            "SELECT state FROM blob_content WHERE blob_id = %s", (object_id,)
        ).fetchone()
        available = blob_row is not None and blob_row[0] == "plaintext"
    else:
        available = semantic_state == "plaintext"
    semantic = semantic if isinstance(semantic, dict) else {}
    return {
        "object_kind": object_kind,
        "structural_type": (structural or {}).get("type"),
        "available": available,
        "privacy": semantic.get("privacy") or {},
        "policy_ref": semantic.get("policy_ref"),
    }


def _policy_ref_of(conn, object_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT plaintext_json -> 'policy_ref' FROM compartment
        WHERE object_id = %s AND compartment = 'semantic' AND state = 'plaintext'
        """,
        (object_id,),
    ).fetchone()
    if row is None or not isinstance(row[0], dict):
        return None
    return row[0]


def _lineage_heads(conn, archive_id: str) -> dict[str, dict]:
    rows = conn.execute(
        """
        SELECT lineage_id, head_record_id, head_record_hash, state
        FROM lineage_head WHERE archive_id = %s
        """,
        (archive_id,),
    ).fetchall()
    return {
        lineage_id: {
            "head_record_id": head_record_id,
            "head_record_hash": head_record_hash,
            "state": state,
        }
        for lineage_id, head_record_id, head_record_hash, state in rows
    }


def _policy_payload(conn, record_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT plaintext_json -> 'payload' FROM compartment
        WHERE object_id = %s AND compartment = 'semantic' AND state = 'plaintext'
        """,
        (record_id,),
    ).fetchone()
    return row[0] if row and isinstance(row[0], dict) else None


def _collect_policy_chain(
    conn,
    lineage_heads: dict,
    lineage_id: str,
    out: list[PolicyDoc],
    unresolved: list[str],
    seen: set[str],
    *,
    via: str,
) -> None:
    """Resolve a policy lineage head and follow its archive-governance chain.

    The chain ends at a self-rooted policy (its ``policy_ref`` names its own
    lineage) or at a policy without a further ``policy_ref``. Cycles other
    than the self-root fail closed as unresolved.
    """
    current = lineage_id
    visited: set[str] = set()
    while True:
        if current in visited:
            unresolved.append(current)
            return
        visited.add(current)
        head = lineage_heads.get(current)
        if head is None:
            unresolved.append(current)
            return
        record_id = head["head_record_id"]
        if record_id not in seen:
            payload = _policy_payload(conn, record_id)
            if payload is None:
                unresolved.append(current)
                return
            seen.add(record_id)
            out.append(
                PolicyDoc(
                    lineage_id=current,
                    head_record_id=record_id,
                    head_record_hash=head["head_record_hash"],
                    payload=payload,
                    overlay=False,
                    via=via,
                )
            )
        next_ref = _policy_ref_of(conn, record_id)
        if next_ref is None or next_ref.get("lineage_id") in (None, current):
            return
        current = next_ref["lineage_id"]
        via = "archive"


def _collect_bound_policy(
    conn,
    lineage_heads: dict,
    target_record_id: str,
    out: list[PolicyDoc],
    unresolved: list[str],
    seen: set[str],
    *,
    via: str,
) -> None:
    """Resolve a ``governed_by`` target to its current policy head."""
    row = conn.execute(
        """
        SELECT plaintext_json -> 'lineage' ->> 'lineage_id' FROM compartment
        WHERE object_id = %s AND compartment = 'structural' AND state = 'plaintext'
        """,
        (target_record_id,),
    ).fetchone()
    lineage_id = row[0] if row else None
    if lineage_id is None:
        payload = _policy_payload(conn, target_record_id)
        if payload is None or target_record_id in seen:
            if payload is None:
                unresolved.append(target_record_id)
            return
        seen.add(target_record_id)
        out.append(
            PolicyDoc(
                lineage_id=None,
                head_record_id=target_record_id,
                head_record_hash=None,
                payload=payload,
                overlay=False,
                via=via,
            )
        )
        return
    _collect_policy_chain(
        conn, lineage_heads, lineage_id, out, unresolved, seen, via=via
    )


def _active_links(
    conn, archive_id: str, link_type: str, actions: dict[str, str]
) -> dict[str, set[str]]:
    """Active edges of one Link type as ``from_id -> {to_id}``."""
    rows = conn.execute(
        """
        SELECT oh.id,
               c.plaintext_json ->> 'from_id' AS from_id,
               c.plaintext_json ->> 'to_id' AS to_id
        FROM object_header oh
        JOIN compartment c
          ON c.object_id = oh.id AND c.compartment = 'structural'
        WHERE oh.archive_id = %s
          AND oh.object_kind = 'link'
          AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = %s
        """,
        (archive_id, link_type),
    ).fetchall()
    edges: dict[str, set[str]] = {}
    for link_id, from_id, to_id in rows:
        if actions.get(link_id) in DEACTIVATING_ACTIONS:
            continue
        if from_id and to_id:
            edges.setdefault(from_id, set()).add(to_id)
    return edges


def _propagating_ancestors(
    conn, archive_id: str, registries, actions: dict[str, str]
) -> dict[str, set[str]]:
    """Derivation ancestors over Link types with ``policy_propagates``."""
    propagating_types = sorted(
        entry["name"]
        for entry in registries.link_entries()
        if entry.get("policy_propagates")
    )
    if not propagating_types:
        return {}
    edges: dict[str, set[str]] = {}
    for link_type in propagating_types:
        for from_id, to_ids in _active_links(conn, archive_id, link_type, actions).items():
            edges.setdefault(from_id, set()).update(to_ids)
    ancestors: dict[str, set[str]] = {}
    for start in edges:
        seen: set[str] = set()
        stack = list(edges[start])
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            stack.extend(edges.get(node, ()))
        ancestors[start] = seen
    return ancestors


def _resolve_subjects(
    declared: list[str],
    same_as: dict[str, set[str]],
    distinct_from: dict[str, set[str]],
) -> list[str]:
    """Merge declared subjects over active ``same_as`` edges.

    ``same_as`` is symmetric (it is its own inverse); a ``distinct_from``
    adjudication vetoes the direct edge between that pair without breaking
    merges through other paths.
    """
    parent: dict[str, str] = {}

    def find(node: str) -> str:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    vetoed = {
        frozenset((a, b))
        for a, targets in distinct_from.items()
        for b in targets
    }
    for a, targets in same_as.items():
        for b in targets:
            if frozenset((a, b)) not in vetoed:
                union(a, b)
    merged: set[str] = set()
    for subject in declared:
        root = find(subject)
        members = {node for node in parent if find(node) == root}
        merged.update(members or {subject})
    return sorted(merged)


def _subject_governance(
    conn, archive_id: str, subjects: list[str]
) -> list[GovernanceRecord]:
    """Current heads of governance lineages for the resolved subjects."""
    if not subjects:
        return []
    rows = conn.execute(
        """
        SELECT lh.lineage_id, lh.head_record_id, lh.head_record_hash, lh.state,
               sc.plaintext_json ->> 'type' AS type,
               mc.plaintext_json -> 'payload' AS payload
        FROM lineage_head lh
        JOIN compartment sc
          ON sc.object_id = lh.head_record_id AND sc.compartment = 'structural'
         AND sc.state = 'plaintext'
        JOIN compartment mc
          ON mc.object_id = lh.head_record_id AND mc.compartment = 'semantic'
         AND mc.state = 'plaintext'
        WHERE lh.archive_id = %s
          AND sc.plaintext_json ->> 'type' = ANY(%s)
        ORDER BY lh.lineage_id
        """,
        (archive_id, list(GOVERNANCE_LINEAGE_TYPES)),
    ).fetchall()
    records: list[GovernanceRecord] = []
    subject_set = set(subjects)
    for lineage_id, head_id, head_hash, state, type_name, payload in rows:
        if not isinstance(payload, dict):
            continue
        if payload.get("subject_id") not in subject_set:
            continue
        records.append(
            GovernanceRecord(
                type=type_name,
                lineage_id=lineage_id,
                head_record_id=head_id,
                head_record_hash=head_hash,
                state=state,
                payload=payload,
            )
        )
    return records


def _closure_hash(closure: PolicyClosure) -> str:
    """Hash binding the exact deterministic inputs the evaluator consumed."""
    doc = {
        "objects": [
            {
                "object_id": obj.object_id,
                "data_classes": obj.data_classes,
                "subjects": obj.subjects,
                "policies": [
                    [doc.lineage_id, doc.head_record_id] for doc in obj.policies
                ],
                "unresolved_policy_lineages": sorted(
                    obj.unresolved_policy_lineages
                ),
            }
            for obj in sorted(closure.objects, key=lambda o: o.object_id)
        ],
        "overlays": [
            [doc.lineage_id, doc.head_record_id]
            for doc in sorted(
                closure.overlays, key=lambda d: (d.head_record_id, d.via)
            )
        ],
        "governance": [
            [rec.type, rec.lineage_id, rec.head_record_id, rec.state]
            for rec in sorted(closure.governance, key=lambda r: r.lineage_id)
        ],
    }
    return canonical_digest("ccf:policy-closure:v1", doc)
