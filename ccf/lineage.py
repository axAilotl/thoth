"""Stateful lineages and the derivation DAG (spec sections 6.6 and 8).

Every registered stateful type declares a state machine in the pinned
``ccf.state-machines`` registry; every transition carries ``lineage_id`` and
``previous_head_id`` and is compare-and-swapped against the current admitted
head inside the serialized admission transaction. The archive never silently
rebases: a stale predecessor yields a ``lineage_conflict`` result.

Active edges of Link types the registry marks ``acyclic`` (notably
``ccf.derived_from``) must form a DAG. New edges — and edges reactivated by
a ``lineage.link_disposition`` ``restore`` transition — are cycle-checked
against the admitted graph plus the effects of the current batch.
"""

from __future__ import annotations

from ccf.registry import PinnedRegistries


class LineageDeclarationError(ValueError):
    """Raised when a submission's lineage block contradicts the registry."""


#: Disposition actions that deactivate the target Link's edges.
DEACTIVATING_ACTIONS: frozenset[str] = frozenset({"retract", "supersede", "tombstone"})


def check_state_transition(
    machine: dict,
    *,
    current_state: str | None,
    transition: str,
) -> str | None:
    """Validate one state-machine transition.

    Returns ``None`` when the transition is legal, otherwise a short reason
    suitable for a ``lineage_conflict`` admission result.
    """
    if current_state is None:
        if transition in machine["initial_transitions"]:
            return None
        return f"transition {transition!r} is not an initial transition of {machine['id']}"
    if current_state in machine["terminal_states"]:
        return f"lineage state {current_state!r} of {machine['id']} is terminal"
    for rule in machine["transitions"]:
        if rule["from"] == current_state:
            if transition in rule["to"]:
                return None
            return (
                f"transition {current_state!r} -> {transition!r} not allowed "
                f"by {machine['id']}"
            )
    return f"state {current_state!r} has no transitions in {machine['id']}"


def declare_lineage(
    submission: dict,
    *,
    type_entry: dict,
    registries: PinnedRegistries,
) -> tuple[str, dict] | None:
    """Check that a Record submission's lineage block matches the registry.

    Returns ``(state_machine_id, lineage_block)`` for stateful types and
    ``None`` for stateless ones. Raises :class:`LineageDeclarationError`
    (fail closed) when a stateful type lacks a transition or a stateless
    type carries one.
    """
    lineage_mode = type_entry.get("lineage_mode", "none")
    block = submission.get("lineage")
    if lineage_mode == "compare_and_swap":
        machine_id = type_entry.get("state_machine_id")
        if not machine_id:
            raise LineageDeclarationError(
                f"type {type_entry['name']} is compare_and_swap but declares no state machine"
            )
        if block is None:
            raise LineageDeclarationError(
                f"type {type_entry['name']} requires a lineage transition"
            )
        registries.state_machine(machine_id)  # fail closed on unknown machine
        return machine_id, block
    if block is not None:
        raise LineageDeclarationError(
            f"type {type_entry['name']} is not stateful but carries a lineage block"
        )
    return None


def load_lineage_heads(conn, archive_id: str) -> dict[str, dict]:
    """Current admitted lineage heads as ``lineage_id -> head info``."""
    rows = conn.execute(
        """
        SELECT lineage_id, head_record_id, head_record_hash, state FROM lineage_head
        WHERE archive_id = %s
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


def current_link_actions(conn, archive_id: str) -> dict[str, str]:
    """Effective disposition action per target Link.

    Each ``lineage.link_disposition`` lineage head declares the current
    action for its target Link; when more than one lineage targets the same
    Link, the latest admitted head wins.
    """
    rows = conn.execute(
        """
        SELECT c.plaintext_json -> 'structural_payload' ->> 'target_link_id' AS target,
               c.plaintext_json -> 'structural_payload' ->> 'action' AS action,
               lh.head_commit_sequence AS seq
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
    actions: dict[str, str] = {}
    for target, action, _seq in rows:
        if target:
            actions[target] = action
    return actions


def load_active_acyclic_edges(
    conn, archive_id: str, acyclic_types: frozenset[str]
) -> dict[str, set[str]]:
    """Adjacency (from_id -> to_ids) of active acyclic-type Links.

    Links deactivated by a current retract/supersede/tombstone disposition
    are excluded; restored or selector-invalidated Links stay active.
    """
    if not acyclic_types:
        return {}
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
          AND c.plaintext_json ->> 'type' = ANY(%s)
        """,
        (archive_id, sorted(acyclic_types)),
    ).fetchall()
    actions = current_link_actions(conn, archive_id)
    edges: dict[str, set[str]] = {}
    for link_id, from_id, to_id in rows:
        if actions.get(link_id) in DEACTIVATING_ACTIONS:
            continue
        if from_id and to_id:
            edges.setdefault(from_id, set()).add(to_id)
    return edges


def creates_cycle(edges: dict[str, set[str]], from_id: str, to_id: str) -> bool:
    """True iff adding edge ``from_id -> to_id`` closes a cycle.

    The new edge creates a cycle exactly when ``to_id`` already reaches
    ``from_id`` through active edges (or the edge is a self-loop).
    """
    if from_id == to_id:
        return True
    stack = [to_id]
    seen: set[str] = set()
    while stack:
        node = stack.pop()
        if node == from_id:
            return True
        if node in seen:
            continue
        seen.add(node)
        stack.extend(edges.get(node, ()))
    return False


def add_edge(edges: dict[str, set[str]], from_id: str, to_id: str) -> None:
    edges.setdefault(from_id, set()).add(to_id)


def remove_edge(edges: dict[str, set[str]], from_id: str, to_id: str) -> None:
    targets = edges.get(from_id)
    if targets:
        targets.discard(to_id)
        if not targets:
            del edges[from_id]
