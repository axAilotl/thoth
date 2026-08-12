"""Human review -> ``governance.review_decision`` + successor (checklist 4, row 7).

Covers both Thoth review flows:

- semantic-memory review records (``core.semantic_memory_review``: actions
  ``confirm|reject|supersede|promote``, stored in candidate metadata), and
- artifact review events (``core.artifact_review_policy``: actions
  ``retry|reject|mark_reviewed`` on ``ingestion_queue.review_json``).

Each becomes a ``governance.review_decision`` Record with a ``ccf.covers``
Link to every reviewed target. Thoth reviewer identity is a free-text
``actor`` string; the caller resolves it to the reviewer's Person Record
URN (``reviewer_ccf_id``) — the converter never invents identities.

When the decision accepts a candidate and the caller supplies
``accepted_payload``/``accepted_type``, an accepted successor Record is
emitted (``person_accepted`` authority) with a ``ccf.supersedes`` Link from
successor to candidate, mirroring the spec's thoth-capture example. The
successor carries no origin tuple: it is governance-produced state, not a
re-importable source record.
"""

from __future__ import annotations

from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    ccf_timestamp,
    claims,
    optional_str,
    origin,
    require_str,
    require_urn,
)

_ACTION_DECISION = {
    "confirm": "accept",
    "promote": "accept",
    "mark_reviewed": "accept",
    "accept": "accept",
    "reject": "reject",
    "supersede": "supersede",
    "retry": "defer",
    "defer": "defer",
    "quarantine": "quarantine",
    "block": "quarantine",
    "release": "release",
}


def review_submissions(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    target_ccf_ids: list[str],
    reviewer_ccf_id: str,
    evidence_ccf_ids: list[str] | None = None,
    revision: str | int | None = "1",
    native_id: str | None = None,
    accepted_type: str | None = None,
    accepted_payload: dict | None = None,
    decision: str | None = None,
) -> MappedSubmissions:
    """Convert one Thoth review record to a ``governance.review_decision``.

    Snapshot keys: ``action``, ``actor``, ``at``, ``reason``, ``metadata``.
    ``target_ccf_ids`` are the reviewed objects (candidate assertions or
    quarantined artifacts) and must be non-empty — a decision without a
    target is meaningless and fails closed.
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    require_urn(reviewer_ccf_id, "record", field="reviewer_ccf_id")
    if not target_ccf_ids:
        raise ThothMapError("review decision requires at least one target")
    for target_id in target_ccf_ids:
        require_urn(target_id, "record", field="target_ccf_ids[]")
    evidence_ccf_ids = list(evidence_ccf_ids or [])
    for evidence_id in evidence_ccf_ids:
        require_urn(evidence_id, "record", field="evidence_ccf_ids[]")

    action = require_str(snapshot, "action", what="review record")
    if decision is None:
        decision = _ACTION_DECISION.get(action)
        if decision is None:
            raise ThothMapError(f"unmappable review action {action!r}")
    at = snapshot.get("at")
    if at is None:
        raise ThothMapError("review record requires 'at' timestamp")
    recorded_at = ccf_timestamp(at, field="review.at")

    if native_id is None:
        native_id = f"review:{action}:{recorded_at}:{','.join(sorted(target_ccf_ids))}"

    reason = optional_str(snapshot, "reason") or f"Thoth review action: {action}"
    reviewer_known = reviewer_ccf_id == ctx.person_id
    decision_record = producer.new_record(
        type="governance.review_decision",
        claims=claims(
            ctx,
            basis="person_accepted",
            asserted_by=reviewer_ccf_id,
            accepted_by=reviewer_ccf_id,
            subjects=(
                [
                    {
                        "person_id": reviewer_ccf_id,
                        "role": "reviewer",
                        "identity_state_at_write": (
                            "verified" if reviewer_known else "probable"
                        ),
                    }
                ]
            ),
        ),
        recorded_at=recorded_at,
        origin=origin(source_ccf_id, native_id, revision),
        payload={
            "target_ids": list(dict.fromkeys(target_ccf_ids)),
            "decision": decision,
            "reason": reason,
            "reviewer_id": reviewer_ccf_id,
            "evidence_refs": list(dict.fromkeys(evidence_ccf_ids)),
            "extensions": {
                "thoth_action": action,
                "thoth_actor": snapshot.get("actor"),
            },
        },
    )

    result = MappedSubmissions(records=[decision_record])
    for target_id in dict.fromkeys(target_ccf_ids):
        result.links.append(
            producer.new_link(
                type="ccf.covers",
                from_id=decision_record["id"],
                to_id=target_id,
                claims=claims(ctx),
                selector={},
            )
        )

    if accepted_payload is not None or accepted_type is not None:
        if not accepted_payload or not accepted_type:
            raise ThothMapError(
                "accepted successor requires both accepted_type and accepted_payload"
            )
        if decision != "accept":
            raise ThothMapError("accepted successor only valid for accept decisions")
        if len(set(target_ccf_ids)) != 1:
            raise ThothMapError("accepted successor requires exactly one target")
        successor = producer.new_record(
            type=accepted_type,
            claims=claims(
                ctx, basis="person_accepted", asserted_by=reviewer_ccf_id,
                accepted_by=reviewer_ccf_id,
            ),
            recorded_at=recorded_at,
            payload=accepted_payload,
        )
        result.records.append(successor)
        result.links.append(
            producer.new_link(
                type="ccf.supersedes",
                from_id=successor["id"],
                to_id=target_ccf_ids[0],
                claims=claims(ctx),
                selector={},
            )
        )
    return result
