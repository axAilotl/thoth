"""thothmap review mapping tests (checklist 4: human review -> governance.review_decision)."""

from __future__ import annotations

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.context import ThothMapError
from ccf.thothmap.review import review_submissions
from ccf.thothmap.semantic import assertion_submissions
from ccf.thothmap.sources import source_submission

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def candidate(rig, ctx):
    """Admitted source plus one proposed memory-candidate assertion."""
    source = source_submission(
        rig.producer,
        ctx,
        {"source_name": "omi", "source_type": "wearable_audio", "collector": "thoth.capture"},
    )
    candidate = assertion_submissions(
        rig.producer,
        ctx,
        {
            "candidate_id": "c4nd1date-review-target",
            "candidate_type": "preference",
            "status": "proposed",
            "subject": "Ada",
            "predicate": "prefers",
            "object_value": "open adoption of the continuity schema",
            "text": "I want the schema open because adoption is still a win.",
            "confidence": 0.9,
        },
        source_ccf_id=source.records[0]["id"],
    )
    admit_mapped(rig, source.extend(candidate))
    return {
        "source_id": source.records[0]["id"],
        "candidate_id": candidate.records[0]["id"],
        "candidate_payload": candidate.records[0]["payload"],
    }


REVIEW = {
    "action": "confirm",
    "actor": "ada",
    "at": "2026-08-11T21:42:20Z",
    "reason": "The extraction matches the stated intent.",
}


def test_confirm_maps_to_decision_and_accepted_successor(rig, ctx, candidate):
    mapped = review_submissions(
        rig.producer,
        ctx,
        REVIEW,
        source_ccf_id=candidate["source_id"],
        target_ccf_ids=[candidate["candidate_id"]],
        reviewer_ccf_id=rig.person_id,
        accepted_type="semantic.assertion",
        accepted_payload=candidate["candidate_payload"],
    )
    decision, successor = mapped.records
    assert decision["type"] == "governance.review_decision"
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.1:payload.governance.review_decision",
        decision["payload"],
        what="governance.review_decision",
    )
    assert decision["payload"]["decision"] == "accept"
    assert decision["payload"]["target_ids"] == [candidate["candidate_id"]]
    assert decision["payload"]["reviewer_id"] == rig.person_id

    link_types = {(l["type"], l["from_id"], l["to_id"]) for l in mapped.links}
    assert ("ccf.covers", decision["id"], candidate["candidate_id"]) in link_types
    assert ("ccf.supersedes", successor["id"], candidate["candidate_id"]) in link_types

    result = admit_mapped(rig, mapped)
    assert result["status"] == "committed"
    # The accepted successor upgrades authority; the original claims stay.
    semantic = compartment(rig, successor["id"], "semantic")
    assert semantic["authority"]["basis"] == "person_accepted"
    assert semantic["authority"]["accepted_by"] == rig.person_id
    # Successor is governance-produced: no origin tuple.
    assert "origin" not in semantic
    decision_semantic = compartment(rig, decision["id"], "semantic")
    assert decision_semantic["origin"]["native_id"].startswith("review:confirm:")


def test_review_action_decision_map(rig, ctx, candidate):
    for action, decision in (
        ("reject", "reject"),
        ("supersede", "supersede"),
        ("retry", "defer"),
        ("mark_reviewed", "accept"),
    ):
        mapped = review_submissions(
            rig.producer,
            ctx,
            dict(REVIEW, action=action),
            source_ccf_id=candidate["source_id"],
            target_ccf_ids=[candidate["candidate_id"]],
            reviewer_ccf_id=rig.person_id,
        )
        assert mapped.records[0]["payload"]["decision"] == decision


def test_review_fails_closed_without_targets(rig, ctx, candidate):
    with pytest.raises(ThothMapError, match="target"):
        review_submissions(
            rig.producer,
            ctx,
            REVIEW,
            source_ccf_id=candidate["source_id"],
            target_ccf_ids=[],
            reviewer_ccf_id=rig.person_id,
        )


def test_review_fails_closed_on_unknown_action(rig, ctx, candidate):
    with pytest.raises(ThothMapError, match="unmappable"):
        review_submissions(
            rig.producer,
            ctx,
            dict(REVIEW, action="shrug"),
            source_ccf_id=candidate["source_id"],
            target_ccf_ids=[candidate["candidate_id"]],
            reviewer_ccf_id=rig.person_id,
        )


def test_successor_requires_accept(rig, ctx, candidate):
    with pytest.raises(ThothMapError, match="accept"):
        review_submissions(
            rig.producer,
            ctx,
            dict(REVIEW, action="reject"),
            source_ccf_id=candidate["source_id"],
            target_ccf_ids=[candidate["candidate_id"]],
            reviewer_ccf_id=rig.person_id,
            accepted_type="semantic.assertion",
            accepted_payload=candidate["candidate_payload"],
        )
