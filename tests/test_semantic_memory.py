from pathlib import Path

import pytest

from core.metadata_db import MetadataDB
from core.semantic_memory import (
    SEMANTIC_MEMORY_PROMOTION_METADATA_KEY,
    SemanticMemoryCandidate,
    SemanticMemoryEvidence,
    SemanticMemoryPromotionConfigError,
    SemanticMemoryPromotionPolicy,
    SemanticMemoryStore,
    SemanticMemoryTransitionError,
    SemanticMemoryValidationError,
)
from core.semantic_memory_review import (
    SEMANTIC_MEMORY_REVIEW_METADATA_KEY,
    SemanticMemoryReviewService,
)


def make_store(
    tmp_path: Path,
    *,
    promotion_policy: SemanticMemoryPromotionPolicy | None = None,
) -> SemanticMemoryStore:
    return SemanticMemoryStore(
        MetadataDB(str(tmp_path / "meta.db")),
        promotion_policy=promotion_policy,
    )


def test_semantic_memory_stores_candidates_and_queryable_evidence(tmp_path: Path):
    store = make_store(tmp_path)
    candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-preference-1",
            candidate_type="preference",
            text="Ada prefers morning writing blocks.",
            subject="Ada",
            predicate="prefers",
            object_value="morning writing blocks",
            entity_id="person:ada",
            entity_type="person",
            entity_name="Ada",
            confidence=0.82,
            privacy_class="personal",
            write_provenance={"writer": "test-extractor"},
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-preference-1",
                evidence_id="evidence-1",
                artifact_id="artifact-1",
                artifact_type="note",
                capture_event_id="event-1",
                source_path="raw/omi/export.json",
                source_timestamp="2026-04-04T10:30:00",
                evidence_text="Ada said mornings are best for writing.",
                confidence=0.76,
                privacy_class="personal",
                write_provenance={"writer": "test-extractor"},
            ),
        ),
    )

    assert candidate.status == "proposed"
    assert candidate.entity_id == "person:ada"

    by_candidate = store.list_evidence(candidate_id=candidate.candidate_id)
    assert [item.evidence_id for item in by_candidate] == ["evidence-1"]

    by_artifact = store.list_evidence(artifact_id="artifact-1")
    assert [item.candidate_id for item in by_artifact] == [candidate.candidate_id]

    by_capture_event = store.list_evidence(capture_event_id="event-1")
    assert [item.evidence_id for item in by_capture_event] == ["evidence-1"]

    by_entity_type_status = store.list_evidence(
        entity_id="person:ada",
        entity_type="person",
        candidate_type="preference",
        candidate_status="proposed",
    )
    assert [item.evidence_id for item in by_entity_type_status] == ["evidence-1"]

    candidates_by_artifact = store.list_candidates(artifact_id="artifact-1")
    assert [item.candidate_id for item in candidates_by_artifact] == [
        candidate.candidate_id
    ]


def test_semantic_memory_enforces_states_and_immutable_types(tmp_path: Path):
    store = make_store(tmp_path)
    proposed = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-fact-1",
            candidate_type="fact",
            text="Project Thoth uses beads for issue tracking.",
        )
    )
    assert proposed.status == "proposed"

    with pytest.raises(SemanticMemoryTransitionError):
        store.transition_candidate(proposed.candidate_id, "promoted")
    assert store.get_candidate(proposed.candidate_id).status == "proposed"

    confirmed = store.transition_candidate(
        proposed.candidate_id,
        "confirmed",
        write_provenance={"actor": "reviewer"},
    )
    assert confirmed.status == "confirmed"
    assert confirmed.write_provenance["last_status_transition"]["from"] == "proposed"

    with pytest.raises(SemanticMemoryTransitionError):
        store.update_candidate(
            SemanticMemoryCandidate(
                candidate_id=confirmed.candidate_id,
                candidate_type="fact",
                status="rejected",
                text="Generic updates must not change review status.",
            )
        )

    promoted = store.transition_candidate(confirmed.candidate_id, "promoted")
    assert promoted.status == "promoted"

    with pytest.raises(SemanticMemoryTransitionError):
        store.transition_candidate(promoted.candidate_id, "rejected")

    with pytest.raises(SemanticMemoryTransitionError):
        store.update_candidate(
            SemanticMemoryCandidate(
                candidate_id=proposed.candidate_id,
                candidate_type="claim",
                status="promoted",
                text="Changing the candidate type must fail.",
            )
        )

    replacement = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-fact-2",
            candidate_type="fact",
            text="Project Thoth uses bd for issue tracking.",
            supersedes_candidate_id=proposed.candidate_id,
        )
    )
    old = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-claim-old",
            candidate_type="claim",
            text="Older wording.",
        )
    )
    superseded = store.transition_candidate(
        old.candidate_id,
        "superseded",
        superseded_by_candidate_id=replacement.candidate_id,
    )
    assert superseded.superseded_by_candidate_id == replacement.candidate_id

    with pytest.raises(SemanticMemoryTransitionError):
        store.transition_candidate(superseded.candidate_id, "confirmed")


def test_semantic_memory_review_service_records_auditable_actions(tmp_path: Path):
    store = make_store(tmp_path)
    candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-review-1",
            candidate_type="preference",
            text="Ada prefers written planning notes.",
            subject="Ada",
            predicate="prefers",
            object_value="written planning notes",
            entity_id="person:ada",
            write_provenance={"writer": "extractor"},
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-review-1",
                evidence_id="evidence-review-1",
                source_path="notes/planning.md",
                evidence_text="Ada asked for written planning notes.",
            ),
        ),
    )
    rejected = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-review-reject",
            candidate_type="claim",
            text="Ada prefers noisy meetings.",
        )
    )
    replacement = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-review-replacement",
            candidate_type="preference",
            text="Ada prefers async written planning.",
        )
    )
    old = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-review-old",
            candidate_type="preference",
            text="Ada prefers planning notes.",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-review-old",
                evidence_id="evidence-review-old",
                source_path="notes/old.md",
            ),
        ),
    )

    service = SemanticMemoryReviewService(store=store)
    listed = service.list_candidates(status="proposed", entity_id="person:ada")
    assert listed["total"] == 1
    assert listed["candidates"][0]["candidate_id"] == candidate.candidate_id
    assert listed["candidates"][0]["evidence_count"] == 1

    detail = service.get_candidate(candidate.candidate_id)
    assert detail["candidate"]["write_provenance"] == {"writer": "extractor"}
    assert detail["evidence"][0]["evidence_text"] == (
        "Ada asked for written planning notes."
    )

    confirmed = service.confirm_candidate(
        candidate.candidate_id,
        actor="operator",
        reason="source reviewed",
        reviewed_at="2026-06-26T12:00:00",
        metadata={"ticket": "thoth-zps.3"},
    )
    confirmed_candidate = confirmed["candidate"]
    assert confirmed_candidate["status"] == "confirmed"
    review_metadata = confirmed_candidate["metadata"][
        SEMANTIC_MEMORY_REVIEW_METADATA_KEY
    ]
    assert review_metadata["action"] == "confirm"
    assert review_metadata["actor"] == "operator"
    assert review_metadata["metadata"] == {"ticket": "thoth-zps.3"}
    transition = confirmed_candidate["write_provenance"]["last_status_transition"]
    assert transition["from"] == "proposed"
    assert transition["to"] == "confirmed"
    assert transition["write_provenance"][SEMANTIC_MEMORY_REVIEW_METADATA_KEY][
        "reason"
    ] == "source reviewed"
    assert confirmed_candidate["write_provenance"]["status_transitions"] == [
        transition
    ]
    assert confirmed["evidence"][0]["evidence_id"] == "evidence-review-1"

    promoted = service.promote_candidate(
        candidate.candidate_id,
        actor="operator",
        reason="confirmed by review",
        reviewed_at="2026-06-26T12:05:00",
    )
    assert promoted["candidate"]["status"] == "promoted"
    transitions = promoted["candidate"]["write_provenance"]["status_transitions"]
    assert [item["to"] for item in transitions] == ["confirmed", "promoted"]
    assert (
        promoted["candidate"]["metadata"][SEMANTIC_MEMORY_REVIEW_METADATA_KEY][
            "action"
        ]
        == "promote"
    )

    rejected_payload = service.reject_candidate(
        rejected.candidate_id,
        actor="operator",
        reason="not supported",
        reviewed_at="2026-06-26T12:10:00",
    )
    assert rejected_payload["candidate"]["status"] == "rejected"

    superseded_payload = service.supersede_candidate(
        old.candidate_id,
        superseded_by_candidate_id=replacement.candidate_id,
        actor="operator",
        reason="better wording",
        reviewed_at="2026-06-26T12:15:00",
    )
    superseded_candidate = superseded_payload["candidate"]
    assert superseded_candidate["status"] == "superseded"
    assert superseded_candidate["superseded_by_candidate_id"] == replacement.candidate_id
    assert superseded_payload["evidence"][0]["evidence_id"] == "evidence-review-old"


def test_semantic_memory_promotion_requires_repeated_evidence(tmp_path: Path):
    store = make_store(tmp_path)
    candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-single-transcript-guess",
            candidate_type="preference",
            text="Ada prefers afternoon meetings.",
            subject="Ada",
            predicate="prefers",
            object_value="afternoon meetings",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-single-transcript-guess",
                evidence_id="evidence-transcript-1",
                artifact_id="transcript-1",
                artifact_type="transcript",
                evidence_text="A model guessed this from one transcript.",
            ),
        ),
    )

    decision = store.evaluate_promotion(candidate.candidate_id)
    assert decision.allowed is False
    assert decision.reason == "insufficient_evidence"
    assert decision.evidence_count == 1

    with pytest.raises(SemanticMemoryTransitionError):
        store.promote_candidate(candidate.candidate_id)
    assert store.get_candidate(candidate.candidate_id).status == "proposed"

    store.add_evidence(
        SemanticMemoryEvidence(
            candidate_id=candidate.candidate_id,
            evidence_id="evidence-transcript-2",
            artifact_id="transcript-2",
            artifact_type="transcript",
            evidence_text="The same preference appeared in a separate transcript.",
        )
    )

    promoted = store.promote_candidate(
        candidate.candidate_id,
        write_provenance={"actor": "semantic-promoter"},
    )
    assert promoted.status == "promoted"
    promotion_gate = promoted.metadata[SEMANTIC_MEMORY_PROMOTION_METADATA_KEY]
    assert promotion_gate["reason"] == "repeated_evidence"
    assert promotion_gate["evidence_count"] == 2
    assert promotion_gate["distinct_source_count"] == 2
    assert (
        promoted.write_provenance["last_status_transition"][
            SEMANTIC_MEMORY_PROMOTION_METADATA_KEY
        ]["reason"]
        == "repeated_evidence"
    )


def test_semantic_memory_promotion_thresholds_are_configurable(tmp_path: Path):
    policy = SemanticMemoryPromotionPolicy.from_mapping(
        {
            "min_evidence_count": 3,
            "min_distinct_sources": 2,
            "trusted_structured_artifact_types": ["operator_record"],
            "trusted_structured_metadata_keys": ["operator_verified"],
        }
    )
    store = make_store(tmp_path, promotion_policy=policy)
    candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-configured-threshold",
            candidate_type="fact",
            text="Project Thoth has a semantic memory review queue.",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-configured-threshold",
                evidence_id="evidence-threshold-1",
                artifact_id="artifact-a",
                artifact_type="note",
            ),
            SemanticMemoryEvidence(
                candidate_id="candidate-configured-threshold",
                evidence_id="evidence-threshold-2",
                artifact_id="artifact-b",
                artifact_type="note",
            ),
        ),
    )

    decision = store.evaluate_promotion(candidate.candidate_id)
    assert decision.allowed is False
    assert decision.min_evidence_count == 3
    assert decision.evidence_count == 2

    store.add_evidence(
        SemanticMemoryEvidence(
            candidate_id=candidate.candidate_id,
            evidence_id="evidence-threshold-3",
            artifact_id="artifact-c",
            artifact_type="note",
        )
    )
    assert store.promote_candidate(candidate.candidate_id).status == "promoted"

    with pytest.raises(SemanticMemoryPromotionConfigError):
        SemanticMemoryPromotionPolicy.from_mapping({"min_evidence_count": 0})


def test_semantic_memory_promotes_trusted_structured_input(tmp_path: Path):
    store = make_store(tmp_path)
    candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-trusted-structured",
            candidate_type="person",
            text="Ada is the operator profile owner.",
            subject="Ada",
            predicate="is",
            object_value="operator profile owner",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-trusted-structured",
                evidence_id="evidence-contact-card",
                artifact_id="contact-card-ada",
                artifact_type="contact_card",
                evidence_text="Structured contact card identifies Ada.",
            ),
        ),
    )

    promoted = store.promote_candidate(candidate.candidate_id)
    assert promoted.status == "promoted"
    assert (
        promoted.metadata[SEMANTIC_MEMORY_PROMOTION_METADATA_KEY]["reason"]
        == "trusted_structured_input"
    )


def test_semantic_memory_rejected_candidates_need_new_evidence_to_reappear(
    tmp_path: Path,
):
    store = make_store(tmp_path)
    rejected = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-rejected-preference",
            candidate_type="preference",
            text="Ada prefers evening standups.",
            subject="Ada",
            predicate="prefers",
            object_value="evening standups",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-rejected-preference",
                evidence_id="evidence-rejected-original",
                source_path="transcripts/day-1.txt",
                evidence_text="A single transcript guess.",
            ),
        ),
    )
    store.transition_candidate(rejected.candidate_id, "rejected")

    with pytest.raises(SemanticMemoryValidationError):
        store.add_candidate(
            SemanticMemoryCandidate(
                candidate_id="candidate-rejected-reappears",
                candidate_type="preference",
                text="Ada prefers evening standups.",
                subject="Ada",
                predicate="prefers",
                object_value="evening standups",
            ),
            evidence=(
                SemanticMemoryEvidence(
                    candidate_id="candidate-rejected-reappears",
                    evidence_id="evidence-rejected-repeat",
                    source_path="transcripts/day-1.txt",
                    evidence_text="The same evidence should not re-open the claim.",
                ),
            ),
        )

    update_candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-rejected-update",
            candidate_type="preference",
            text="Ada prefers written updates.",
            subject="Ada",
            predicate="prefers",
            object_value="written updates",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-rejected-update",
                evidence_id="evidence-rejected-update",
                source_path="transcripts/day-1.txt",
                evidence_text="The same source should not revive rejected claims.",
            ),
        ),
    )
    with pytest.raises(SemanticMemoryValidationError):
        store.update_candidate(
            SemanticMemoryCandidate(
                candidate_id=update_candidate.candidate_id,
                candidate_type="preference",
                text="Ada prefers evening standups.",
                subject="Ada",
                predicate="prefers",
                object_value="evening standups",
            )
        )

    reappeared = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-rejected-new-evidence",
            candidate_type="preference",
            text="Ada prefers evening standups.",
            subject="Ada",
            predicate="prefers",
            object_value="evening standups",
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-rejected-new-evidence",
                evidence_id="evidence-rejected-new",
                source_path="transcripts/day-2.txt",
                evidence_text="A separate source repeats the claim.",
            ),
        ),
    )
    assert reappeared.status == "proposed"


def test_semantic_memory_fails_closed_on_invalid_inputs(tmp_path: Path):
    store = make_store(tmp_path)

    with pytest.raises(SemanticMemoryValidationError):
        SemanticMemoryCandidate(candidate_type="budget", text="Unsupported type.")

    with pytest.raises(SemanticMemoryValidationError):
        SemanticMemoryCandidate(
            candidate_type="fact",
            status="archived",
            text="Unsupported status.",
        )

    candidate = store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-topic-1",
            candidate_type="topic",
            text="Semantic memory",
        )
    )

    with pytest.raises(SemanticMemoryValidationError):
        SemanticMemoryEvidence(
            candidate_id=candidate.candidate_id,
            evidence_text="Evidence without a link is not enough.",
        )

    with pytest.raises(SemanticMemoryValidationError):
        store.list_evidence(candidate_type="unknown")
