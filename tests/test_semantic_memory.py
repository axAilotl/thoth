from pathlib import Path

import pytest

from core.metadata_db import MetadataDB
from core.semantic_memory import (
    SemanticMemoryCandidate,
    SemanticMemoryEvidence,
    SemanticMemoryStore,
    SemanticMemoryTransitionError,
    SemanticMemoryValidationError,
)


def make_store(tmp_path: Path) -> SemanticMemoryStore:
    return SemanticMemoryStore(MetadataDB(str(tmp_path / "meta.db")))


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

    confirmed = store.transition_candidate(
        proposed.candidate_id,
        "confirmed",
        write_provenance={"actor": "reviewer"},
    )
    assert confirmed.status == "confirmed"
    assert confirmed.write_provenance["last_status_transition"]["from"] == "proposed"

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
