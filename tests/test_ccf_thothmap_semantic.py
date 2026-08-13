"""thothmap semantic mapping tests (checklist 4: entities/assertions -> candidates)."""

from __future__ import annotations

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.context import ThothMapError
from ccf.thothmap.semantic import assertion_submissions, entity_submission
from ccf.thothmap.sources import source_submission

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def source_id(rig, ctx):
    mapped = source_submission(
        rig.producer,
        ctx,
        {"source_name": "omi", "source_type": "wearable_audio", "collector": "thoth.capture"},
    )
    admit_mapped(rig, mapped)
    return mapped.records[0]["id"]


ENTITY = {
    "canonical_id": "paper:arxiv_id:2401.12345",
    "entity_type": "paper",
    "primary_artifact_id": "arxiv_2401.12345",
    "display_name": "Continuity Core Format",
    "wiki_slug": "continuity-core-format",
    "metadata": {"aliases": ["CCF", "continuity format"]},
}

CANDIDATE = {
    "candidate_id": "c4nd1date-7e8f-4a5b-9c6d-2e1f0a3b4c5d",
    "candidate_type": "preference",
    "status": "proposed",
    "subject": "Ada",
    "predicate": "prefers",
    "object_value": "open adoption of the continuity schema",
    "text": "I want the schema open because adoption is still a win.",
    "confidence": 0.9,
    "entity_id": None,
}


def test_entity_maps_with_native_canonical_id(rig, ctx, source_id):
    mapped = entity_submission(rig.producer, ctx, ENTITY, source_ccf_id=source_id)
    record = mapped.records[0]
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.semantic.entity",
        record["payload"],
        what="semantic.entity",
    )
    assert record["payload"]["entity_kind"] == "paper"
    assert record["payload"]["aliases"] == ["CCF", "continuity format"]

    result = admit_mapped(rig, mapped)
    assert outcome_for(result, record["id"])["status"] == "admitted"
    semantic = compartment(rig, record["id"], "semantic")
    assert semantic["origin"]["native_id"] == "paper:arxiv_id:2401.12345"
    assert semantic["authority"]["basis"] == "deterministic_derivation"


def test_candidate_maps_to_machine_inferred_assertion(rig, ctx, source_id):
    entity = entity_submission(rig.producer, ctx, ENTITY, source_ccf_id=source_id)
    admit_mapped(rig, entity)
    entity_id = entity.records[0]["id"]

    mapped = assertion_submissions(
        rig.producer,
        ctx,
        CANDIDATE,
        source_ccf_id=source_id,
        subject_ccf_id=entity_id,
        evidence_ccf_ids=[entity_id],
    )
    record = mapped.records[0]
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.semantic.assertion",
        record["payload"],
        what="semantic.assertion",
    )
    payload = record["payload"]
    # Free-form Thoth predicate normalized; original preserved in qualifiers.
    assert payload["predicate"] == "thoth.preference.prefers"
    assert payload["qualifiers"]["thoth_predicate"] == "prefers"
    assert payload["qualifiers"]["thoth_confidence"] == 0.9
    assert payload["subject"] == {"ref": entity_id}
    assert payload["object"] == {"value": "open adoption of the continuity schema",
                                 "datatype": "string"}

    evidence_link = mapped.links[0]
    assert evidence_link["type"] == "ccf.evidence_for"
    assert evidence_link["from_id"] == entity_id
    assert evidence_link["to_id"] == record["id"]

    result = admit_mapped(rig, mapped)
    assert result["status"] == "accepted"
    semantic = compartment(rig, record["id"], "semantic")
    assert semantic["origin"]["native_id"] == CANDIDATE["candidate_id"]
    assert semantic["authority"]["basis"] == "machine_inference"
    structural = compartment(rig, evidence_link["id"], "structural")
    assert structural["from_id"] == entity_id


def test_candidate_literal_subject_when_unmapped(rig, ctx, source_id):
    mapped = assertion_submissions(rig.producer, ctx, CANDIDATE, source_ccf_id=source_id)
    assert mapped.records[0]["payload"]["subject"] == {"value": "Ada", "datatype": "string"}


def test_candidate_requires_object_value(rig, ctx, source_id):
    bad = {k: v for k, v in CANDIDATE.items() if k != "object_value"}
    with pytest.raises(ThothMapError, match="object_value"):
        assertion_submissions(rig.producer, ctx, bad, source_ccf_id=source_id)


def test_candidate_reimport_idempotent(rig, ctx, source_id):
    mapped = assertion_submissions(rig.producer, ctx, CANDIDATE, source_ccf_id=source_id)
    admit_mapped(rig, mapped)
    replay = admit_mapped(rig, mapped)
    assert outcome_for(replay, mapped.records[0]["id"])["status"] == "existing"
