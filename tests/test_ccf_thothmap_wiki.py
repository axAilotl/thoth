"""thothmap wiki projection tests (checklist 4: wiki pages -> projections)."""

from __future__ import annotations

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.context import ThothMapError
from ccf.thothmap.semantic import entity_submission
from ccf.thothmap.sources import source_submission
from ccf.thothmap.wiki import wiki_projection_submissions

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def inputs(rig, ctx):
    """Admitted source plus one entity the wiki page was compiled from."""
    source = source_submission(
        rig.producer,
        ctx,
        {"source_name": "arxiv", "source_type": "paper_feed", "collector": "arxiv"},
    )
    entity = entity_submission(
        rig.producer,
        ctx,
        {
            "canonical_id": "paper:arxiv_id:2401.12345",
            "entity_type": "paper",
            "display_name": "Continuity Core Format",
            "metadata": {},
        },
        source_ccf_id=source.records[0]["id"],
    )
    admit_mapped(rig, source.extend(entity))
    return {"source_id": source.records[0]["id"], "entity_id": entity.records[0]["id"]}


WIKI_PAGE = {
    "slug": "continuity-core-format",
    "title": "Continuity Core Format",
    "summary": "Canonical capture format used by Thoth.",
    "kind": "entity",
    "input_hash": "b7c1d2e3" * 8,
    "source_paths": ["knowledge_vault/library/arxiv/2401.12345.md"],
    "event_ids": [],
    "semantic_candidate_ids": ["c4nd1date-1"],
    "updated_at": "2026-08-11T23:00:00Z",
}


def test_wiki_page_maps_to_projection_artifact_with_evidence(rig, ctx, inputs):
    mapped = wiki_projection_submissions(
        rig.producer,
        ctx,
        WIKI_PAGE,
        source_ccf_id=inputs["source_id"],
        evidence_ccf_ids=[inputs["entity_id"]],
    )
    record = mapped.records[0]
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.experience.artifact",
        record["payload"],
        what="experience.artifact",
    )
    assert record["payload"]["artifact_role"] == "wiki_projection"
    assert record["payload"]["extensions"]["thoth_input_hash"] == WIKI_PAGE["input_hash"]

    derived = mapped.links[0]
    assert derived["type"] == "ccf.derived_from"
    assert derived["from_id"] == record["id"]
    assert derived["to_id"] == inputs["entity_id"]

    result = admit_mapped(rig, mapped)
    assert outcome_for(result, record["id"])["status"] == "admitted"
    semantic = compartment(rig, record["id"], "semantic")
    assert semantic["origin"]["native_id"] == "wiki:continuity-core-format"
    assert semantic["authority"]["basis"] == "deterministic_derivation"


def test_wiki_projection_refused_without_evidence(rig, ctx, inputs):
    with pytest.raises(ThothMapError, match="evidence"):
        wiki_projection_submissions(
            rig.producer,
            ctx,
            WIKI_PAGE,
            source_ccf_id=inputs["source_id"],
            evidence_ccf_ids=[],
        )
