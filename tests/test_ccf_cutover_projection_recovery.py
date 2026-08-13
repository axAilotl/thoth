"""Projection-destruction recovery cutover gate."""

from __future__ import annotations

import pytest

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.ids import generate_id
from ccf_helpers import authority
from ccf_cutover_test_support import (
    PROJECTION_TABLES,
    drop_all_projections,
    make_cutover_rig,
    reprovision_projection_tables,
)


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_cutover_rig(ccf_settings, tmp_path, ccf_package_root)


# ---------------------------------------------------------------------------
# Gate 2: projection destruction, human decisions survive
# ---------------------------------------------------------------------------

def _drop_all_projections(settings: CcfPostgresSettings) -> None:
    drop_all_projections(settings)


def _reprovision_projection_tables(settings: CcfPostgresSettings) -> None:
    """Recreate destroyed projection tables from the pinned migration.

    Migration 0002 is recorded as applied, so ``migrate_ccf_store`` will
    not re-run it; its statements are idempotent by design
    (``CREATE TABLE/INDEX IF NOT EXISTS``), which is the operator recovery
    path after destroying projection tables.
    """
    reprovision_projection_tables(settings)


def _semantic(rig, object_id: str) -> dict:
    obj = rig.archive.get_object(object_id)
    assert obj is not None, f"object missing: {object_id}"
    envelope = obj["compartments"]["semantic"]["envelope"]
    assert envelope is not None, f"semantic compartment unavailable: {object_id}"
    return envelope["content"]


def _lineage_states(rig) -> dict:
    with open_ccf_connection(rig.settings) as conn:
        return {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT lineage_id, state, head_record_id FROM lineage_head "
                "WHERE archive_id = %s",
                (rig.archive.archive_id,),
            ).fetchall()
        }


@pytest.fixture()
def decision_archive(rig):
    """An archive holding every human-decision class the gate covers."""
    from ccf.thothmap import MapContext
    from ccf.thothmap.review import review_submissions
    from ccf.thothmap.semantic import assertion_submissions
    from ccf.thothmap.sources import source_submission

    ctx = MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)

    # -- review decision over a memory candidate (governance.review_decision)
    source = source_submission(
        rig.producer,
        ctx,
        {"source_name": "omi", "source_type": "wearable_audio",
         "collector": "thoth.capture"},
    )
    candidate = assertion_submissions(
        rig.producer,
        ctx,
        {
            "candidate_id": "gate2-candidate",
            "candidate_type": "preference",
            "status": "proposed",
            "subject": "Ada",
            "predicate": "prefers",
            "object_value": "projection destruction drills",
            "text": "I prefer drills before cutover.",
            "confidence": 0.8,
        },
        source_ccf_id=source.records[0]["id"],
    )
    mapped = review_submissions(
        rig.producer,
        ctx,
        {"action": "confirm", "actor": "ada",
         "at": "2026-08-12T00:00:00Z", "reason": "gate 2 review"},
        source_ccf_id=source.records[0]["id"],
        target_ccf_ids=[candidate.records[0]["id"]],
        reviewer_ccf_id=rig.person_id,
        accepted_type="semantic.assertion",
        accepted_payload=candidate.records[0]["payload"],
    )
    batch = rig.producer.create_batch(
        records=source.records + candidate.records + mapped.records,
        links=mapped.links,
    )
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result
    review_decision_id = mapped.records[0]["id"]

    # -- entity resolution (human merge adjudication) --------------------
    def _entity(label):
        return rig.producer.new_record(
            type="semantic.entity",
            claims=rig.claims(),
            payload={
                "entity_kind": "person",
                "label": label,
                "aliases": [],
                "description": f"entity {label}",
                "extensions": {},
            },
        )

    e1, e2 = _entity("Ada Lovelace"), _entity("A. Lovelace")
    same_as = rig.producer.new_link(
        type="ccf.same_as", from_id=e1["id"], to_id=e2["id"], claims=rig.claims()
    )
    resolution_claims = rig.claims()
    resolution_claims["authority"] = authority("person_accepted", rig.person_id)
    resolution = rig.producer.new_record(
        type="semantic.entity_resolution",
        claims=resolution_claims,
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "create",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "action": "same_as",
            "entity_ids": [e1["id"], e2["id"]],
            "canonical_entity_id": e1["id"],
            "reason": "gate 2 merge",
            "evidence_refs": [],
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(
        records=[e1, e2, resolution], links=[same_as]
    )
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    # -- consent lineage (governance.consent) ----------------------------
    consent_claims = rig.claims()
    consent_claims["authority"] = authority(
        "first_person_statement", rig.person_id, rig.person_id
    )
    consent_lineage_id = generate_id("lineage")
    consent = rig.producer.new_record(
        type="governance.consent",
        claims=consent_claims,
        lineage={
            "lineage_id": consent_lineage_id,
            "previous_head_id": None,
            "transition": "give",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "subject_id": rig.person_id,
            "controller_id": rig.person_id,
            "decision": "given",
            "purposes": ["gate-2"],
            "operations": ["read_local"],
            "data_classes": ["document_content"],
            "scope": {},
            "valid_from": "2026-08-12T00:00:00.000Z",
            "expires_at": None,
            "evidence_refs": [],
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(records=[consent])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    # -- link disposition (retract a derived_from link) ------------------
    target_a = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "gate2-a", "definition": "a", "aliases": [],
                 "extensions": {}},
    )
    target_b = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "gate2-b", "definition": "b", "aliases": [],
                 "extensions": {}},
    )
    target_c = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "gate2-c", "definition": "c", "aliases": [],
                 "extensions": {}},
    )
    derived = rig.producer.new_link(
        type="ccf.derived_from", from_id=target_b["id"], to_id=target_a["id"],
        claims=rig.claims(), selector={},
    )
    # A second derived_from link stays active so the derivation closure
    # projection has rows to destroy and recover.
    active_derived = rig.producer.new_link(
        type="ccf.derived_from", from_id=target_c["id"], to_id=target_a["id"],
        claims=rig.claims(), selector={},
    )
    retract = rig.producer.new_record(
        type="lineage.link_disposition",
        claims=rig.claims(),
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "retract",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "target_link_id": derived["id"],
            "action": "retract",
            "reason": "gate 2 disposition",
            "previous_disposition_id": None,
            "replacement_link_id": None,
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(
        records=[target_a, target_b, target_c, retract],
        links=[derived, active_derived],
    )
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    # -- erasure saga to receipt (lineage.erasure_receipt) ---------------
    utterance = rig.producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        payload={
            "text": "gate 2 erases this utterance",
            "language": "en",
            "speaker_id": None,
            "sequence": None,
            "transcription": None,
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(records=[utterance])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    svc = rig.archive.erasure()
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={"targets": [
            {"object_id": utterance["id"], "compartments": ["semantic"]}
        ]},
        reason="gate 2 erasure",
        authority=authority("first_person_statement", rig.person_id, rig.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=[{"object_id": utterance["id"], "compartments": ["semantic"]}],
        reasoning="approved for gate 2",
        decided_by=rig.person_id,
        authority=authority(
            "explicit_authorization", rig.person_id, rig.person_id
        ),
    )
    status = svc.execute(decided["operation_id"])
    assert status["stage"] == "receipt", status

    with open_ccf_connection(rig.settings) as conn:
        receipt_id = conn.execute(
            """
            SELECT object_id FROM compartment
            WHERE compartment = 'structural' AND state = 'plaintext'
              AND plaintext_json ->> 'type' = 'lineage.erasure_receipt'
            LIMIT 1
            """
        ).fetchone()[0]

    return {
        "review_decision_id": review_decision_id,
        "entity_ids": [e1["id"], e2["id"]],
        "resolution_id": resolution["id"],
        "canonical_entity_id": e1["id"],
        "same_as_link_id": same_as["id"],
        "consent_id": consent["id"],
        "consent_lineage_id": consent_lineage_id,
        "derived_link_id": derived["id"],
        "disposition_id": retract["id"],
        "erased_utterance_id": utterance["id"],
        "receipt_id": receipt_id,
        "operation_id": decided["operation_id"],
    }


def test_gate2_projection_destruction_recovers_every_decision(rig, decision_archive):
    ids = decision_archive

    # Rebuild so every projection is populated, then snapshot canonical
    # human-decision state and projection rows.
    rebuilt = rig.archive.projections.rebuild_all()
    assert all(count >= 1 for count in rebuilt.values()), rebuilt
    before_semantics = {
        label: _semantic(rig, ids[key])
        for label, key in (
            ("review", "review_decision_id"),
            ("resolution", "resolution_id"),
            ("consent", "consent_id"),
            ("disposition", "disposition_id"),
            ("receipt", "receipt_id"),
        )
    }
    before_lineages = _lineage_states(rig)
    content_projections = (
        "projection_link_state",
        "projection_derivation_closure",
        "projection_entity_cluster",
        "projection_full_text",
    )
    with open_ccf_connection(rig.settings) as conn:
        # Content columns only: the trailing metadata pair
        # (computed_through_sequence, generation) is machinery state — the
        # fence generation legitimately resets when the fence table is
        # destroyed, and is asserted separately below.
        before_projections = {
            table: [
                row[:-1]
                for row in conn.execute(
                    f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY 1, 2",
                    (rig.archive.archive_id,),
                ).fetchall()
            ]
            for table in content_projections
        }
        before_link_state = conn.execute(
            "SELECT state, selector_available FROM projection_link_state "
            "WHERE archive_id = %s AND link_id = %s",
            (rig.archive.archive_id, ids["derived_link_id"]),
        ).fetchone()
        before_cluster = conn.execute(
            "SELECT cluster_id, canonical_member_id FROM projection_entity_cluster "
            "WHERE archive_id = %s AND member_id = %s",
            (rig.archive.archive_id, ids["entity_ids"][0]),
        ).fetchone()
    assert before_link_state == ("retracted", True)
    assert before_cluster is not None
    assert before_cluster[1] == ids["canonical_entity_id"]

    # Destroy every projection table outright (not truncate).
    _drop_all_projections(rig.settings)
    with open_ccf_connection(rig.settings) as conn:
        for table in PROJECTION_TABLES:
            assert conn.execute(
                "SELECT 1 FROM pg_tables WHERE tablename = %s "
                "AND schemaname = current_schema()",
                (table,),
            ).fetchone() is None

    # Rebuild from canonical state only.
    _reprovision_projection_tables(rig.settings)
    rebuilt = rig.archive.projections.rebuild_all()
    assert all(count >= 1 for count in rebuilt.values()), rebuilt

    # The signed chain is intact through the whole drill.
    report = rig.archive.verify_chain()
    assert report["commits_verified"] >= 8

    # Every human decision survived with its exact semantic content.
    for label, key in (
        ("review", "review_decision_id"),
        ("resolution", "resolution_id"),
        ("consent", "consent_id"),
        ("disposition", "disposition_id"),
        ("receipt", "receipt_id"),
    ):
        assert _semantic(rig, ids[key]) == before_semantics[label], label

    # Lineage heads survived exactly (policy, consent, resolution,
    # disposition, erasure decision lineage, credential).
    assert _lineage_states(rig) == before_lineages
    assert before_lineages[ids["consent_lineage_id"]][0] == "give"

    # The erasure stayed real: erased compartment serves no plaintext.
    erased = rig.archive.get_object(ids["erased_utterance_id"])
    assert erased["compartments"]["semantic"]["state"] == "erased"
    assert erased["compartments"]["semantic"]["envelope"] is None

    # Projection content recovered row-for-row; every row is stamped with
    # the current head sequence under the fresh fence generation.
    head_sequence = int(rig.archive.head()["sequence"])
    with open_ccf_connection(rig.settings) as conn:
        for table, before_rows in before_projections.items():
            after_rows = conn.execute(
                f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY 1, 2",
                (rig.archive.archive_id,),
            ).fetchall()
            assert [row[:-1] for row in after_rows] == before_rows, table
            assert all(
                int(row[-2]) == head_sequence for row in after_rows
            ), table
        link_state = conn.execute(
            "SELECT state, selector_available FROM projection_link_state "
            "WHERE archive_id = %s AND link_id = %s",
            (rig.archive.archive_id, ids["derived_link_id"]),
        ).fetchone()
        cluster = conn.execute(
            "SELECT cluster_id, canonical_member_id FROM projection_entity_cluster "
            "WHERE archive_id = %s AND member_id = %s",
            (rig.archive.archive_id, ids["entity_ids"][0]),
        ).fetchone()
    assert link_state == ("retracted", True)
    assert cluster == before_cluster

    # The embedding table is recreated by migration but explicitly not
    # rebuildable from canonical state (fail closed, never zeroed).
    from ccf.projections import EMBEDDING
    from ccf.projections.rebuild import RebuildError

    with pytest.raises(RebuildError, match="caller-supplied"):
        rig.archive.projections.rebuild(EMBEDDING)


# ---------------------------------------------------------------------------
