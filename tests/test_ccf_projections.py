"""Projection tests (checklist phase 5, spec sections 8.4-8.7, 10.3-10.6).

Covers: current Link state from dispositions; the active derived_from
closure matching the recursive-CTE baseline on a branched DAG; projection
invalidation (stale after admission and after lineage transitions, fast
path refusing stale rows); destroying every projection table and
rebuilding from canonical state with zero loss; entity merge/split via
same_as links; tsvector full-text search; pgvector round-trip;
checkpoint save/corruption/fallback-replay; and the cross-projection
snapshot pin.
"""

from __future__ import annotations

import pytest

from ccf.db import open_ccf_connection
from ccf.ids import generate_id
from ccf.projections import DERIVATION, LINK_STATE
from ccf.projections import checkpoints, consistency, derivation, entities, fulltext, links
from ccf.projections.invalidation import (
    ProjectionStaleError,
    fence_state,
    pending_invalidations,
    row_usable,
)
from ccf.projections.rebuild import rebuild_all
from ccf.projections.vectors import VectorSupportError

from ccf_helpers import authority, make_rig


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture(scope="module")
def schemas(ccf_package_root):
    from ccf.schemas import SchemaSet

    return SchemaSet.load(ccf_package_root)


def _admit(rig, *, records=None, links=None):
    batch = rig.producer.create_batch(records=records or [], links=links or [])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "accepted", result
    return result


def _concept(rig, text):
    return rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": text,
            "definition": f"definition of {text}",
            "aliases": [],
            "extensions": {},
        },
    )


def _derived_link(rig, from_id, to_id):
    return rig.producer.new_link(
        type="ccf.derived_from", from_id=from_id, to_id=to_id, claims=rig.claims()
    )


def _disposition(rig, target_link_id, action, lineage_id, previous_head_id):
    return rig.producer.new_record(
        type="lineage.link_disposition",
        claims=rig.claims(),
        lineage={
            "lineage_id": lineage_id,
            "previous_head_id": previous_head_id,
            "transition": action,
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "target_link_id": target_link_id,
            "action": action,
            "reason": "projection test",
            "previous_disposition_id": previous_head_id,
            "replacement_link_id": None,
            "extensions": {},
        },
    )


def _entity(rig, label):
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


def _same_as(rig, a, b):
    return rig.producer.new_link(
        type="ccf.same_as", from_id=a, to_id=b, claims=rig.claims()
    )


def _resolution(rig, entity_ids, canonical_entity_id):
    # semantic.entity_resolution requires person_accepted_or_reviewed
    # authority: the merge is a human adjudication, not runtime output.
    claims = rig.claims()
    claims["authority"] = authority("person_accepted", rig.person_id)
    return rig.producer.new_record(
        type="semantic.entity_resolution",
        claims=claims,
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "create",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "action": "same_as",
            "entity_ids": entity_ids,
            "canonical_entity_id": canonical_entity_id,
            "reason": "reviewed merge",
            "evidence_refs": [],
            "extensions": {},
        },
    )


def _utterance(rig, text):
    return rig.producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        payload={
            "text": text,
            "language": "en",
            "speaker_id": None,
            "sequence": None,
            "transcription": None,
            "extensions": {},
        },
    )


def _table_rows(conn, archive_id, table):
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY 1, 2", (archive_id,)
    ).fetchall()
    return [tuple(str(value) for value in row) for row in rows]


# ---------------------------------------------------------------------------
# Current Link state (spec 8.4)
# ---------------------------------------------------------------------------


def test_link_state_reflects_disposition_transitions(rig):
    a, b = _concept(rig, "alpha"), _concept(rig, "beta")
    link = _derived_link(rig, b["id"], a["id"])
    _admit(rig, records=[a, b], links=[link])

    with open_ccf_connection(rig.settings) as conn:
        assert links.rebuild(conn, rig.archive.archive_id) == 1
        state = links.get_link_state(conn, rig.archive.archive_id, link["id"])
        assert state["state"] == "active"
        assert state["selector_available"] is True

    retract = _disposition(rig, link["id"], "retract", generate_id("lineage"), None)
    _admit(rig, records=[retract])

    with open_ccf_connection(rig.settings) as conn:
        # Stale fast path: the admission advanced the fence synchronously.
        with pytest.raises(ProjectionStaleError):
            links.get_link_state(conn, rig.archive.archive_id, link["id"])
        links.rebuild(conn, rig.archive.archive_id)
        state = links.get_link_state(conn, rig.archive.archive_id, link["id"])
        assert state["state"] == "retracted"
        assert state["disposition_record_id"] == retract["id"]

    restore = _disposition(
        rig, link["id"], "restore", retract["lineage"]["lineage_id"], retract["id"]
    )
    _admit(rig, records=[restore])
    with open_ccf_connection(rig.settings) as conn:
        links.rebuild(conn, rig.archive.archive_id)
        state = links.get_link_state(conn, rig.archive.archive_id, link["id"])
        assert state["state"] == "active"


def test_invalidate_selector_keeps_link_active_but_unavailable(rig):
    a, b = _concept(rig, "gamma"), _concept(rig, "delta")
    link = _derived_link(rig, b["id"], a["id"])
    disposition = _disposition(
        rig, link["id"], "invalidate_selector", generate_id("lineage"), None
    )
    _admit(rig, records=[a, b, disposition], links=[link])
    with open_ccf_connection(rig.settings) as conn:
        links.rebuild(conn, rig.archive.archive_id)
        state = links.get_link_state(conn, rig.archive.archive_id, link["id"])
        assert state["state"] == "active"
        assert state["selector_available"] is False


# ---------------------------------------------------------------------------
# Derivation closure vs recursive CTE (spec 8.6, 10.3)
# ---------------------------------------------------------------------------


def _branched_dag(rig):
    """A <- B, A <- C, B <- D, C <- D (D derived from B and C)."""
    a, b, c, d = (_concept(rig, name) for name in ("a", "b", "c", "d"))
    edges = [
        _derived_link(rig, b["id"], a["id"]),
        _derived_link(rig, c["id"], a["id"]),
        _derived_link(rig, d["id"], b["id"]),
        _derived_link(rig, d["id"], c["id"]),
    ]
    _admit(rig, records=[a, b, c, d], links=edges)
    return a, b, c, d, edges


def test_closure_matches_recursive_cte_on_branched_dag(rig):
    a, b, c, d, _edges = _branched_dag(rig)
    with open_ccf_connection(rig.settings) as conn:
        derivation.rebuild(conn, rig.archive.archive_id)
        pairs = derivation.closure_pairs(conn, rig.archive.archive_id)

        # Every closure pair equals the CTE baseline, node by node.
        for node in (a["id"], b["id"], c["id"], d["id"]):
            baseline = derivation.ancestors_of(conn, rig.archive.archive_id, node)
            from_closure = {
                ancestor: meta["minimum_depth"]
                for (ancestor, descendant), meta in pairs.items()
                if descendant == node
            }
            assert from_closure == baseline

        assert pairs[(a["id"], d["id"])] == {
            "minimum_depth": 2,
            "active_path_count": 2,
        }
        assert set(derivation.descendants_of(conn, rig.archive.archive_id, a["id"])) == {
            b["id"],
            c["id"],
            d["id"],
        }


def test_retracted_edge_leaves_the_active_graph(rig):
    a, b, c, d, edges = _branched_dag(rig)
    retract = _disposition(
        rig,
        edges[-1]["id"],  # retract the D -> C edge
        "retract",
        generate_id("lineage"),
        None,
    )
    _admit(rig, records=[retract])
    with open_ccf_connection(rig.settings) as conn:
        derivation.rebuild(conn, rig.archive.archive_id)
        baseline = derivation.ancestors_of(conn, rig.archive.archive_id, d["id"])
        assert set(baseline) == {a["id"], b["id"]}  # C path is gone
        pairs = derivation.closure_pairs(conn, rig.archive.archive_id)
        assert pairs[(a["id"], d["id"])]["active_path_count"] == 1


# ---------------------------------------------------------------------------
# Invalidation (spec 10.4)
# ---------------------------------------------------------------------------


def test_admission_marks_projections_stale_until_rebuilt(rig):
    a, b = _concept(rig, "one"), _concept(rig, "two")
    link = _derived_link(rig, b["id"], a["id"])
    _admit(rig, records=[a, b], links=[link])

    with open_ccf_connection(rig.settings) as conn:
        rebuild_all(conn, archive_id=rig.archive.archive_id)
        assert not pending_invalidations(
            conn, archive_id=rig.archive.archive_id, projection_name=DERIVATION
        )
        fence_before = fence_state(
            conn, archive_id=rig.archive.archive_id, projection_name=DERIVATION
        )

    c = _concept(rig, "three")
    link2 = _derived_link(rig, c["id"], b["id"])
    _admit(rig, records=[c], links=[link2])

    with open_ccf_connection(rig.settings) as conn:
        fence_after = fence_state(
            conn, archive_id=rig.archive.archive_id, projection_name=DERIVATION
        )
        assert fence_after.generation > fence_before.generation
        causes = pending_invalidations(
            conn, archive_id=rig.archive.archive_id, projection_name=DERIVATION
        )
        assert {cause["cause_object_id"] for cause in causes} == {link2["id"]}
        # Aggregate reads fail closed while stale.
        with pytest.raises(ProjectionStaleError):
            derivation.closure_pairs(conn, rig.archive.archive_id)
        # The fast path decides from metadata alone.
        row = conn.execute(
            """
            SELECT computed_through_sequence, generation
            FROM projection_derivation_closure WHERE archive_id = %s LIMIT 1
            """,
            (rig.archive.archive_id,),
        ).fetchone()
        assert not row_usable(
            conn,
            archive_id=rig.archive.archive_id,
            projection_name=DERIVATION,
            computed_through_sequence=int(row[0]),
            generation=int(row[1]),
        )

        derivation.rebuild(conn, rig.archive.archive_id)
        assert row_usable(
            conn,
            archive_id=rig.archive.archive_id,
            projection_name=DERIVATION,
            computed_through_sequence=int(
                conn.execute(
                    "SELECT max(computed_through_sequence) FROM "
                    "projection_derivation_closure WHERE archive_id = %s",
                    (rig.archive.archive_id,),
                ).fetchone()[0]
            ),
            generation=fence_after.generation,
        )


# ---------------------------------------------------------------------------
# Destroy every projection and rebuild with zero loss (spec 8.7)
# ---------------------------------------------------------------------------


def test_destroy_all_projection_tables_and_rebuild_zero_loss(rig):
    a, b, c, d, _edges = _branched_dag(rig)
    utterance = _utterance(rig, "the quarterly report mentioned dolphins")
    entity = _entity(rig, "Zero Loss")
    _admit(rig, records=[utterance, entity])

    with open_ccf_connection(rig.settings) as conn:
        rebuild_all(conn, archive_id=rig.archive.archive_id)
        before = {
            table: _table_rows(conn, rig.archive.archive_id, table)
            for table in (
                "projection_link_state",
                "projection_derivation_closure",
                "projection_entity_cluster",
                "projection_full_text",
            )
        }
        assert all(before.values())

        with conn.transaction():
            for table in before:
                conn.execute(f"TRUNCATE {table}")

        rebuilt = rebuild_all(conn, archive_id=rig.archive.archive_id)
        after = {
            table: _table_rows(conn, rig.archive.archive_id, table) for table in before
        }
        assert after == before
        assert rebuilt["full_text"] >= 1

    # Canonical state (including human decisions) was never in projections.
    assert rig.archive.get_object(utterance["id"]) is not None
    assert rig.archive.verify_chain()["commits_verified"] >= 3


# ---------------------------------------------------------------------------
# Entity clusters (spec 8.5)
# ---------------------------------------------------------------------------


def test_entity_merge_and_split_update_clusters(rig):
    e1, e2 = _entity(rig, "Ada"), _entity(rig, "Ada L.")
    _admit(rig, records=[e1, e2])
    with open_ccf_connection(rig.settings) as conn:
        entities.rebuild(conn, rig.archive.archive_id)
        grouped = entities.clusters(conn, rig.archive.archive_id)
        assert len(grouped) == 2  # singletons keep their own IDs

    link = _same_as(rig, e1["id"], e2["id"])
    resolution = _resolution(rig, [e1["id"], e2["id"]], e1["id"])
    _admit(rig, records=[resolution], links=[link])
    with open_ccf_connection(rig.settings) as conn:
        entities.rebuild(conn, rig.archive.archive_id)
        merged = entities.cluster_of(conn, rig.archive.archive_id, e2["id"])
        assert merged["canonical_member_id"] == e1["id"]
        grouped = entities.clusters(conn, rig.archive.archive_id)
        assert len(grouped) == 1
        assert sorted(next(iter(grouped.values()))) == sorted([e1["id"], e2["id"]])

    # Split: retract the same_as Link; the projection follows.
    retract = _disposition(rig, link["id"], "retract", generate_id("lineage"), None)
    _admit(rig, records=[retract])
    with open_ccf_connection(rig.settings) as conn:
        with pytest.raises(ProjectionStaleError):
            entities.clusters(conn, rig.archive.archive_id)
        entities.rebuild(conn, rig.archive.archive_id)
        assert len(entities.clusters(conn, rig.archive.archive_id)) == 2


# ---------------------------------------------------------------------------
# Full text (spec 10.1)
# ---------------------------------------------------------------------------


def test_tsvector_search_finds_utterance_text(rig):
    utterance = _utterance(rig, "the quarterly report mentioned dolphins")
    other = _utterance(rig, "an unrelated note about bookkeeping")
    _admit(rig, records=[utterance, other])

    with open_ccf_connection(rig.settings) as conn:
        with pytest.raises(ProjectionStaleError):
            fulltext.search(conn, rig.archive.archive_id, "dolphins")
        fulltext.rebuild(conn, rig.archive.archive_id)
        hits = fulltext.search(conn, rig.archive.archive_id, "dolphins")
        assert [hit["object_id"] for hit in hits] == [utterance["id"]]


# ---------------------------------------------------------------------------
# Vectors (spec 10.1; pgvector optional, caller-supplied)
# ---------------------------------------------------------------------------


def test_pgvector_round_trip(rig):
    record = _concept(rig, "vector subject")
    _admit(rig, records=[record])
    with open_ccf_connection(rig.settings) as conn:
        from ccf.projections import vectors

        try:
            with conn.transaction():
                vectors.put_embedding(
                    conn,
                    archive_id=rig.archive.archive_id,
                    object_id=record["id"],
                    model_id="test-model-v1",
                    vector=[0.1, 0.2, 0.3],
                )
                vectors.put_embedding(
                    conn,
                    archive_id=rig.archive.archive_id,
                    object_id=rig.person_id,
                    model_id="test-model-v1",
                    vector=[9.0, 9.0, 9.0],
                )
        except VectorSupportError as exc:
            pytest.skip(f"pgvector unavailable: {exc}")

        stored = vectors.get_embedding(
            conn,
            archive_id=rig.archive.archive_id,
            object_id=record["id"],
            model_id="test-model-v1",
        )
        assert stored == pytest.approx([0.1, 0.2, 0.3])
        nearest = vectors.nearest(
            conn,
            archive_id=rig.archive.archive_id,
            model_id="test-model-v1",
            query_vector=[0.1, 0.2, 0.31],
        )
        assert nearest[0]["object_id"] == record["id"]

        with pytest.raises(VectorSupportError):
            vectors.put_embedding(
                conn,
                archive_id=rig.archive.archive_id,
                object_id=generate_id("record"),
                model_id="test-model-v1",
                vector=[1.0],
            )


# ---------------------------------------------------------------------------
# Checkpoints (spec 10.5)
# ---------------------------------------------------------------------------


def test_checkpoint_save_corrupt_and_fallback_replay(rig, schemas):
    a, b, c, d, _edges = _branched_dag(rig)
    with open_ccf_connection(rig.settings) as conn:
        rebuild_all(conn, archive_id=rig.archive.archive_id)
        first = checkpoints.save_checkpoint(
            conn, archive_id=rig.archive.archive_id, projection_name=LINK_STATE
        )
        schemas.validate(
            "urn:ccf:schema:0.1.2-rc1:operational.projection-checkpoint",
            first,
            what="checkpoint document",
        )
        first_generation = int(first["generation"])
        assert first_generation >= 1
        assert first["snapshot_digest"].startswith("sha256:")

    # Advance canonical state, rebuild, checkpoint again.
    e = _concept(rig, "epsilon")
    link = _derived_link(rig, e["id"], a["id"])
    _admit(rig, records=[e], links=[link])
    with open_ccf_connection(rig.settings) as conn:
        rebuild_all(conn, archive_id=rig.archive.archive_id)
        second = checkpoints.save_checkpoint(
            conn, archive_id=rig.archive.archive_id, projection_name=LINK_STATE
        )
        second_generation = int(second["generation"])
        assert second_generation > first_generation

        # Corrupt the newest checkpoint's snapshot: validation fails closed.
        conn.execute(
            """
            UPDATE projection_checkpoint
            SET snapshot_payload = '{"corrupted": true}'::jsonb
            WHERE archive_id = %s AND projection_name = %s AND generation = %s
            """,
            (rig.archive.archive_id, LINK_STATE, second_generation),
        )
        assert not checkpoints.validate_checkpoint(
            conn,
            archive_id=rig.archive.archive_id,
            projection_name=LINK_STATE,
            generation=second_generation,
        )

        expected = _table_rows(conn, rig.archive.archive_id, "projection_link_state")
        conn.execute("TRUNCATE projection_link_state")

        recovery = checkpoints.recover(
            conn, archive_id=rig.archive.archive_id, projection_name=LINK_STATE
        )
        assert recovery["restored_from_generation"] == first_generation
        assert recovery["replayed_to_completion"] is True
        # Replay recomputes from canonical state: the post-recovery table
        # matches the pre-corruption rebuild exactly.
        assert (
            _table_rows(conn, rig.archive.archive_id, "projection_link_state")
            == expected
        )


# ---------------------------------------------------------------------------
# Cross-projection consistency (spec 10.6)
# ---------------------------------------------------------------------------


def test_snapshot_pin_refuses_mixed_reads_after_admission(rig):
    a, b = _concept(rig, "pin-a"), _concept(rig, "pin-b")
    link = _derived_link(rig, b["id"], a["id"])
    _admit(rig, records=[a, b], links=[link])

    with open_ccf_connection(rig.settings) as conn:
        rebuild_all(conn, archive_id=rig.archive.archive_id)
        pin = consistency.pin_snapshot(conn, rig.archive.archive_id)
        consistency.require_current(conn, pin, LINK_STATE)
        consistency.require_current(conn, pin, DERIVATION)

    c = _concept(rig, "pin-c")
    _admit(rig, records=[c], links=[_derived_link(rig, c["id"], a["id"])])

    with open_ccf_connection(rig.settings) as conn:
        with pytest.raises(ProjectionStaleError):
            consistency.require_current(conn, pin, DERIVATION)
        # Rebuild + re-pin restores the contract.
        rebuild_all(conn, archive_id=rig.archive.archive_id)
        fresh = consistency.pin_snapshot(conn, rig.archive.archive_id)
        consistency.require_current(conn, fresh, DERIVATION)
        assert fresh.head_sequence > pin.head_sequence


# ---------------------------------------------------------------------------
# Wiki rebuild
# ---------------------------------------------------------------------------


def test_wiki_rebuild_from_canonical_state(rig, tmp_path):
    e1, e2 = _entity(rig, "Grace Hopper"), _entity(rig, "Alan Turing")
    _admit(rig, records=[e1, e2])
    staging = tmp_path / "wiki-staging"

    report = rig.archive.projections.rebuild_wiki(staging)
    assert report["pages"] == 2
    index = (staging / "index.md").read_text()
    assert "Grace Hopper" in index and "Alan Turing" in index
    pages = sorted((staging / "pages").glob("*.md"))
    assert len(pages) == 2
    body = pages[0].read_text()
    assert "thoth_type: wiki_page" in body
    assert "source_records:" in body
    assert "urn:ccf:record:" in body

    # Rebuild is a pure function of canonical state.
    before = {p.name: p.read_text() for p in pages}
    rig.archive.projections.rebuild_wiki(staging)
    after = {p.name: (staging / "pages" / p.name).read_text() for p in pages}
    assert after == before

    # Fail closed on an unmanaged directory.
    unmanaged = tmp_path / "handwritten"
    unmanaged.mkdir()
    (unmanaged / "notes.md").write_text("handwritten")
    from ccf.projections.wiki import WikiRebuildError

    with pytest.raises(WikiRebuildError):
        rig.archive.projections.rebuild_wiki(unmanaged)


def test_projection_service_entrypoint(rig):
    a, b = _concept(rig, "svc-a"), _concept(rig, "svc-b")
    _admit(rig, records=[a, b], links=[_derived_link(rig, b["id"], a["id"])])
    service = rig.archive.projections
    report = service.rebuild_all()
    assert report[LINK_STATE] == 1
    assert service.link_state(service_pin_link(rig, a, b)) is not None
    pin = service.pin_snapshot()
    assert pin.generations[LINK_STATE] >= 1
    assert service.entity_clusters(pin=pin) is not None


def service_pin_link(rig, a, b):
    """The link admitted in the service test (single link in the archive)."""
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            """
            SELECT link_id FROM projection_link_state WHERE archive_id = %s
            """,
            (rig.archive.archive_id,),
        ).fetchone()
    return row[0]
