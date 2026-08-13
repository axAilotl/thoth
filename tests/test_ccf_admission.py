"""Canonical admission tests (checklist phase 3, spec sections 2.2, 6.4-6.6, 8).

Covers checklist section 13.2 basics: exact retry idempotency, source
revision conflicts, provenance-distinct identical content from two sources,
same-batch cross references, lineage compare-and-swap (races, stale heads,
rebase-and-resubmit), derivation cycle rejection including via restored
edges, crash-safe outcomes, and the thoth-capture example batch admitted
end-to-end with schema-conformant results.
"""

from __future__ import annotations

import json
import os
import shutil
import threading

import pytest

from ccf.db import open_ccf_connection
from ccf.ids import generate_id
from ccf.schemas import SchemaSet

from ccf_helpers import authority, make_clock, make_rig, privacy

SCHEMA_BATCH_RESULT = "urn:ccf:schema:0.1.1:operational.batch-result"
SCHEMA_ADMISSION = "urn:ccf:schema:0.1.1:operational.admission"


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture(scope="module")
def schemas(ccf_package_root):
    return SchemaSet.load(ccf_package_root)


def _assert_result_conforms(schemas, result):
    schemas.validate(SCHEMA_BATCH_RESULT, result, what="batch result")
    for admission in result["admissions"]:
        schemas.validate(SCHEMA_ADMISSION, admission, what="admission entry")


def _concept(rig, text="concept", **overrides):
    return rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": text,
            "definition": f"definition of {text}",
            "aliases": [],
            "extensions": {},
        },
        **overrides,
    )


def _session(rig, source_id, native_id, revision="1", **payload_overrides):
    payload = {
        "source_id": source_id,
        "native_id": native_id,
        "channel": "ambient",
        "started_at": "2026-08-12T00:00:00.000Z",
        "ended_at": "2026-08-12T00:00:01.000Z",
        "participants": [rig.person_id],
        "capture_mode": "test",
        "extensions": {},
    }
    payload.update(payload_overrides)
    return rig.producer.new_record(
        type="core.session",
        claims=rig.claims(),
        origin={"source_id": source_id, "native_id": native_id, "revision": revision},
        payload=payload,
    )


def _source(rig, name="source"):
    return rig.producer.new_record(
        type="core.source",
        claims=rig.claims(),
        payload={
            "kind": "wearable_audio",
            "name": name,
            "connector": "thoth.capture",
            "native_identity": f"device:{name}",
            "trust_class": "authenticated",
            "producer_key_id": rig.device_key_id,
            "extensions": {},
        },
    )


def _run_record(rig, lineage_id, previous_head_id, transition, status, producer=None):
    producer = producer or rig.producer
    now = rig.clock()
    return producer.new_record(
        type="process.run",
        claims=rig.claims(),
        lineage={
            "lineage_id": lineage_id,
            "previous_head_id": previous_head_id,
            "transition": transition,
            "valid_from": now,
            "expires_at": None,
        },
        payload={
            "run_kind": "transcription",
            "framework": "thoth",
            "task": "test",
            "status": status,
            "configuration_ref": None,
            "parent_run_id": None,
            "started_at": now,
            "terminal_at": None,
            "extensions": {},
        },
    )


def _disposition(rig, target_link_id, action, lineage_id, previous_head_id, previous_disposition_id=None):
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
            "reason": "test disposition",
            "previous_disposition_id": previous_disposition_id,
            "replacement_link_id": None,
            "extensions": {},
        },
    )


# ---------------------------------------------------------------------------
# Idempotency (spec 2.2, 6.5)
# ---------------------------------------------------------------------------


def test_exact_retry_returns_stored_result(rig, schemas):
    record = _concept(rig)
    batch = rig.producer.create_batch(records=[record])
    first = rig.archive.admit_batch(batch)
    _assert_result_conforms(schemas, first)
    assert first["status"] == "committed"
    head = rig.archive.head()

    retry = rig.archive.admit_batch(batch)
    assert retry == first
    assert rig.archive.head() == head
    _assert_result_conforms(schemas, retry)


def test_same_object_in_new_batch_is_existing(rig, schemas):
    record = _concept(rig)
    result1 = rig.archive.admit_batch(rig.producer.create_batch(records=[record]))
    assert result1["status"] == "committed"

    # A later batch carrying the identical submission (retry after a lost
    # acknowledgement) resolves to the existing object, no new commit.
    batch2 = rig.producer.create_batch(records=[record])
    result2 = rig.archive.admit_batch(batch2)
    _assert_result_conforms(schemas, result2)
    assert result2["status"] == "committed"
    assert result2["commit_sequence"] is None
    admission = result2["admissions"][0]
    assert admission["status"] == "existing"
    assert admission["commit_sequence"] == result1["commit_sequence"]
    assert admission["payload_available"] is True


def test_origin_revision_conflict(rig, schemas):
    source = _source(rig)
    rig.archive.admit_batch(rig.producer.create_batch(records=[source]))

    session_v1 = _session(rig, source["id"], "boot-1/session-1")
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[session_v1]))
    assert result["status"] == "committed"

    changed = _session(rig, source["id"], "boot-1/session-1", channel="focused")
    conflict = rig.archive.admit_batch(rig.producer.create_batch(records=[changed]))
    _assert_result_conforms(schemas, conflict)
    assert conflict["status"] == "conflict"
    assert conflict["commit_sequence"] is None
    assert conflict["admissions"][0]["status"] == "origin_revision_conflict"

    # The original object is untouched.
    assert rig.archive.get_object(session_v1["id"]) is not None
    assert rig.archive.get_object(changed["id"]) is None


def test_same_content_from_two_sources_stays_provenance_distinct(rig):
    source_a = _source(rig, "alpha")
    source_b = _source(rig, "beta")
    rig.archive.admit_batch(rig.producer.create_batch(records=[source_a, source_b]))

    session_a = _session(rig, source_a["id"], "shared/native-1")
    session_b = _session(rig, source_b["id"], "shared/native-1")
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[session_a, session_b])
    )
    assert result["status"] == "committed"
    statuses = {a["object_id"]: a["status"] for a in result["admissions"]}
    assert statuses == {session_a["id"]: "admitted", session_b["id"]: "admitted"}
    hash_a = rig.archive.get_object(session_a["id"])["header"]["object_hash"]
    hash_b = rig.archive.get_object(session_b["id"])["header"]["object_hash"]
    assert hash_a != hash_b  # provenance differs, so the objects differ


def test_hard_id_collision_is_a_batch_conflict(rig):
    record = _concept(rig)
    rig.archive.admit_batch(rig.producer.create_batch(records=[record]))

    tampered = _concept(rig, text="different", object_id=record["id"])
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[tampered]))
    assert result["status"] == "conflict"
    assert "hard ID collision" in result["extensions"]["reason"]


# ---------------------------------------------------------------------------
# Same-batch references (spec 2.3)
# ---------------------------------------------------------------------------


def test_same_batch_cross_references_admit_atomically(rig):
    source = _source(rig)
    session = _session(rig, source["id"], "boot-9/session-1")
    link = rig.producer.new_link(
        type="ccf.captured_in",
        from_id=session["id"],
        to_id=source["id"],
        claims=rig.claims(),
    )
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[source, session], links=[link])
    )
    assert result["status"] == "committed"
    assert {a["status"] for a in result["admissions"]} == {"admitted"}
    positions = sorted(a["commit_position"] for a in result["admissions"])
    assert positions == [0, 1, 2]


def test_dangling_reference_rejects_whole_batch(rig):
    link = rig.producer.new_link(
        type="ccf.about",
        from_id=generate_id("record"),
        to_id=generate_id("record"),
        claims=rig.claims(),
    )
    record = _concept(rig)
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[record], links=[link])
    )
    assert result["status"] == "rejected"
    assert "references unknown ID" in result["extensions"]["reason"]
    # Nothing was committed, not even the otherwise-valid record.
    assert rig.archive.get_object(record["id"]) is None


# ---------------------------------------------------------------------------
# Lineage compare-and-swap (spec 6.6, 8.2)
# ---------------------------------------------------------------------------


def test_lineage_cas_full_lifecycle(rig):
    lineage_id = generate_id("lineage")
    run1 = _run_record(rig, lineage_id, None, "start", "running")
    result1 = rig.archive.admit_batch(rig.producer.create_batch(records=[run1]))
    assert result1["status"] == "committed"

    # Two transitions on the same predecessor: the first wins, the second
    # gets a lineage conflict; the archive never silently rebases.
    run2 = _run_record(rig, lineage_id, run1["id"], "succeed", "succeeded")
    result2 = rig.archive.admit_batch(rig.producer.create_batch(records=[run2]))
    assert result2["status"] == "committed"

    run3 = _run_record(rig, lineage_id, run1["id"], "fail", "failed")
    result3 = rig.archive.admit_batch(rig.producer.create_batch(records=[run3]))
    assert result3["status"] == "conflict"
    assert result3["admissions"][0]["status"] == "lineage_conflict"
    assert "stale predecessor" in result3["admissions"][0]["reason"]
    assert rig.archive.get_object(run3["id"]) is None

    # The stale transition cannot succeed retroactively either: the head is
    # terminal now, so even a correctly-rebased transition is checked
    # against the state machine.
    run4 = _run_record(rig, lineage_id, run2["id"], "start", "running")
    result4 = rig.archive.admit_batch(rig.producer.create_batch(records=[run4]))
    assert result4["status"] == "conflict"
    assert "terminal" in result4["admissions"][0]["reason"]


def test_stale_head_rebase_and_resubmit_succeeds(rig):
    lineage_id = generate_id("lineage")
    run1 = _run_record(rig, lineage_id, None, "queue", "queued")
    rig.archive.admit_batch(rig.producer.create_batch(records=[run1]))
    run2 = _run_record(rig, lineage_id, run1["id"], "start", "running")
    rig.archive.admit_batch(rig.producer.create_batch(records=[run2]))

    stale = _run_record(rig, lineage_id, run1["id"], "cancel", "cancelled")
    conflict = rig.archive.admit_batch(rig.producer.create_batch(records=[stale]))
    assert conflict["status"] == "conflict"

    # Caller reads the current head, rebases explicitly, and resubmits.
    rebased = _run_record(rig, lineage_id, run2["id"], "cancel", "cancelled")
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[rebased]))
    assert result["status"] == "committed"
    assert result["admissions"][0]["status"] == "admitted"


def test_two_transitions_chained_within_one_batch(rig):
    lineage_id = generate_id("lineage")
    run1 = _run_record(rig, lineage_id, None, "queue", "queued")
    run2 = _run_record(rig, lineage_id, run1["id"], "start", "running")
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[run1, run2]))
    assert result["status"] == "committed"
    assert [a["status"] for a in result["admissions"]] == ["admitted", "admitted"]


def test_racing_lineage_transitions_one_wins(rig, tmp_path):
    from ccf_helpers import add_producer

    producer_b = add_producer(rig, tmp_path, "runtime-b")
    lineage_id = generate_id("lineage")
    run1 = _run_record(rig, lineage_id, None, "start", "running")
    rig.archive.admit_batch(rig.producer.create_batch(records=[run1]))

    batch_a = rig.producer.create_batch(
        records=[_run_record(rig, lineage_id, run1["id"], "succeed", "succeeded")]
    )
    batch_b = producer_b.create_batch(
        records=[
            _run_record(rig, lineage_id, run1["id"], "succeed", "succeeded", producer=producer_b)
        ]
    )

    results = {}

    def _admit(name, batch):
        results[name] = rig.archive.admit_batch(batch)

    threads = [
        threading.Thread(target=_admit, args=("a", batch_a)),
        threading.Thread(target=_admit, args=("b", batch_b)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Two independent producer chains race one archive head: both are
    # admissible in either arrival order, so one commits and the other is
    # serialized after it and loses the lineage compare-and-swap.
    statuses = sorted(r["status"] for r in results.values())
    assert statuses == ["committed", "conflict"]
    loser = next(r for r in results.values() if r["status"] == "conflict")
    assert loser["admissions"][0]["status"] == "lineage_conflict"
    # genesis, two bootstrap commits, the run1 commit, exactly one winner
    assert rig.archive.head()["sequence"] == "4"
    rig.archive.verify_chain()


def test_racing_identical_batch_admits_exactly_once(rig):
    batch = rig.producer.create_batch(records=[_concept(rig)])
    results = []

    def _admit():
        results.append(rig.archive.admit_batch(batch))

    threads = [threading.Thread(target=_admit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert all(r["status"] == "committed" for r in results)
    assert results[0] == results[1]
    assert rig.archive.head()["sequence"] == "2"
    rig.archive.verify_chain()


# ---------------------------------------------------------------------------
# Derivation cycles (spec 8.6)
# ---------------------------------------------------------------------------


def _derived_from(rig, from_id, to_id):
    return rig.producer.new_link(
        type="ccf.derived_from", from_id=from_id, to_id=to_id, claims=rig.claims()
    )


def test_derivation_cycle_rejected(rig):
    record_a = _concept(rig, "a")
    record_b = _concept(rig, "b")
    edge = _derived_from(rig, record_a["id"], record_b["id"])
    rig.archive.admit_batch(
        rig.producer.create_batch(records=[record_a, record_b], links=[edge])
    )

    closing = _derived_from(rig, record_b["id"], record_a["id"])
    result = rig.archive.admit_batch(rig.producer.create_batch(links=[closing]))
    assert result["status"] == "conflict"
    admission = result["admissions"][0]
    assert admission["status"] == "rejected"
    assert "cycle" in admission["reason"]
    assert rig.archive.get_object(closing["id"]) is None


def test_cycle_via_restored_edge_rejected(rig):
    record_a = _concept(rig, "a")
    record_b = _concept(rig, "b")
    edge = _derived_from(rig, record_a["id"], record_b["id"])
    rig.archive.admit_batch(
        rig.producer.create_batch(records=[record_a, record_b], links=[edge])
    )

    # Retract the edge; the reverse edge then becomes admissible.
    disposition_lineage = generate_id("lineage")
    retract = _disposition(rig, edge["id"], "retract", disposition_lineage, None)
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[retract]))
    assert result["status"] == "committed"

    reverse = _derived_from(rig, record_b["id"], record_a["id"])
    result = rig.archive.admit_batch(rig.producer.create_batch(links=[reverse]))
    assert result["status"] == "committed"

    # Restoring the retracted edge would close a cycle: rejected.
    restore = _disposition(
        rig,
        edge["id"],
        "restore",
        disposition_lineage,
        retract["id"],
        previous_disposition_id=retract["id"],
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[restore]))
    assert result["status"] == "conflict"
    assert "cycle" in result["admissions"][0]["reason"]
    assert rig.archive.get_object(restore["id"]) is None

    # ...while retracting the reverse edge first makes the restore legal.
    reverse_lineage = generate_id("lineage")
    retract_reverse = _disposition(rig, reverse["id"], "retract", reverse_lineage, None)
    rig.archive.admit_batch(rig.producer.create_batch(records=[retract_reverse]))
    restore2 = _disposition(
        rig,
        edge["id"],
        "restore",
        disposition_lineage,
        retract["id"],
        previous_disposition_id=retract["id"],
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[restore2]))
    assert result["status"] == "committed"


# ---------------------------------------------------------------------------
# Credential / chain / catalog enforcement
# ---------------------------------------------------------------------------


def _resigned_batch(rig, batch, **mutations):
    """A structurally valid, correctly signed batch with chosen mutations."""
    from ccf.hashing import (
        encode_b64url,
        producer_batch_hash,
        producer_batch_signing_digest,
        sign_digest,
    )

    mutated = {**batch, **mutations}
    mutated.pop("batch_hash", None)
    mutated.pop("signature", None)
    mutated["batch_hash"] = producer_batch_hash(mutated)
    mutated["signature"] = encode_b64url(
        sign_digest(
            rig.credential.private_key,
            producer_batch_signing_digest(mutated["batch_hash"]),
        )
    )
    return mutated


def test_bad_signature_rejected(rig):
    batch = rig.producer.create_batch(records=[_concept(rig)])
    tampered = dict(batch)
    tampered["signature"] = batch["signature"][:-2] + "XX"
    result = rig.archive.admit_batch(tampered)
    assert result["status"] == "rejected"
    assert "signature" in result["extensions"]["reason"]


def test_unknown_credential_rejected(rig):
    batch = rig.producer.create_batch(records=[_concept(rig)])
    forged = _resigned_batch(rig, batch, credential_id=generate_id("credential"))
    result = rig.archive.admit_batch(forged)
    assert result["status"] == "rejected"
    assert "unknown credential" in result["extensions"]["reason"]


def test_catalog_root_mismatch_rejected(rig):
    batch = rig.producer.create_batch(records=[_concept(rig)])
    wrong_root = "sha256:" + "0" * 64
    forged = _resigned_batch(rig, batch, semantic_catalog_root=wrong_root)
    result = rig.archive.admit_batch(forged)
    assert result["status"] == "rejected"
    assert "catalog root mismatch" in result["extensions"]["reason"]


def test_producer_chain_gap_rejected(rig):
    rig.archive.admit_batch(rig.producer.create_batch(records=[_concept(rig)]))
    batch2 = rig.producer.create_batch(records=[_concept(rig)])
    forged = _resigned_batch(rig, batch2, previous_batch_hash=None)
    result = rig.archive.admit_batch(forged)
    assert result["status"] == "rejected"
    assert "producer chain conflict" in result["extensions"]["reason"]


def test_out_of_order_batch_waits_for_exact_predecessor_then_admits(rig):
    batch1 = rig.producer.create_batch(records=[_concept(rig)])
    batch2 = rig.producer.create_batch(records=[_concept(rig)])

    # Simulate a disconnected producer: the archive has its credential but
    # has not received either locally spooled batch yet.
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            conn.execute(
                "DELETE FROM producer_batch WHERE producer_id = %s",
                (batch1["producer_id"],),
            )
            conn.execute(
                "DELETE FROM producer_head WHERE producer_id = %s",
                (batch1["producer_id"],),
            )

    early = rig.archive.admit_batch(batch2)
    assert early["status"] == "queued", early
    assert early["extensions"]["reason"].startswith("predecessor_missing:")

    first = rig.archive.admit_batch(batch1)
    assert first["status"] == "committed", first
    retried = rig.archive.admit_batch(batch2)
    assert retried["status"] == "committed", retried
    assert int(retried["commit_sequence"]) > int(first["commit_sequence"])


def test_unknown_type_fails_closed(rig):
    record = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "x", "aliases": [], "description": None, "extensions": {}},
    )
    record["type"] = "bogus.type"
    # new_record already validated; rebuild the batch manually so the unknown
    # type reaches the archive.
    batch = rig.producer.create_batch(records=[record])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "rejected"
    assert "unknown type registry entry" in result["extensions"]["reason"]


# ---------------------------------------------------------------------------
# Crash safety (spec 6.4: no torn canonical state)
# ---------------------------------------------------------------------------


def test_crash_before_commit_signing_leaves_no_trace(rig, monkeypatch):
    import ccf.admission as admission_mod

    record = _concept(rig)
    batch = rig.producer.create_batch(records=[record])

    def _explode(*args, **kwargs):
        raise RuntimeError("simulated crash before commit signing")

    monkeypatch.setattr(admission_mod, "commit_objects", _explode)
    with pytest.raises(RuntimeError, match="simulated crash"):
        rig.archive.admit_batch(batch)
    monkeypatch.undo()

    assert rig.archive.get_object(record["id"]) is None
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed"
    rig.archive.verify_chain()


def test_crash_after_commit_write_rolls_back_and_replays(rig, monkeypatch):
    import ccf.admission as admission_mod

    record = _concept(rig)
    batch = rig.producer.create_batch(records=[record])

    original = admission_mod._record_batch_outcome

    def _explode(conn, batch, result, *, committed_sequence):
        raise RuntimeError("simulated crash after commit write")

    monkeypatch.setattr(admission_mod, "_record_batch_outcome", _explode)
    with pytest.raises(RuntimeError, match="simulated crash"):
        rig.archive.admit_batch(batch)
    monkeypatch.setattr(admission_mod, "_record_batch_outcome", original)

    # The commit itself rolled back with the transaction: replaying the
    # exact batch admits cleanly and the chain still verifies.
    assert rig.archive.get_object(record["id"]) is None
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed"
    report = rig.archive.verify_chain()
    assert report["head_sequence"] == "2"


# ---------------------------------------------------------------------------
# The vendored thoth-capture example through real admission
# ---------------------------------------------------------------------------


def _copy_test_key(src: str, dst) -> None:
    shutil.copyfile(src, dst)
    os.chmod(dst, 0o600)


def test_thoth_capture_example_end_to_end(
    ccf_settings, tmp_path, ccf_package_root, ccf_examples_dir, ccf_vectors_dir, schemas
):
    """Admit the vendored example batch for real and re-verify everything.

    Uses the package's TEST-ONLY keys and exact example IDs so the signed
    batch verifies as published; the archive is rebuilt locally (fresh
    salts), then object hashes, Merkle roots, and commit signatures are
    re-verified from storage end-to-end.
    """
    from ccf.archive import Archive
    from ccf.keys import load_verification_key, public_key_text

    ids = json.loads((ccf_examples_dir / "ids.json").read_text())
    batch = json.loads((ccf_examples_dir / "producer-batch.json").read_text())
    blob_data = (ccf_examples_dir / "segment-1842.wav").read_bytes()

    archive_key = tmp_path / "archive.pem"
    _copy_test_key(ccf_vectors_dir / "TEST-ONLY-archive-ed25519-private.pem", archive_key)
    device_public = load_verification_key(ccf_vectors_dir / "device-ed25519-public.pem")

    clock = make_clock("2026-08-12T02:00:00.000Z")
    archive = Archive.create(
        ccf_settings,
        package_root=ccf_package_root,
        archive_key_path=archive_key,
        active_profiles=[
            "ccf-core-0.1.1",
            "ccf-local-sync-0.1.1",
            "ccf-continuity-pack-0.1.1",
        ],
        clock=clock,
    )

    ts = clock()
    policy_id = ids["policy"]
    archive.admit_bootstrap(
        [
            {
                "type": "governance.policy",
                "object_id": policy_id,
                "recorded_by": ids["runtime"],
                "recorded_at": ts,
                "person_id": ids["person"],
                "authority": authority(
                    "explicit_authorization", ids["person"], ids["person"]
                ),
                "privacy": privacy(["identity_data"]),
                "policy_hint": ids["policyLineage"],
                "lineage": {
                    "lineage_id": ids["policyLineage"],
                    "previous_head_id": None,
                    "transition": "create",
                    "valid_from": ts,
                    "expires_at": None,
                },
                "payload": {
                    "profile": "ccf.policy/0.1.1",
                    "evaluator_profile": "ccf-deny-overrides-v1",
                    "combining_algorithm": "deny_overrides_v1",
                    "default_effect": "deny",
                    "rules": [],
                    "provenance_requirement": "lineage_only",
                    "retention": {
                        "minimum_until": None,
                        "maximum_until": None,
                        "on_expiry": "review",
                    },
                    "extensions": {},
                },
            },
            {
                "type": "core.person",
                "object_id": ids["person"],
                "recorded_by": ids["runtime"],
                "recorded_at": ts,
                "person_id": ids["person"],
                "perspective_id": ids["person"],
                "authority": authority(
                    "first_person_statement", ids["person"], ids["person"]
                ),
                "privacy": privacy(
                    ["identity_data"],
                    [
                        {
                            "person_id": ids["person"],
                            "role": "archive_principal",
                            "identity_state_at_write": "verified",
                        }
                    ],
                ),
                "policy_hint": ids["policyLineage"],
                "payload": {
                    "kind": "human",
                    "display_name": "Example Person",
                    "aliases": [],
                    "identity_anchors": [],
                    "extensions": {},
                },
            },
            {
                "type": "core.runtime",
                "object_id": ids["runtime"],
                "recorded_by": ids["runtime"],
                "recorded_at": ts,
                "person_id": ids["person"],
                "authority": authority("runtime_import", ids["runtime"]),
                "privacy": privacy(),
                "policy_hint": ids["policyLineage"],
                "payload": {
                    "kind": "backend",
                    "name": "Thoth CCF adapter",
                    "version": "0.1.1-example",
                    "instance_id": "thoth-local",
                    "capabilities": ["capture", "transcribe", "extract", "sync"],
                    "operator_id": ids["person"],
                    "extensions": {},
                },
            },
            {
                "type": "core.device_credential",
                "object_id": ids["credential"],
                "recorded_by": ids["runtime"],
                "recorded_at": ts,
                "authority": authority(
                    "explicit_authorization", ids["person"], ids["person"]
                ),
                "privacy": privacy(),
                "policy_hint": ids["policyLineage"],
                "semantic": False,
                "structural_payload": {
                    "credential_id": ids["credentialId"],
                    "subject_id": ids["runtime"],
                    "issuer_key_id": ids["archiveKey"],
                    "signing_key": {
                        "profile": "ed25519",
                        "public_key": public_key_text(device_public),
                        "key_id": ids["deviceKey"],
                    },
                    "encryption_key": None,
                    "scopes": ["capture", "sync", "derive"],
                    "valid_from": ts,
                    "expires_at": None,
                    "offline_grace_until": None,
                    "extensions": {},
                },
                "lineage": {
                    "lineage_id": ids["credentialLineage"],
                    "previous_head_id": None,
                    "transition": "issue",
                    "valid_from": ts,
                    "expires_at": None,
                },
                "payload": {},
            },
        ]
    )

    result = archive.admit_batch(batch, blob_bytes={ids["blob"]: blob_data})
    _assert_result_conforms(schemas, result)
    assert result["status"] == "committed"
    assert len(result["admissions"]) == (
        len(batch["records"]) + len(batch["links"]) + len(batch["blobs"])
    )
    assert {a["status"] for a in result["admissions"]} == {"admitted"}

    # Producer claims remain inspectable and separate from archive stamps.
    source_object = archive.get_object(ids["source"])
    semantic = source_object["compartments"]["semantic"]["envelope"]["content"]
    assert semantic["claimed"]["policy_hint"] == ids["policyLineage"]
    assert semantic["producer_evidence"]["batch_id"] == ids["batch"]
    session_object = archive.get_object(ids["session"])
    session_semantic = session_object["compartments"]["semantic"]["envelope"]["content"]
    assert session_semantic["origin"]["source_id"] == ids["source"]
    assert session_semantic["origin"]["submission_hash"].startswith("sha256:")
    assert source_object["admission"]["commit_sequence"] == result["commit_sequence"]

    # The run lineage advanced via compare-and-swap to its admitted head.
    run_object = archive.get_object(ids["run"])
    lineage = run_object["compartments"]["structural"]["envelope"]["content"]["lineage"]
    assert lineage["lineage_id"] == ids["runLineage"]

    # End-to-end re-verification: object hashes, member Merkle roots, and
    # commit signatures from genesis through the new head.
    report = archive.verify_chain()
    assert report["commits_verified"] == 3
    assert report["members_verified"] == 4 + len(result["admissions"])

    # Blob bytes survived with their salted content commitment intact.
    from ccf.hashing import blob_content_commitment

    blob = archive.get_object(ids["blob"])
    blob_semantic = blob["compartments"]["semantic"]["envelope"]["content"]
    assert (
        blob_content_commitment(blob_semantic["content_salt"], blob_data)
        == batch["blobs"][0]["content_commitment"]
    )
