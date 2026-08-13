"""Adversarial concurrency suite (checklist 10b).

Concurrent actors must never produce duplicates, torn state, stale
allows, or resurrected plaintext: admission serializes on the archive
head, idempotency keys absorb races, governance fences invalidate cached
decisions, and erasure flips reads to erased atomically.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from ccf.db import open_ccf_connection
from ccf.governance import LOCAL_DESTINATION
from ccf.ids import generate_id

from ccf_helpers import add_producer, authority, claims, make_rig

START = "2026-08-12T00:00:00.000Z"


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _concept(rig, label, *, origin=None, producer=None):
    producer = producer or rig.producer
    kwargs = {"origin": origin} if origin else {}
    return producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": label,
            "definition": f"definition of {label}",
            "aliases": [],
            "extensions": {},
        },
        **kwargs,
    )


def _source(rig):
    sub = rig.producer.new_record(
        type="core.source",
        claims=rig.claims(),
        payload={
            "kind": "wearable_audio", "name": "race-source",
            "connector": "thoth.capture", "native_identity": "device:race",
            "trust_class": "authenticated", "producer_key_id": rig.device_key_id,
            "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[sub]))
    assert result["status"] == "committed", result
    return sub["id"]


# ---------------------------------------------------------------------------
# Parallel collectors racing the same origin tuples
# ---------------------------------------------------------------------------


def test_parallel_producers_race_same_origin_tuple(rig, tmp_path):
    source_id = _source(rig)
    producer_b = add_producer(rig, tmp_path, "racer-b")
    origin = {"source_id": source_id, "native_id": "utt-race", "revision": "1"}

    def _submission(producer):
        return producer.new_record(
            type="experience.utterance",
            claims=rig.claims(),
            origin=origin,
            payload={
                "text": "raced utterance", "language": "en", "speaker_id": None,
                "sequence": None, "transcription": None, "extensions": {},
            },
        )

    batch_a = rig.producer.create_batch(records=[_submission(rig.producer)])
    batch_b = producer_b.create_batch(records=[_submission(producer_b)])

    barrier = threading.Barrier(3)

    def _admit(batch):
        barrier.wait(timeout=30)
        return rig.archive.admit_batch(batch)

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(_admit, batch_a)
        future_b = pool.submit(_admit, batch_b)
        barrier.wait(timeout=30)
        result_a = future_a.result(timeout=60)
        result_b = future_b.result(timeout=60)

    outcomes = sorted([result_a["status"], result_b["status"]])
    assert outcomes == ["committed", "conflict"], (result_a, result_b)

    # Exactly one object was admitted for the raced tuple — no duplicates.
    with open_ccf_connection(rig.settings) as conn:
        rows = conn.execute(
            "SELECT object_id FROM origin_index WHERE native_id = 'utt-race'"
        ).fetchall()
    assert len(rows) == 1
    assert rig.archive.verify_chain()["commits_verified"] >= 3


def test_identical_batch_race_admits_once(rig):
    batch = rig.producer.create_batch(records=[_concept(rig, "raced-batch")])
    barrier = threading.Barrier(5)

    def _admit():
        barrier.wait(timeout=30)
        return rig.archive.admit_batch(batch)

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_admit) for _ in range(4)]
        barrier.wait(timeout=30)
        results = [f.result(timeout=60) for f in futures]

    sequences = {r["commit_sequence"] for r in results}
    assert {r["status"] for r in results} == {"committed"}, results
    assert len(sequences) == 1, results
    with open_ccf_connection(rig.settings) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM object_header WHERE object_kind = 'record' "
            "AND id = %s",
            (batch["records"][0]["id"],),
        ).fetchone()[0]
    assert count == 1
    assert rig.archive.verify_chain()["commits_verified"] >= 2


# ---------------------------------------------------------------------------
# Concurrent governance mutation vs cached decision use
# ---------------------------------------------------------------------------


def _policy(rig, lineage_id, rules, *, previous=None, transition="create"):
    now = rig.clock()
    record_claims = claims(rig.person_id, rig.runtime_id,
                           policy_hint=rig.policy_lineage_id)
    record_claims["authority"] = authority(
        "explicit_authorization", rig.person_id, rig.person_id
    )
    return rig.producer.new_record(
        type="governance.policy",
        claims=record_claims,
        lineage={
            "lineage_id": lineage_id,
            "previous_head_id": previous,
            "transition": transition,
            "valid_from": now,
            "expires_at": None,
        },
        payload={
            "profile": "ccf.policy/0.1.1",
            "evaluator_profile": "ccf-deny-overrides-v1",
            "combining_algorithm": "deny_overrides_v1",
            "default_effect": "deny",
            "rules": rules,
            "provenance_requirement": "none",
            "retention": {
                "minimum_until": None, "maximum_until": None, "on_expiry": "review"
            },
            "extensions": {},
        },
    )


def _rule(rule_id, effect):
    return {
        "rule_id": rule_id, "effect": effect, "operations": ["read_local"],
        "purposes": [], "recipients": [], "destinations": [], "data_classes": [],
        "conditions": [], "obligations": [], "valid_from": None, "expires_at": None,
    }


def test_governance_mutation_never_serves_stale_allow(rig):
    engine = rig.archive.governance()
    policy_lineage = generate_id("lineage")
    concept_claims = rig.claims()
    concept_claims["policy_hint"] = policy_lineage
    concept = rig.producer.new_record(
        type="semantic.concept",
        claims=concept_claims,
        payload={"label": "governed", "definition": "d", "aliases": [],
                 "extensions": {}},
    )
    policy = _policy(rig, policy_lineage, [_rule("allow-read", "allow")])
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[concept, policy])
    )
    assert result["status"] == "committed", result

    kwargs = {
        "operation": "read_local",
        "purpose": "research",
        "requester": rig.runtime_id,
        "runtime": rig.runtime_id,
        "destination": LOCAL_DESTINATION,
        "object_ids": [concept["id"]],
        "requested_at": START,
    }
    allowed = engine.authorize(**kwargs)
    assert allowed.decision["decision"] == "allow"
    assert engine.authorize(**kwargs).from_cache is True

    # Readers hammer the cached decision while the policy tightens.
    tightened = _policy(
        rig,
        policy_lineage,
        [_rule("allow-read", "allow"), _rule("deny-all", "deny")],
        previous=policy["id"],
        transition="supersede",
    )
    stop = threading.Event()
    tightened_committed = threading.Event()
    observed_post_commit = threading.Condition()
    errors: list[BaseException] = []
    decisions: list[str] = []
    post_commit_decisions: list[str] = []

    def _reader():
        while not stop.is_set():
            try:
                began_after_commit = tightened_committed.is_set()
                result = engine.authorize(**kwargs)
                decisions.append(result.decision["decision"])
                if began_after_commit:
                    with observed_post_commit:
                        post_commit_decisions.append(result.decision["decision"])
                        observed_post_commit.notify_all()
            except BaseException as exc:  # noqa: BLE001 - collected, asserted
                errors.append(exc)

    readers = [threading.Thread(target=_reader) for _ in range(6)]
    for thread in readers:
        thread.start()
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[tightened]))
    assert result["status"] == "committed", result
    tightened_committed.set()
    with observed_post_commit:
        observed = observed_post_commit.wait_for(
            lambda: len(post_commit_decisions) >= len(readers), timeout=30
        )
    stop.set()
    for thread in readers:
        thread.join(timeout=60)

    assert not errors, errors[:3]
    assert decisions  # the readers actually ran during the mutation
    assert set(decisions) <= {"allow", "deny"}
    assert observed, "readers never exercised the post-commit safety window"
    assert set(post_commit_decisions) == {"deny"}, post_commit_decisions
    # Post-mutation reads deny, from a fresh evaluation.
    denied = engine.authorize(**kwargs)
    assert denied.decision["decision"] == "deny"
    assert denied.decision["reason_codes"] == ["deny_rule:deny-all"]
    # And the deny itself caches cleanly (no stale allow can reappear).
    assert engine.authorize(**kwargs).decision["decision"] == "deny"


# ---------------------------------------------------------------------------
# Concurrent erasure vs read of the same object
# ---------------------------------------------------------------------------


def test_concurrent_erasure_never_returns_erased_plaintext(rig):
    source_id = _source(rig)
    record = rig.producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        origin={"source_id": source_id, "native_id": "utt-erase-race", "revision": "1"},
        payload={
            "text": "sensitive plaintext under erasure race",
            "language": "en", "speaker_id": None, "sequence": None,
            "transcription": None, "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[record]))
    assert result["status"] == "committed", result

    svc = rig.archive.erasure()
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={"targets": [
            {"object_id": record["id"], "compartments": ["semantic"]}
        ]},
        reason="race drill",
        authority=authority("first_person_statement", rig.person_id, rig.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=[{"object_id": record["id"], "compartments": ["semantic"]}],
        reasoning="approved",
        decided_by=rig.person_id,
        authority=authority("explicit_authorization", rig.person_id, rig.person_id),
        authorized_producers=[rig.producer.producer_id],
    )

    errors: list[BaseException] = []
    saw_plaintext_after_block: list[str] = []
    stop = threading.Event()
    block_committed = threading.Event()
    observed_post_block = threading.Condition()
    post_block_reads: list[str] = []

    def _reader():
        while not stop.is_set():
            try:
                began_after_block = block_committed.is_set()
                obj = rig.archive.get_object(record["id"])
                assert obj is not None
                semantic = obj["compartments"]["semantic"]
                if semantic["state"] == "plaintext":
                    content = semantic["envelope"]["content"]
                    assert content["payload"]["text"] == (
                        "sensitive plaintext under erasure race"
                    )
                else:
                    # Once erased, no plaintext form may ever be served.
                    assert semantic["envelope"] is None
                if began_after_block:
                    if semantic["state"] == "plaintext":
                        saw_plaintext_after_block.append(record["id"])
                    with observed_post_block:
                        post_block_reads.append(semantic["state"])
                        observed_post_block.notify_all()
            except BaseException as exc:  # noqa: BLE001 - collected, asserted
                errors.append(exc)

    readers = [threading.Thread(target=_reader) for _ in range(4)]
    for thread in readers:
        thread.start()
    blocked = svc.advance(decided["operation_id"])
    assert blocked["stage"] == "block", blocked
    block_committed.set()
    with observed_post_block:
        observed = observed_post_block.wait_for(
            lambda: len(post_block_reads) >= len(readers), timeout=30
        )
    status = svc.execute(decided["operation_id"])
    stop.set()
    for thread in readers:
        thread.join(timeout=60)

    assert status["stage"] == "receipt", status
    assert not errors, errors[:3]
    assert observed, "readers never exercised the post-block safety window"
    assert set(post_block_reads) == {"erased"}, post_block_reads
    assert not saw_plaintext_after_block

    # Post-saga reads: erased, and re-admission is suppressed/refused.
    obj = rig.archive.get_object(record["id"])
    assert obj["compartments"]["semantic"]["state"] == "erased"
    assert obj["compartments"]["semantic"]["envelope"] is None
    assert rig.archive.verify_chain()["commits_verified"] >= 4
