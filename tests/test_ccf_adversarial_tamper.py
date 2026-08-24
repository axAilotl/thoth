"""Adversarial tampering suite (checklist 10b).

Byte-level tampering with canonical and operational state must be
detected precisely — by ``verify_chain``, by admission, or by the spool's
own integrity checks — and must never silently serve forged state.

Also covers the forged-producer-batch attacks (wrong key, replayed
sequence, broken previous-hash chain) and the chain-continuity regression:
a rejected batch must never brick the honest producer's later batches.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from ccf.db import open_ccf_connection
from ccf.erasure.suppression import generate_suppression_key
from ccf.journal import JournalError
from ccf.spool import SpoolError

from ccf_helpers import authority, make_rig


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _concept(rig, label="tamper-target"):
    return rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": label,
            "definition": f"definition of {label}",
            "aliases": [],
            "extensions": {},
        },
    )


def _admit(rig, record):
    batch = rig.producer.create_batch(records=[record])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "accepted", result
    return batch, result


def _tamper(rig, sql, params=()):
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            conn.execute(sql, params)


# ---------------------------------------------------------------------------
# Canonical-state tampering -> verify_chain detects
# ---------------------------------------------------------------------------


def test_tampered_compartment_content_detected(rig):
    record = _concept(rig)
    _admit(rig, record)
    _tamper(
        rig,
        """
        UPDATE compartment
        SET plaintext_json = jsonb_set(plaintext_json, '{payload,label}', '"forged"')
        WHERE object_id = %s AND compartment = 'semantic'
        """,
        (record["id"],),
    )
    with pytest.raises(JournalError, match="commitment mismatch"):
        rig.archive.verify_chain()


def test_tampered_object_header_hash_detected(rig):
    record = _concept(rig)
    _admit(rig, record)
    _tamper(
        rig,
        "UPDATE object_header SET object_hash = 'sha256:' || repeat('f', 64) "
        "WHERE id = %s",
        (record["id"],),
    )
    with pytest.raises(JournalError, match="object hash mismatch"):
        rig.archive.verify_chain()


def test_tampered_commit_member_detected(rig):
    record = _concept(rig)
    _admit(rig, record)
    _tamper(
        rig,
        "UPDATE commit_member SET object_hash = 'sha256:' || repeat('e', 64) "
        "WHERE object_id = %s",
        (record["id"],),
    )
    with pytest.raises(JournalError, match="member leaf mismatch"):
        rig.archive.verify_chain()


def test_tampered_journal_parent_link_detected(rig):
    _admit(rig, _concept(rig, "one"))
    _admit(rig, _concept(rig, "two"))
    _tamper(
        rig,
        "UPDATE commit_journal SET parent_commit_hash = 'sha256:' || repeat('d', 64) "
        "WHERE sequence = 2",
    )
    with pytest.raises(JournalError, match="parent hash chain broken"):
        rig.archive.verify_chain()


def test_tampered_commit_signature_detected(rig):
    _admit(rig, _concept(rig))
    _tamper(
        rig,
        """
        UPDATE compartment
        SET plaintext_json = jsonb_set(
            plaintext_json, '{structural_payload,signature}', to_jsonb('AAAA'::text))
        WHERE object_id = (
            SELECT commit_record_id FROM commit_journal WHERE sequence = 1
        )
        """,
    )
    with pytest.raises(JournalError, match="signature invalid|commitment mismatch"):
        rig.archive.verify_chain()


def test_tampered_admission_row_detected(rig):
    """Admission rows mirror the journal; flipping one must not go unnoticed."""
    record = _concept(rig)
    _admit(rig, record)
    _tamper(
        rig,
        "UPDATE admission SET object_hash = 'sha256:' || repeat('c', 64) "
        "WHERE object_id = %s",
        (record["id"],),
    )
    with pytest.raises(JournalError, match="admission rows do not match"):
        rig.archive.verify_chain()


def test_dropped_admission_row_detected(rig):
    record = _concept(rig)
    _admit(rig, record)
    _tamper(rig, "DELETE FROM admission WHERE object_id = %s", (record["id"],))
    with pytest.raises(JournalError, match="admission rows do not match"):
        rig.archive.verify_chain()


# ---------------------------------------------------------------------------
# Producer spool tampering -> spool/admission detects
# ---------------------------------------------------------------------------


def test_tampered_spooled_blob_payload_detected(rig):
    record = rig.producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        payload={
            "text": "payload", "language": "en", "speaker_id": None,
            "sequence": None, "transcription": None, "extensions": {},
        },
    )
    blob_sub, blob_data = rig.producer.new_blob(
        data=b"spooled bytes", media_type="text/plain", claims=rig.claims()
    )
    batch = rig.producer.create_batch(
        records=[record], blobs=[blob_sub],
        blob_data={blob_sub["id"]: blob_data},
    )
    # Flip the spooled payload bytes without touching the digest.
    _tamper(
        rig,
        "UPDATE producer_blob_spool SET payload = '\\x00'::bytea "
        "WHERE batch_id = %s",
        (batch["batch_id"],),
    )
    with pytest.raises(SpoolError, match="corrupted"):
        rig.producer.spooled_blob_bytes(batch["batch_id"])


def test_tampered_spooled_batch_rejected_on_admission(rig):
    """A spooled batch whose JSON was altered fails admission loudly — and
    the honest producer's chain is not poisoned by the rejection."""
    record = _concept(rig)
    batch = rig.producer.create_batch(records=[record])
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT batch_json FROM producer_batch WHERE batch_id = %s",
            (batch["batch_id"],),
        ).fetchone()
    forged = dict(row[0])
    forged["records"][0]["payload"]["label"] = "tampered-in-spool"
    from psycopg.types.json import Jsonb

    _tamper(
        rig,
        "UPDATE producer_batch SET batch_json = %s WHERE batch_id = %s",
        (Jsonb(forged), batch["batch_id"]),
    )
    result = rig.archive.admit_batch(forged)
    assert result["status"] == "quarantined"
    assert "batch_hash does not match" in result["extensions"]["reason"]

    # The producer chains past the forged spool row: its next honest batch
    # still admits (envelope rejections never anchor the chain).
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[_concept(rig, "after-forgery")])
    )
    assert result["status"] == "accepted", result


# ---------------------------------------------------------------------------
# Suppression tampering -> no resurrection
# ---------------------------------------------------------------------------


@pytest.fixture()
def erasure_rig(ccf_settings, tmp_path, ccf_package_root):
    key_path = generate_suppression_key(tmp_path / "suppression.key")
    settings = replace(ccf_settings, suppression_key_path=str(key_path))
    return make_rig(settings, tmp_path, ccf_package_root)


def _origin_utterance(rig):
    source = rig.producer.new_record(
        type="core.source",
        claims=rig.claims(),
        payload={
            "kind": "wearable_audio", "name": "src", "connector": "thoth.capture",
            "native_identity": "device:src", "trust_class": "authenticated",
            "producer_key_id": rig.device_key_id, "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[source]))
    assert result["status"] == "accepted", result
    utterance = rig.producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        origin={"source_id": source["id"], "native_id": "utt-1", "revision": "1"},
        payload={
            "text": "erase me", "language": "en", "speaker_id": None,
            "sequence": None, "transcription": None, "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))
    assert result["status"] == "accepted", result
    return source["id"], utterance


def _erase(erasure_rig, object_id):
    svc = erasure_rig.archive.erasure()
    targets = [{"object_id": object_id, "compartments": ["semantic"]}]
    request = svc.submit_request(
        requester_id=erasure_rig.person_id,
        subject_id=erasure_rig.person_id,
        requested_scope={"targets": targets},
        reason="tamper drill",
        authority=authority(
            "first_person_statement", erasure_rig.person_id, erasure_rig.person_id
        ),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=targets,
        reasoning="approved",
        decided_by=erasure_rig.person_id,
        authority=authority(
            "explicit_authorization", erasure_rig.person_id, erasure_rig.person_id
        ),
        authorized_producers=[erasure_rig.producer.producer_id],
    )
    status = svc.execute(decided["operation_id"])
    assert status["stage"] == "receipt", status


def test_tampered_suppression_metadata_cannot_resurrect(erasure_rig):
    source_id, utterance = _origin_utterance(erasure_rig)
    _erase(erasure_rig, utterance["id"])

    # Tamper with the suppression entry's shaping metadata (kind and the
    # authorized-producer list): the commitment itself still matches, so
    # the erased origin stays blocked no matter who asks.
    _tamper(
        erasure_rig,
        "UPDATE suppression_entry SET kind = 'content', "
        "authorized_producers = '[]'::jsonb",
    )
    replay = erasure_rig.producer.new_record(
        type="experience.utterance",
        claims=erasure_rig.claims(),
        origin={"source_id": source_id, "native_id": "utt-1", "revision": "1"},
        payload={
            "text": "erase me", "language": "en", "speaker_id": None,
            "sequence": None, "transcription": None, "extensions": {},
        },
    )
    result = erasure_rig.archive.admit_batch(
        erasure_rig.producer.create_batch(records=[replay])
    )
    statuses = {a["status"] for a in result["admissions"]}
    # Generic refusal, an erased-lifecycle echo, or an origin-tuple
    # conflict — never re-admission.
    assert statuses <= {"rejected", "existing", "origin_revision_conflict"}, (
        result["admissions"]
    )
    for admission in result["admissions"]:
        assert admission["payload_available"] is False
        assert admission.get("current_lifecycle") in (None, "suppressed", "erased")


def test_erased_origin_tuple_stays_blocked_after_commitment_tamper(erasure_rig):
    """A rewritten suppression commitment is detected at admission
    (fail closed), and after a canonical rebuild the erased tuple stays
    blocked (spec 12.7)."""
    from ccf.erasure.errors import SuppressionProjectionError
    from ccf.erasure.suppression_set import rebuild_projection

    source_id, utterance = _origin_utterance(erasure_rig)
    _erase(erasure_rig, utterance["id"])
    _tamper(
        erasure_rig,
        "UPDATE suppression_entry SET commitment = 'hmac-sha256:' || repeat('0', 64) "
        "WHERE commitment = (SELECT MIN(commitment) FROM suppression_entry)",
    )
    replay = erasure_rig.producer.new_record(
        type="experience.utterance",
        claims=erasure_rig.claims(),
        origin={"source_id": source_id, "native_id": "utt-1", "revision": "1"},
        payload={
            "text": "erase me", "language": "en", "speaker_id": None,
            "sequence": None, "transcription": None, "extensions": {},
        },
    )
    batch = erasure_rig.producer.create_batch(records=[replay])
    # The tampered row no longer matches canonical state: admission
    # refuses to run on a drifted projection at all.
    with pytest.raises(SuppressionProjectionError):
        erasure_rig.archive.admit_batch(batch)

    from ccf.db import open_ccf_connection

    with open_ccf_connection(erasure_rig.settings) as conn:
        with conn.transaction():
            rebuild_projection(conn, erasure_rig.archive.archive_id, now=erasure_rig.clock())
    result = erasure_rig.archive.admit_batch(batch)
    for admission in result["admissions"]:
        assert admission["status"] in ("rejected", "origin_revision_conflict", "existing")
        assert admission["payload_available"] is False
    assert result["status"] != "accepted" or all(
        a["status"] != "admitted" for a in result["admissions"]
    )


# ---------------------------------------------------------------------------
# Forged producer batches
# ---------------------------------------------------------------------------


def _resign(rig, batch, credential, **mutations):
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
            credential.private_key,
            producer_batch_signing_digest(mutated["batch_hash"]),
        )
    )
    return mutated


def test_forged_batch_wrong_key_rejected_and_not_anchored(rig, tmp_path):
    """A batch signed by an unattested credential is rejected, leaves no
    chain anchor, and cannot brick the honest producer."""
    from ccf.credentials import DeviceCredential
    from ccf.ids import generate_id
    from ccf.keys import generate_signing_key

    evil_path = tmp_path / "evil.pem"
    generate_signing_key(evil_path)
    evil = DeviceCredential.load(
        evil_path,
        credential_id=generate_id("credential"),
        key_id=generate_id("key"),
    )
    batch = rig.producer.create_batch(records=[_concept(rig)])
    forged = _resign(rig, batch, evil, credential_id=evil.credential_id)
    result = rig.archive.admit_batch(forged)
    assert result["status"] == "quarantined"
    assert "unknown credential" in result["extensions"]["reason"]

    # No anchor was planted: the honest producer's next batch commits.
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[_concept(rig, "post-attack")])
    )
    assert result["status"] == "accepted", result


def test_replayed_committed_batch_is_idempotent_not_double_admitted(rig):
    record = _concept(rig)
    batch, first = _admit(rig, record)
    head_after_first = rig.archive.head()

    replay = rig.archive.admit_batch(batch)
    assert replay["status"] == "accepted"
    assert replay["commit_sequence"] == first["commit_sequence"]
    assert rig.archive.head() == head_after_first
    assert rig.archive.verify_chain()["commits_verified"] >= 2


def test_broken_previous_hash_chain_rejected(rig):
    _admit(rig, _concept(rig, "first"))
    batch2 = rig.producer.create_batch(records=[_concept(rig, "second")])
    forged = _resign(
        rig, batch2, rig.credential,
        previous_batch_hash="sha256:" + "0" * 64,
    )
    result = rig.archive.admit_batch(forged)
    assert result["status"] == "quarantined"
    assert "producer chain conflict" in result["extensions"]["reason"]

    # The honest chain is untouched: batch2 itself still admits.
    result = rig.archive.admit_batch(batch2)
    assert result["status"] == "accepted", result


def test_content_rejected_batch_does_not_brick_producer(rig):
    """Regression: a batch rejected for content (unknown reference) is a
    signed member of the producer chain; later batches must still admit."""
    from ccf.ids import generate_id

    ghost_link = rig.producer.new_link(
        type="ccf.about",
        from_id=generate_id("record"),
        to_id=generate_id("record"),
        claims=rig.claims(),
        selector={},
    )
    rejected = rig.archive.admit_batch(
        rig.producer.create_batch(links=[ghost_link])
    )
    assert rejected["status"] == "content_rejected", rejected
    assert "unknown ID" in rejected["extensions"]["reason"]

    # Before the fix, the archive anchored the chain on the last
    # non-rejected batch, so every later batch died of "producer chain
    # conflict" — permanent producer brick.
    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[_concept(rig, "recovery")])
    )
    assert result["status"] == "accepted", result
    assert rig.archive.verify_chain()["commits_verified"] >= 2
