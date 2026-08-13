"""Erasure saga tests (checklist phase 7; spec 3.6-3.10, 6.5, 8.4, 12.7).

Covers: erasing an ordinary Record's semantic compartment while header,
commitments, and journal stay valid; selector erasure with structurally
retained ``derived_from`` endpoints; retention-profile refusals (fail
closed); crash-safe resume (a crash after content destruction never
reports the content recoverable); projection/checkpoint/wiki purge and
rebuild without the erased content; suppression of silent reintroduction
with a generic response for unauthorized callers; terminal erasure
tombstones; Blob content erasure with salt destruction; and the
multi-subject media decision shape.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from ccf.db import open_ccf_connection
from ccf.erasure.errors import ErasureError, RetentionViolation
from ccf.erasure.suppression import generate_suppression_key
from ccf.ids import generate_id

from ccf_helpers import add_producer, authority, make_rig


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    key_path = generate_suppression_key(tmp_path / "suppression.key")
    settings = replace(ccf_settings, suppression_key_path=str(key_path))
    return make_rig(settings, tmp_path, ccf_package_root)


def _requester_authority(rig):
    return authority("first_person_statement", rig.person_id, rig.person_id)


def _governance_authority(rig):
    return authority("explicit_authorization", rig.person_id, rig.person_id)


def _source(rig):
    sub = rig.producer.new_record(
        type="core.source",
        claims=rig.claims(),
        payload={
            "kind": "wearable_audio",
            "name": "source",
            "connector": "thoth.capture",
            "native_identity": "device:source",
            "trust_class": "authenticated",
            "producer_key_id": rig.device_key_id,
            "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[sub]))
    assert result["status"] == "accepted", result
    return sub["id"]


def _utterance(rig, text, *, source_id=None, native_id="utt-1", revision="1", producer=None):
    producer = producer or rig.producer
    kwargs = {}
    if source_id is not None:
        kwargs["origin"] = {
            "source_id": source_id,
            "native_id": native_id,
            "revision": revision,
        }
    return producer.new_record(
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
        **kwargs,
    )


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


def _admit(rig, *, records=None, links=None, blobs=None, blob_bytes=None, producer=None):
    producer = producer or rig.producer
    batch = producer.create_batch(
        records=records or [], links=links or [], blobs=blobs or []
    )
    return rig.archive.admit_batch(batch, blob_bytes=blob_bytes)


def _erase(rig, targets, *, staging=None, authorized_producers=()):
    """Run a full request -> decide -> execute saga; return final status."""
    svc = rig.archive.erasure(wiki_staging_dir=staging)
    scope = {"targets": targets}
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope=scope,
        reason="test erasure",
        authority=_requester_authority(rig),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=targets,
        reasoning="approved in test",
        decided_by=rig.person_id,
        authority=_governance_authority(rig),
        authorized_producers=list(authorized_producers),
    )
    return svc, svc.execute(decided["operation_id"])


def _compartment_state(rig, object_id, compartment):
    obj = rig.archive.get_object(object_id)
    assert obj is not None
    return obj["compartments"][compartment]


def _type_id(rig, type_name):
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            """
            SELECT object_id FROM compartment
            WHERE compartment = 'structural' AND state = 'plaintext'
              AND plaintext_json ->> 'type' = %s
            LIMIT 1
            """,
            (type_name,),
        ).fetchone()
    assert row is not None, f"no admitted {type_name}"
    return row[0]


# ---------------------------------------------------------------------------
# Ordinary record erasure (spec 13.3: header/structure valid, chain verifies)
# ---------------------------------------------------------------------------


def test_erase_record_semantic_compartment(rig):
    source_id = _source(rig)
    utterance = _utterance(rig, "the quarterly report is ready", source_id=source_id)
    result = _admit(rig, records=[utterance])
    assert result["status"] == "accepted", result
    object_id = utterance["id"]
    before = rig.archive.get_object(object_id)
    semantic_commitment = before["header"]["semantic_commitment"]

    _svc, status = _erase(
        rig,
        [{"object_id": object_id, "compartments": ["semantic"]}],
        authorized_producers=[rig.producer.producer_id],
    )
    assert status["stage"] == "receipt"
    assert status["profile"] == "logical"
    assert status["content_recoverable"] is False

    after = rig.archive.get_object(object_id)
    # Header and commitments remain; the semantic compartment reports its
    # erased state without fabricated content (spec 3.6, 3.10).
    assert after["header"]["semantic_commitment"] == semantic_commitment
    assert after["header"]["object_hash"] == before["header"]["object_hash"]
    semantic = _compartment_state(rig, object_id, "semantic")
    assert semantic["state"] == "erased"
    assert semantic["envelope"] is None
    structural = _compartment_state(rig, object_id, "structural")
    assert structural["state"] == "plaintext"

    # Origin tuple lifecycle updated; no bytes restored.
    with open_ccf_connection(rig.settings) as conn:
        lifecycle = conn.execute(
            "SELECT lifecycle FROM origin_index WHERE object_id = %s", (object_id,)
        ).fetchone()[0]
    assert lifecycle == "erased"

    # Journal: commitments, headers, and commit/catalog structures remain.
    report = rig.archive.verify_chain()
    assert report["commits_verified"] > 0


def test_fulltext_excludes_erased_semantic(rig):
    keep = _utterance(rig, "a wholly unrelated topic")
    drop = _utterance(rig, "unique erased zebra phrase")
    result = _admit(rig, records=[keep, drop])
    assert result["status"] == "accepted", result
    rig.archive.projections.rebuild_all()
    hits = rig.archive.projections.search_text("zebra")
    assert [hit["object_id"] for hit in hits] == [drop["id"]]

    _erase(rig, [{"object_id": drop["id"], "compartments": ["semantic"]}])
    rig.archive.projections.rebuild_all()
    assert rig.archive.projections.search_text("zebra") == []
    hits = rig.archive.projections.search_text("unrelated")
    assert [hit["object_id"] for hit in hits] == [keep["id"]]


# ---------------------------------------------------------------------------
# Selector erasure with retained endpoints (spec 3.10, 5.3, 13.3)
# ---------------------------------------------------------------------------


def test_erase_link_selector_endpoints_retained(rig):
    ancestor = _concept(rig, "ancestor concept")
    descendant = _concept(rig, "descendant concept")
    link = rig.producer.new_link(
        type="ccf.derived_from",
        from_id=descendant["id"],
        to_id=ancestor["id"],
        claims=rig.claims(),
        selector={"span": "lines 1-3"},
    )
    result = _admit(rig, records=[ancestor, descendant], links=[link])
    assert result["status"] == "accepted", result

    _erase(rig, [{"object_id": link["id"], "compartments": ["semantic"]}])

    structural = _compartment_state(rig, link["id"], "structural")
    assert structural["state"] == "plaintext"
    content = structural["envelope"]["content"]
    assert content["from_id"] == descendant["id"]
    assert content["to_id"] == ancestor["id"]
    semantic = _compartment_state(rig, link["id"], "semantic")
    assert semantic["state"] == "erased"

    rig.archive.projections.rebuild_all()
    # Derivation closure keeps the structural endpoints (spec 3.10).
    closure = rig.archive.projections.closure_pairs()
    assert (ancestor["id"], descendant["id"]) in closure
    # The link stays active but its selector is gone.
    state = rig.archive.projections.link_state(link["id"])
    assert state["state"] == "active"
    assert state["selector_available"] is False

    # The saga admitted a canonical invalidate_selector disposition.
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            """
            SELECT c.plaintext_json -> 'structural_payload' ->> 'action'
            FROM lineage_head lh
            JOIN compartment c
              ON c.object_id = lh.head_record_id AND c.compartment = 'structural'
            WHERE c.plaintext_json ->> 'type' = 'lineage.link_disposition'
              AND c.plaintext_json -> 'structural_payload' ->> 'target_link_id' = %s
            """,
            (link["id"],),
        ).fetchone()
    assert row is not None and row[0] == "invalidate_selector"
    assert rig.archive.verify_chain()["commits_verified"] > 0


# ---------------------------------------------------------------------------
# Retention-profile enforcement (spec 1.3; fail closed)
# ---------------------------------------------------------------------------


def test_retention_profile_violations_refused(rig):
    utterance = _utterance(rig, "payload erasable record")
    result = _admit(rig, records=[utterance])
    assert result["status"] == "accepted", result
    svc = rig.archive.erasure()
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={"targets": []},
        reason="retention tests",
        authority=_requester_authority(rig),
    )

    def _decide(targets):
        return svc.decide(
            request_id=request["request_id"],
            decision="approve",
            targets=targets,
            reasoning="should be refused",
            decided_by=rig.person_id,
            authority=_governance_authority(rig),
        )

    # payload_erasable: the structural compartment is not erasable.
    with pytest.raises(RetentionViolation):
        _decide([{"object_id": utterance["id"], "compartments": ["structural"]}])

    # epoch_lifetime_required: nothing is erasable.
    credential_id = _type_id(rig, "core.device_credential")
    with pytest.raises(RetentionViolation):
        _decide([{"object_id": credential_id, "compartments": ["semantic"]}])

    # structural_retention_required: semantic erasable, structural refused.
    policy_id = _type_id(rig, "governance.policy")
    with pytest.raises(RetentionViolation):
        _decide([{"object_id": policy_id, "compartments": ["structural"]}])

    # Unknown parts and non-Blob content erasure fail closed too.
    with pytest.raises(ErasureError):
        _decide([{"object_id": utterance["id"], "compartments": ["nope"]}])
    with pytest.raises(ErasureError):
        _decide([{"object_id": utterance["id"], "compartments": ["content"]}])

    # Storage-verified / cryptographic assurance is never claimed.
    with pytest.raises(ErasureError):
        svc.decide(
            request_id=request["request_id"],
            decision="approve",
            targets=[{"object_id": utterance["id"], "compartments": ["semantic"]}],
            reasoning="honest labeling",
            decided_by=rig.person_id,
            authority=_governance_authority(rig),
            assurance="cryptographic",
        )


# ---------------------------------------------------------------------------
# Crash-safe resume (spec 3.8)
# ---------------------------------------------------------------------------


def test_crash_after_purge_resumes_and_never_reports_recoverable(rig):
    drop = _utterance(rig, "crash window content")
    result = _admit(rig, records=[drop])
    assert result["status"] == "accepted", result

    svc = rig.archive.erasure()
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={"targets": []},
        reason="crash test",
        authority=_requester_authority(rig),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=[{"object_id": drop["id"], "compartments": ["semantic"]}],
        reasoning="approved",
        decided_by=rig.person_id,
        authority=_governance_authority(rig),
    )
    operation_id = decided["operation_id"]

    # Advance through block (content destroyed) and destroy (copies
    # purged), then simulate the crash: no receipt stage runs.
    assert svc.advance(operation_id)["stage"] == "block"
    mid = svc.advance(operation_id)
    assert mid["stage"] == "destroy"
    assert mid["content_recoverable"] is False
    assert mid["receipt_id"] is None

    # A fresh service (post-crash) resumes from durable state.
    recovered = rig.archive.erasure()
    pending = recovered.resume_pending()
    assert [entry["operation_id"] for entry in pending] == [operation_id]
    final = recovered.status(operation_id)
    assert final["stage"] == "receipt"
    assert final["content_recoverable"] is False
    assert final["receipt_id"] is not None

    receipt = rig.archive.get_object(final["receipt_id"])
    payload = receipt["compartments"]["semantic"]["envelope"]["content"]["payload"]
    assert payload["status"] == "verified"
    assert payload["keys_destroyed"] == "0"
    structural = receipt["compartments"]["structural"]["envelope"]["content"]
    assert structural["type"] == "lineage.erasure_receipt"
    assert structural["structural_payload"]["profile"] == "logical"
    assert rig.archive.verify_chain()["commits_verified"] > 0


# ---------------------------------------------------------------------------
# Purge verification: projections, checkpoints, wiki staging (spec 3.8.5)
# ---------------------------------------------------------------------------


def test_projection_checkpoint_and_wiki_purge(rig, tmp_path):
    entity = _entity(rig, "purge me")
    result = _admit(rig, records=[entity])
    assert result["status"] == "accepted", result
    rig.archive.projections.rebuild_all()
    staging = tmp_path / "wiki-staging"
    rig.archive.projections.rebuild_wiki(staging)
    pages = list((staging / "pages").glob("*.md"))
    assert len(pages) == 1
    assert entity["id"] in pages[0].read_text()
    # A checkpoint embeds derived plaintext — a controlled copy.
    rig.archive.projections.save_checkpoint("full_text")

    _svc, status = _erase(
        rig,
        [{"object_id": entity["id"], "compartments": ["semantic"]}],
        staging=staging,
    )
    purged_stores = {entry["store"] for entry in status["purged"]}
    assert "projection_full_text" in purged_stores
    assert "projection_checkpoint" in purged_stores
    assert "wiki_staging" in purged_stores
    assert "verification" in purged_stores

    with open_ccf_connection(rig.settings) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM projection_full_text WHERE object_id = %s",
            (entity["id"],),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM projection_checkpoint WHERE projection_name = 'full_text'"
        ).fetchone()[0] == 0
    # The wiki rebuild skips the erased entity entirely.
    assert list((staging / "pages").glob("*.md")) == []
    rig.archive.projections.rebuild_all()
    assert rig.archive.projections.search_text("purge") == [] or all(
        hit["object_id"] != entity["id"]
        for hit in rig.archive.projections.search_text("purge")
    )


# ---------------------------------------------------------------------------
# Suppression after erasure (spec 12.7, 6.5)
# ---------------------------------------------------------------------------


def test_retry_after_erasure_returns_lifecycle_without_bytes(rig):
    source_id = _source(rig)
    utterance = _utterance(rig, "retract this utterance", source_id=source_id)
    result = _admit(rig, records=[utterance])
    assert result["status"] == "accepted", result
    _erase(
        rig,
        [{"object_id": utterance["id"], "compartments": ["semantic"]}],
        authorized_producers=[rig.producer.producer_id],
    )

    # Same submission retried after erasure (spec 6.5): the authorized
    # producer gets the current lifecycle; no bytes come back.
    retry = _admit(rig, records=[utterance])
    assert retry["status"] == "accepted", retry
    outcome = retry["admissions"][0]
    assert outcome["status"] == "existing"
    assert outcome["current_lifecycle"] == "erased"
    assert outcome["payload_available"] is False


def test_suppression_blocks_silent_reintroduction(rig, tmp_path):
    source_id = _source(rig)
    original = _utterance(rig, "sensitive capture", source_id=source_id)
    result = _admit(rig, records=[original])
    assert result["status"] == "accepted", result
    _erase(
        rig,
        [{"object_id": original["id"], "compartments": ["semantic"]}],
        authorized_producers=[rig.producer.producer_id],
    )

    # Authorized producer, same source item under a new revision: a
    # lifecycle result, still no admission.
    recapture = _utterance(
        rig, "sensitive capture", source_id=source_id, revision="2"
    )
    result = _admit(rig, records=[recapture])
    outcome = result["admissions"][0]
    assert outcome["status"] == "existing"
    assert outcome["current_lifecycle"] == "suppressed"
    assert outcome["payload_available"] is False
    assert rig.archive.get_object(recapture["id"]) is None

    # Unauthorized caller: an indistinguishable generic refusal — no
    # lifecycle, no mention of erasure or suppression.
    other = add_producer(rig, tmp_path, "other")
    foreign = _utterance(
        rig, "sensitive capture", source_id=source_id, revision="3", producer=other
    )
    result = _admit(rig, records=[foreign], producer=other)
    outcome = result["admissions"][0]
    assert outcome["status"] == "rejected"
    assert outcome["payload_available"] is False
    assert "current_lifecycle" not in outcome
    reason = outcome.get("reason", "")
    assert "eras" not in reason and "suppress" not in reason
    assert rig.archive.get_object(foreign["id"]) is None


# ---------------------------------------------------------------------------
# Tombstone terminality (spec 8.4)
# ---------------------------------------------------------------------------


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
            "reason": "erasure test",
            "previous_disposition_id": previous_head_id,
            "replacement_link_id": None,
            "extensions": {},
        },
    )


def test_tombstone_is_terminal(rig):
    ancestor = _concept(rig, "tombstone ancestor")
    descendant = _concept(rig, "tombstone descendant")
    link = rig.producer.new_link(
        type="ccf.derived_from",
        from_id=descendant["id"],
        to_id=ancestor["id"],
        claims=rig.claims(),
    )
    result = _admit(rig, records=[ancestor, descendant], links=[link])
    assert result["status"] == "accepted", result

    lineage_id = generate_id("lineage")
    tombstone = _disposition(rig, link["id"], "tombstone", lineage_id, None)
    result = _admit(rig, records=[tombstone])
    assert result["status"] == "accepted", result

    # A physical/logical erasure tombstone is terminal: restore is refused.
    restore = _disposition(rig, link["id"], "restore", lineage_id, tombstone["id"])
    result = _admit(rig, records=[restore])
    assert result["status"] == "conflict", result
    outcome = result["admissions"][0]
    assert outcome["status"] == "lineage_conflict"

    rig.archive.projections.rebuild_all()
    state = rig.archive.projections.link_state(link["id"])
    assert state["state"] == "tombstoned"
    # The tombstoned edge leaves the derivation closure.
    assert (ancestor["id"], descendant["id"]) not in (
        rig.archive.projections.closure_pairs()
    )


# ---------------------------------------------------------------------------
# Blob content erasure (spec 4.4, 3.9) and multi-subject decisions
# ---------------------------------------------------------------------------


def test_blob_content_erasure_destroys_salt(rig):
    data = b"mixed-subject audio bytes"
    blob, blob_data = rig.producer.new_blob(
        data=data, media_type="audio/raw", claims=rig.claims()
    )
    result = _admit(rig, blobs=[blob], blob_bytes={blob["id"]: blob_data})
    assert result["status"] == "accepted", result

    _erase(rig, [{"object_id": blob["id"], "compartments": ["content", "semantic"]}])

    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT state, plaintext_bytes, content_salt FROM blob_content WHERE blob_id = %s",
            (blob["id"],),
        ).fetchone()
    assert row[0] == "erased"
    assert row[1] is None
    # The content salt is erased with the bytes (spec 4.4).
    assert row[2] is None
    # The manifest (structural compartment) is retained.
    structural = _compartment_state(rig, blob["id"], "structural")
    assert structural["state"] == "plaintext"
    assert structural["envelope"]["content"]["content_commitment"].startswith("sha256:")
    assert rig.archive.verify_chain()["commits_verified"] > 0


def test_multi_subject_media_decision_shapes(rig):
    svc = rig.archive.erasure()
    blob_id = generate_id("blob")
    replacement_id = generate_id("blob")
    subjects = [rig.person_id, generate_id("record")]

    whole = svc.plan_media_decision(blob_id=blob_id, subject_ids=subjects)
    assert whole["action"] == "erase_blob"

    restricted = svc.plan_media_decision(
        blob_id=blob_id, subject_ids=subjects, restrict_pending_review=True
    )
    assert restricted["action"] == "restrict"

    replaced = svc.plan_media_decision(
        blob_id=blob_id,
        subject_ids=subjects,
        reviewed_replacement_blob_id=replacement_id,
    )
    assert replaced["action"] == "replace"
    assert replaced["reviewed_replacement_blob_id"] == replacement_id

    # Contradictory or malformed decisions fail closed.
    with pytest.raises(ErasureError):
        svc.plan_media_decision(
            blob_id=blob_id,
            subject_ids=subjects,
            restrict_pending_review=True,
            reviewed_replacement_blob_id=replacement_id,
        )
    with pytest.raises(ErasureError):
        svc.plan_media_decision(blob_id=blob_id, subject_ids=[])
    with pytest.raises(ErasureError):
        svc.plan_media_decision(blob_id=rig.person_id, subject_ids=subjects)
    # No surgical span-level API exists.
    with pytest.raises(TypeError):
        svc.plan_media_decision(
            blob_id=blob_id, subject_ids=subjects, spans=[(0, 1000)]
        )


# ---------------------------------------------------------------------------
# Receipt membership links (spec 1.6)
# ---------------------------------------------------------------------------


def test_receipt_membership_links(rig):
    drop = _utterance(rig, "covered by a receipt")
    result = _admit(rig, records=[drop])
    assert result["status"] == "accepted", result
    _svc, status = _erase(
        rig, [{"object_id": drop["id"], "compartments": ["semantic"]}]
    )
    receipt_id = status["receipt_id"]

    with open_ccf_connection(rig.settings) as conn:
        rows = conn.execute(
            """
            SELECT oh.id FROM object_header oh
            JOIN compartment c
              ON c.object_id = oh.id AND c.compartment = 'structural'
            WHERE oh.object_kind = 'link' AND c.state = 'plaintext'
              AND c.plaintext_json ->> 'type' = 'ccf.covers'
              AND c.plaintext_json ->> 'from_id' = %s
              AND c.plaintext_json ->> 'to_id' = %s
            """,
            (receipt_id, drop["id"]),
        ).fetchall()
    assert len(rows) == 1


def test_suppression_key_is_owner_only_and_never_overwritten(tmp_path):
    """L1: the suppression HMAC key is 0600 and refuses silent overwrite."""
    import stat

    from ccf.erasure.errors import SuppressionKeyError

    key_path = generate_suppression_key(tmp_path / "suppression.key")
    assert stat.S_IMODE(key_path.stat().st_mode) == 0o600
    with pytest.raises(SuppressionKeyError, match="overwrite"):
        generate_suppression_key(key_path)
