"""Governance baseline tests (checklist phase 6, spec sections 9.1-9.6).

Covers the pinned ``ccf-deny-overrides-v1`` evaluator end to end against
the ephemeral Postgres archive: exact positive allows, deny-overrides,
obligation accumulation, alternative legal bases, valid-time behavior,
entity-merge consent propagation, generation fences blocking stale allows,
bounded pending that resolves, unknown-context handling, destination
overlays, and registry ``required_authority`` enforcement at admission.
"""

from __future__ import annotations

import pytest

from ccf.governance import LOCAL_DESTINATION
from ccf.governance.decisions import cached_decision
from ccf.governance.fences import ALL_FENCES, FENCE_POLICY, snapshot_fences
from ccf.ids import generate_id

from ccf_helpers import authority, claims, make_rig, privacy

START = "2026-08-12T00:00:00.000Z"
BEFORE = "2026-08-11T00:00:00.000Z"
AFTER = "2026-08-13T00:00:00.000Z"


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def engine(rig):
    return rig.archive.governance()


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _rule(rule_id, effect, *, operations=None, purposes=None, obligations=None,
          data_classes=None, destinations=None, recipients=None, conditions=None,
          valid_from=None, expires_at=None):
    rule = {
        "rule_id": rule_id,
        "effect": effect,
        "operations": operations or [],
        "purposes": purposes or [],
        "recipients": recipients or [],
        "destinations": destinations or [],
        "data_classes": data_classes or [],
        "conditions": conditions or [],
        "obligations": obligations or [],
        "valid_from": valid_from,
        "expires_at": expires_at,
    }
    return rule


def _policy_payload(rules, *, default_effect="deny"):
    return {
        "profile": "ccf.policy/0.1.2-rc1",
        "evaluator_profile": "ccf-deny-overrides-v1",
        "combining_algorithm": "deny_overrides_v1",
        "default_effect": default_effect,
        "rules": rules,
        "provenance_requirement": "none",
        "retention": {"minimum_until": None, "maximum_until": None, "on_expiry": "review"},
        "extensions": {},
    }


def _policy(rig, lineage_id, rules, *, previous=None, transition="create",
            default_effect="deny", policy_hint=None):
    now = rig.clock()
    record_claims = claims(
        rig.person_id,
        rig.runtime_id,
        policy_hint=policy_hint if policy_hint is not None else rig.policy_lineage_id,
    )
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
        payload=_policy_payload(rules, default_effect=default_effect),
    )


def _consent(rig, lineage_id, subject_id, *, purposes, operations, classes,
             decision="given", previous=None, transition="give",
             valid_from=START, expires_at=None):
    now = rig.clock()
    record_claims = claims(rig.person_id, rig.runtime_id)
    record_claims["authority"] = authority(
        "first_person_statement", subject_id, subject_id
    )
    return rig.producer.new_record(
        type="governance.consent",
        claims=record_claims,
        lineage={
            "lineage_id": lineage_id,
            "previous_head_id": previous,
            "transition": transition,
            "valid_from": now,
            "expires_at": None,
        },
        payload={
            "subject_id": subject_id,
            "controller_id": rig.person_id,
            "decision": decision,
            "purposes": purposes,
            "operations": operations,
            "data_classes": classes,
            "scope": {},
            "valid_from": valid_from,
            "expires_at": expires_at,
            "evidence_refs": [],
            "extensions": {},
        },
    )


def _legal_basis(rig, lineage_id, subject_id, *, purposes, operations, classes,
                 jurisdiction=None, previous=None, transition="create",
                 valid_from=START, expires_at=None):
    now = rig.clock()
    record_claims = claims(rig.person_id, rig.runtime_id)
    record_claims["authority"] = authority(
        "explicit_authorization", rig.person_id, rig.person_id
    )
    return rig.producer.new_record(
        type="governance.legal_basis",
        claims=record_claims,
        lineage={
            "lineage_id": lineage_id,
            "previous_head_id": previous,
            "transition": transition,
            "valid_from": now,
            "expires_at": None,
        },
        payload={
            "subject_id": subject_id,
            "controller_id": rig.person_id,
            "basis_code": "legitimate_interest",
            "purposes": purposes,
            "operations": operations,
            "data_classes": classes,
            "jurisdiction": jurisdiction or {},
            "valid_from": valid_from,
            "expires_at": expires_at,
            "evidence_refs": [],
            "extensions": {},
        },
    )


def _governance_record(rig, type_name, lineage_id, transition, payload, *,
                       previous=None, basis="explicit_authorization",
                       asserted_by=None):
    now = rig.clock()
    asserted_by = asserted_by or rig.person_id
    record_claims = claims(rig.person_id, rig.runtime_id)
    record_claims["authority"] = authority(basis, asserted_by, asserted_by)
    return rig.producer.new_record(
        type=type_name,
        claims=record_claims,
        lineage={
            "lineage_id": lineage_id,
            "previous_head_id": previous,
            "transition": transition,
            "valid_from": now,
            "expires_at": None,
        },
        payload=payload,
    )


def _concept(rig, policy_lineage_id=None, *, subjects=None, classes=None):
    record_claims = claims(
        rig.person_id,
        rig.runtime_id,
        policy_hint=policy_lineage_id,
        classes=classes,
        subjects=subjects,
    )
    return rig.producer.new_record(
        type="semantic.concept",
        claims=record_claims,
        payload={
            "label": "governed concept",
            "definition": "a concept under test",
            "aliases": [],
            "extensions": {},
        },
    )


def _subject(person_id):
    return {
        "person_id": person_id,
        "role": "participant",
        "identity_state_at_write": "verified",
    }


def _person(rig, person_id, name):
    return rig.producer.new_record(
        type="core.person",
        object_id=person_id,
        claims=claims(rig.person_id, rig.runtime_id),
        payload={
            "kind": "human",
            "display_name": name,
            "aliases": [],
            "identity_anchors": [],
            "extensions": {},
        },
    )


def _admit(rig, *records, links=None):
    batch = rig.producer.create_batch(records=list(records), links=list(links or []))
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "accepted", result
    return result


def _read_kwargs(rig, object_id, *, purpose="research", requester=None):
    return {
        "operation": "read_local",
        "purpose": purpose,
        "requester": requester or rig.runtime_id,
        "runtime": rig.runtime_id,
        "destination": LOCAL_DESTINATION,
        "object_ids": [object_id],
    }


# ---------------------------------------------------------------------------
# Positive allows and the combining algorithm (spec 9.1)
# ---------------------------------------------------------------------------


def test_positive_allow_exact(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig,
        policy_lineage,
        [_rule("allow-research", "allow", operations=["read_local"], purposes=["research"])],
    )
    _admit(rig, concept, policy)

    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    decision = result.decision
    assert result.pending == []
    assert decision["decision"] == "allow"
    assert decision["reason_codes"] == ["allowed"]
    assert decision["evaluator_profile"] == "ccf-deny-overrides-v1"
    assert decision["evaluator_version"] == "1"
    assert decision["valid_until"] is None
    assert set(decision["generation_vector"]) == set(ALL_FENCES)
    assert decision["policy_closure_hash"].startswith("sha256:")
    assert decision["decision_context_hash"].startswith("sha256:")


def test_root_default_deny_without_applicable_allow(rig, engine):
    concept = _concept(rig)  # root policy: rules=[], default deny
    _admit(rig, concept)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["default_deny"]


def test_deny_overrides_allow(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig,
        policy_lineage,
        [
            _rule("allow-read", "allow", operations=["read_local"]),
            _rule("deny-research", "deny", purposes=["research"]),
        ],
    )
    _admit(rig, concept, policy)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["deny_rule:deny-research"]


def test_obligations_accumulate(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig,
        policy_lineage,
        [
            _rule(
                "allow-read",
                "allow",
                operations=["read_local"],
                obligations=["log_access"],
            ),
            _rule(
                "oblige-research",
                "oblige",
                purposes=["research"],
                obligations=["cite_source"],
            ),
        ],
    )
    _admit(rig, concept, policy)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "allow"
    names = [o["obligation"] for o in result.decision["obligations"]]
    assert names == ["cite_source", "log_access"]


# ---------------------------------------------------------------------------
# Alternative legal bases and valid time (spec 9.1, 8.3)
# ---------------------------------------------------------------------------


def test_consent_as_legal_basis_allow_and_required(rig, engine):
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    subject = _subject(rig.person_id)
    concept = _concept(
        rig, policy_lineage, subjects=[subject], classes=["document_content"]
    )
    policy = _policy(
        rig,
        policy_lineage,
        [_rule("allow-read", "allow", operations=["read_local"])],
    )
    _admit(rig, concept, policy)

    # No basis yet: deny, never pending-forever.
    denied = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert denied.decision["decision"] == "deny"
    assert denied.decision["reason_codes"] == ["no_legal_basis"]

    consent = _consent(
        rig,
        consent_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
    )
    _admit(rig, consent)
    allowed = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert allowed.decision["decision"] == "allow"


def test_legal_basis_is_an_alternative_to_consent(rig, engine):
    policy_lineage = generate_id("lineage")
    basis_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    basis = _legal_basis(
        rig,
        basis_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
    )
    _admit(rig, concept, policy, basis)
    # A legal-basis record alone (no consent anywhere) suffices.
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "allow"


def test_legal_basis_jurisdiction_must_cover(rig, engine):
    policy_lineage = generate_id("lineage")
    basis_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    basis = _legal_basis(
        rig,
        basis_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
        jurisdiction={"country": "DE"},
    )
    _admit(rig, concept, policy, basis)
    kwargs = _read_kwargs(rig, concept["id"])
    kwargs["jurisdiction"] = {"country": "US"}
    assert engine.authorize(**kwargs).decision["decision"] == "deny"
    kwargs["jurisdiction"] = {"country": "DE"}
    assert engine.authorize(**kwargs).decision["decision"] == "allow"


def test_valid_time_gaps_expiry_and_backdating(rig, engine):
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    consent = _consent(
        rig,
        consent_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
        valid_from=START,
        expires_at="2026-08-12T12:00:00.000Z",
    )
    _admit(rig, concept, policy, consent)
    kwargs = _read_kwargs(rig, concept["id"])

    # Backdated before the consent window: not yet valid.
    kwargs["requested_at"] = BEFORE
    assert engine.authorize(**kwargs).decision["decision"] == "deny"
    # Inside the window.
    kwargs["requested_at"] = "2026-08-12T06:00:00.000Z"
    in_window = engine.authorize(**kwargs)
    assert in_window.decision["decision"] == "allow"
    assert in_window.decision["valid_until"] == "2026-08-12T12:00:00.000Z"
    # After expiry.
    kwargs["requested_at"] = AFTER
    assert engine.authorize(**kwargs).decision["decision"] == "deny"


def test_rule_valid_time_window(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig,
        policy_lineage,
        [
            _rule(
                "allow-window",
                "allow",
                operations=["read_local"],
                valid_from=START,
                expires_at="2026-08-12T12:00:00.000Z",
            )
        ],
    )
    _admit(rig, concept, policy)
    kwargs = _read_kwargs(rig, concept["id"])
    kwargs["requested_at"] = BEFORE
    assert engine.authorize(**kwargs).decision["decision"] == "deny"
    kwargs["requested_at"] = "2026-08-12T06:00:00.000Z"
    assert engine.authorize(**kwargs).decision["decision"] == "allow"
    kwargs["requested_at"] = AFTER
    assert engine.authorize(**kwargs).decision["decision"] == "deny"


# ---------------------------------------------------------------------------
# Entity resolution (spec 8.5) and governance lineage states
# ---------------------------------------------------------------------------


def test_entity_merge_propagates_consent(rig, engine):
    person_a = generate_id("record")
    person_b = generate_id("record")
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    concept = _concept(
        rig, policy_lineage, subjects=[_subject(person_b)], classes=["document_content"]
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    consent = _consent(
        rig,
        consent_lineage,
        person_a,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
    )
    _admit(rig, _person(rig, person_a, "A"), _person(rig, person_b, "B"))
    merge = rig.producer.new_link(
        type="ccf.same_as",
        from_id=person_a,
        to_id=person_b,
        claims=claims(rig.person_id, rig.runtime_id),
    )
    _admit(rig, concept, policy, consent, links=[merge])

    # Without the merge the consent for A would not cover B's data.
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "allow"


def test_distinct_from_vetoes_the_merge(rig, engine):
    person_a = generate_id("record")
    person_b = generate_id("record")
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    concept = _concept(
        rig, policy_lineage, subjects=[_subject(person_b)], classes=["document_content"]
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    consent = _consent(
        rig,
        consent_lineage,
        person_a,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
    )
    merge = rig.producer.new_link(
        type="ccf.same_as",
        from_id=person_a,
        to_id=person_b,
        claims=claims(rig.person_id, rig.runtime_id),
    )
    split = rig.producer.new_link(
        type="ccf.distinct_from",
        from_id=person_a,
        to_id=person_b,
        claims=claims(rig.person_id, rig.runtime_id),
    )
    _admit(rig, _person(rig, person_a, "A"), _person(rig, person_b, "B"))
    _admit(rig, concept, policy, consent, links=[merge, split])

    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["no_legal_basis"]


def test_restriction_denies_despite_allow_and_consent(rig, engine):
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    restriction_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    consent = _consent(
        rig,
        consent_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
    )
    restriction = _governance_record(
        rig,
        "governance.restriction",
        restriction_lineage,
        "impose",
        {
            "subject_id": rig.person_id,
            "decision": "impose",
            "scope": {},
            "reason": "subject request",
            "effective_at": BEFORE,
            "evidence_refs": [],
            "extensions": {},
        },
    )
    _admit(rig, concept, policy, consent, restriction)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["restriction_active"]


def test_legal_hold_blocks_destructive_operation(rig, engine):
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    hold_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-erase", "allow", operations=["erase"])]
    )
    consent = _consent(
        rig,
        consent_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["erase"],
        classes=["document_content"],
    )
    hold = _governance_record(
        rig,
        "governance.legal_hold",
        hold_lineage,
        "impose",
        {
            "subject_id": rig.person_id,
            "decision": "impose",
            "scope": {},
            "reason": "litigation",
            "effective_at": BEFORE,
            "evidence_refs": [],
            "extensions": {},
        },
    )
    _admit(rig, concept, policy, consent, hold)
    result = engine.authorize(
        operation="erase",
        purpose="research",
        requester=rig.runtime_id,
        runtime=rig.runtime_id,
        destination=LOCAL_DESTINATION,
        object_ids=[concept["id"]],
    )
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["legal_hold_active"]


# ---------------------------------------------------------------------------
# Generation fences (spec 9.5)
# ---------------------------------------------------------------------------


def test_generation_fence_blocks_stale_allow_after_mutation(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    _admit(rig, concept, policy)

    kwargs = _read_kwargs(rig, concept["id"])
    kwargs["requested_at"] = START  # identical context -> cacheable
    allowed = engine.authorize(**kwargs)
    assert allowed.decision["decision"] == "allow"
    # A repeat read with the same head is served from the local cache.
    cached = engine.authorize(**kwargs)
    assert cached.from_cache is True

    # Tightening mutation: supersede the policy with a deny rule. The fence
    # advances in the admission transaction.
    tightened = _policy(
        rig,
        policy_lineage,
        [
            _rule("allow-read", "allow", operations=["read_local"]),
            _rule("deny-all", "deny", operations=["read_local"]),
        ],
        previous=policy["id"],
        transition="supersede",
    )
    _admit(rig, tightened)

    denied = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert denied.from_cache is False
    assert denied.decision["decision"] == "deny"
    assert denied.decision["reason_codes"] == ["deny_rule:deny-all"]


def test_cached_decision_requires_matching_generations(rig, engine, ccf_settings):
    """The cache itself refuses a stale allow even for an identical context."""
    from ccf.db import open_ccf_connection

    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    _admit(rig, concept, policy)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "allow"

    with open_ccf_connection(ccf_settings) as conn:
        # Same context hash, but a fence moved underneath: no stale allow.
        current = snapshot_fences(conn, rig.archive.archive_id)
        shifted = dict(current)
        shifted[FENCE_POLICY] = str(int(shifted[FENCE_POLICY]) + 1)
        served = cached_decision(
            conn,
            archive_id=rig.archive.archive_id,
            decision_context_hash=result.decision["decision_context_hash"],
            current_generations=shifted,
            now=rig.clock(),
        )
        assert served is None
        # And the untouched vector still serves it.
        served = cached_decision(
            conn,
            archive_id=rig.archive.archive_id,
            decision_context_hash=result.decision["decision_context_hash"],
            current_generations=current,
            now=rig.clock(),
        )
        assert served is not None and served["decision"] == "allow"


def test_widening_recomputed_not_served_from_cache(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(rig, policy_lineage, [])  # default deny
    _admit(rig, concept, policy)
    denied = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert denied.decision["decision"] == "deny"

    widened = _policy(
        rig,
        policy_lineage,
        [_rule("allow-read", "allow", operations=["read_local"])],
        previous=policy["id"],
        transition="supersede",
    )
    _admit(rig, widened)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.from_cache is False
    assert result.decision["decision"] == "allow"


def test_governed_by_link_mutation_advances_fence(rig, engine, ccf_settings):
    from ccf.db import open_ccf_connection
    from ccf.governance.fences import FENCE_LINKS, fence_last_change

    policy_lineage = generate_id("lineage")
    concept = _concept(rig)  # root policy: default deny
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    _admit(rig, concept, policy)
    assert engine.authorize(**_read_kwargs(rig, concept["id"])).decision["decision"] == "deny"

    binding = rig.producer.new_link(
        type="ccf.governed_by",
        from_id=concept["id"],
        to_id=policy["id"],
        claims=claims(rig.person_id, rig.runtime_id),
    )
    _admit(rig, links=[binding])
    with open_ccf_connection(ccf_settings) as conn:
        assert fence_last_change(conn, rig.archive.archive_id, FENCE_LINKS) is not None
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "allow"


# ---------------------------------------------------------------------------
# Bounded pending (spec 9.6) and unknown context (spec 9.1)
# ---------------------------------------------------------------------------


def test_pending_on_unresolved_policy_lineage_then_resolves(rig, engine, ccf_package_root):
    from ccf.schemas import SchemaSet

    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    _admit(rig, concept)  # policy_hint names a lineage with no head yet

    pending = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert pending.decision["decision"] == "pending"
    assert pending.decision["reason_codes"] == ["policy_lineage_unresolved"]
    assert len(pending.pending) == 1
    document = pending.pending[0]
    assert document["status"] == "policy_resolution_pending"
    assert document["object_id"] == concept["id"]
    assert int(document["remaining_dependencies_estimate"]) >= 1
    assert int(document["retry_after_ms"]) >= 1
    assert document["request_id"]
    SchemaSet.load(ccf_package_root).validate(
        "urn:ccf:schema:0.1.2-rc1:operational.policy-pending",
        document,
        what="pending document",
    )

    # The dependency lands: pending resolves to a real allow.
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    _admit(rig, policy)
    resolved = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert resolved.decision["decision"] == "allow"


def test_pending_on_raised_objection_then_resolves(rig, engine):
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    objection_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    consent = _consent(
        rig,
        consent_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["read_local"],
        classes=["document_content"],
    )
    objection = _governance_record(
        rig,
        "governance.objection",
        objection_lineage,
        "raise",
        {
            "subject_id": rig.person_id,
            "decision": "raise",
            "scope": {},
            "reason": "not happy",
            "effective_at": BEFORE,
            "evidence_refs": [],
            "extensions": {},
        },
        basis="first_person_statement",
    )
    _admit(rig, concept, policy, consent, objection)
    pending = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert pending.decision["decision"] == "pending"
    assert pending.decision["reason_codes"] == ["objection_pending"]
    assert pending.pending[0]["object_id"] == concept["id"]

    # Resolution via the state machine: override lifts the objection.
    resolved_record = _governance_record(
        rig,
        "governance.objection",
        objection_lineage,
        "override",
        {
            "subject_id": rig.person_id,
            "decision": "override",
            "scope": {},
            "reason": "compelling grounds",
            "effective_at": BEFORE,
            "evidence_refs": [],
            "extensions": {},
        },
        previous=objection["id"],
    )
    _admit(rig, resolved_record)
    resolved = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert resolved.decision["decision"] == "allow"


def test_unknown_rule_condition_yields_pending(rig, engine):
    policy_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig,
        policy_lineage,
        [
            _rule(
                "allow-conditional",
                "allow",
                operations=["read_local"],
                conditions=[
                    {"attribute": "extensions.tenant", "operator": "equals", "value": "a"}
                ],
            )
        ],
    )
    _admit(rig, concept, policy)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "pending"
    assert result.decision["reason_codes"] == ["unknown_rule_context"]

    # Supplying the context resolves deterministically.
    kwargs = _read_kwargs(rig, concept["id"])
    kwargs["extensions"] = {"tenant": "a"}
    assert engine.authorize(**kwargs).decision["decision"] == "allow"
    kwargs["extensions"] = {"tenant": "b"}
    assert engine.authorize(**kwargs).decision["decision"] == "deny"


def test_incomplete_subject_coverage_denies(rig, engine):
    policy_lineage = generate_id("lineage")
    record_claims = claims(
        rig.person_id, rig.runtime_id, policy_hint=policy_lineage
    )
    record_claims["privacy"] = privacy(["document_content"], [_subject(rig.person_id)])
    record_claims["privacy"]["subject_coverage"] = "partial"
    concept = rig.producer.new_record(
        type="semantic.concept",
        claims=record_claims,
        payload={
            "label": "x",
            "definition": "x",
            "aliases": [],
            "extensions": {},
        },
    )
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    _admit(rig, concept, policy)
    result = engine.authorize(**_read_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["subject_coverage_unknown"]


def test_unknown_object_denies(rig, engine):
    result = engine.authorize(**_read_kwargs(rig, generate_id("record")))
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["object_unavailable"]


# ---------------------------------------------------------------------------
# Destination overlays (spec 9.1): tighten, never widen
# ---------------------------------------------------------------------------


def test_destination_overlay_tightens(rig, engine):
    recipient = _person(rig, generate_id("record"), "partner")
    policy_lineage = generate_id("lineage")
    overlay_lineage = generate_id("lineage")
    concept = _concept(rig, policy_lineage)
    policy = _policy(
        rig, policy_lineage, [_rule("allow-read", "allow", operations=["read_local"])]
    )
    overlay = _policy(
        rig,
        overlay_lineage,
        [_rule("deny-partner", "deny", destinations=["partner"])],
    )
    binding = rig.producer.new_link(
        type="ccf.governed_by",
        from_id=recipient["id"],
        to_id=overlay["id"],
        claims=claims(rig.person_id, rig.runtime_id),
    )
    _admit(rig, recipient, concept, policy, overlay, links=[binding])

    kwargs = _read_kwargs(rig, concept["id"])
    kwargs["destination"] = "partner"
    kwargs["recipient"] = recipient["id"]
    result = engine.authorize(**kwargs)
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["deny_rule:deny-partner"]

    # Same request without the overlay destination still allows.
    assert engine.authorize(**_read_kwargs(rig, concept["id"])).decision["decision"] == "allow"


def test_destination_overlay_cannot_widen(rig, engine):
    recipient = _person(rig, generate_id("record"), "partner")
    overlay_lineage = generate_id("lineage")
    concept = _concept(rig)  # root policy: default deny
    overlay = _policy(
        rig,
        overlay_lineage,
        [_rule("allow-partner", "allow", operations=["read_local"])],
    )
    binding = rig.producer.new_link(
        type="ccf.governed_by",
        from_id=recipient["id"],
        to_id=overlay["id"],
        claims=claims(rig.person_id, rig.runtime_id),
    )
    _admit(rig, recipient, concept, overlay, links=[binding])
    kwargs = _read_kwargs(rig, concept["id"])
    kwargs["destination"] = "partner"
    kwargs["recipient"] = recipient["id"]
    result = engine.authorize(**kwargs)
    assert result.decision["decision"] == "deny"
    assert result.decision["reason_codes"] == ["default_deny"]


# ---------------------------------------------------------------------------
# required_authority enforcement at admission (spec 5.5)
# ---------------------------------------------------------------------------


def test_governance_policy_requires_authorized_actor(rig):
    record_claims = claims(rig.person_id, rig.runtime_id)  # runtime_import basis
    policy = rig.producer.new_record(
        type="governance.policy",
        claims=record_claims,
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "create",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload=_policy_payload([]),
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[policy]))
    assert result["status"] == "conflict"
    admission = result["admissions"][0]
    assert admission["status"] == "rejected"
    assert "authorized_governance_actor" in admission["reason"]


def test_consent_requires_subject_or_representative(rig):
    record_claims = claims(rig.person_id, rig.runtime_id)
    consent = rig.producer.new_record(
        type="governance.consent",
        claims=record_claims,
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "give",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "subject_id": rig.person_id,
            "controller_id": rig.person_id,
            "decision": "given",
            "purposes": [],
            "operations": [],
            "data_classes": [],
            "scope": {},
            "valid_from": START,
            "expires_at": None,
            "evidence_refs": [],
            "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[consent]))
    assert result["status"] == "conflict"
    admission = result["admissions"][0]
    assert admission["status"] == "rejected"
    assert "subject_or_authorized_representative" in admission["reason"]


def test_unknown_authority_basis_fails_closed(ccf_package_root):
    """A basis outside the pinned registry never satisfies any class."""
    from ccf.governance.authority import check_required_authority
    from ccf.registry import PinnedRegistries

    registries = PinnedRegistries.load(ccf_package_root)
    claim = {
        "basis": "divine_revelation",
        "asserted_by": "urn:ccf:record:00000000-0000-4000-8000-000000000000",
        "accepted_by": None,
    }
    for required in ("source_or_runtime", "runtime_authenticated"):
        reason = check_required_authority(
            required,
            claim=claim,
            recorded_by=claim["asserted_by"],
            admitted_by_archive=False,
            registries=registries,
        )
        # Rejection reasons are the pinned registry's normative
        # failure_reason strings, verbatim (0.1.2-rc1).
        assert reason == registries.authority_class(required)["failure_reason"]
    # Unknown required_authority classes fail closed too.
    reason = check_required_authority(
        "unlisted_class",
        claim=None,
        recorded_by=claim["asserted_by"],
        admitted_by_archive=False,
        registries=registries,
    )
    assert reason is not None and "unknown required_authority" in reason
