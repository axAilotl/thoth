"""Consequential egress tests (checklist phase 6, spec sections 9.4, 9.7-9.8).

The egress boundary is explicit: ``authorize_egress`` always evaluates
fresh and issues a short-expiry, use-counted capability binding the exact
operation, purpose, objects, parties, destination, head, generation
vector, and availability; ``consume_egress_capability`` rechecks every
fence at the boundary; consequential actions create canonical receipts.
Local reads never depend on the network.
"""

from __future__ import annotations

import pytest

from ccf.governance import LOCAL_DESTINATION, GovernanceError
from ccf.governance.errors import CapabilityError
from ccf.ids import generate_id

from ccf_helpers import authority, claims, make_rig

from test_ccf_governance import (
    START,
    _admit,
    _concept,
    _consent,
    _policy,
    _rule,
    _subject,
)


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def engine(rig):
    return rig.archive.governance()


@pytest.fixture()
def governed_concept(rig):
    """A concept allowed for research disclosure with covering consent."""
    policy_lineage = generate_id("lineage")
    consent_lineage = generate_id("lineage")
    concept = _concept(
        rig,
        policy_lineage,
        subjects=[_subject(rig.person_id)],
        classes=["document_content"],
    )
    policy = _policy(
        rig,
        policy_lineage,
        [_rule("allow-disclose", "allow", operations=["disclose_external"])],
    )
    consent = _consent(
        rig,
        consent_lineage,
        rig.person_id,
        purposes=["research"],
        operations=["disclose_external"],
        classes=["document_content"],
    )
    _admit(rig, concept, policy, consent)
    return concept, policy_lineage, policy


def _egress_kwargs(rig, concept_id, **overrides):
    kwargs = {
        "operation": "disclose_external",
        "purpose": "research",
        "requester": rig.runtime_id,
        "runtime": rig.runtime_id,
        "destination": "partner",
        "recipient": rig.person_id,
        "object_ids": [concept_id],
    }
    kwargs.update(overrides)
    return kwargs


# ---------------------------------------------------------------------------
# The egress boundary (spec 9.4, 9.7)
# ---------------------------------------------------------------------------


def test_egress_allow_issues_consumable_capability(rig, engine, governed_concept):
    concept, _lineage, _policy = governed_concept
    result, capability = engine.authorize_egress(**_egress_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "allow"
    assert result.from_cache is False  # consequential egress is always fresh
    assert capability is not None
    assert capability["object_ids"] == [concept["id"]]
    assert capability["destination"] == "partner"
    assert capability["remaining_uses"] == 1
    assert capability["generation_vector"] == result.decision["generation_vector"]

    consumption = engine.consume_egress_capability(capability["capability_id"])
    assert consumption["capability_id"] == capability["capability_id"]
    assert consumption["remaining_uses"] == 0
    # Single use: the capability is now exhausted.
    with pytest.raises(CapabilityError, match="exhausted"):
        engine.consume_egress_capability(capability["capability_id"])


def test_egress_deny_issues_no_capability(rig, engine):
    concept = _concept(rig)  # root default deny
    _admit(rig, concept)
    result, capability = engine.authorize_egress(**_egress_kwargs(rig, concept["id"]))
    assert result.decision["decision"] == "deny"
    assert capability is None


def test_stale_capability_fails_at_the_boundary(rig, engine, governed_concept):
    concept, policy_lineage, policy = governed_concept
    _result, capability = engine.authorize_egress(**_egress_kwargs(rig, concept["id"]))
    assert capability is not None

    # A governance mutation lands after issuance: the fence advance
    # invalidates the capability even though nothing consumed it.
    tightened = _policy(
        rig,
        policy_lineage,
        [_rule("deny-disclose", "deny", operations=["disclose_external"])],
        previous=policy["id"],
        transition="supersede",
    )
    _admit(rig, tightened)
    with pytest.raises(CapabilityError, match="generation-stale"):
        engine.consume_egress_capability(capability["capability_id"])


def test_expired_capability_fails(rig, engine, governed_concept):
    concept, _lineage, _policy = governed_concept
    # One-millisecond TTL: the rig clock advances one second per tick, so
    # consumption happens strictly after expiry.
    _result, capability = engine.authorize_egress(
        **_egress_kwargs(rig, concept["id"], ttl_ms=1)
    )
    with pytest.raises(CapabilityError, match="expired"):
        engine.consume_egress_capability(capability["capability_id"])


def test_unknown_capability_fails_closed(rig, engine):
    with pytest.raises(CapabilityError, match="unknown egress capability"):
        engine.consume_egress_capability("cap-does-not-exist")


def test_egress_requires_consequential_operation(rig, engine, governed_concept):
    concept, _lineage, _policy = governed_concept
    with pytest.raises(GovernanceError, match="not a consequential"):
        engine.authorize_egress(**_egress_kwargs(rig, concept["id"], operation="read_local"))


def test_egress_requires_nonlocal_destination(rig, engine, governed_concept):
    concept, _lineage, _policy = governed_concept
    with pytest.raises(GovernanceError, match="non-local"):
        engine.authorize_egress(
            **_egress_kwargs(rig, concept["id"], destination=LOCAL_DESTINATION)
        )


# ---------------------------------------------------------------------------
# Consequential receipts (spec 9.8)
# ---------------------------------------------------------------------------


def test_consequential_receipt_is_admitted(rig, engine, governed_concept):
    concept, _lineage, _policy = governed_concept
    result, capability = engine.authorize_egress(**_egress_kwargs(rig, concept["id"]))
    consumption = engine.consume_egress_capability(capability["capability_id"])

    receipt_id = engine.record_consequential_receipt(
        context=result.context,
        capability_id=capability["capability_id"],
        consumption=consumption,
        summary="disclosed concept to partner",
    )
    receipt = rig.archive.get_object(receipt_id)
    assert receipt is not None
    semantic = receipt["compartments"]["semantic"]["envelope"]["content"]
    assert semantic["payload"]["status"] == "completed"
    details = semantic["payload"]["details"]
    assert details["capability_id"] == capability["capability_id"]
    assert details["decision_context_hash"] == consumption["decision_context_hash"]
    assert semantic["payload"]["evidence_refs"] == [concept["id"]]
    # The receipt is a canonical object: admitted and chained.
    assert receipt["admission"]["commit_sequence"] is not None
    rig.archive.verify_chain()


# ---------------------------------------------------------------------------
# Local reads are local (spec 9.4)
# ---------------------------------------------------------------------------


def test_local_read_path_never_touches_network(rig, engine, governed_concept, monkeypatch):
    import http.client
    import urllib.request

    def _blocked(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("governance layer attempted a network call")

    monkeypatch.setattr(http.client.HTTPConnection, "connect", _blocked)
    monkeypatch.setattr(urllib.request, "urlopen", _blocked)

    concept, _lineage, _policy = governed_concept
    result = engine.authorize(
        operation="read_local",
        purpose="research",
        requester=rig.runtime_id,
        runtime=rig.runtime_id,
        destination=LOCAL_DESTINATION,
        object_ids=[concept["id"]],
    )
    assert result.decision["decision"] in ("allow", "deny")
