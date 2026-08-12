"""thothmap source mapping tests (checklist 4: capture source -> core.source)."""

from __future__ import annotations

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.context import ThothMapError
from ccf.thothmap.sources import source_submission

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


CAPTURE_SOURCE = {
    "source_id": "3f6c2e64-5c8a-4e0c-9c6f-0f6b1a2d3e4f",
    "source_name": "omi",
    "source_type": "wearable_audio",
    "collector": "thoth.capture",
    "account": None,
    "native_source_id": "device:maxc-test",
    "base_uri": None,
    "status": "active",
}


def test_source_maps_to_core_source(rig, ctx):
    mapped = source_submission(rig.producer, ctx, CAPTURE_SOURCE, trust_class="authenticated")
    record = mapped.records[0]
    assert record["type"] == "core.source"
    # The source is the origin root: no origin tuple of its own.
    assert "origin" not in record
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.1:payload.core.source", record["payload"], what="core.source"
    )
    assert record["payload"]["native_identity"] == "device:maxc-test"
    assert record["payload"]["trust_class"] == "authenticated"

    result = admit_mapped(rig, mapped)
    assert result["status"] == "committed"
    assert outcome_for(result, record["id"])["status"] == "admitted"
    admitted = compartment(rig, record["id"], "semantic")
    assert admitted["payload"]["connector"] == "thoth.capture"


def test_source_reimport_is_idempotent(rig, ctx):
    mapped = source_submission(rig.producer, ctx, CAPTURE_SOURCE)
    first = admit_mapped(rig, mapped)
    assert outcome_for(first, mapped.records[0]["id"])["status"] == "admitted"
    # Replay of the same submissions in a new batch resolves to existing.
    second = admit_mapped(rig, mapped)
    assert second["status"] == "committed"
    assert outcome_for(second, mapped.records[0]["id"])["status"] == "existing"


def test_source_fails_closed_on_bad_trust_class(rig, ctx):
    with pytest.raises(ThothMapError, match="trust_class"):
        source_submission(rig.producer, ctx, CAPTURE_SOURCE, trust_class="friendly")


def test_source_requires_source_name(rig, ctx):
    with pytest.raises(ThothMapError, match="source_name"):
        source_submission(rig.producer, ctx, {"source_type": "web"})
