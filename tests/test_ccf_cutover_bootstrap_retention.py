"""Bootstrap-compartment retention cutover gate."""

from __future__ import annotations

import pytest

from ccf.db import open_ccf_connection
from ccf.ids import generate_id
from ccf_helpers import authority
from ccf_cutover_test_support import (
    drop_all_projections,
    make_cutover_rig,
    reprovision_projection_tables,
)


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_cutover_rig(ccf_settings, tmp_path, ccf_package_root)


# Gate 5b: bootstrap compartment retention
# ---------------------------------------------------------------------------


def test_gate5b_bootstrap_compartments_survive_projection_destruction(
    rig, ccf_package_root
):
    # A fifth bootstrap class beyond the rig's four: an operator-admitted
    # core.source (bootstrap compartment coverage: policy, person,
    # runtime, credential, source).
    ts = rig.clock()
    source_id = generate_id("record")
    rig.archive.admit_bootstrap(
        [
            {
                "type": "core.source",
                "object_id": source_id,
                "recorded_by": rig.runtime_id,
                "recorded_at": ts,
                "person_id": rig.person_id,
                "authority": authority("runtime_import", rig.runtime_id),
                "privacy": {
                    "data_subjects": [],
                    "data_classes": [],
                    "consent_refs": [],
                    "legal_basis_refs": [],
                    "subject_coverage": "unknown",
                },
                "policy_hint": rig.policy_lineage_id,
                "payload": {
                    "kind": "obsidian_vault",
                    "name": "gate5b vault",
                    "connector": "ccf.obsidian",
                    "native_identity": "gate5b",
                    "trust_class": "trusted",
                    "producer_key_id": None,
                    "extensions": {},
                },
            }
        ]
    )

    bootstrap_ids = {
        "policy": None,  # resolved below from the policy lineage head
        "person": rig.person_id,
        "runtime": rig.runtime_id,
        "credential": None,
        "source": source_id,
    }
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT head_record_id FROM lineage_head WHERE lineage_id = %s",
            (rig.policy_lineage_id,),
        ).fetchone()
        bootstrap_ids["policy"] = row[0]
        row = conn.execute(
            "SELECT head_record_id FROM lineage_head WHERE lineage_id = %s",
            (rig.credential_lineage_id,),
        ).fetchone()
        bootstrap_ids["credential"] = row[0]

    def snapshot() -> dict:
        snap = {}
        for label, object_id in bootstrap_ids.items():
            obj = rig.archive.get_object(object_id)
            assert obj is not None, label
            snap[label] = {
                compartment: obj["compartments"].get(compartment, {}).get(
                    "envelope"
                )
                for compartment in ("structural", "semantic")
            }
            if label == "credential":
                # The device credential is admitted semantic=False:
                # structural-only by design.
                assert snap[label]["semantic"] is None
                assert snap[label]["structural"] is not None
            else:
                # Every other bootstrap class carries a semantic compartment.
                assert snap[label]["semantic"] is not None, label
        return snap

    before = snapshot()
    before_head = rig.archive.head()

    # Destroy projections, rebuild, and reload the archive from scratch.
    drop_all_projections(rig.settings)
    reprovision_projection_tables(rig.settings)
    rig.archive.projections.rebuild_all()

    from ccf.archive import Archive

    reloaded = Archive.open(
        rig.settings,
        package_root=ccf_package_root,
        archive_key_path=rig.archive_key_path,
    )
    assert reloaded.archive_id == rig.archive.archive_id
    assert reloaded.head() == before_head
    assert reloaded.verify_chain()["commits_verified"] >= 3

    after = snapshot()
    assert after == before
    # Spot-check one semantic payload per class for semantic retention.
    assert after["person"]["semantic"]["content"]["payload"]["kind"] == "human"
    assert after["runtime"]["semantic"]["content"]["payload"]["kind"] == "backend"
    assert after["source"]["semantic"]["content"]["payload"]["kind"] == (
        "obsidian_vault"
    )
    assert (
        after["policy"]["semantic"]["content"]["payload"]["profile"]
        == "ccf.policy/0.1.1"
    )
    credential_structural = after["credential"]["structural"]["content"]
    assert credential_structural["type"] == "core.device_credential"
