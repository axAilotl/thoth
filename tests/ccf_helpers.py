"""Shared helpers for CCF producer/archive DB tests.

Builds a real archive (genesis + bootstrap Records) and a signed producer
against the ephemeral Postgres fixture, using freshly generated Ed25519
keys in pytest's tmp_path. Nothing here touches the vendored spec package.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ccf.archive import Archive
from ccf.credentials import DeviceCredential, device_credential_structural_payload
from ccf.ids import generate_id
from ccf.keys import generate_signing_key
from ccf.producer import Producer


def make_clock(start: str = "2026-08-12T00:00:00.000Z", step_ms: int = 1000):
    """Deterministic, strictly increasing canonical-timestamp clock."""
    current = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    )

    def _clock() -> str:
        nonlocal current
        stamp = current.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        current = current + timedelta(milliseconds=step_ms)
        return stamp

    return _clock


def make_keypair(path):
    """Generate an Ed25519 keypair at path; return the private key."""
    return generate_signing_key(path)


def privacy(classes=None, subjects=None):
    subjects = subjects or []
    return {
        "data_subjects": subjects,
        "data_classes": classes or [],
        "consent_refs": [],
        "legal_basis_refs": [],
        "subject_coverage": "complete" if subjects else "unknown",
    }


def authority(basis: str, asserted_by: str, accepted_by=None):
    return {"basis": basis, "asserted_by": asserted_by, "accepted_by": accepted_by}


def claims(person_id, runtime_id, *, classes=None, subjects=None, policy_hint=None):
    return {
        "person_id": person_id,
        "perspective_id": person_id,
        "privacy": privacy(classes, subjects),
        "authority": authority("runtime_import", runtime_id),
        "policy_hint": policy_hint,
        "extensions": {},
    }


class CcfTestRig:
    """Archive + producer + the bootstrap IDs tests need."""

    def __init__(self, *, settings, tmp_path, ccf_package_root, clock=None):
        self.clock = clock or make_clock()
        self.package_root = ccf_package_root
        self.settings = settings
        self.archive_key_path = tmp_path / "archive-ed25519.pem"
        self.device_key_path = tmp_path / "device-ed25519.pem"
        make_keypair(self.archive_key_path)
        make_keypair(self.device_key_path)

        self.archive = Archive.create(
            settings,
            package_root=ccf_package_root,
            archive_key_path=self.archive_key_path,
            clock=self.clock,
        )
        self.person_id = generate_id("record")
        self.runtime_id = generate_id("record")
        self.policy_lineage_id = generate_id("lineage")
        self.credential_lineage_id = generate_id("lineage")
        self.device_key_id = generate_id("key")
        self.archive_key_id = generate_id("key")
        self.credential_id = generate_id("credential")
        self.credential = DeviceCredential.load(
            self.device_key_path,
            credential_id=self.credential_id,
            key_id=self.device_key_id,
        )

        ts = self.clock()
        self.archive.admit_bootstrap(
            [
                {
                    "type": "governance.policy",
                    "object_id": generate_id("record"),
                    "recorded_by": self.runtime_id,
                    "recorded_at": ts,
                    "person_id": self.person_id,
                    "authority": authority(
                        "explicit_authorization", self.person_id, self.person_id
                    ),
                    "privacy": privacy(["identity_data"]),
                    "policy_hint": self.policy_lineage_id,
                    "lineage": {
                        "lineage_id": self.policy_lineage_id,
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
                    "object_id": self.person_id,
                    "recorded_by": self.runtime_id,
                    "recorded_at": ts,
                    "person_id": self.person_id,
                    "perspective_id": self.person_id,
                    "authority": authority(
                        "first_person_statement", self.person_id, self.person_id
                    ),
                    "privacy": privacy(
                        ["identity_data"],
                        [
                            {
                                "person_id": self.person_id,
                                "role": "archive_principal",
                                "identity_state_at_write": "verified",
                            }
                        ],
                    ),
                    "policy_hint": self.policy_lineage_id,
                    "payload": {
                        "kind": "human",
                        "display_name": "Test Person",
                        "aliases": [],
                        "identity_anchors": [],
                        "extensions": {},
                    },
                },
                {
                    "type": "core.runtime",
                    "object_id": self.runtime_id,
                    "recorded_by": self.runtime_id,
                    "recorded_at": ts,
                    "person_id": self.person_id,
                    "authority": authority("runtime_import", self.runtime_id),
                    "privacy": privacy(),
                    "policy_hint": self.policy_lineage_id,
                    "payload": {
                        "kind": "backend",
                        "name": "thoth-test",
                        "version": "0.0.0-test",
                        "instance_id": "thoth-test",
                        "capabilities": ["capture", "sync"],
                        "operator_id": self.person_id,
                        "extensions": {},
                    },
                },
                {
                    "type": "core.device_credential",
                    "object_id": generate_id("record"),
                    "recorded_by": self.runtime_id,
                    "recorded_at": ts,
                    "authority": authority(
                        "explicit_authorization", self.person_id, self.person_id
                    ),
                    "privacy": privacy(),
                    "policy_hint": self.policy_lineage_id,
                    "semantic": False,
                    "structural_payload": device_credential_structural_payload(
                        self.credential,
                        subject_id=self.runtime_id,
                        issuer_key_id=self.archive_key_id,
                        scopes=["capture", "sync", "derive"],
                        valid_from=ts,
                    ),
                    "lineage": {
                        "lineage_id": self.credential_lineage_id,
                        "previous_head_id": None,
                        "transition": "issue",
                        "valid_from": ts,
                        "expires_at": None,
                    },
                    "payload": {},
                },
            ]
        )
        self.policy_head_id = self.archive.head()  # informational

        from ccf.catalog import SemanticCatalog
        from ccf.registry import PinnedRegistries
        from ccf.schemas import SchemaSet

        catalog = SemanticCatalog.load(ccf_package_root)
        self.producer = Producer(
            settings=settings,
            producer_id=self.runtime_id,
            credential=self.credential,
            catalog=catalog,
            registries=PinnedRegistries.load(ccf_package_root, catalog),
            schemas=SchemaSet.load(ccf_package_root),
            clock=self.clock,
        )

    def claims(self, **kwargs):
        return claims(self.person_id, self.runtime_id, policy_hint=self.policy_lineage_id, **kwargs)


def make_rig(settings, tmp_path, ccf_package_root, clock=None) -> CcfTestRig:
    return CcfTestRig(
        settings=settings, tmp_path=tmp_path, ccf_package_root=ccf_package_root, clock=clock
    )


def add_producer(rig: CcfTestRig, tmp_path, name: str) -> None:
    """Admit a second device credential and attach its Producer to the rig.

    Each producer owns an independent signed-batch chain, which is what
    makes cross-producer admission races order-independent.
    """
    from ccf.catalog import SemanticCatalog
    from ccf.registry import PinnedRegistries
    from ccf.schemas import SchemaSet

    key_path = tmp_path / f"device-{name}.pem"
    make_keypair(key_path)
    credential = DeviceCredential.load(
        key_path, credential_id=generate_id("credential"), key_id=generate_id("key")
    )
    runtime_id = generate_id("record")
    ts = rig.clock()
    rig.archive.admit_bootstrap(
        [
            {
                "type": "core.runtime",
                "object_id": runtime_id,
                "recorded_by": rig.runtime_id,
                "recorded_at": ts,
                "person_id": rig.person_id,
                "authority": authority("runtime_import", rig.runtime_id),
                "privacy": privacy(),
                "policy_hint": rig.policy_lineage_id,
                "payload": {
                    "kind": "backend",
                    "name": name,
                    "version": "0.0.0-test",
                    "instance_id": name,
                    "capabilities": ["capture", "sync"],
                    "operator_id": rig.person_id,
                    "extensions": {},
                },
            },
            {
                "type": "core.device_credential",
                "object_id": generate_id("record"),
                "recorded_by": rig.runtime_id,
                "recorded_at": ts,
                "authority": authority(
                    "explicit_authorization", rig.person_id, rig.person_id
                ),
                "privacy": privacy(),
                "policy_hint": rig.policy_lineage_id,
                "semantic": False,
                "structural_payload": device_credential_structural_payload(
                    credential,
                    subject_id=runtime_id,
                    issuer_key_id=rig.archive_key_id,
                    scopes=["capture", "sync"],
                    valid_from=ts,
                ),
                "lineage": {
                    "lineage_id": generate_id("lineage"),
                    "previous_head_id": None,
                    "transition": "issue",
                    "valid_from": ts,
                    "expires_at": None,
                },
                "payload": {},
            },
        ]
    )
    catalog = SemanticCatalog.load(rig.package_root)
    return Producer(
        settings=rig.settings,
        producer_id=runtime_id,
        credential=credential,
        catalog=catalog,
        registries=PinnedRegistries.load(rig.package_root, catalog),
        schemas=SchemaSet.load(rig.package_root),
        clock=rig.clock,
    )
