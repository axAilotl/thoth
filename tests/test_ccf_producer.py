"""Producer-path tests (checklist phase 2, spec sections 2.1, 4.7, 6.2-6.3).

Covers: explicit key storage failing closed, the signed producer-batch chain
(sequence, previous hash, catalog root, credential, signature), the durable
spool surviving a producer restart, provisional local objects never being
represented as canonical, and idempotent replay of spooled batches.
"""

from __future__ import annotations

import os

import pytest

from ccf.credentials import DeviceCredential
from ccf.db import resolve_ccf_postgres_settings
from ccf.hashing import (
    decode_b64url,
    producer_batch_hash,
    producer_batch_signing_digest,
    verify_digest,
)
from ccf.ids import generate_id
from ccf.keys import CcfKeyError, generate_signing_key, load_signing_key, public_key_from_b64url
from ccf.producer import ProducerError

from ccf_helpers import make_clock, make_rig


# ---------------------------------------------------------------------------
# Key storage (no DB required)
# ---------------------------------------------------------------------------


def test_generate_and_load_signing_key_roundtrip(tmp_path):
    path = tmp_path / "device.pem"
    key = generate_signing_key(path)
    loaded = load_signing_key(path)
    assert loaded.public_key().public_bytes_raw() == key.public_key().public_bytes_raw()
    assert oct(os.stat(path).st_mode & 0o777) == "0o600"


def test_load_signing_key_fails_closed_when_missing(tmp_path):
    with pytest.raises(CcfKeyError, match="not found"):
        load_signing_key(tmp_path / "absent.pem")


def test_generate_signing_key_refuses_overwrite(tmp_path):
    path = tmp_path / "device.pem"
    generate_signing_key(path)
    with pytest.raises(CcfKeyError, match="refusing to overwrite"):
        generate_signing_key(path)


def test_load_signing_key_rejects_group_readable(tmp_path):
    path = tmp_path / "device.pem"
    generate_signing_key(path)
    os.chmod(path, 0o640)
    with pytest.raises(CcfKeyError, match="chmod 600"):
        load_signing_key(path)


def test_load_signing_key_rejects_non_ed25519(tmp_path):
    path = tmp_path / "not-a-key.pem"
    path.write_text("-----BEGIN PRIVATE KEY-----\nbogus\n-----END PRIVATE KEY-----\n")
    os.chmod(path, 0o600)
    with pytest.raises(CcfKeyError):
        load_signing_key(path)


# ---------------------------------------------------------------------------
# Settings resolution (fail closed, no silent fallbacks)
# ---------------------------------------------------------------------------


def test_settings_disabled_by_default():
    settings = resolve_ccf_postgres_settings({}, environ={})
    assert settings.enabled is False
    assert settings.dsn is None


def test_settings_enabled_requires_dsn():
    with pytest.raises(Exception, match="THOTH_CCF_POSTGRES_DSN"):
        resolve_ccf_postgres_settings(
            {"database.ccf_archive": {"enabled": True}}, environ={}
        )


def test_settings_resolve_key_paths_from_env():
    settings = resolve_ccf_postgres_settings(
        {"database.ccf_archive": {"enabled": True}},
        environ={
            "THOTH_CCF_POSTGRES_DSN": "postgresql://example/db",
            "THOTH_CCF_DEVICE_KEY": "/keys/device.pem",
            "THOTH_CCF_ARCHIVE_KEY": "/keys/archive.pem",
        },
    )
    assert settings.device_key_path == "/keys/device.pem"
    assert settings.archive_key_path == "/keys/archive.pem"


# ---------------------------------------------------------------------------
# Signed batch chain + durable spool (DB)
# ---------------------------------------------------------------------------


def _simple_record(rig, **overrides):
    payload = {
        "kind": "human",
        "display_name": "x",
        "aliases": [],
        "identity_anchors": [],
        "extensions": {},
    }
    return rig.producer.new_record(
        type="core.person",
        claims=rig.claims(),
        payload=payload,
        **overrides,
    )


def test_batch_chain_links_sequence_and_previous_hash(ccf_settings, tmp_path, ccf_package_root):
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    batch1 = rig.producer.create_batch(records=[_simple_record(rig)])
    batch2 = rig.producer.create_batch(records=[_simple_record(rig)])

    assert batch1["producer_sequence"] == "1"
    assert batch1["previous_batch_hash"] is None
    assert batch2["producer_sequence"] == "2"
    assert batch2["previous_batch_hash"] == batch1["batch_hash"]
    assert batch2["batch_id"] != batch1["batch_id"]
    assert batch1["semantic_catalog_root"] == rig.producer.catalog.root

    # Signature verifies against the device credential's public key.
    for batch in (batch1, batch2):
        assert producer_batch_hash(batch) == batch["batch_hash"]
        verify_digest(
            public_key_from_b64url(rig.credential.public_key_b64url),
            decode_b64url(batch["signature"]),
            producer_batch_signing_digest(batch["batch_hash"]),
        )


def test_empty_batch_refused(ccf_settings, tmp_path, ccf_package_root):
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    with pytest.raises(ProducerError, match="empty"):
        rig.producer.create_batch()


def test_spool_survives_producer_restart(ccf_settings, tmp_path, ccf_package_root):
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    batch1 = rig.producer.create_batch(records=[_simple_record(rig)])

    # Simulate a restart: a brand-new Producer over the same store must
    # continue the chain and still see the pending batch.
    from ccf.catalog import SemanticCatalog
    from ccf.registry import PinnedRegistries
    from ccf.schemas import SchemaSet

    catalog = SemanticCatalog.load(ccf_package_root)
    restarted = type(rig.producer)(
        settings=ccf_settings,
        producer_id=rig.runtime_id,
        credential=rig.credential,
        catalog=catalog,
        registries=PinnedRegistries.load(ccf_package_root, catalog),
        schemas=SchemaSet.load(ccf_package_root),
        clock=make_clock("2026-08-12T01:00:00.000Z"),
    )
    pending = restarted.pending_batches()
    assert [b["batch_id"] for b in pending] == [batch1["batch_id"]]
    batch2 = restarted.create_batch(records=[_simple_record(rig)])
    assert batch2["producer_sequence"] == "2"
    assert batch2["previous_batch_hash"] == batch1["batch_hash"]


def test_provisional_objects_are_never_canonical(ccf_settings, tmp_path, ccf_package_root):
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    record = _simple_record(rig)
    batch = rig.producer.create_batch(records=[record])

    provisional = rig.producer.provisional_objects()
    assert len(provisional) == 1
    entry = provisional[0]
    assert entry["object_id"] == record["id"]
    assert entry["status"] == "provisional"
    assert entry["submission_hash"].startswith("sha256:")
    # No admission order exists yet, and the archive does not know the ID.
    assert "commit_sequence" not in entry
    assert rig.archive.get_object(record["id"]) is None

    result = rig.archive.admit_batch(batch)
    assert result["status"] == "accepted"
    assert rig.producer.provisional_objects() == []
    admitted = rig.archive.get_object(record["id"])
    assert admitted["admission"]["commit_sequence"] == result["commit_sequence"]


def test_replay_of_spooled_batch_is_idempotent(ccf_settings, tmp_path, ccf_package_root):
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    record = _simple_record(rig)
    batch = rig.producer.create_batch(records=[record])

    first = rig.archive.admit_batch(batch)
    head_after_first = rig.archive.head()
    second = rig.archive.admit_batch(batch)
    assert first == second
    assert rig.archive.head() == head_after_first


def test_device_credential_load_fails_closed_without_key(tmp_path):
    with pytest.raises(CcfKeyError):
        DeviceCredential.load(
            tmp_path / "missing.pem",
            credential_id=generate_id("credential"),
            key_id=generate_id("key"),
        )
