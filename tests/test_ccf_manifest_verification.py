"""Manifest cross-check conformance tests (CCF 0.1.2, spec 11.5).

``manifest.json`` is an unsigned, non-authoritative transport index.
These tests tamper one valid mindpack per case and assert that the
verifier independently derives the pack inventory from digest-verified
contents and fails closed — with a stable ``manifest_*`` reason — before
any destination state is created or modified.
"""

from __future__ import annotations

import json
import copy
import uuid
from dataclasses import replace

import pytest

from ccf.db import CcfPostgresSettings
from ccf.sync.manifest import DerivedInventory
from ccf.sync.packio import PackError
from ccf.sync.restore import restore_mindpack, verify_mindpack
from ccf.sync.verify import PackVerificationError

from ccf_helpers import authority, make_rig
from test_ccf_mindpack import _populate, _remove_object_from_pack


def test_manifest_tamper_vector_coverage(ccf_vectors_dir):
    """Pin every published final manifest mutation to this rejection suite."""
    path = ccf_vectors_dir / "mindpack-manifest-tamper.json"
    vector = json.loads(path.read_text(encoding="utf-8"))
    assert vector["format"] == "ccf.manifest-tamper-vectors/0.1.2"
    assert {case["id"] for case in vector["cases"]} == {
        "forged-low-count",
        "forged-high-count",
        "removed-stream-entry",
        "extra-unlisted-stream",
        "available-changed-to-erased",
        "erased-changed-to-available",
        "missing-dependency",
        "fabricated-dependency",
        "changed-genesis",
        "changed-head",
        "changed-archive-id",
        "changed-catalog-root",
        "complete-changed-to-partial",
        "restore-changed-to-foreign-merge",
        "duplicate-availability-row",
        "contradictory-availability-row",
        "absent-optional-stream",
        "changed-dependency-metadata",
        "fabricated-custody-proof",
    }
    assert {case["expected"] for case in vector["cases"]} == {"reject"}


def test_withheld_inventory_is_partial_and_not_restore_capable():
    inventory = DerivedInventory(
        counts={"records": 1, "links": 0, "blobs": 0, "commits": 1},
        unavailable_ids={"urn:ccf:record:00000000-0000-4000-8000-000000000001"},
        withheld={"urn:ccf:record:00000000-0000-4000-8000-000000000001"},
        erased=set(),
    )
    assert inventory.custody_complete is False
    assert inventory.restore_capable is False


@pytest.fixture()
def settings_factory(ccf_postgres_dsn):
    """Factory for extra store schemas in one test (restores, merges)."""
    import psycopg

    made: list[str] = []

    def _make() -> CcfPostgresSettings:
        schema = f"ccf_test_{uuid.uuid4().hex[:12]}"
        made.append(schema)
        return CcfPostgresSettings(enabled=True, dsn=ccf_postgres_dsn, schema=schema)

    yield _make
    with psycopg.connect(ccf_postgres_dsn, autocommit=True) as conn:
        for schema in made:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def pack(rig, tmp_path):
    """One valid exported mindpack directory plus its manifest."""
    ids = _populate(rig)
    pack_dir = tmp_path / "pack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    return pack_dir, manifest, ids


@pytest.fixture()
def erased_pack(settings_factory, tmp_path, ccf_package_root):
    """A valid mindpack carrying one object with an erased semantic."""
    from ccf.erasure.suppression import generate_suppression_key

    key_path = generate_suppression_key(tmp_path / "suppression.key")
    rig_dir = tmp_path / "a"
    rig_dir.mkdir()
    rig_a = make_rig(
        replace(settings_factory(), suppression_key_path=str(key_path)),
        rig_dir,
        ccf_package_root,
    )
    ids = _populate(rig_a)
    svc = rig_a.archive.erasure()
    targets = [{"object_id": ids["session"], "compartments": ["semantic"]}]
    request = svc.submit_request(
        requester_id=rig_a.person_id,
        subject_id=rig_a.person_id,
        requested_scope={"targets": targets},
        reason="manifest conformance erasure",
        authority=authority("first_person_statement", rig_a.person_id, rig_a.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=targets,
        reasoning="approved",
        decided_by=rig_a.person_id,
        authority=authority("explicit_authorization", rig_a.person_id, rig_a.person_id),
        authorized_producers=[rig_a.producer.producer_id],
    )
    assert svc.execute(decided["operation_id"])["stage"] == "receipt"
    pack_dir = tmp_path / "erased-pack"
    manifest = rig_a.archive.sync().export_mindpack(pack_dir)
    assert ids["session"] in manifest["erased"]
    return pack_dir, manifest, ids


def _tamper(pack_dir, fn):
    """Rewrite manifest.json in place after applying ``fn`` to it."""
    path = pack_dir / "manifest.json"
    manifest = json.loads(path.read_text())
    fn(manifest)
    path.write_text(json.dumps(manifest, indent=2) + "\n")


def _rewrite_operational_stream(pack_dir, relative_path, mutate, *, ndjson=False):
    """Mutate a stream and update only its unsigned manifest digest claim."""
    from ccf.hashing import digest_string

    path = pack_dir / relative_path
    if ndjson:
        document = [json.loads(line) for line in path.read_text().splitlines()]
        mutate(document)
        data = ("\n".join(json.dumps(row) for row in document) + "\n").encode()
    else:
        document = json.loads(path.read_text())
        mutate(document)
        data = (json.dumps(document, indent=2) + "\n").encode()
    path.write_bytes(data)

    def edit(manifest):
        stream = next(
            row for row in manifest["streams"] if row["path"] == relative_path
        )
        stream["digest"] = digest_string(data)
        stream["byte_length"] = str(len(data))

    _tamper(pack_dir, edit)


def _assert_store_untouched(ccf_postgres_dsn, settings):
    """The destination schema was never even migrated (no DB mutation)."""
    import psycopg

    with psycopg.connect(ccf_postgres_dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT to_regclass(%s)", (f"{settings.schema}.archive",)
        ).fetchone()
    assert row[0] is None


def _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, *, reason, allow_partial=False):
    settings = settings_factory()
    with pytest.raises(PackError) as excinfo:
        restore_mindpack(
            settings,
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
            allow_partial=allow_partial,
        )
    assert excinfo.value.reason == reason, excinfo.value
    _assert_store_untouched(ccf_postgres_dsn, settings)
    return excinfo.value


# ---------------------------------------------------------------------------
# 1. Object and commit counts: increase and decrease both fail closed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["records", "links", "blobs", "commits"])
@pytest.mark.parametrize("delta", [1, -1])
def test_manifest_count_mismatch(pack, settings_factory, ccf_package_root,
                                 ccf_postgres_dsn, field, delta):
    pack_dir, manifest, _ = pack

    def edit(m):
        m["counts"][field] = str(int(m["counts"][field]) + delta)

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_count_mismatch")


# ---------------------------------------------------------------------------
# 2/3. Availability state flips.
# ---------------------------------------------------------------------------


def test_available_compartment_marked_erased(pack, settings_factory,
                                             ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, ids = pack

    def edit(m):
        entry = next(
            e for e in m["compartment_availability"]
            if e["object_id"] == ids["session"] and e["compartment"] == "semantic"
        )
        assert entry["availability"] == "available"
        entry["availability"] = "erased"
        entry["source_custody_proof"] = "commit:0:0"
        entry["unavailability_lineage_id"] = ids["session"]  # valid record URN

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


def test_erased_compartment_marked_available(erased_pack, settings_factory,
                                             ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, ids = erased_pack

    def edit(m):
        entry = next(
            e for e in m["compartment_availability"]
            if e["object_id"] == ids["session"] and e["compartment"] == "semantic"
        )
        assert entry["availability"] == "erased"
        entry["availability"] = "available"
        entry["source_custody_proof"] = None
        entry["unavailability_lineage_id"] = None

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


def test_object_level_erased_marked_available(erased_pack, settings_factory,
                                              ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, ids = erased_pack
    _tamper(pack_dir, lambda m: m["erased"].remove(ids["session"]))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


def test_available_object_marked_erased(pack, settings_factory, ccf_package_root,
                                        ccf_postgres_dsn):
    pack_dir, manifest, ids = pack
    _tamper(pack_dir, lambda m: m["erased"].append(ids["session"]))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


# ---------------------------------------------------------------------------
# 4. Removing a real erased availability entry fails closed.
# ---------------------------------------------------------------------------


def test_erased_availability_entry_removed(erased_pack, settings_factory,
                                           ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, ids = erased_pack

    def edit(m):
        m["compartment_availability"] = [
            e for e in m["compartment_availability"]
            if not (e["object_id"] == ids["session"]
                    and e["compartment"] == "semantic")
        ]

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


# ---------------------------------------------------------------------------
# 11. Duplicate / contradictory availability entries.
# ---------------------------------------------------------------------------


def test_duplicate_availability_entry(pack, settings_factory, ccf_package_root,
                                      ccf_postgres_dsn):
    pack_dir, manifest, _ = pack

    def edit(m):
        m["compartment_availability"].append(
            dict(m["compartment_availability"][0])
        )

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


def test_contradictory_availability_entry(pack, settings_factory,
                                          ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, _ = pack

    def edit(m):
        entry = dict(m["compartment_availability"][0])
        entry["availability"] = "erased"
        entry["source_custody_proof"] = "commit:0:0"
        entry["unavailability_lineage_id"] = (
            "urn:ccf:record:00000000-0000-4000-8000-000000000000"
        )
        m["compartment_availability"].append(entry)

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_availability_mismatch")


# ---------------------------------------------------------------------------
# 5/6. External dependencies must be real, unresolved references.
# ---------------------------------------------------------------------------


def test_phantom_external_dependency(pack, settings_factory, ccf_package_root,
                                     ccf_postgres_dsn):
    pack_dir, manifest, _ = pack

    def edit(m):
        m["external_dependencies"].append(
            {"object_id": f"urn:ccf:record:{uuid.uuid4()}",
             "reason": "manifest-only dependency"}
        )

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_external_dependency_mismatch")


def test_external_dependency_for_included_object(pack, settings_factory,
                                                 ccf_package_root,
                                                 ccf_postgres_dsn):
    """Claiming an included object as external contradicts the contents."""
    pack_dir, manifest, ids = pack

    def edit(m):
        m["external_dependencies"].append(
            {"object_id": ids["source"], "reason": "unresolved_reference"}
        )

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_external_dependency_mismatch")


def _partial_pack_with_external_dep(rig, tmp_path, ccf_package_root):
    """A pack missing one referenced object, honestly declared external."""
    ids = _populate(rig)
    pack_dir = tmp_path / "partial"
    rig.archive.sync().export_mindpack(pack_dir)
    _remove_object_from_pack(pack_dir, ids["source"], declare_partial=True)
    return pack_dir, ids


def test_real_external_dependency_accepted(rig, tmp_path, ccf_package_root):
    """Control: a declared dependency for a genuine unresolved reference."""
    pack_dir, ids = _partial_pack_with_external_dep(rig, tmp_path,
                                                    ccf_package_root)
    pack = verify_mindpack(
        pack_dir,
        package_root=ccf_package_root,
        allow_partial=True,
        allow_missing_member_objects=True,
    )
    assert ids["source"] in pack.completeness.external
    assert pack.completeness.complete
    assert pack.custody_complete is False
    assert pack.restore_capable is False


def test_real_external_dependency_removed(rig, tmp_path, ccf_package_root):
    """Removing the declaration leaves an undeclared dangling reference."""
    pack_dir, ids = _partial_pack_with_external_dep(rig, tmp_path,
                                                    ccf_package_root)
    _tamper(pack_dir, lambda m: m["external_dependencies"].clear())
    with pytest.raises(PackError) as excinfo:
        verify_mindpack(
            pack_dir,
            package_root=ccf_package_root,
            allow_missing_member_objects=True,
        )
    assert ids["source"] in str(excinfo.value)


def test_real_external_dependency_removed_even_for_partial_import(
    rig, tmp_path, ccf_package_root
):
    """Explicit partial custody does not excuse a missing dependency row."""
    pack_dir, ids = _partial_pack_with_external_dep(
        rig, tmp_path, ccf_package_root
    )
    _tamper(pack_dir, lambda m: m["external_dependencies"].clear())
    with pytest.raises(PackVerificationError) as excinfo:
        verify_mindpack(
            pack_dir,
            package_root=ccf_package_root,
            allow_partial=True,
            allow_missing_member_objects=True,
        )
    assert excinfo.value.reason == "manifest_external_dependency_mismatch"
    assert ids["source"] in str(excinfo.value)


@pytest.mark.parametrize(
    "edit_dependency",
    [
        lambda dep: dep.update(reason="attacker-selected"),
        lambda dep: dep.update(locator="https://attacker.invalid/object"),
    ],
)
def test_external_dependency_metadata_must_be_derived(
    rig, tmp_path, ccf_package_root, edit_dependency
):
    pack_dir, _ = _partial_pack_with_external_dep(
        rig, tmp_path, ccf_package_root
    )

    def edit(manifest):
        edit_dependency(manifest["external_dependencies"][0])

    _tamper(pack_dir, edit)
    with pytest.raises(PackVerificationError) as excinfo:
        verify_mindpack(
            pack_dir,
            package_root=ccf_package_root,
            allow_partial=True,
            allow_missing_member_objects=True,
        )
    assert excinfo.value.reason == "manifest_external_dependency_mismatch"


# ---------------------------------------------------------------------------
# 7. Completeness flips.
# ---------------------------------------------------------------------------


def test_completeness_underclaim_fails_without_partial_request(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    """A complete pack claimed partial fails a requested restore."""
    pack_dir, manifest, _ = pack

    def edit(m):
        m["custody"] = {"completeness": "partial", "restore_capable": False}

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_completeness_mismatch")


def test_completeness_underclaim_fails_even_with_explicit_partial(
    pack, ccf_package_root
):
    """Caller permission never excuses an unsigned custody mismatch."""
    pack_dir, manifest, _ = pack

    def edit(m):
        m["custody"] = {"completeness": "partial", "restore_capable": False}

    _tamper(pack_dir, edit)
    with pytest.raises(PackVerificationError) as excinfo:
        verify_mindpack(
            pack_dir,
            package_root=ccf_package_root,
            allow_partial=True,
            operation="restore",
        )
    assert excinfo.value.reason == "manifest_completeness_mismatch"


def test_completeness_overclaim_fails_restore(rig, tmp_path, ccf_package_root):
    """Claimed complete + verifiably partial fails a restore request."""
    pack_dir, _ = _partial_pack_with_external_dep(rig, tmp_path, ccf_package_root)
    _tamper(
        pack_dir,
        lambda m: m.update(
            custody={"completeness": "complete", "restore_capable": True}
        ),
    )
    with pytest.raises(PackVerificationError) as excinfo:
        verify_mindpack(
            pack_dir,
            package_root=ccf_package_root,
            allow_partial=True,
            allow_missing_member_objects=True,
            operation="restore",
        )
    assert excinfo.value.reason == "manifest_completeness_mismatch"


# ---------------------------------------------------------------------------
# 8. Mode consistency with the caller-requested operation.
# ---------------------------------------------------------------------------


def test_restore_rejects_foreign_merge_mode(pack, settings_factory,
                                            ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m.update(mode="foreign_merge"))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_mode_mismatch")


def test_import_rejects_delta_mode(rig, pack, ccf_package_root):
    pack_dir, _, _ = pack
    _tamper(pack_dir, lambda m: m.update(mode="delta"))
    head_before = rig.archive.head()
    with pytest.raises(PackVerificationError) as excinfo:
        rig.archive.sync().import_mindpack(pack_dir)
    assert excinfo.value.reason == "manifest_mode_mismatch"
    assert rig.archive.head() == head_before  # destination untouched


def test_same_identity_import_rejects_foreign_merge_mode(rig, pack,
                                                         ccf_package_root):
    pack_dir, _, _ = pack
    _tamper(pack_dir, lambda m: m.update(mode="foreign_merge"))
    with pytest.raises(PackVerificationError) as excinfo:
        rig.archive.sync().import_mindpack(pack_dir)
    assert excinfo.value.reason == "manifest_mode_mismatch"


def test_unsigned_archive_id_cannot_select_foreign_merge(rig, pack):
    pack_dir, _, _ = pack
    _tamper(
        pack_dir,
        lambda m: m.update(archive_id=f"urn:ccf:archive:{uuid.uuid4()}"),
    )
    head_before = rig.archive.head()
    with pytest.raises(PackVerificationError) as excinfo:
        rig.archive.sync().import_mindpack(pack_dir, allow_partial=True)
    assert excinfo.value.reason == "manifest_head_mismatch"
    assert rig.archive.head() == head_before


def test_foreign_merge_mode_consistent_with_merge(rig, pack, settings_factory,
                                                  tmp_path, ccf_package_root):
    """Control: mode=foreign_merge is consistent with a foreign merge."""
    pack_dir, _, _ = pack
    _tamper(pack_dir, lambda m: m.update(mode="foreign_merge"))
    b_dir = tmp_path / "b"
    b_dir.mkdir()
    rig_b = make_rig(settings_factory(), b_dir, ccf_package_root)
    report = rig_b.archive.sync().import_mindpack(pack_dir)
    assert report["status"] == "merged"


# ---------------------------------------------------------------------------
# 9. Head / genesis claims.
# ---------------------------------------------------------------------------


def test_manifest_head_hash_mismatch(pack, settings_factory, ccf_package_root,
                                     ccf_postgres_dsn):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m.update(head_commit_hash="sha256:" + "00" * 32))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_head_mismatch")


def test_manifest_head_sequence_mismatch(pack, settings_factory,
                                         ccf_package_root, ccf_postgres_dsn):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m.update(head_sequence="99"))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_head_mismatch")


def test_manifest_genesis_mismatch(pack, settings_factory, ccf_package_root,
                                   ccf_postgres_dsn):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir,
            lambda m: m.update(genesis_commit_hash="sha256:" + "00" * 32))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_head_mismatch")


@pytest.mark.parametrize(
    "field,value",
    [
        ("archive_id", f"urn:ccf:archive:{uuid.uuid4()}"),
        ("epoch_id", f"urn:ccf:lineage:{uuid.uuid4()}"),
        ("semantic_catalog_root", "sha256:" + "44" * 32),
    ],
)
def test_manifest_signed_chain_identity_mismatch(
    pack,
    settings_factory,
    ccf_package_root,
    ccf_postgres_dsn,
    field,
    value,
):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m.update({field: value}))
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_head_mismatch",
    )


def test_manifest_profiles_mismatch(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m["profiles"].reverse())
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_head_mismatch",
    )


def test_actual_semantic_catalog_must_reproduce_signed_root(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    from ccf.hashing import digest_string

    pack_dir, manifest, _ = pack
    catalog_path = pack_dir / "semantic-catalog.json"
    catalog = json.loads(catalog_path.read_text())
    catalog["version"] = "attacker-selected"
    data = (json.dumps(catalog, indent=2) + "\n").encode()
    catalog_path.write_bytes(data)

    def edit(claim):
        stream = next(
            entry
            for entry in claim["streams"]
            if entry["path"] == "semantic-catalog.json"
        )
        stream["digest"] = digest_string(data)
        stream["byte_length"] = str(len(data))

    _tamper(pack_dir, edit)
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_head_mismatch",
    )


def test_packaged_catalog_artifact_must_match_signed_catalog(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    """Unsigned stream digests cannot authorize substituted semantics."""
    from ccf.hashing import digest_string

    pack_dir, manifest, _ = pack
    artifact_path = pack_dir / "schemas" / "common" / "defs.schema.json"
    artifact = json.loads(artifact_path.read_text())
    artifact["title"] = "attacker-selected schema"
    data = (json.dumps(artifact, indent=2) + "\n").encode()
    artifact_path.write_bytes(data)

    def edit(claim):
        stream = next(
            entry
            for entry in claim["streams"]
            if entry["path"] == "schemas/common/defs.schema.json"
        )
        stream["digest"] = digest_string(data)
        stream["byte_length"] = str(len(data))

    _tamper(pack_dir, edit)
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_head_mismatch",
    )


def test_normal_object_cannot_be_relabelled_as_foreign_custody(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    pack_dir, manifest, ids = pack
    headers = {
        row["id"]: row
        for row in (
            json.loads(line)
            for line in (pack_dir / "objects" / "records.ndjson")
            .read_text()
            .splitlines()
        )
    }
    forged = f"urn:ccf:archive:{uuid.uuid4()}:{headers[ids['session']]['object_hash']}"
    _tamper(pack_dir, lambda m: m["foreign_custody_proofs"].append(forged))
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_head_mismatch",
    )


@pytest.mark.parametrize(
    "relative_path,ndjson,mutate",
    [
        (
            "archive.json",
            False,
            lambda row: row.update(signer_key_id=f"urn:ccf:key:{uuid.uuid4()}"),
        ),
        (
            "lineage-heads.ndjson",
            True,
            lambda rows: rows[0].update(head_record_hash="sha256:" + "66" * 32),
        ),
        (
            "origin-index.ndjson",
            True,
            lambda rows: rows[0].update(native_id="attacker-selected"),
        ),
        (
            "producer-heads.ndjson",
            True,
            lambda rows: rows[0].update(batch_hash="sha256:" + "77" * 32),
        ),
    ],
)
def test_unsigned_operational_stream_cannot_mutate_restored_state(
    pack,
    settings_factory,
    ccf_package_root,
    ccf_postgres_dsn,
    relative_path,
    ndjson,
    mutate,
):
    pack_dir, manifest, _ = pack
    _rewrite_operational_stream(
        pack_dir, relative_path, mutate, ndjson=ndjson
    )
    settings = settings_factory()
    with pytest.raises(PackVerificationError):
        restore_mindpack(
            settings,
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
        )
    _assert_store_untouched(ccf_postgres_dsn, settings)


def test_unsigned_producer_batch_stream_requires_valid_signature(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    pack_dir, manifest, _ = pack
    batch_path = next((pack_dir / "producer-batches").glob("*.json"))
    relative = str(batch_path.relative_to(pack_dir))
    _rewrite_operational_stream(
        pack_dir,
        relative,
        lambda batch: batch.update(created_at="2026-08-13T00:00:00.000Z"),
    )
    settings = settings_factory()
    with pytest.raises(PackVerificationError, match="not independently valid"):
        restore_mindpack(
            settings,
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
        )
    _assert_store_untouched(ccf_postgres_dsn, settings)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (
            lambda batch: batch.update(
                producer_id=f"urn:ccf:record:{uuid.uuid4()}"
            ),
            "credential subject does not match producer",
        ),
        (
            lambda batch: batch.update(created_at="2000-01-01T00:00:00.000Z"),
            "credential is not valid at batch creation time",
        ),
    ],
)
def test_portable_producer_credential_binding_and_validity(
    rig, tmp_path, ccf_package_root, mutate, match
):
    """A capture key cannot authorize another producer or invalid time."""
    from ccf.hashing import (
        encode_b64url,
        producer_batch_hash,
        producer_batch_signing_digest,
        sign_digest,
    )
    from ccf.schemas import SchemaSet
    from ccf.sync.operational import verify_producer_state

    _populate(rig)
    pack_dir = tmp_path / "credential-binding.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    verified = verify_mindpack(pack_dir, package_root=ccf_package_root)
    batch = copy.deepcopy(verified.batch_claims[0])
    mutate(batch)
    batch.pop("batch_hash", None)
    batch.pop("signature", None)
    batch["batch_hash"] = producer_batch_hash(batch)
    batch["signature"] = encode_b64url(
        sign_digest(
            rig.credential.private_key,
            producer_batch_signing_digest(batch["batch_hash"]),
        )
    )
    with pytest.raises(PackVerificationError, match=match):
        verify_producer_state(
            verified.objects,
            verified.members,
            [batch],
            [],
            catalog_root=verified.chain["semantic_catalog_root"],
            schemas=SchemaSet.load(ccf_package_root),
        )


# ---------------------------------------------------------------------------
# 10. Stream digest claims.
# ---------------------------------------------------------------------------


def test_stream_digest_mismatch(pack, settings_factory, ccf_package_root,
                                ccf_postgres_dsn):
    pack_dir, manifest, _ = pack

    def edit(m):
        entry = next(
            e for e in m["streams"] if e["path"] == "objects/records.ndjson"
        )
        entry["digest"] = "sha256:" + "11" * 32

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root,
                   ccf_postgres_dsn, manifest,
                   reason="manifest_stream_digest_mismatch")


def test_removed_stream_entry_fails(pack, settings_factory, ccf_package_root,
                                    ccf_postgres_dsn):
    """A real container member omitted from the unsigned index is rejected."""
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m["streams"].pop())
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_stream_digest_mismatch",
    )


def test_absent_optional_stream_claim_fails(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    """An optional claim cannot fabricate a container member."""
    pack_dir, manifest, _ = pack
    _tamper(
        pack_dir,
        lambda m: m["streams"].append(
            {
                "path": "blob-data/absent.bin",
                "digest": "sha256:" + "00" * 32,
                "byte_length": "0",
                "required": False,
            }
        ),
    )
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_stream_digest_mismatch",
    )


@pytest.mark.parametrize(
    "path,required",
    [
        ("objects/records.ndjson", False),
        pytest.param(None, True, id="blob-data-made-required"),
    ],
)
def test_stream_required_flag_is_independently_derived(
    pack,
    settings_factory,
    ccf_package_root,
    ccf_postgres_dsn,
    path,
    required,
):
    pack_dir, manifest, _ = pack

    def edit(claim):
        entry = next(
            stream
            for stream in claim["streams"]
            if stream["path"] == path
            or (path is None and stream["path"].startswith("blob-data/"))
        )
        entry["required"] = required

    _tamper(pack_dir, edit)
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_stream_digest_mismatch",
    )


def test_valid_unjournaled_object_is_rejected_before_manifest_comparison(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    """A self-consistent object stream cannot manufacture custody evidence."""
    from ccf.hashing import compartment_commitment, digest_string, object_hash

    pack_dir, manifest, ids = pack
    records_path = pack_dir / "objects" / "records.ndjson"
    headers = [json.loads(line) for line in records_path.read_text().splitlines()]
    source = next(header for header in headers if header["id"] == ids["session"])
    forged = copy.deepcopy(source)
    forged["id"] = f"urn:ccf:record:{uuid.uuid4()}"
    stem = forged["id"].removeprefix("urn:ccf:record:")
    old_stem = source["id"].removeprefix("urn:ccf:record:")
    old_structural = pack_dir / "compartments" / "records" / f"{old_stem}.structural.json"
    old_semantic = pack_dir / "compartments" / "records" / f"{old_stem}.semantic.json"
    structural = json.loads(old_structural.read_text())
    semantic = json.loads(old_semantic.read_text())
    forged["structural_commitment"] = compartment_commitment(
        "record", "structural", structural
    )
    forged["semantic_commitment"] = compartment_commitment(
        "record", "semantic", semantic
    )
    forged["object_hash"] = object_hash(forged)
    headers.append(forged)
    records_data = ("\n".join(json.dumps(header) for header in headers) + "\n").encode()
    records_path.write_bytes(records_data)
    new_structural = pack_dir / "compartments" / "records" / f"{stem}.structural.json"
    new_semantic = pack_dir / "compartments" / "records" / f"{stem}.semantic.json"
    new_structural.write_bytes(old_structural.read_bytes())
    new_semantic.write_bytes(old_semantic.read_bytes())

    def edit(claim):
        stream = next(
            entry for entry in claim["streams"]
            if entry["path"] == "objects/records.ndjson"
        )
        stream["digest"] = digest_string(records_data)
        stream["byte_length"] = str(len(records_data))
        for new_path in (new_structural, new_semantic):
            data = new_path.read_bytes()
            claim["streams"].append(
                {
                    "path": str(new_path.relative_to(pack_dir)),
                    "digest": digest_string(data),
                    "byte_length": str(len(data)),
                    "required": True,
                }
            )
        claim["counts"]["records"] = str(int(claim["counts"]["records"]) + 1)
        for compartment, commitment in (
            ("structural", forged["structural_commitment"]),
            ("semantic", forged["semantic_commitment"]),
        ):
            claim["compartment_availability"].append(
                {
                    "object_kind": "record",
                    "object_id": forged["id"],
                    "compartment": compartment,
                    "availability": "available",
                    "commitment": commitment,
                    "retention_profile": structural["content"]["retention_profile"],
                    "source_custody_proof": None,
                    "unavailability_lineage_id": None,
                }
            )

    _tamper(pack_dir, edit)
    settings = settings_factory()
    with pytest.raises(PackVerificationError, match="outside verified journal"):
        restore_mindpack(
            settings,
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
        )
    _assert_store_untouched(ccf_postgres_dsn, settings)


def test_restore_capability_flip_fails(pack, settings_factory, ccf_package_root,
                                       ccf_postgres_dsn):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m["custody"].update(restore_capable=False))
    _restore_fails(
        pack_dir,
        settings_factory,
        ccf_package_root,
        ccf_postgres_dsn,
        manifest,
        reason="manifest_completeness_mismatch",
    )


def test_unlisted_pack_content_fails(pack, settings_factory, ccf_package_root,
                                     ccf_postgres_dsn):
    """Pack bytes the manifest does not declare fail closed."""
    pack_dir, manifest, _ = pack
    (pack_dir / "smuggled.bin").write_bytes(b"undeclared")
    _restore_fails(pack_dir, settings_factory, ccf_package_root,
                   ccf_postgres_dsn, manifest,
                   reason="manifest_stream_digest_mismatch")


def test_duplicate_stream_entry_fails(pack, settings_factory, ccf_package_root,
                                      ccf_postgres_dsn):
    pack_dir, manifest, _ = pack

    def edit(m):
        m["streams"].append(dict(m["streams"][0]))

    _tamper(pack_dir, edit)
    _restore_fails(pack_dir, settings_factory, ccf_package_root,
                   ccf_postgres_dsn, manifest,
                   reason="manifest_stream_digest_mismatch")


# ---------------------------------------------------------------------------
# Unknown extensions.
# ---------------------------------------------------------------------------


def test_unknown_extension_fails(pack, settings_factory, ccf_package_root,
                                 ccf_postgres_dsn):
    pack_dir, manifest, _ = pack
    _tamper(pack_dir, lambda m: m["extensions"].update(unknown_claim={}))
    _restore_fails(pack_dir, settings_factory, ccf_package_root, ccf_postgres_dsn,
                   manifest, reason="manifest_unknown_extension_mismatch")


# ---------------------------------------------------------------------------
# A maliciously reduced count must not truncate verification.
# ---------------------------------------------------------------------------


def test_reduced_count_does_not_truncate_verification(
    pack, settings_factory, ccf_package_root, ccf_postgres_dsn
):
    """counts.records = 0 plus a tampered compartment deep in the pack:
    verification still reaches and rejects the tampered object instead of
    stopping at the manifest's count claim."""
    from ccf.hashing import digest_string

    pack_dir, manifest, ids = pack
    target = pack_dir / "compartments" / "records" / (
        ids["session"].removeprefix("urn:ccf:record:") + ".structural.json"
    )
    envelope = json.loads(target.read_text())
    envelope["content"]["_tamper"] = "x"
    data = json.dumps(envelope, indent=2).encode() + b"\n"
    target.write_bytes(data)

    def edit(m):
        m["counts"]["records"] = "0"
        for entry in m["streams"]:
            if entry["path"] == f"compartments/records/{target.name}":
                entry["digest"] = digest_string(data)
                entry["byte_length"] = str(len(data))

    _tamper(pack_dir, edit)
    settings = settings_factory()
    with pytest.raises(PackVerificationError) as excinfo:
        restore_mindpack(
            settings,
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
        )
    # The object-verification failure proves iteration was not bounded by
    # the reduced count claim.
    assert excinfo.value.reason != "manifest_count_mismatch"
    assert "verification failed" in str(excinfo.value) or "commitment" in str(
        excinfo.value
    )
    _assert_store_untouched(ccf_postgres_dsn, settings)


# ---------------------------------------------------------------------------
# 13. Positive control: the untouched manifest reproduces the derived
# inventory exactly.
# ---------------------------------------------------------------------------


def test_untouched_manifest_matches_derived_inventory(pack, settings_factory,
                                                      ccf_package_root):
    pack_dir, manifest, _ = pack
    verified = verify_mindpack(
        pack_dir, package_root=ccf_package_root, operation="restore"
    )
    for field in ("records", "links", "blobs", "commits"):
        assert int(manifest["counts"][field]) == verified.inventory.counts[field]
    assert set(manifest["withheld"]) == verified.inventory.withheld == set()
    assert set(manifest["erased"]) == verified.inventory.erased == set()
    claimed = {
        (e["object_id"], e["compartment"]): e
        for e in manifest["compartment_availability"]
    }
    assert claimed == verified.inventory.availability
    report = restore_mindpack(
        settings_factory(),
        package_root=ccf_package_root,
        pack_path=pack_dir,
        trusted_genesis_hash=manifest["genesis_commit_hash"],
    )
    assert report["status"] == "restored"
    assert report["partial"] is False
