"""Signed-journal verification tests (spec sections 4.8-4.9, 7.2-7.4).

Covers genesis pinning, multi-commit chain verification, and prefix
integrity: mutating a member, a compartment, a leaf hash, a parent hash,
or the archive head must make verification fail; restoring the bytes makes
it pass again.
"""

from __future__ import annotations

import pytest

from ccf.db import open_ccf_connection
from ccf.journal import JournalError

from ccf_helpers import make_rig


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    # One producer commit on top of genesis + bootstrap.
    record = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": "journal",
            "definition": "journal test concept",
            "aliases": [],
            "extensions": {},
        },
    )
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[record]))
    assert result["status"] == "accepted"
    rig._test_record_id = record["id"]
    return rig


def _mutate(rig, sql, params=()):
    """Apply a committed mutation; returns a callable that restores it."""
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            conn.execute(sql, params)


def test_fresh_chain_verifies(rig):
    report = rig.archive.verify_chain()
    assert report["commits_verified"] == 3
    assert report["head_sequence"] == "2"
    assert report["genesis_commit_hash"].startswith("sha256:")
    assert report["signer_public_key"]


def test_trusted_genesis_hash_enforced(rig):
    report = rig.archive.verify_chain()
    rig.archive.verify_chain(trusted_genesis_hash=report["genesis_commit_hash"])
    with pytest.raises(JournalError, match="trusted"):
        rig.archive.verify_chain(trusted_genesis_hash="sha256:" + "0" * 64)


def test_member_mutation_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT object_hash FROM commit_member WHERE commit_sequence = 2 LIMIT 1"
        ).fetchone()
    original = row[0]
    bogus = "sha256:" + "1" * 64
    _mutate(
        rig,
        "UPDATE commit_member SET object_hash = %s WHERE commit_sequence = 2",
        (bogus,),
    )
    try:
        with pytest.raises(JournalError):
            rig.archive.verify_chain()
    finally:
        _mutate(
            rig,
            "UPDATE commit_member SET object_hash = %s WHERE commit_sequence = 2",
            (original,),
        )
    rig.archive.verify_chain()


def test_compartment_mutation_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            """
            SELECT plaintext_json FROM compartment
            WHERE object_id = %s AND compartment = 'semantic'
            """,
            (rig._test_record_id,),
        ).fetchone()
    original = row[0]
    tampered = dict(original, payload=dict(original["payload"], label="forged"))
    from psycopg.types.json import Jsonb

    _mutate(
        rig,
        "UPDATE compartment SET plaintext_json = %s WHERE object_id = %s AND compartment = 'semantic'",
        (Jsonb(tampered), rig._test_record_id),
    )
    try:
        with pytest.raises(JournalError, match="semantic commitment mismatch"):
            rig.archive.verify_chain()
    finally:
        _mutate(
            rig,
            "UPDATE compartment SET plaintext_json = %s WHERE object_id = %s AND compartment = 'semantic'",
            (Jsonb(original), rig._test_record_id),
        )
    rig.archive.verify_chain()


def test_leaf_hash_mutation_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        original = conn.execute(
            "SELECT leaf_hash FROM commit_member WHERE commit_sequence = 2 LIMIT 1"
        ).fetchone()[0]
    _mutate(
        rig,
        "UPDATE commit_member SET leaf_hash = %s WHERE commit_sequence = 2",
        ("sha256:" + "2" * 64,),
    )
    try:
        with pytest.raises(JournalError, match="member leaf mismatch"):
            rig.archive.verify_chain()
    finally:
        _mutate(
            rig,
            "UPDATE commit_member SET leaf_hash = %s WHERE commit_sequence = 2",
            (original,),
        )
    rig.archive.verify_chain()


def test_parent_hash_mutation_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        original = conn.execute(
            "SELECT parent_commit_hash FROM commit_journal WHERE sequence = 2"
        ).fetchone()[0]
    _mutate(
        rig,
        "UPDATE commit_journal SET parent_commit_hash = %s WHERE sequence = 2",
        ("sha256:" + "3" * 64,),
    )
    try:
        with pytest.raises(JournalError, match="parent hash chain broken"):
            rig.archive.verify_chain()
    finally:
        _mutate(
            rig,
            "UPDATE commit_journal SET parent_commit_hash = %s WHERE sequence = 2",
            (original,),
        )
    rig.archive.verify_chain()


def test_commit_signature_tampering_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            """
            SELECT c.plaintext_json FROM compartment c
            JOIN commit_journal j ON j.commit_record_id = c.object_id
            WHERE j.sequence = 2 AND c.compartment = 'structural'
            """
        ).fetchone()
    original = row[0]
    payload = dict(original["structural_payload"])
    payload["committed_at"] = "2026-08-12T09:09:09.999Z"
    tampered = dict(original, structural_payload=payload)
    from psycopg.types.json import Jsonb

    _mutate(
        rig,
        """
        UPDATE compartment c SET plaintext_json = %s
        FROM commit_journal j
        WHERE j.commit_record_id = c.object_id AND j.sequence = 2
          AND c.compartment = 'structural'
        """,
        (Jsonb(tampered),),
    )
    try:
        with pytest.raises(JournalError):
            rig.archive.verify_chain()
    finally:
        _mutate(
            rig,
            """
            UPDATE compartment c SET plaintext_json = %s
            FROM commit_journal j
            WHERE j.commit_record_id = c.object_id AND j.sequence = 2
              AND c.compartment = 'structural'
            """,
            (Jsonb(original),),
        )
    rig.archive.verify_chain()


def test_head_mutation_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        original = conn.execute(
            "SELECT commit_hash FROM archive_head"
        ).fetchone()[0]
    _mutate(rig, "UPDATE archive_head SET commit_hash = %s", ("sha256:" + "4" * 64,))
    try:
        with pytest.raises(JournalError, match="head does not match"):
            rig.archive.verify_chain()
    finally:
        _mutate(rig, "UPDATE archive_head SET commit_hash = %s", (original,))
    rig.archive.verify_chain()


def test_member_deletion_detected(rig):
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT object_id FROM commit_member WHERE commit_sequence = 2 LIMIT 1"
        ).fetchone()
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            deleted = conn.execute(
                """
                DELETE FROM commit_member WHERE commit_sequence = 2 AND object_id = %s
                RETURNING commit_position, object_kind, object_hash, admitted_at, leaf_hash
                """,
                (row[0],),
            ).fetchone()
    try:
        with pytest.raises(JournalError, match="member count mismatch"):
            rig.archive.verify_chain()
    finally:
        with open_ccf_connection(rig.settings) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO commit_member (
                        archive_id, commit_sequence, commit_position, object_kind,
                        object_id, object_hash, admitted_at, leaf_hash
                    ) VALUES (%s, 2, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rig.archive.archive_id,
                        deleted[0],
                        deleted[1],
                        row[0],
                        deleted[2],
                        deleted[3],
                        deleted[4],
                    ),
                )
    rig.archive.verify_chain()
