"""CCF 0.1.2-rc1 conformance: new vectors and the section 13.8 regressions.

Covers the 0.1.2-rc1 delta against the vendored package:

- ``vectors/suppression-canonical.json`` — preimage schema, keyed token
  derivation, Merkle construction, governed Blob bytes, and the receipt's
  suppression commitment (spec 12.7);
- ``vectors/admission-authority-classes.json`` — the full positive/negative
  matrix over the pinned authority mapping, with rejection reasons equal to
  the registry's normative ``failure_reason`` strings verbatim;
- ``vectors/foreign-unavailability.json`` — erased/withheld compartment
  descriptors survive foreign merge unchanged;
- ``vectors/conformance-0.1.2-rc1.json`` — the twelve implementation-informed
  regressions, each wired to named tests in this suite;
- the canonical-suppression invariant: the lookup projection is rebuildable
  from canonical state, drift fails closed at admission, and erased content
  stays blocked after total projection destruction.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from ccf.db import open_ccf_connection
from ccf.erasure import suppression, suppression_set
from ccf.erasure.errors import SuppressionProjectionError
from ccf.erasure.suppression import generate_suppression_key
from ccf.hashing import blob_content_commitment
from ccf.ids import generate_id
from ccf.jcs import canonical_bytes
from ccf.registry import PinnedRegistries
from ccf.schemas import CcfSchemaError, SchemaSet

from ccf_helpers import add_producer, authority, make_rig


@pytest.fixture()
def schemas(ccf_package_root):
    return SchemaSet.load(ccf_package_root)


@pytest.fixture()
def settings_factory(ccf_postgres_dsn):
    """Factory for extra store schemas in one test (merges)."""
    import uuid

    import psycopg

    made: list[str] = []

    def _make():
        from ccf.db import CcfPostgresSettings

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


# ---------------------------------------------------------------------------
# vectors/suppression-canonical.json (spec 12.7)
# ---------------------------------------------------------------------------


def test_suppression_canonical_vector(ccf_vectors_dir, schemas):
    """Reproduce the pinned suppression vector end to end."""
    vector = json.loads((ccf_vectors_dir / "suppression-canonical.json").read_text())
    key = bytes.fromhex(vector["key_hex"])

    # Preimages validate against the closed catalog schema.
    for preimage in vector["preimages"]:
        schemas.validate(
            suppression.SCHEMA_PREIMAGE, preimage, what="suppression preimage"
        )

    # Tokens: HMAC-SHA-256 over the profile domain + canonical preimage,
    # ascending code-point order.
    tokens = sorted(suppression.suppression_token(key, p) for p in vector["preimages"])
    assert tokens == vector["entries"]

    # Merkle root: pinned leaf/node/empty domains, power-of-two split.
    assert suppression_set.entries_merkle_root(tokens) == (
        vector["expected_entries_merkle_root"]
    )

    # The governed Blob is the exact JCS document the vector pins, and its
    # salted content commitment matches.
    blob = suppression_set.suppression_blob_bytes(tokens)
    assert base64.b64encode(blob).decode() == vector["encoded_blob_base64"]
    assert str(len(blob)) == vector["blob_structural_content"]["byte_length"]
    commitment = blob_content_commitment(
        vector["blob_semantic_content"]["content_salt"], blob
    )
    assert commitment == vector["expected_content_commitment"]
    assert commitment == vector["blob_structural_content"]["content_commitment"]

    # The pinned Record/Blob/receipt shapes validate against their schemas.
    schemas.validate(
        suppression_set.SCHEMA_SET_STRUCTURAL,
        vector["record_structural_payload"],
        what="suppression set structural payload",
    )
    schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:structural.lineage.erasure_receipt",
        vector["receipt_structural_payload"],
        what="erasure receipt structural payload",
    )
    receipt_commitment = vector["receipt_structural_payload"]["suppression_commitment"]
    assert receipt_commitment["entries_merkle_root"] == (
        vector["expected_entries_merkle_root"]
    )
    assert receipt_commitment["entry_count"] == str(len(tokens))


def test_suppression_preimage_rejections(ccf_vectors_dir, schemas):
    """Every rejected preimage in the vector fails the closed schema."""
    vector = json.loads((ccf_vectors_dir / "suppression-canonical.json").read_text())
    for rejected in vector["rejected_preimages"]:
        with pytest.raises(CcfSchemaError):
            schemas.validate(
                suppression.SCHEMA_PREIMAGE,
                rejected["value"],
                what=f"rejected preimage {rejected['name']}",
            )


# ---------------------------------------------------------------------------
# vectors/admission-authority-classes.json (spec 5.5, 0.1.2-rc1 registry)
# ---------------------------------------------------------------------------


def test_authority_class_vector_matrix(ccf_package_root, ccf_vectors_dir):
    """Every authority class: positive and negative vectors, verbatim reasons."""
    from ccf.governance.authority import check_required_authority

    registries = PinnedRegistries.load(ccf_package_root)
    vector = json.loads(
        (ccf_vectors_dir / "admission-authority-classes.json").read_text()
    )
    assert vector["evaluator_profile"] == "ccf-admission-authority-v1"
    classes_covered = set()
    for case in vector["cases"]:
        classes_covered.add(case["authority_class"])
        fixture = case["fixture"]
        reason = check_required_authority(
            case["authority_class"],
            claim=fixture["claim"],
            recorded_by=(fixture["claim"] or {}).get(
                "asserted_by", "urn:ccf:record:00000000-0000-4000-8000-000000000000"
            ),
            admitted_by_archive=fixture["admitted_by_archive"],
            registries=registries,
            lineage_state_machine_passed=fixture["lineage_state_machine_passed"],
        )
        if case["expected"] == "accept":
            assert reason is None, f"{case['name']}: {reason}"
        else:
            # Rejection reasons are the registry's normative failure_reason
            # values, verbatim (0.1.2-rc1 pins them prose-only).
            assert reason == registries.authority_class(case["authority_class"])[
                "failure_reason"
            ], f"{case['name']}: {reason!r}"
    # The matrix is complete: every pinned class has both signs.
    assert classes_covered == set(registries.authority_class_names())
    assert len(vector["cases"]) == 2 * len(classes_covered) + 1  # issuer has 3


# ---------------------------------------------------------------------------
# Erasure helpers for the DB-backed suppression tests
# ---------------------------------------------------------------------------


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


def _utterance(rig, text, *, source_id, native_id="utt-1", revision="1", producer=None):
    producer = producer or rig.producer
    return producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        origin={"source_id": source_id, "native_id": native_id, "revision": revision},
        payload={
            "text": text,
            "language": "en",
            "speaker_id": None,
            "sequence": None,
            "transcription": None,
            "extensions": {},
        },
    )


def _erase(rig, object_id, *, authorized_producers=()):
    svc = rig.archive.erasure()
    targets = [{"object_id": object_id, "compartments": ["semantic"]}]
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={"targets": targets},
        reason="rc1 suppression drill",
        authority=authority("first_person_statement", rig.person_id, rig.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=targets,
        reasoning="approved",
        decided_by=rig.person_id,
        authority=authority("explicit_authorization", rig.person_id, rig.person_id),
        authorized_producers=list(authorized_producers),
    )
    status = svc.execute(decided["operation_id"])
    assert status["stage"] == "receipt", status
    return decided["operation_id"]


# ---------------------------------------------------------------------------
# The canonical suppression invariant (spec 12.7; conformance cases
# suppression-row-rebuild and suppression-reintroduction)
# ---------------------------------------------------------------------------


def test_suppression_set_is_canonical_and_journal_covered(rig):
    """The suppression set Record + governed Blob are admitted through the
    canonical path: commit members, signed journal, authorized_erasure_worker
    authority."""
    source_id = _source(rig)
    utterance = _utterance(rig, "canonical suppression", source_id=source_id)
    assert (
        rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))[
            "status"
        ]
        == "accepted"
    )
    _erase(rig, utterance["id"])

    with open_ccf_connection(rig.settings) as conn:
        sets = suppression_set.load_canonical_sets(conn, rig.archive.archive_id)
        assert len(sets) == 1
        canonical = sets[0]
        assert canonical.entry_count == 2  # origin + content tokens
        # Journal coverage: both objects are signed commit members.
        members = {
            row[0]
            for row in conn.execute(
                "SELECT object_id FROM commit_member WHERE archive_id = %s",
                (rig.archive.archive_id,),
            ).fetchall()
        }
        assert canonical.record_id in members
        assert canonical.blob_id in members
        # The receipt commits back to the set (structural compartment).
        receipt = conn.execute(
            """
            SELECT plaintext_json -> 'structural_payload' -> 'suppression_commitment'
            FROM compartment
            WHERE compartment = 'structural' AND state = 'plaintext'
              AND plaintext_json ->> 'type' = 'lineage.erasure_receipt'
            """
        ).fetchone()
        assert receipt is not None
        commitment = receipt[0]
        assert commitment["suppression_set_record_id"] == canonical.record_id
        assert commitment["suppression_blob_id"] == canonical.blob_id
        assert commitment["entries_merkle_root"] == canonical.merkle_root
        assert commitment["profile"] == "ccf-hmac-sha256-suppression-v1"
    assert rig.archive.verify_chain()["commits_verified"] >= 1


def test_suppression_lookup_rebuilds_from_canonical_state(rig, tmp_path):
    """THE INVARIANT (spec 12.7): destroy every suppression lookup row, and
    admission fails closed rather than silently permitting reintroduction;
    rebuild from canonical state and the erased content stays blocked."""
    source_id = _source(rig)
    utterance = _utterance(rig, "rebuild invariant", source_id=source_id)
    assert (
        rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))[
            "status"
        ]
        == "accepted"
    )
    _erase(rig, utterance["id"], authorized_producers=[rig.producer.producer_id])

    # Destroy every suppression lookup row.
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            deleted = conn.execute(
                "DELETE FROM suppression_entry WHERE archive_id = %s",
                (rig.archive.archive_id,),
            ).rowcount
            assert deleted == 2
            report = suppression_set.verify_projection(conn, rig.archive.archive_id)
            assert not report["ok"]
            assert len(report["missing"]) == 2

    # Reintroduction of the same content under a fresh ID and revision:
    # admission refuses to run on the destroyed projection (fail closed).
    recapture = _utterance(rig, "rebuild invariant", source_id=source_id, revision="9")
    batch = rig.producer.create_batch(records=[recapture])
    with pytest.raises(SuppressionProjectionError):
        rig.archive.admit_batch(batch)
    assert rig.archive.get_object(recapture["id"]) is None

    # Rebuild from canonical state; the erased content is blocked again.
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            rebuilt = suppression_set.rebuild_projection(
                conn, rig.archive.archive_id, now=rig.clock()
            )
            assert rebuilt == 2
            suppression_set.audit_projection(conn, rig.archive.archive_id)
    result = rig.archive.admit_batch(batch)
    outcome = result["admissions"][0]
    # Authorized producer: lifecycle result, no bytes restored (spec 6.5).
    assert outcome["status"] == "existing"
    assert outcome["current_lifecycle"] == "suppressed"
    assert outcome["payload_available"] is False
    assert rig.archive.get_object(recapture["id"]) is None


def test_suppression_reintroduction_blocked_after_total_projection_destruction(
    rig, tmp_path
):
    """Conformance case suppression-reintroduction: erased content remains
    blocked after ALL projections — including every lookup row — are
    destroyed and rebuilt from canonical state."""
    source_id = _source(rig)
    utterance = _utterance(rig, "total destruction", source_id=source_id)
    assert (
        rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))[
            "status"
        ]
        == "accepted"
    )
    _erase(rig, utterance["id"])

    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            conn.execute(
                "DELETE FROM suppression_entry WHERE archive_id = %s",
                (rig.archive.archive_id,),
            )
            rebuilt = suppression_set.rebuild_projection(
                conn, rig.archive.archive_id, now=rig.clock()
            )
            assert rebuilt == 2
            suppression_set.audit_projection(conn, rig.archive.archive_id)

    # An unauthorized producer re-submitting the erased content under a
    # fresh origin tuple gets the generic, indistinguishable refusal.
    other = add_producer(rig, tmp_path, "outsider")
    foreign = _utterance(
        rig, "total destruction", source_id=source_id, revision="2", producer=other
    )
    result = rig.archive.admit_batch(other.create_batch(records=[foreign]))
    outcome = result["admissions"][0]
    assert outcome["status"] == "rejected"
    assert outcome["payload_available"] is False
    assert "current_lifecycle" not in outcome
    reason = outcome.get("reason", "")
    assert "eras" not in reason and "suppress" not in reason
    assert rig.archive.get_object(foreign["id"]) is None


def test_tampered_canonical_suppression_blob_detected(rig):
    """A mutated canonical token Blob is caught by the pinned Merkle root."""
    source_id = _source(rig)
    utterance = _utterance(rig, "canonical tamper", source_id=source_id)
    assert (
        rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))[
            "status"
        ]
        == "accepted"
    )
    _erase(rig, utterance["id"])

    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE blob_content SET plaintext_bytes = %s
                WHERE blob_id IN (
                    SELECT (plaintext_json -> 'structural_payload'
                            ->> 'suppression_blob_id')
                    FROM compartment
                    WHERE compartment = 'structural'
                      AND plaintext_json ->> 'type' = 'lineage.suppression_set'
                )
                """,
                (b'{"entries":[],"profile":"ccf-hmac-sha256-suppression-v1"}',),
            )
            with pytest.raises(SuppressionProjectionError):
                suppression_set.load_canonical_sets(conn, rig.archive.archive_id)


def test_erasure_requires_suppression_key(rig):
    """0.1.2-rc1: the receipt schema requires the suppression commitment, so
    an erasure decision without a configured key fails before admission."""
    from dataclasses import replace

    from ccf.erasure.errors import ErasureError

    source_id = _source(rig)
    utterance = _utterance(rig, "no key", source_id=source_id)
    assert (
        rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))[
            "status"
        ]
        == "accepted"
    )
    unkeyed_settings = replace(rig.settings, suppression_key_path=None)
    from ccf.archive import Archive

    unkeyed = Archive.open(
        unkeyed_settings,
        package_root=rig.package_root,
        archive_key_path=rig.archive_key_path,
    )
    svc = unkeyed.erasure()
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={
            "targets": [{"object_id": utterance["id"], "compartments": ["semantic"]}]
        },
        reason="no key configured",
        authority=authority("first_person_statement", rig.person_id, rig.person_id),
    )
    with pytest.raises(ErasureError, match="suppression key"):
        svc.decide(
            request_id=request["request_id"],
            decision="approve",
            targets=[{"object_id": utterance["id"], "compartments": ["semantic"]}],
            reasoning="approved",
            decided_by=rig.person_id,
            authority=authority(
                "explicit_authorization", rig.person_id, rig.person_id
            ),
        )


# ---------------------------------------------------------------------------
# vectors/foreign-unavailability.json (spec 11.3)
# ---------------------------------------------------------------------------


def test_foreign_unavailability_vector(
    rig, settings_factory, tmp_path, ccf_package_root, load_ccf_json
):
    """Erased and withheld compartments survive foreign merge unchanged.

    The vector pins per-compartment descriptors (availability, commitment,
    retention profile, custody proof, unavailability lineage, no
    plaintext). Our merge preserves exactly those facts: the destination
    compartment row keeps the source state and commitment, stores no
    plaintext, and the source chain is kept as a foreign custody proof.
    """
    vector = load_ccf_json(ccf_package_root / "vectors" / "foreign-unavailability.json")
    inputs = {
        (c["object_kind"], c["compartment"]): c
        for c in vector["input"]["compartments"]
    }
    expected = {
        (c["object_kind"], c["compartment"]): c
        for c in vector["expected_destination_compartments"]
    }
    assert vector["input"]["mode"] == "foreign_merge"
    assert set(inputs) == set(expected) == {
        ("record", "semantic"),
        ("blob", "blob_content"),
    }
    for key, descriptor in expected.items():
        # The pinned expectations our model must honor, whatever the IDs.
        assert descriptor["plaintext"] is None
        assert descriptor["availability"] in ("erased", "withheld")
        assert descriptor["commitment"].startswith("sha256:")
        assert descriptor["source_custody_proof"]
        assert descriptor["unavailability_lineage_id"].startswith("urn:ccf:record:")

    # Exercise the invariant against real archives: export a pack carrying
    # an erased record compartment, merge it foreign, and verify the
    # destination preserves availability and commitment with no plaintext.
    source_id = _source(rig)
    utterance = _utterance(rig, "foreign unavailability", source_id=source_id)
    assert (
        rig.archive.admit_batch(rig.producer.create_batch(records=[utterance]))[
            "status"
        ]
        == "accepted"
    )
    _erase(rig, utterance["id"])
    with open_ccf_connection(rig.settings) as conn:
        source_state = conn.execute(
            """
            SELECT state FROM compartment
            WHERE object_id = %s AND compartment = 'semantic'
            """,
            (utterance["id"],),
        ).fetchone()[0]
        source_commitment = conn.execute(
            "SELECT semantic_commitment FROM object_header WHERE id = %s",
            (utterance["id"],),
        ).fetchone()[0]
    assert source_state == "erased"

    pack_path = tmp_path / "pack"
    rig.archive.sync().export_mindpack(pack_path)

    (tmp_path / "dest").mkdir()
    dest_rig = make_rig(settings_factory(), tmp_path / "dest", ccf_package_root)
    result = dest_rig.archive.sync().import_mindpack(pack_path)
    assert result["status"] == "merged", result
    with open_ccf_connection(dest_rig.settings) as conn:
        row = conn.execute(
            """
            SELECT state, plaintext_json FROM compartment
            WHERE object_id = %s AND compartment = 'semantic'
            """,
            (utterance["id"],),
        ).fetchone()
        assert row is not None
        assert row[0] == "erased"  # availability preserved
        assert row[1] is None  # no plaintext fabricated
        commitment = conn.execute(
            "SELECT semantic_commitment FROM object_header WHERE id = %s",
            (utterance["id"],),
        ).fetchone()[0]
        assert commitment == source_commitment  # commitment preserved exactly
        custody = conn.execute(
            "SELECT 1 FROM foreign_custody WHERE archive_id = %s",
            (dest_rig.archive.archive_id,),
        ).fetchone()
        assert custody is not None  # source custody proof preserved


# ---------------------------------------------------------------------------
# vectors/conformance-0.1.2-rc1.json: coverage gate (spec 13.8)
# ---------------------------------------------------------------------------

#: Each pinned conformance case maps to the named tests that exercise it.
#: The gate below parses those modules and requires the names to exist, so
#: this mapping cannot silently rot.
CONFORMANCE_CASE_TESTS = {
    "origin-cross-kind": {
        # Record and Blob share one origin tuple (object_kind differs).
        "test_ccf_admission.py": {"test_thoth_capture_example_end_to_end"},
    },
    "origin-same-kind": {
        "test_ccf_012_rc1.py": {"test_origin_same_kind_multiplicity"},
    },
    "foreign-unavailability": {
        "test_ccf_012_rc1.py": {"test_foreign_unavailability_vector"},
    },
    "bootstrap-rebuild": {
        "test_ccf_cutover_bootstrap_retention.py": {
            "test_gate5b_bootstrap_compartments_survive_projection_destruction"
        },
    },
    "content-rejection-liveness": {
        "test_ccf_adversarial_tamper.py": {
            "test_content_rejected_batch_does_not_brick_producer"
        },
    },
    "predecessor-pending": {
        "test_ccf_admission.py": {
            "test_out_of_order_batch_waits_for_exact_predecessor_then_admits"
        },
    },
    "suppression-row-rebuild": {
        "test_ccf_012_rc1.py": {
            "test_suppression_lookup_rebuilds_from_canonical_state"
        },
    },
    "suppression-reintroduction": {
        "test_ccf_012_rc1.py": {
            "test_suppression_reintroduction_blocked_after_total_projection_destruction"
        },
    },
    "admission-membership": {
        "test_ccf_adversarial_tamper.py": {
            "test_tampered_admission_row_detected",
            "test_dropped_admission_row_detected",
        },
    },
    "pgvector-multischema": {
        # Exercised by the vendored package's own multi-schema fixture
        # (tools/verify-postgres-fixture.sh) via spec/ccf/run-checks.sh;
        # our pgvector projection round-trips in test_ccf_projections.py.
        "test_ccf_projections.py": {"test_pgvector_round_trip"},
    },
    "git-three-commit": {
        "test_ccf_git_source.py": {"test_git_worktree_revision_behavior"},
    },
    "authority-classes": {
        "test_ccf_012_rc1.py": {"test_authority_class_vector_matrix"},
    },
}


def test_conformance_case_coverage(ccf_vectors_dir, load_ccf_json):
    """Every pinned 0.1.2-rc1 conformance case has named test coverage."""
    import ast

    vector = load_ccf_json(ccf_vectors_dir / "conformance-0.1.2-rc1.json")
    case_ids = {case["id"] for case in vector["cases"]}
    assert case_ids == set(CONFORMANCE_CASE_TESTS), (
        f"conformance cases drifted: {sorted(case_ids ^ set(CONFORMANCE_CASE_TESTS))}"
    )
    test_root = Path(__file__).parent
    for case_id, modules in CONFORMANCE_CASE_TESTS.items():
        for module_name, expected_tests in modules.items():
            source = (test_root / module_name).read_text(encoding="utf-8")
            tree = ast.parse(source, filename=module_name)
            actual = {
                node.name
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            missing = expected_tests - actual
            assert not missing, f"{case_id}: {module_name} missing {sorted(missing)}"


def test_origin_same_kind_multiplicity(rig):
    """Conformance case origin-same-kind: two same-kind objects conflict on
    one origin tuple; distinct stable native IDs both admit (spec 6.5)."""
    source_id = _source(rig)
    first = _utterance(rig, "one", source_id=source_id, native_id="seg-1")
    changed = _utterance(rig, "one-changed", source_id=source_id, native_id="seg-1")

    result = rig.archive.admit_batch(
        rig.producer.create_batch(records=[first, changed])
    )
    # Same native ID + revision + kind, different content: per-object
    # conflict; the rest of the batch still commits.
    assert result["status"] == "partially_accepted", result
    statuses = sorted(a["status"] for a in result["admissions"])
    assert statuses == ["admitted", "origin_revision_conflict"], result

    # A stable distinct native ID admits alongside the first.
    second = _utterance(rig, "two", source_id=source_id, native_id="seg-2")
    result = rig.archive.admit_batch(rig.producer.create_batch(records=[second]))
    assert result["status"] == "accepted", result
