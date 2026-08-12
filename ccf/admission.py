"""Canonical admission (spec sections 2.2-2.3, 6.4-6.6).

One serialized archive-head transaction: the admission locks the
``archive_head`` row ``FOR UPDATE``, which totally orders all commits for an
archive without SERIALIZABLE retry loops (the reference envelope prescribes
exactly this head lock). Inside that transaction admission:

1. validates the batch schema, credential, signature, catalog root, and
   producer chain position;
2. enforces ID and origin-tuple uniqueness with submission-hash comparison;
3. validates same-batch references and payload schemas;
4. enforces lineage compare-and-swap and ``derived_from`` cycle rules
   (including edges restored by Link dispositions);
5. constructs canonical compartments and object hashes;
6. writes objects, admissions, members, lineage heads, and the signed
   commit, then advances the head — all atomically.

Per-object outcomes follow ``schemas/operational/admission.schema.json`` and
batch outcomes ``batch-result.schema.json``: ``queued`` / ``committed`` /
``partial`` / ``rejected`` / ``conflict``.
"""

from __future__ import annotations

from dataclasses import dataclass

from psycopg.types.json import Jsonb

from ccf import CCF_SPEC
from ccf.catalog import SemanticCatalog
from ccf.credentials import CredentialError, resolve_credential_public_key
from ccf.governance.authority import check_required_authority
from ccf.governance.fences import advance_fences, classify_governance_mutations
from ccf.hashing import (
    blob_content_commitment,
    commit_leaf,
    decode_b64url,
    producer_batch_hash,
    producer_batch_signing_digest,
    submission_hash,
    verify_digest,
)
from ccf.ids import generate_id, parse_id
from ccf.journal import build_commit_record
from ccf.lineage import (
    DEACTIVATING_ACTIONS,
    LineageDeclarationError,
    add_edge,
    check_state_transition,
    creates_cycle,
    current_link_actions,
    declare_lineage,
    load_active_acyclic_edges,
    load_lineage_heads,
    remove_edge,
)
from ccf.objects import now_timestamp
from ccf.registry import PinnedRegistries
from ccf.schemas import CcfSchemaError, SchemaSet

SCHEMA_PRODUCER_BATCH = "urn:ccf:schema:0.1.1:sync.producer-batch"
SCHEMA_RECORD_SUBMISSION = "urn:ccf:schema:0.1.1:submissions.record"
SCHEMA_LINK_SUBMISSION = "urn:ccf:schema:0.1.1:submissions.link"
SCHEMA_BLOB_SUBMISSION = "urn:ccf:schema:0.1.1:submissions.blob"
SCHEMA_LINK_SEMANTIC_CONTENT = "urn:ccf:schema:0.1.1:objects.link-semantic-content"
SCHEMA_RECORD_STRUCTURAL = "urn:ccf:schema:0.1.1:objects.record-structural-content"
SCHEMA_RECORD_SEMANTIC = "urn:ccf:schema:0.1.1:objects.record-semantic-content"
SCHEMA_LINK_STRUCTURAL = "urn:ccf:schema:0.1.1:objects.link-structural-content"
SCHEMA_LINK_SEMANTIC = "urn:ccf:schema:0.1.1:objects.link-semantic-content"
SCHEMA_BLOB_STRUCTURAL = "urn:ccf:schema:0.1.1:objects.blob-structural-content"
SCHEMA_BLOB_SEMANTIC = "urn:ccf:schema:0.1.1:objects.blob-semantic-content"

DEFAULT_EVALUATOR_PROFILE = "ccf-deny-overrides-v1"


class AdmissionError(RuntimeError):
    """Raised on unexpected admission failures (not per-object outcomes)."""


@dataclass
class ResolvedObject:
    """A fully resolved object ready for atomic commit."""

    object_kind: str
    object_id: str
    header: dict
    structural: dict
    semantic: dict | None
    submission_hash: str | None
    origin: dict | None
    lineage_update: tuple[str, str] | None  # (lineage_id, new state)
    blob_data: bytes | None


def _batch_result(
    batch_id: str,
    status: str,
    archive_id: str,
    *,
    commit_sequence: str | None = None,
    commit_hash: str | None = None,
    admissions: list[dict] | None = None,
    reason: str | None = None,
) -> dict:
    extensions: dict = {}
    if reason is not None:
        extensions["reason"] = reason
    return {
        "batch_id": batch_id,
        "status": status,
        "archive_id": archive_id,
        "commit_sequence": commit_sequence,
        "commit_hash": commit_hash,
        "admissions": admissions or [],
        "extensions": extensions,
    }


def _object_outcome(
    object_id: str,
    status: str,
    *,
    object_hash: str | None = None,
    commit_sequence: str | None = None,
    commit_position: int | None = None,
    current_lifecycle: str | None = None,
    payload_available: bool = False,
    reason: str | None = None,
) -> dict:
    outcome = {
        "object_id": object_id,
        "status": status,
        "object_hash": object_hash,
        "commit_sequence": commit_sequence,
        "commit_position": commit_position,
        "payload_available": payload_available,
    }
    if current_lifecycle is not None:
        outcome["current_lifecycle"] = current_lifecycle
    if reason is not None:
        outcome["reason"] = reason
    return outcome


def lock_archive_head(conn, archive_id: str) -> dict:
    """Row-lock and read the archive head, serializing all admissions."""
    row = conn.execute(
        """
        SELECT sequence, commit_record_id, commit_hash FROM archive_head
        WHERE archive_id = %s FOR UPDATE
        """,
        (archive_id,),
    ).fetchone()
    if row is None:
        raise AdmissionError(f"archive {archive_id} has no head (genesis missing)")
    return {"sequence": int(row[0]), "commit_record_id": row[1], "commit_hash": row[2]}


def load_archive(conn, archive_id: str) -> dict:
    row = conn.execute(
        """
        SELECT epoch_id, genesis_commit_hash, semantic_catalog_root,
               active_profiles, signer_key_id, erasure_domain_id
        FROM archive WHERE archive_id = %s
        """,
        (archive_id,),
    ).fetchone()
    if row is None:
        raise AdmissionError(f"unknown archive: {archive_id}")
    return {
        "archive_id": archive_id,
        "epoch_id": row[0],
        "genesis_commit_hash": row[1],
        "semantic_catalog_root": row[2],
        "active_profiles": list(row[3]),
        "signer_key_id": row[4],
        "erasure_domain_id": row[5],
    }


def commit_objects(
    conn,
    *,
    archive: dict,
    head: dict,
    objects: list[ResolvedObject],
    catalog: SemanticCatalog,
    registries: PinnedRegistries,
    signer,
    committed_at: str,
    salt_fn,
) -> tuple[int, str]:
    """Atomically write objects, members, the signed commit, and the head.

    Caller must hold :func:`lock_archive_head`. There is no
    durable-but-secretly-canonical intermediate state: everything below is
    one transaction, and the head advances before the caller may
    acknowledge success (spec section 6.4).
    """
    sequence = head["sequence"] + 1
    members = [
        {
            "commit_sequence": str(sequence),
            "commit_position": position,
            "admitted_at": committed_at,
            "object_kind": obj.object_kind,
            "object_id": obj.object_id,
            "object_hash": obj.header["object_hash"],
        }
        for position, obj in enumerate(objects)
    ]
    commit = build_commit_record(
        commit_record_id=generate_id("record"),
        archive_id=archive["archive_id"],
        epoch_id=archive["epoch_id"],
        sequence=sequence,
        parent_commit_hash=head["commit_hash"],
        members=members,
        signer=signer,
        signer_key_id=archive["signer_key_id"],
        semantic_catalog_root=archive["semantic_catalog_root"],
        active_profiles=archive["active_profiles"],
        committed_at=committed_at,
        catalog=catalog,
        registries=registries,
        salt_fn=salt_fn,
    )

    def _insert_object(obj: ResolvedObject) -> None:
        conn.execute(
            """
            INSERT INTO object_header (
                id, archive_id, object_kind, spec, hash_profile,
                structural_commitment, semantic_commitment, object_hash,
                submission_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                obj.object_id,
                archive["archive_id"],
                obj.object_kind,
                CCF_SPEC,
                "ccf-jcs-sha256-v2",
                obj.header["structural_commitment"],
                obj.header["semantic_commitment"],
                obj.header["object_hash"],
                obj.submission_hash,
            ),
        )
        for compartment, envelope in (("structural", obj.structural), ("semantic", obj.semantic)):
            if envelope is None:
                continue
            conn.execute(
                """
                INSERT INTO compartment (
                    object_id, compartment, state, format, salt,
                    plaintext_json, updated_at
                ) VALUES (%s, %s, 'plaintext', %s, %s, %s, %s)
                """,
                (
                    obj.object_id,
                    compartment,
                    envelope["format"],
                    decode_b64url(envelope["salt"]),
                    Jsonb(envelope["content"]),
                    committed_at,
                ),
            )
        if obj.object_kind == "blob":
            content_salt = obj.semantic["content"]["content_salt"] if obj.semantic else None
            if obj.blob_data is not None:
                conn.execute(
                    """
                    INSERT INTO blob_content (
                        blob_id, state, byte_length, plaintext_bytes,
                        content_salt, updated_at
                    ) VALUES (%s, 'plaintext', %s, %s, %s, %s)
                    """,
                    (
                        obj.object_id,
                        len(obj.blob_data),
                        obj.blob_data,
                        decode_b64url(content_salt) if content_salt else None,
                        committed_at,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO blob_content (
                        blob_id, state, byte_length, content_salt, updated_at
                    ) VALUES (%s, 'withheld', %s, %s, %s)
                    """,
                    (
                        obj.object_id,
                        int(obj.structural["content"]["byte_length"]),
                        decode_b64url(content_salt) if content_salt else None,
                        committed_at,
                    ),
                )
        if obj.origin is not None:
            conn.execute(
                """
                INSERT INTO origin_index (
                    archive_id, source_id, native_id, revision,
                    submission_hash, object_kind, object_id, lifecycle
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
                """,
                (
                    archive["archive_id"],
                    obj.origin["source_id"],
                    obj.origin["native_id"],
                    obj.origin["revision"],
                    obj.submission_hash,
                    obj.object_kind,
                    obj.object_id,
                ),
            )

    for position, obj in enumerate(objects):
        _insert_object(obj)
        conn.execute(
            """
            INSERT INTO admission (
                archive_id, commit_sequence, commit_position, object_kind,
                object_id, object_hash, admitted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                archive["archive_id"],
                sequence,
                position,
                obj.object_kind,
                obj.object_id,
                obj.header["object_hash"],
                committed_at,
            ),
        )
        if obj.lineage_update is not None:
            lineage_id, new_state = obj.lineage_update
            block = obj.structural["content"]["lineage"]
            conn.execute(
                """
                INSERT INTO lineage_head (
                    archive_id, lineage_id, head_record_id, head_record_hash,
                    head_commit_sequence, state, valid_from, expires_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (archive_id, lineage_id) DO UPDATE SET
                    head_record_id = EXCLUDED.head_record_id,
                    head_record_hash = EXCLUDED.head_record_hash,
                    head_commit_sequence = EXCLUDED.head_commit_sequence,
                    state = EXCLUDED.state,
                    valid_from = EXCLUDED.valid_from,
                    expires_at = EXCLUDED.expires_at
                """,
                (
                    archive["archive_id"],
                    lineage_id,
                    obj.object_id,
                    obj.header["object_hash"],
                    sequence,
                    new_state,
                    block["valid_from"],
                    block["expires_at"],
                ),
            )

    # The commit Record itself: portable object, but excluded from its own
    # member root (spec section 4.9).
    _insert_object(
        ResolvedObject(
            object_kind="record",
            object_id=commit.record_id,
            header=commit.header,
            structural=commit.structural_envelope,
            semantic=None,
            submission_hash=None,
            origin=None,
            lineage_update=None,
            blob_data=None,
        )
    )
    conn.execute(
        """
        INSERT INTO commit_journal (
            archive_id, sequence, commit_record_id, parent_commit_hash,
            commit_hash, batch_merkle_root, member_count, signer_key_id,
            semantic_catalog_root, committed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            archive["archive_id"],
            sequence,
            commit.record_id,
            head["commit_hash"],
            commit.commit_hash,
            commit.merkle_root,
            len(members),
            archive["signer_key_id"],
            archive["semantic_catalog_root"],
            committed_at,
        ),
    )
    for member in members:
        conn.execute(
            """
            INSERT INTO commit_member (
                archive_id, commit_sequence, commit_position, object_kind,
                object_id, object_hash, admitted_at, leaf_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                archive["archive_id"],
                sequence,
                member["commit_position"],
                member["object_kind"],
                member["object_id"],
                member["object_hash"],
                committed_at,
                "sha256:" + commit_leaf(member).hex(),
            ),
        )
    # Governance generation fences (spec 9.5): a governance mutation
    # advances its fences in this same transaction, so no cached decision
    # computed from the old state can be served afterwards.
    governance_fences = classify_governance_mutations(conn, objects)
    if governance_fences:
        advance_fences(
            conn, archive["archive_id"], governance_fences, sequence, committed_at
        )
    conn.execute(
        """
        UPDATE archive_head
        SET sequence = %s, commit_record_id = %s, commit_hash = %s,
            semantic_catalog_root = %s, signer_key_id = %s, updated_at = %s
        WHERE archive_id = %s
        """,
        (
            sequence,
            commit.record_id,
            commit.commit_hash,
            archive["semantic_catalog_root"],
            archive["signer_key_id"],
            committed_at,
            archive["archive_id"],
        ),
    )
    return sequence, commit.commit_hash


# ---------------------------------------------------------------------------
# Producer batch admission
# ---------------------------------------------------------------------------


class _BatchRejected(Exception):
    """Internal: whole-batch rejection with a reason."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _verify_batch_envelope(conn, archive: dict, batch: dict, schemas: SchemaSet) -> None:
    """Schema, catalog, credential, signature, and producer-chain checks."""
    try:
        schemas.validate(SCHEMA_PRODUCER_BATCH, batch, what="producer batch")
    except CcfSchemaError as exc:
        raise _BatchRejected(f"batch schema invalid: {exc}") from exc
    if batch["semantic_catalog_root"] != archive["semantic_catalog_root"]:
        raise _BatchRejected(
            "semantic catalog root mismatch: "
            f"batch {batch['semantic_catalog_root']} != "
            f"archive {archive['semantic_catalog_root']}"
        )
    try:
        public_key_text = resolve_credential_public_key(conn, batch["credential_id"])
    except CredentialError as exc:
        raise _BatchRejected(str(exc)) from exc

    recomputed = producer_batch_hash(batch)
    if recomputed != batch["batch_hash"]:
        raise _BatchRejected("batch_hash does not match recomputed hash")
    from ccf.keys import public_key_from_b64url

    try:
        verify_digest(
            public_key_from_b64url(public_key_text),
            decode_b64url(batch["signature"]),
            producer_batch_signing_digest(batch["batch_hash"]),
        )
    except Exception as exc:
        raise _BatchRejected(f"batch signature invalid: {exc}") from exc

    last = conn.execute(
        """
        SELECT batch_hash, producer_sequence FROM producer_batch
        WHERE producer_id = %s AND status IN ('committed', 'partial', 'conflict')
        ORDER BY producer_sequence DESC LIMIT 1
        """,
        (batch["producer_id"],),
    ).fetchone()
    if last is None:
        if batch["previous_batch_hash"] is not None:
            raise _BatchRejected(
                "producer chain conflict: first known batch must have no previous hash"
            )
    else:
        if batch["previous_batch_hash"] != last[0] or int(
            batch["producer_sequence"]
        ) != int(last[1]) + 1:
            raise _BatchRejected(
                "producer chain conflict: expected sequence "
                f"{int(last[1]) + 1} with previous hash {last[0]}, got sequence "
                f"{batch['producer_sequence']} with previous "
                f"{batch['previous_batch_hash']}"
            )


def _resolve_policy_ref(conn, lineage_heads: dict, policy_hint, catalog_root: str) -> dict:
    """Resolve a producer policy hint against current lineage heads (5.2)."""
    head = lineage_heads.get(policy_hint)
    if head is None:
        row = conn.execute(
            """
            SELECT head_record_id, head_record_hash FROM lineage_head
            WHERE lineage_id = %s
            """,
            (policy_hint,),
        ).fetchone()
        head = (
            {"head_record_id": row[0], "head_record_hash": row[1]} if row else None
        )
    if head is None:
        head_id, head_hash = None, None
    else:
        head_id = head["head_record_id"]
        head_hash = head.get("head_record_hash")
    return {
        "lineage_id": policy_hint,
        "head_id_at_write": head_id,
        "policy_object_hash": head_hash,
        "evaluator_profile": DEFAULT_EVALUATOR_PROFILE,
        "semantic_catalog_root": catalog_root,
    }


def _producer_evidence(batch: dict, sub_hash: str) -> dict:
    return {
        "batch_id": batch["batch_id"],
        "credential_id": batch["credential_id"],
        "producer_sequence": batch["producer_sequence"],
        "submission_hash": sub_hash,
    }


def admit_producer_batch(
    conn,
    *,
    archive: dict,
    batch: dict,
    catalog: SemanticCatalog,
    registries: PinnedRegistries,
    schemas: SchemaSet,
    signer,
    clock=now_timestamp,
    blob_bytes: dict[str, bytes] | None = None,
    salt_fn=None,
) -> dict:
    """Admit one signed producer batch in the serialized head transaction.

    Caller must hold an open transaction; this function commits nothing on
    its own. Never raises for expected outcomes — those are batch results;
    raises only for infrastructure failures.
    """
    from ccf.objects import new_salt as default_salt

    salt_fn = salt_fn or default_salt
    archive_id = archive["archive_id"]
    head = lock_archive_head(conn, archive_id)

    # Batch-level idempotent replay: a previously answered batch returns its
    # stored outcome without touching canonical state.
    stored = conn.execute(
        "SELECT status, result_json FROM producer_batch WHERE batch_id = %s",
        (batch.get("batch_id"),),
    ).fetchone()
    if stored is not None and stored[0] in ("committed", "partial", "rejected", "conflict"):
        return stored[1]

    try:
        _verify_batch_envelope(conn, archive, batch, schemas)
    except _BatchRejected as exc:
        result = _batch_result(
            batch.get("batch_id", "unknown"), "rejected", archive_id, reason=exc.reason
        )
        _record_batch_outcome(conn, batch, result, committed_sequence=None)
        return result

    lineage_heads = load_lineage_heads(conn, archive_id)
    acyclic_types = registries.acyclic_link_types()
    active_edges = load_active_acyclic_edges(conn, archive_id, acyclic_types)
    link_actions = current_link_actions(conn, archive_id)

    admitted: list[ResolvedObject] = []
    outcomes: list[dict] = []
    conflicts = 0
    admitted_ids: set[str] = set()
    pending: list[tuple[dict, str, str]] = []  # (submission, kind, submission_hash)

    # -- Pass 1: schema/registry validation, idempotency, lineage, cycles. --
    for kind, schema_id in (
        ("records", SCHEMA_RECORD_SUBMISSION),
        ("links", SCHEMA_LINK_SUBMISSION),
        ("blobs", SCHEMA_BLOB_SUBMISSION),
    ):
        for sub in batch[kind]:
            sub_hash = submission_hash(sub)
            try:
                schemas.validate(schema_id, sub, what=f"{kind} submission")
                _check_id_kind(sub)
            except (CcfSchemaError, _BatchRejected) as exc:
                reason = getattr(exc, "reason", str(exc))
                result = _batch_result(batch["batch_id"], "rejected", archive_id, reason=reason)
                _record_batch_outcome(conn, batch, result, committed_sequence=None)
                return result
            pending.append((sub, kind[:-1], sub_hash))  # record/link/blob

    for sub, obj_kind, sub_hash in pending:
        object_id = sub["id"]

        # ID uniqueness (spec 2.2): existing ID + same submission hash is an
        # idempotent retry; different content is a hard conflict.
        existing = conn.execute(
            "SELECT submission_hash, object_hash FROM object_header WHERE id = %s",
            (object_id,),
        ).fetchone()
        if existing is not None:
            if existing[0] == sub_hash:
                outcomes.append(
                    _existing_outcome(conn, archive_id, object_id, existing[1])
                )
                admitted_ids.add(object_id)
                continue
            result = _batch_result(
                batch["batch_id"],
                "conflict",
                archive_id,
                reason=f"hard ID collision at {object_id}: same ID, different content",
            )
            _record_batch_outcome(conn, batch, result, committed_sequence=None)
            return result

        # Origin-tuple idempotency (spec 6.5).
        origin = sub.get("origin")
        if origin is not None:
            row = conn.execute(
                """
                SELECT submission_hash, object_id, lifecycle FROM origin_index
                WHERE archive_id = %s AND source_id = %s AND native_id = %s
                  AND revision = %s AND object_kind = %s
                """,
                (
                    archive_id,
                    origin["source_id"],
                    origin["native_id"],
                    origin["revision"],
                    obj_kind,
                ),
            ).fetchone()
            if row is not None:
                if row[0] == sub_hash:
                    existing_hash = conn.execute(
                        "SELECT object_hash FROM object_header WHERE id = %s",
                        (row[1],),
                    ).fetchone()[0]
                    outcomes.append(
                        _existing_outcome(
                            conn, archive_id, row[1], existing_hash, lifecycle=row[2]
                        )
                    )
                    admitted_ids.add(row[1])
                else:
                    conflicts += 1
                    outcomes.append(
                        _object_outcome(
                            object_id,
                            "origin_revision_conflict",
                            reason="origin tuple already admitted with different content",
                        )
                    )
                continue

        # Registry + payload validation and lineage declaration.
        try:
            resolved = _resolve_submission(
                conn,
                archive=archive,
                batch=batch,
                sub=sub,
                obj_kind=obj_kind,
                sub_hash=sub_hash,
                catalog=catalog,
                registries=registries,
                schemas=schemas,
                lineage_heads=lineage_heads,
                active_edges=active_edges,
                link_actions=link_actions,
                salt_fn=salt_fn,
                blob_bytes=blob_bytes,
            )
        except _BatchRejected as exc:
            result = _batch_result(batch["batch_id"], "rejected", archive_id, reason=exc.reason)
            _record_batch_outcome(conn, batch, result, committed_sequence=None)
            return result
        except _ObjectConflict as exc:
            conflicts += 1
            outcomes.append(
                _object_outcome(object_id, exc.status, reason=exc.reason)
            )
            continue
        admitted.append(resolved)
        outcomes.append(resolved)
        admitted_ids.add(object_id)

    # -- Pass 2: same-batch / archive reference completeness (spec 2.3). --
    try:
        _validate_references(conn, archive_id, admitted, admitted_ids)
    except _BatchRejected as exc:
        result = _batch_result(batch["batch_id"], "rejected", archive_id, reason=exc.reason)
        _record_batch_outcome(conn, batch, result, committed_sequence=None)
        return result

    # -- Pass 3: commit admitted objects (if any) and answer. --
    commit_sequence: int | None = None
    commit_hash: str | None = None
    if admitted:
        committed_at = clock()
        commit_sequence, commit_hash = commit_objects(
            conn,
            archive=archive,
            head=head,
            objects=admitted,
            catalog=catalog,
            registries=registries,
            signer=signer,
            committed_at=committed_at,
            salt_fn=salt_fn,
        )

    final: list[dict] = []
    position_of = {obj.object_id: i for i, obj in enumerate(admitted)}
    for entry in outcomes:
        if isinstance(entry, ResolvedObject):
            final.append(
                _object_outcome(
                    entry.object_id,
                    "admitted",
                    object_hash=entry.header["object_hash"],
                    commit_sequence=str(commit_sequence),
                    commit_position=position_of[entry.object_id],
                    current_lifecycle="active",
                    payload_available=entry.blob_data is not None
                    or entry.object_kind != "blob",
                )
            )
        else:
            final.append(entry)

    if conflicts:
        status = "partial" if admitted else "conflict"
    else:
        status = "committed"
    result = _batch_result(
        batch["batch_id"],
        status,
        archive_id,
        commit_sequence=str(commit_sequence) if commit_sequence is not None else None,
        commit_hash=commit_hash,
        admissions=final,
    )
    _record_batch_outcome(conn, batch, result, committed_sequence=commit_sequence)
    return result


class _ObjectConflict(Exception):
    """Internal: per-object conflict outcome (batch continues)."""

    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


def _check_id_kind(sub: dict) -> None:
    expected = {"record": "record", "link": "link", "blob": "blob"}[
        sub["submission_kind"]
    ]
    try:
        parsed = parse_id(sub["id"])
    except Exception as exc:
        raise _BatchRejected(f"malformed object ID {sub['id']!r}: {exc}") from exc
    if parsed.kind != expected:
        raise _BatchRejected(
            f"object ID kind {parsed.kind!r} does not match submission kind {expected!r}"
        )


def _existing_outcome(conn, archive_id, object_id, object_hash, lifecycle=None) -> dict:
    row = conn.execute(
        """
        SELECT commit_sequence, commit_position FROM admission
        WHERE archive_id = %s AND object_id = %s
        """,
        (archive_id, object_id),
    ).fetchone()
    if lifecycle is None:
        lifecycle_row = conn.execute(
            "SELECT lifecycle FROM origin_index WHERE archive_id = %s AND object_id = %s",
            (archive_id, object_id),
        ).fetchone()
        lifecycle = lifecycle_row[0] if lifecycle_row else "active"
    payload_row = conn.execute(
        """
        SELECT state FROM compartment
        WHERE object_id = %s AND compartment = 'semantic'
        """,
        (object_id,),
    ).fetchone()
    payload_available = payload_row is not None and payload_row[0] == "plaintext"
    return _object_outcome(
        object_id,
        "existing",
        object_hash=object_hash,
        commit_sequence=str(int(row[0])) if row else None,
        commit_position=int(row[1]) if row else None,
        current_lifecycle=lifecycle,
        payload_available=payload_available,
    )


def _resolve_submission(
    conn,
    *,
    archive: dict,
    batch: dict,
    sub: dict,
    obj_kind: str,
    sub_hash: str,
    catalog: SemanticCatalog,
    registries: PinnedRegistries,
    schemas: SchemaSet,
    lineage_heads: dict,
    active_edges: dict,
    link_actions: dict,
    salt_fn,
    blob_bytes: dict[str, bytes] | None,
) -> ResolvedObject:
    from ccf.registry import RegistryError

    try:
        if obj_kind == "record":
            return _resolve_record(
                conn,
                archive=archive,
                batch=batch,
                sub=sub,
                sub_hash=sub_hash,
                catalog=catalog,
                registries=registries,
                schemas=schemas,
                lineage_heads=lineage_heads,
                active_edges=active_edges,
                link_actions=link_actions,
                salt_fn=salt_fn,
            )
        if obj_kind == "link":
            return _resolve_link(
                conn,
                archive=archive,
                batch=batch,
                sub=sub,
                sub_hash=sub_hash,
                catalog=catalog,
                registries=registries,
                schemas=schemas,
                lineage_heads=lineage_heads,
                active_edges=active_edges,
                salt_fn=salt_fn,
            )
        return _resolve_blob(
            conn,
            archive=archive,
            batch=batch,
            sub=sub,
            sub_hash=sub_hash,
            catalog=catalog,
            registries=registries,
            schemas=schemas,
            lineage_heads=lineage_heads,
            salt_fn=salt_fn,
            blob_bytes=blob_bytes,
        )
    except RegistryError as exc:
        # Unknown type/Link/state machine: fail closed (spec 7.6).
        raise _BatchRejected(str(exc)) from exc


def _make_envelope(kind: str, compartment: str, content: dict, salt_fn) -> dict:
    return {
        "format": f"ccf.{kind}-{compartment}/0.1.1",
        "salt": salt_fn(),
        "content": content,
    }


def _make_header(kind: str, object_id: str, structural: dict, semantic: dict | None) -> dict:
    from ccf.hashing import compartment_commitment, object_hash

    fields = {
        "spec": CCF_SPEC,
        "object_kind": kind,
        "id": object_id,
        "hash_profile": "ccf-jcs-sha256-v2",
        "structural_commitment": compartment_commitment(kind, "structural", structural),
        "semantic_commitment": (
            compartment_commitment(kind, "semantic", semantic)
            if semantic is not None
            else None
        ),
    }
    return dict(fields, object_hash=object_hash(fields))


def _resolve_record(
    conn,
    *,
    archive,
    batch,
    sub,
    sub_hash,
    catalog,
    registries,
    schemas,
    lineage_heads,
    active_edges,
    link_actions,
    salt_fn,
) -> ResolvedObject:
    entry = registries.type_entry(sub["type"], sub["type_version"])
    payload_schema = entry.get("semantic_schema_id")
    if payload_schema:
        try:
            schemas.validate(payload_schema, sub["payload"], what=f"{sub['type']} payload")
        except CcfSchemaError as exc:
            raise _BatchRejected(str(exc)) from exc

    # Resolve the policy reference against the pre-transition lineage state
    # (a stateful Record's own transition must not become its own policy head).
    claims = sub["claims"]
    # Registry-declared authority classes are enforced at admission
    # (spec 5.5): fail closed per object when the claim does not satisfy
    # the type's required_authority.
    authority_reason = check_required_authority(
        entry.get("required_authority"),
        claim=claims.get("authority"),
        recorded_by=sub["recorded_by"],
        admitted_by_archive=False,
        registries=registries,
    )
    if authority_reason is not None:
        raise _ObjectConflict("rejected", authority_reason)
    policy_ref = None
    if claims.get("policy_hint") is not None:
        policy_ref = _resolve_policy_ref(
            conn, lineage_heads, claims["policy_hint"], archive["semantic_catalog_root"]
        )

    lineage_update: tuple[str, str] | None = None
    try:
        declared = declare_lineage(sub, type_entry=entry, registries=registries)
    except LineageDeclarationError as exc:
        raise _BatchRejected(str(exc)) from exc
    if declared is not None:
        machine_id, block = declared
        machine = registries.state_machine(machine_id)
        current = lineage_heads.get(block["lineage_id"])
        previous = block["previous_head_id"]
        if previous is None:
            if current is not None:
                raise _ObjectConflict(
                    "lineage_conflict",
                    f"lineage {block['lineage_id']} already has head "
                    f"{current['head_record_id']}",
                )
            reason = check_state_transition(
                machine, current_state=None, transition=block["transition"]
            )
        else:
            if current is None:
                raise _ObjectConflict(
                    "lineage_conflict",
                    f"lineage {block['lineage_id']} has no admitted head",
                )
            if current["head_record_id"] != previous:
                raise _ObjectConflict(
                    "lineage_conflict",
                    f"stale predecessor {previous}: current head is "
                    f"{current['head_record_id']}",
                )
            reason = check_state_transition(
                machine, current_state=current["state"], transition=block["transition"]
            )
        if reason is not None:
            raise _ObjectConflict("lineage_conflict", reason)
        lineage_update = (block["lineage_id"], block["transition"])

    visibility = sub["type_visibility"]
    structural_content = {
        "type": "sealed.record" if visibility == "sealed" else sub["type"],
        "type_version": sub["type_version"],
        "type_visibility": visibility,
        "schema_digest": catalog.schema_digest(entry["semantic_schema_id"]),
        "registry_entry_digest": registries.entry_digest(entry),
        "retention_profile": sub["retention_profile_hint"],
        "structural_payload": _structural_payload_for(sub, entry),
        "extensions": {},
    }
    if declared is not None:
        structural_content["lineage"] = sub["lineage"]

    semantic_content = {
        "recorded_by": sub["recorded_by"],
        "recorded_at": sub["recorded_at"],
        "claimed": claims,
        "producer_evidence": _producer_evidence(batch, sub_hash),
        "payload": sub["payload"],
        "extensions": sub["extensions"],
    }
    if claims.get("person_id") is not None:
        semantic_content["person_id"] = claims["person_id"]
    if claims.get("perspective_id") is not None:
        semantic_content["perspective_id"] = claims["perspective_id"]
    if sub.get("occurred_at") is not None:
        semantic_content["occurred_at"] = sub["occurred_at"]
    if sub.get("origin") is not None:
        semantic_content["origin"] = {**sub["origin"], "submission_hash": sub_hash}
    if claims.get("privacy") is not None:
        semantic_content["privacy"] = claims["privacy"]
    if claims.get("authority") is not None:
        semantic_content["authority"] = claims["authority"]
    if claims.get("policy_hint") is not None:
        semantic_content["policy_ref"] = policy_ref

    try:
        schemas.validate(
            SCHEMA_RECORD_STRUCTURAL, structural_content, what="record structural content"
        )
        schemas.validate(
            SCHEMA_RECORD_SEMANTIC, semantic_content, what="record semantic content"
        )
    except CcfSchemaError as exc:
        raise _BatchRejected(str(exc)) from exc

    # Link dispositions take effect on the active-edge working set.
    if sub["type"] == "lineage.link_disposition":
        _apply_disposition(
            conn,
            archive_id=archive["archive_id"],
            sub=sub,
            active_edges=active_edges,
            link_actions=link_actions,
            acyclic_types=registries.acyclic_link_types(),
        )

    structural = _make_envelope("record", "structural", structural_content, salt_fn)
    semantic = _make_envelope("record", "semantic", semantic_content, salt_fn)
    header = _make_header("record", sub["id"], structural, semantic)
    if lineage_update is not None:
        lineage_heads[lineage_update[0]] = {
            "head_record_id": sub["id"],
            "head_record_hash": header["object_hash"],
            "state": lineage_update[1],
        }
    return ResolvedObject(
        object_kind="record",
        object_id=sub["id"],
        header=header,
        structural=structural,
        semantic=semantic,
        submission_hash=sub_hash,
        origin=sub.get("origin"),
        lineage_update=lineage_update,
        blob_data=None,
    )


def _structural_payload_for(sub: dict, entry: dict) -> dict:
    """Structurally retained payload fields (spec 8.4 for dispositions)."""
    if sub["type"] == "lineage.link_disposition":
        payload = sub["payload"]
        return {
            "target_link_id": payload["target_link_id"],
            "action": payload["action"],
            "predecessor_id": sub["lineage"]["previous_head_id"],
            "replacement_link_id": payload.get("replacement_link_id"),
            "terminal": payload["action"] == "tombstone",
        }
    return {}


def _apply_disposition(
    conn, *, archive_id, sub, active_edges, link_actions, acyclic_types
) -> None:
    payload = sub["payload"]
    target = payload["target_link_id"]
    action = payload["action"]
    endpoints = _link_endpoints(conn, archive_id, target, pending=None)
    if action in DEACTIVATING_ACTIONS:
        link_actions[target] = action
        if endpoints and _is_acyclic(conn, archive_id, target, acyclic_types):
            remove_edge(active_edges, endpoints[0], endpoints[1])
    elif action == "restore":
        if endpoints and _is_acyclic(conn, archive_id, target, acyclic_types):
            if creates_cycle(active_edges, endpoints[0], endpoints[1]):
                raise _ObjectConflict(
                    "rejected",
                    f"restoring Link {target} would create a derivation cycle",
                )
            add_edge(active_edges, endpoints[0], endpoints[1])
        link_actions[target] = action
    else:  # invalidate_selector: edge stays active
        link_actions[target] = action


def _link_endpoints(conn, archive_id, link_id, pending) -> tuple[str, str] | None:
    row = conn.execute(
        """
        SELECT plaintext_json ->> 'from_id', plaintext_json ->> 'to_id'
        FROM compartment
        WHERE object_id = %s AND compartment = 'structural' AND state = 'plaintext'
        """,
        (link_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def _is_acyclic(conn, archive_id, link_id, acyclic_types) -> bool:
    row = conn.execute(
        """
        SELECT plaintext_json ->> 'type' FROM compartment
        WHERE object_id = %s AND compartment = 'structural' AND state = 'plaintext'
        """,
        (link_id,),
    ).fetchone()
    return row is not None and row[0] in acyclic_types


def _resolve_link(
    conn,
    *,
    archive,
    batch,
    sub,
    sub_hash,
    catalog,
    registries,
    schemas,
    lineage_heads,
    active_edges,
    salt_fn,
) -> ResolvedObject:
    entry = registries.link_entry(sub["type"], sub["type_version"])
    visibility = sub["type_visibility"]
    structural_content = {
        "type": "sealed.link" if visibility == "sealed" else sub["type"],
        "type_version": sub["type_version"],
        "type_visibility": visibility,
        "schema_digest": catalog.schema_digest(SCHEMA_LINK_SEMANTIC_CONTENT),
        "registry_entry_digest": registries.entry_digest(entry),
        "retention_profile": entry["retention_profile"],
        "structural_payload": {},
        "extensions": {},
    }
    claims = sub["claims"]
    semantic_content = {
        "recorded_by": sub["recorded_by"],
        "recorded_at": sub["recorded_at"],
        "claimed": claims,
        "producer_evidence": _producer_evidence(batch, sub_hash),
        "payload": sub["payload"],
        "extensions": sub["extensions"],
    }
    if entry["endpoints_location"] == "structural":
        structural_content["from_id"] = sub["from_id"]
        structural_content["to_id"] = sub["to_id"]
    else:
        semantic_content["endpoints"] = {"from_id": sub["from_id"], "to_id": sub["to_id"]}
    if sub.get("selector") is not None:
        semantic_content["selector"] = sub["selector"]
    if claims.get("privacy") is not None:
        semantic_content["privacy"] = claims["privacy"]
    if claims.get("authority") is not None:
        semantic_content["authority"] = claims["authority"]
    if claims.get("policy_hint") is not None:
        semantic_content["policy_ref"] = _resolve_policy_ref(
            conn, lineage_heads, claims["policy_hint"], archive["semantic_catalog_root"]
        )

    try:
        schemas.validate(
            SCHEMA_LINK_STRUCTURAL, structural_content, what="link structural content"
        )
        schemas.validate(SCHEMA_LINK_SEMANTIC, semantic_content, what="link semantic content")
    except CcfSchemaError as exc:
        raise _BatchRejected(str(exc)) from exc

    if entry.get("acyclic") and entry["endpoints_location"] == "structural":
        if creates_cycle(active_edges, sub["from_id"], sub["to_id"]):
            raise _ObjectConflict(
                "rejected",
                f"Link {sub['id']} ({sub['type']}) would create a derivation cycle",
            )
        add_edge(active_edges, sub["from_id"], sub["to_id"])

    structural = _make_envelope("link", "structural", structural_content, salt_fn)
    semantic = _make_envelope("link", "semantic", semantic_content, salt_fn)
    header = _make_header("link", sub["id"], structural, semantic)
    return ResolvedObject(
        object_kind="link",
        object_id=sub["id"],
        header=header,
        structural=structural,
        semantic=semantic,
        submission_hash=sub_hash,
        origin=None,
        lineage_update=None,
        blob_data=None,
    )


def _resolve_blob(
    conn,
    *,
    archive,
    batch,
    sub,
    sub_hash,
    catalog,
    registries,
    schemas,
    lineage_heads,
    salt_fn,
    blob_bytes,
) -> ResolvedObject:
    entry = registries.blob_entry
    structural_content = {
        "type": "blob.manifest",
        "type_version": 1,
        "type_visibility": "clear",
        "schema_digest": catalog.schema_digest(entry["semantic_schema_id"]),
        "registry_entry_digest": registries.entry_digest(entry),
        "retention_profile": sub["retention_profile_hint"],
        "media_type": sub["media_type"],
        "byte_length": sub["byte_length"],
        "content_commitment": sub["content_commitment"],
        "content_profile": sub["content_profile"],
        "availability_class": "controlled",
        "erasure_domain_id": archive["erasure_domain_id"],
        "structural_payload": {},
        "extensions": {},
    }

    data = (blob_bytes or {}).get(sub["id"])
    if data is not None:
        if str(len(data)) != sub["byte_length"]:
            raise _BatchRejected(
                f"blob {sub['id']} byte_length {sub['byte_length']} != "
                f"transferred {len(data)}"
            )
        if blob_content_commitment(sub["content_salt"], data) != sub["content_commitment"]:
            raise _BatchRejected(f"blob {sub['id']} content commitment mismatch")

    claims = sub["claims"]
    semantic_content = {
        "content_salt": sub["content_salt"],
        "producer_evidence": _producer_evidence(batch, sub_hash),
        "content_encryption_profile": "none",
        "content_key_ref": None,
        "extensions": sub["extensions"],
    }
    transfer = next(
        (t for t in batch.get("blob_transfers", []) if t["blob_id"] == sub["id"]), None
    )
    if transfer is not None:
        semantic_content["filename"] = transfer["transfer_ref"]
    if sub.get("origin") is not None:
        semantic_content["origin"] = {**sub["origin"], "submission_hash": sub_hash}
    if claims.get("privacy") is not None:
        semantic_content["privacy"] = claims["privacy"]
    if claims.get("policy_hint") is not None:
        semantic_content["policy_ref"] = _resolve_policy_ref(
            conn, lineage_heads, claims["policy_hint"], archive["semantic_catalog_root"]
        )

    try:
        schemas.validate(
            SCHEMA_BLOB_STRUCTURAL, structural_content, what="blob structural content"
        )
        schemas.validate(SCHEMA_BLOB_SEMANTIC, semantic_content, what="blob semantic content")
    except CcfSchemaError as exc:
        raise _BatchRejected(str(exc)) from exc

    structural = _make_envelope("blob", "structural", structural_content, salt_fn)
    semantic = _make_envelope("blob", "semantic", semantic_content, salt_fn)
    header = _make_header("blob", sub["id"], structural, semantic)
    return ResolvedObject(
        object_kind="blob",
        object_id=sub["id"],
        header=header,
        structural=structural,
        semantic=semantic,
        submission_hash=sub_hash,
        origin=sub.get("origin"),
        lineage_update=None,
        blob_data=data,
    )


def _validate_references(conn, archive_id, admitted: list[ResolvedObject], known: set[str]) -> None:
    """Every required reference must resolve to the archive or this batch."""

    def exists(object_id: str) -> bool:
        if object_id in known:
            return True
        row = conn.execute(
            "SELECT 1 FROM object_header WHERE id = %s", (object_id,)
        ).fetchone()
        return row is not None

    for obj in admitted:
        content = obj.structural["content"]
        refs: list[str] = []
        if obj.object_kind == "link" and "from_id" in content:
            refs.extend([content["from_id"], content["to_id"]])
        semantic = obj.semantic["content"] if obj.semantic else {}
        if semantic.get("origin"):
            refs.append(semantic["origin"]["source_id"])
        if semantic.get("recorded_by"):
            refs.append(semantic["recorded_by"])
        claimed = semantic.get("claimed") or {}
        for key in ("person_id", "perspective_id"):
            if claimed.get(key):
                refs.append(claimed[key])
        if content.get("lineage") and content["lineage"].get("previous_head_id"):
            refs.append(content["lineage"]["previous_head_id"])
        if content.get("type") == "lineage.link_disposition":
            payload = content["structural_payload"]
            refs.append(payload["target_link_id"])
            if payload.get("replacement_link_id"):
                refs.append(payload["replacement_link_id"])
        for ref in refs:
            if not exists(ref):
                raise _BatchRejected(
                    f"object {obj.object_id} references unknown ID {ref}: neither "
                    "admitted nor present in the atomic batch"
                )


def _record_batch_outcome(conn, batch: dict, result: dict, *, committed_sequence) -> None:
    """Upsert the spool/receipt row with the batch's terminal outcome."""
    batch_id = batch.get("batch_id")
    required = (
        "producer_id",
        "producer_sequence",
        "previous_batch_hash",
        "credential_id",
        "created_at",
        "semantic_catalog_root",
        "batch_hash",
        "signature",
    )
    # A batch too malformed to persist (rejected before schema validation
    # could pass) is still answered; there is simply no well-formed row to
    # store for it.
    if not batch_id or not all(key in batch for key in required):
        return
    existing = conn.execute(
        "SELECT 1 FROM producer_batch WHERE batch_id = %s", (batch_id,)
    ).fetchone()
    if existing is None:
        # Batch arrived from elsewhere: persist the received envelope too.
        conn.execute(
            """
            INSERT INTO producer_batch (
                batch_id, producer_id, producer_sequence, previous_batch_hash,
                credential_id, created_at, semantic_catalog_root, batch_hash,
                signature, batch_json, status, spooled_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'verifying', %s)
            ON CONFLICT (batch_id) DO NOTHING
            """,
            (
                batch_id,
                batch["producer_id"],
                int(batch["producer_sequence"]),
                batch["previous_batch_hash"],
                batch["credential_id"],
                batch["created_at"],
                batch["semantic_catalog_root"],
                batch["batch_hash"],
                decode_b64url(batch["signature"]),
                Jsonb(batch),
                batch["created_at"],
            ),
        )
    conn.execute(
        """
        UPDATE producer_batch
        SET status = %s, committed_sequence = %s, result_json = %s
        WHERE batch_id = %s
        """,
        (result["status"], committed_sequence, Jsonb(result), batch_id),
    )
