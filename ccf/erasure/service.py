"""The erasure saga (spec 3.8) as a durable state machine.

Stages, one database transaction each, durable state in
``erasure_operation``:

1. **request** — a ``governance.erasure_request`` Record admitted through
   the canonical path (:meth:`ErasureService.submit_request`);
2. **decision** — a ``governance.erasure_decision`` Record (lineage
   ``ccf.state.erasure-v1``) plus the durable operation row
   (:meth:`ErasureService.decide`); retention profiles are enforced here,
   fail closed;
3. **block** — target compartments and Blob bytes flip to ``erased``
   (content and salts destroyed), origin lifecycles update, suppression
   commitments are written, and Link dispositions
   (``invalidate_selector`` / terminal ``tombstone``) are admitted.
   Ordinary reads are blocked from this point: every read path serves
   plaintext only;
4. **destroy** — controlled copies purged: projection rows, checkpoints,
   egress capabilities, generated wiki plaintext; the purge list is
   recorded on the operation row;
5. **verify** — affected projections rebuild from canonical state and the
   archive asserts zero remaining controlled copies;
6. **receipt** — a ``lineage.erasure_receipt`` Record with ``ccf.covers``
   membership Links, admitted with ``authorized_erasure_worker``
   authority in the same transaction that marks the operation complete.

A crash after ``block`` but before ``receipt`` resumes from the durable
row (:meth:`ErasureService.resume_pending`) and never reports the content
recoverable — the salts are already gone.

Assurance honesty (spec 3.7): only ``logical`` is recorded. This
implementation does not verify WAL/PITR, replicas, or backups (no
``storage_verified`` claim) and does not implement per-object DEKs (no
``cryptographic`` claim).
"""

from __future__ import annotations

from pathlib import Path

from ccf.admission import commit_objects, load_archive, lock_archive_head
from ccf.db import open_ccf_connection
from ccf.erasure import operations, purge, receipts, retention, suppression
from ccf.erasure.errors import ErasureError
from ccf.erasure.media import decide_multi_subject
from ccf.ids import generate_id
from ccf.lineage import load_lineage_heads
from ccf.projections import EMBEDDING, WIKI

SCHEMA_REQUEST_PAYLOAD = "urn:ccf:schema:0.1.2:payload.governance.erasure_request"
SCHEMA_DECISION_PAYLOAD = "urn:ccf:schema:0.1.2:payload.governance.erasure_decision"
SCHEMA_DISPOSITION_PAYLOAD = "urn:ccf:schema:0.1.2:payload.lineage.link_disposition"
SCHEMA_DISPOSITION_STRUCTURAL = "urn:ccf:schema:0.1.2:structural.lineage.link_disposition"

_REQUEST_TYPE = "governance.erasure_request"
_DECISION_TYPE = "governance.erasure_decision"
_DISPOSITION_TYPE = "lineage.link_disposition"

#: Lineage transition each stage marker carries on the decision lineage
#: (``ccf.state.erasure-v1``).
_STAGE_TRANSITIONS = {
    "block": "block",
    "destroy": "destroy",
    "verify": "verify",
    "receipt": "verified",
}


class ErasureService:
    """Retention and deletion facade for one archive (``Archive.erasure()``)."""

    def __init__(self, archive, *, wiki_staging_dir: str | Path | None = None) -> None:
        self._archive = archive
        self._wiki_staging_dir = (
            Path(wiki_staging_dir) if wiki_staging_dir is not None else None
        )
        self._suppression_key = suppression.load_suppression_key(archive.settings)

    @classmethod
    def from_archive(cls, archive, **kwargs) -> "ErasureService":
        return cls(archive, **kwargs)

    # ------------------------------------------------------------------
    # Internal: canonical admission inside a caller-owned transaction
    # ------------------------------------------------------------------

    def _commit_canonical(
        self, conn, record_specs: list[dict], extra_resolved: list | None = None
    ) -> list:
        """Commit operator Records through the canonical path on ``conn``.

        Same machinery as ``Archive.admit_bootstrap`` (serialized head
        lock, registry authority checks, signed commit, fences and
        invalidation effects), but inside the caller's transaction so the
        saga's durable state and the canonical Records commit atomically.
        ``extra_resolved`` carries pre-resolved objects (membership Links)
        into the same commit.
        """
        archive_row = load_archive(conn, self._archive.archive_id)
        head = lock_archive_head(conn, self._archive.archive_id)
        lineage_heads = load_lineage_heads(conn, self._archive.archive_id)
        resolved = [
            self._archive._resolve_bootstrap_record(
                conn, archive_row, spec, lineage_heads
            )
            for spec in record_specs
        ]
        resolved.extend(extra_resolved or [])
        commit_objects(
            conn,
            archive=archive_row,
            head=head,
            objects=resolved,
            catalog=self._archive.catalog,
            registries=self._archive.registries,
            signer=self._archive._signer,
            committed_at=self._archive.clock(),
            salt_fn=self._archive._salt_fn,
        )
        return resolved

    def _decision_payload(
        self,
        row_or_request: dict,
        *,
        decision: str,
        reasoning: str,
        decided_at: str,
        stage: str | None = None,
    ) -> dict:
        payload = {
            "request_id": row_or_request["request_id"],
            "decision": decision,
            "approved_scope": row_or_request["approved_scope"],
            "reasoning": reasoning,
            "competing_obligations": row_or_request["competing_obligations"],
            "approved_operations": row_or_request["approved_operations"],
            "decided_at": decided_at,
            "extensions": {"stage": stage} if stage else {},
        }
        self._archive.schemas.validate(
            SCHEMA_DECISION_PAYLOAD, payload, what="erasure decision payload"
        )
        return payload

    def _stage_marker_spec(self, row: dict, stage: str, now: str) -> dict:
        """A ``governance.erasure_decision`` stage marker on the lineage."""
        payload = self._decision_payload(
            {
                "request_id": row["request_id"],
                "approved_scope": {"targets": row["targets"]},
                "competing_obligations": [],
                "approved_operations": [],
            },
            decision=row["decision"],
            reasoning=f"erasure saga stage: {stage}",
            decided_at=now,
            stage=stage,
        )
        return {
            "type": _DECISION_TYPE,
            "recorded_by": row["actor"]["recorded_by"],
            "recorded_at": now,
            "authority": row["actor"]["authority"],
            "payload": payload,
            "lineage": {
                "lineage_id": row["lineage_id"],
                "previous_head_id": row["stage_head_id"],
                "transition": _STAGE_TRANSITIONS[stage],
                "valid_from": now,
                "expires_at": None,
            },
        }

    # ------------------------------------------------------------------
    # Stage 1: request
    # ------------------------------------------------------------------

    def submit_request(
        self,
        *,
        requester_id: str,
        subject_id: str,
        requested_scope: dict,
        reason: str,
        authority: dict,
        authentication_evidence_refs: list[str] | None = None,
    ) -> dict:
        """Admit a ``governance.erasure_request`` Record (saga stage 1).

        ``authority`` must satisfy the type's ``required_authority``
        (``subject_or_authorized_representative``).
        """
        now = self._archive.clock()
        payload = {
            "requester_id": requester_id,
            "subject_id": subject_id,
            "requested_scope": requested_scope,
            "reason": reason,
            "requested_at": now,
            "authentication_evidence_refs": list(authentication_evidence_refs or []),
            "extensions": {},
        }
        self._archive.schemas.validate(
            SCHEMA_REQUEST_PAYLOAD, payload, what="erasure request payload"
        )
        spec = {
            "type": _REQUEST_TYPE,
            "recorded_by": requester_id,
            "recorded_at": now,
            "authority": authority,
            "payload": payload,
        }
        with open_ccf_connection(self._archive.settings) as conn:
            with conn.transaction():
                resolved = self._commit_canonical(conn, [spec])
        return {"request_id": resolved[0].object_id}

    # ------------------------------------------------------------------
    # Stage 2: decision (+ retention enforcement, durable operation row)
    # ------------------------------------------------------------------

    def decide(
        self,
        *,
        request_id: str,
        decision: str,
        targets: list[dict] | None = None,
        reasoning: str,
        decided_by: str,
        authority: dict,
        assurance: str = "logical",
        competing_obligations: list[str] | None = None,
        approved_operations: list[str] | None = None,
        authorized_producers: list[str] | None = None,
    ) -> dict:
        """Admit the erasure decision and open the durable operation.

        Retention profiles are enforced here (fail closed): a target
        beyond its profile raises :class:`RetentionViolation` before
        anything is admitted. Only ``assurance="logical"`` is supported —
        see the module docstring.
        """
        if assurance != "logical":
            raise ErasureError(
                f"assurance {assurance!r} is not attainable: this deployment "
                "verifies the controlled envelope only (no WAL/PITR, replica, "
                "or backup verification) and implements no per-object DEKs"
            )
        if decision in ("approve", "partial") and not targets:
            raise ErasureError(f"decision {decision!r} requires targets")
        if decision not in ("approve", "deny", "restrict", "defer", "partial"):
            raise ErasureError(f"unknown erasure decision {decision!r}")
        if (
            decision in ("approve", "partial")
            and targets
            and self._suppression_key is None
        ):
            # 0.1.2 (spec 12.7): suppression commitments are canonical,
            # journal-covered erasure lineage, and the receipt schema
            # requires the suppression commitment — erasure without a
            # suppression key cannot complete, so fail before anything is
            # admitted.
            raise ErasureError(
                "erasure requires a configured suppression key "
                "(database.ccf_archive.suppression_key_path or "
                "THOTH_CCF_SUPPRESSION_KEY): canonical suppression lineage "
                "is mandatory (spec 12.7)"
            )

        now = self._archive.clock()
        lineage_id = generate_id("lineage")
        scope_targets = list(targets or [])
        payload = self._decision_payload(
            {
                "request_id": request_id,
                "approved_scope": {"targets": scope_targets},
                "competing_obligations": list(competing_obligations or []),
                "approved_operations": list(
                    approved_operations
                    or ["block", "invalidate_selector", "suppress_origin"]
                ),
            },
            decision=decision,
            reasoning=reasoning,
            decided_at=now,
        )
        spec = {
            "type": _DECISION_TYPE,
            "recorded_by": decided_by,
            "recorded_at": now,
            "authority": authority,
            "payload": payload,
            "lineage": {
                "lineage_id": lineage_id,
                "previous_head_id": None,
                "transition": decision,
                "valid_from": now,
                "expires_at": None,
            },
        }
        operation_id = generate_id("record")
        plans: list[dict] = []
        with open_ccf_connection(self._archive.settings) as conn:
            with conn.transaction():
                plans = retention.plan_targets(
                    conn,
                    archive_id=self._archive.archive_id,
                    targets=scope_targets,
                    registries=self._archive.registries,
                ) if scope_targets else []
                resolved = self._commit_canonical(conn, [spec])
                if decision in ("approve", "partial"):
                    operations.create_operation(
                        conn,
                        schemas=self._archive.schemas,
                        archive_id=self._archive.archive_id,
                        operation_id=operation_id,
                        request_id=request_id,
                        decision_id=resolved[0].object_id,
                        plans=plans,
                        assurance=assurance,
                        lineage_id=lineage_id,
                        decision=decision,
                        actor={"recorded_by": decided_by, "authority": authority},
                        authorized_producers=list(authorized_producers or []),
                        now=now,
                    )
        return {
            "decision_id": resolved[0].object_id,
            "operation_id": (
                operation_id if decision in ("approve", "partial") else None
            ),
            "lineage_id": lineage_id,
            "targets": [plan["object_id"] for plan in plans],
        }

    # ------------------------------------------------------------------
    # Stages 3-6: advance / execute / resume
    # ------------------------------------------------------------------

    def advance(self, operation_id: str) -> dict:
        """Run exactly one saga stage in its own transaction.

        On failure the partial stage rolls back, the durable row records
        ``failed`` with the error, and the exception propagates.
        """
        settings = self._archive.settings
        with open_ccf_connection(settings) as conn:
            with conn.transaction():
                row = operations.lock_operation(conn, operation_id)
                stage = operations.next_stage(row["stage"])
            try:
                with conn.transaction():
                    row = operations.lock_operation(conn, operation_id)
                    self._run_stage(conn, row, stage)
                    operations.advance_stage(
                        conn,
                        schemas=self._archive.schemas,
                        operation_id=operation_id,
                        stage=stage,
                        now=self._archive.clock(),
                    )
            except Exception as exc:
                with conn.transaction():
                    operations.record_failure(
                        conn,
                        operation_id=operation_id,
                        error=f"{stage}: {exc}",
                        now=self._archive.clock(),
                    )
                raise
        return self.status(operation_id)

    def execute(self, operation_id: str) -> dict:
        """Advance the saga to a terminal stage; returns the final status."""
        while True:
            status = self.status(operation_id)
            if status["stage"] in operations.TERMINAL_STAGES:
                return status
            self.advance(operation_id)

    def resume_pending(self) -> list[dict]:
        """Resume every non-terminal operation (crash recovery).

        Per-operation failures are surfaced in the returned list, never
        swallowed; other pending operations still resume.
        """
        with open_ccf_connection(self._archive.settings) as conn:
            pending = operations.pending_operations(conn, self._archive.archive_id)
        results = []
        for row in pending:
            try:
                results.append(self.execute(row["operation_id"]))
            except Exception as exc:
                results.append(
                    {
                        "operation_id": row["operation_id"],
                        "stage": "failed",
                        "content_recoverable": False,
                        "error": str(exc),
                    }
                )
        return results

    def status(self, operation_id: str) -> dict:
        """Honest operation status (see :func:`operations.status_of`)."""
        with open_ccf_connection(self._archive.settings) as conn:
            row = operations.load_operation(conn, operation_id)
        status = operations.status_of(row)
        status["request_id"] = row["request_id"]
        return status

    # ------------------------------------------------------------------
    # Stage bodies (run inside ``advance``'s stage transaction)
    # ------------------------------------------------------------------

    def _run_stage(self, conn, row: dict, stage: str) -> None:
        handler = {
            "block": self._stage_block,
            "destroy": self._stage_destroy,
            "verify": self._stage_verify,
            "receipt": self._stage_receipt,
        }.get(stage)
        if handler is None:
            raise ErasureError(f"no stage handler for {stage!r}")
        handler(conn, row)

    def _stage_block(self, conn, row: dict) -> None:
        """Destroy envelope content/salts; block ordinary reads at once."""
        now = self._archive.clock()
        for plan in row["plans"]:
            object_id = plan["object_id"]
            for compartment in ("structural", "semantic"):
                if not plan[f"erase_{compartment}"]:
                    continue
                cursor = conn.execute(
                    """
                    UPDATE compartment
                    SET state = 'erased', format = NULL, salt = NULL,
                        plaintext_json = NULL, ciphertext = NULL,
                        ciphertext_digest = NULL, storage_ref = NULL,
                        updated_at = %s
                    WHERE object_id = %s AND compartment = %s
                      AND state IN ('plaintext', 'encrypted', 'withheld')
                    """,
                    (now, object_id, compartment),
                )
                if cursor.rowcount == 0:
                    state = conn.execute(
                        """
                        SELECT state FROM compartment
                        WHERE object_id = %s AND compartment = %s
                        """,
                        (object_id, compartment),
                    ).fetchone()
                    if state is None or state[0] != "erased":
                        raise ErasureError(
                            f"cannot erase {compartment} of {object_id}: "
                            f"state {state[0] if state else 'missing'}"
                        )
            if plan["erase_content"]:
                # Blob content salt is erased with the bytes (spec 4.4).
                conn.execute(
                    """
                    UPDATE blob_content
                    SET state = 'erased', plaintext_bytes = NULL,
                        content_salt = NULL, storage_ref = NULL, updated_at = %s
                    WHERE blob_id = %s AND state <> 'erased'
                    """,
                    (now, object_id),
                )
            conn.execute(
                """
                UPDATE origin_index SET lifecycle = 'erased'
                WHERE archive_id = %s AND object_id = %s
                """,
                (row["archive_id"], object_id),
            )

        if row["plans"]:
            # Canonical suppression (spec 12.7, 0.1.2): the keyed
            # tokens are committed as a governed Blob plus a canonical
            # ``lineage.suppression_set`` Record — journal-covered in this
            # same commit — and the lookup rows become a rebuildable
            # projection of that lineage. The key is guaranteed by
            # ``decide``; double-check rather than skip (fail closed).
            if self._suppression_key is None:
                raise ErasureError(
                    "suppression key missing at block stage; refusing to "
                    "erase without canonical suppression lineage"
                )
            from ccf.erasure import suppression_set

            kind_tokens: list[tuple[str, str]] = []
            seen_tokens: set[str] = set()
            for plan in row["plans"]:
                for kind, token in suppression.tokens_for_plan(
                    self._suppression_key, plan
                ):
                    if token not in seen_tokens:
                        seen_tokens.add(token)
                        kind_tokens.append((kind, token))
            tokens = [token for _, token in kind_tokens]
            set_record_id = generate_id("record")
            blob_id = generate_id("blob")
            archive_row = load_archive(conn, self._archive.archive_id)
            blob_resolved = suppression_set.build_suppression_blob(
                tokens,
                blob_id=blob_id,
                archive=archive_row,
                catalog=self._archive.catalog,
                registries=self._archive.registries,
                schemas=self._archive.schemas,
                salt_fn=self._archive._salt_fn,
            )
            set_spec = suppression_set.build_suppression_set_spec(
                tokens,
                record_id=set_record_id,
                blob_id=blob_id,
                plans=row["plans"],
                worker_id=row["actor"]["recorded_by"],
                authority=row["actor"]["authority"],
                recorded_at=now,
                schemas=self._archive.schemas,
            )
        record_specs = [self._stage_marker_spec(row, "block", now)]
        record_specs.extend(self._disposition_specs(conn, row, now))
        if row["plans"]:
            record_specs.append(set_spec)
            resolved = self._commit_canonical(conn, record_specs, [blob_resolved])
            count = suppression.record_suppression(
                conn,
                archive_id=row["archive_id"],
                operation_id=row["operation_id"],
                set_record_id=set_record_id,
                blob_id=blob_id,
                key_profile_id=suppression_set.KEY_PROFILE_ID,
                kind_tokens=kind_tokens,
                authorized_producers=row["authorized_producers"],
                created_at=now,
            )
            operations.record_suppression_set(
                conn,
                operation_id=row["operation_id"],
                descriptor=suppression_set.set_descriptor(set_spec, blob_id),
                now=now,
            )
            operations.record_purged(
                conn,
                operation_id=row["operation_id"],
                purged=[{"store": "suppression_entry", "rows": count}],
                now=now,
            )
        else:
            resolved = self._commit_canonical(conn, record_specs)
        operations.record_stage_head(
            conn,
            operation_id=row["operation_id"],
            stage_head_id=resolved[0].object_id,
            now=now,
        )

    def _disposition_specs(self, conn, row: dict, now: str) -> list[dict]:
        """Link disposition Records for erased Links (spec 8.4).

        Full structural erasure of a Link is a terminal ``tombstone``;
        selector (semantic) erasure of a still-active Link is an
        ``invalidate_selector``. A Link already under a disposition
        lineage is tombstoned through that lineage when the machine
        permits; selector invalidation is skipped when the Link is already
        inactive or already selector-invalidated.
        """
        specs: list[dict] = []
        for plan in row["plans"]:
            if plan["object_kind"] != "link":
                continue
            if plan["erase_structural"]:
                action = "tombstone"
            elif plan["erase_semantic"]:
                action = "invalidate_selector"
            else:
                continue
            link_id = plan["object_id"]
            existing = conn.execute(
                """
                SELECT lh.lineage_id, lh.head_record_id, lh.state
                FROM lineage_head lh
                JOIN compartment c
                  ON c.object_id = lh.head_record_id AND c.compartment = 'structural'
                WHERE lh.archive_id = %s
                  AND c.state = 'plaintext'
                  AND c.plaintext_json ->> 'type' = 'lineage.link_disposition'
                  AND c.plaintext_json -> 'structural_payload' ->> 'target_link_id' = %s
                ORDER BY lh.head_commit_sequence DESC
                LIMIT 1
                """,
                (row["archive_id"], link_id),
            ).fetchone()
            if existing is not None:
                lineage_id, previous_head_id, state = existing
                if state == "tombstone":
                    continue  # already terminal
                if action == "invalidate_selector" and state != "restore":
                    continue  # inactive or already selector-invalidated
            else:
                lineage_id = generate_id("lineage")
                previous_head_id = None
            payload = {
                "target_link_id": link_id,
                "action": action,
                "reason": f"erasure operation {row['operation_id']}",
                "previous_disposition_id": previous_head_id,
                "replacement_link_id": None,
                "extensions": {},
            }
            structural_payload = {
                "target_link_id": link_id,
                "action": action,
                "previous_disposition_id": previous_head_id,
                "replacement_link_id": None,
                "terminal": action == "tombstone",
            }
            self._archive.schemas.validate(
                SCHEMA_DISPOSITION_PAYLOAD, payload, what="link disposition payload"
            )
            self._archive.schemas.validate(
                SCHEMA_DISPOSITION_STRUCTURAL,
                structural_payload,
                what="link disposition structural payload",
            )
            specs.append(
                {
                    "type": _DISPOSITION_TYPE,
                    "recorded_by": row["actor"]["recorded_by"],
                    "recorded_at": now,
                    "authority": row["actor"]["authority"],
                    "payload": payload,
                    "structural_payload": structural_payload,
                    "lineage": {
                        "lineage_id": lineage_id,
                        "previous_head_id": previous_head_id,
                        "transition": action,
                        "valid_from": now,
                        "expires_at": None,
                    },
                }
            )
        return specs

    def _stage_destroy(self, conn, row: dict) -> None:
        """Purge controlled copies; record the purge list durably."""
        now = self._archive.clock()
        plans = row["plans"]
        purged, _affected = purge.purge_controlled_copies(
            conn, archive_id=row["archive_id"], plans=plans
        )
        record_ids = [
            plan["object_id"] for plan in plans if plan["object_kind"] == "record"
        ]
        if record_ids and self._wiki_staging_dir is not None:
            removed = purge.purge_wiki_staging(self._wiki_staging_dir, record_ids)
            purged.append({"store": "wiki_staging", "files": removed})
        operations.record_purged(
            conn, operation_id=row["operation_id"], purged=purged, now=now
        )
        marker = self._stage_marker_spec(row, "destroy", now)
        resolved = self._commit_canonical(conn, [marker])
        operations.record_stage_head(
            conn,
            operation_id=row["operation_id"],
            stage_head_id=resolved[0].object_id,
            now=now,
        )

    def _stage_verify(self, conn, row: dict) -> None:
        """Rebuild affected projections; assert zero controlled copies left."""
        now = self._archive.clock()
        plans = row["plans"]
        affected = purge.affected_projections(plans)
        from ccf.projections.rebuild import rebuild_projection

        for name in sorted(affected):
            if name in (EMBEDDING, WIKI):
                continue  # not table-rebuildable from canonical state
            rebuild_projection(conn, archive_id=row["archive_id"], name=name)
        if WIKI in affected and self._wiki_staging_dir is not None:
            from ccf.projections import wiki

            wiki.rebuild_wiki(conn, row["archive_id"], self._wiki_staging_dir)
            record_ids = [
                plan["object_id"] for plan in plans if plan["object_kind"] == "record"
            ]
            remaining = purge.purge_wiki_staging(self._wiki_staging_dir, record_ids)
            if remaining:
                raise ErasureError(
                    f"wiki staging still references erased records: {remaining}"
                )
        verification = purge.verify_controlled_copies(
            conn, archive_id=row["archive_id"], plans=plans, affected=affected
        )
        # Suppression self-heal (spec 12.7): the lookup projection is
        # rebuilt from the canonical suppression lineage committed at
        # block; canonical corruption or projection drift fails the saga
        # loudly here rather than surfacing as silent reintroduction.
        from ccf.erasure import suppression_set

        rebuilt_suppression = suppression_set.rebuild_projection(
            conn, row["archive_id"], now=now
        )
        suppression_set.audit_projection(conn, row["archive_id"])
        verification["suppression"] = {
            "canonical_sets": True,
            "rebuilt_rows": rebuilt_suppression,
        }
        operations.record_purged(
            conn,
            operation_id=row["operation_id"],
            purged=[{"store": "verification", "verification": verification}],
            now=now,
        )
        marker = self._stage_marker_spec(row, "verify", now)
        resolved = self._commit_canonical(conn, [marker])
        operations.record_stage_head(
            conn,
            operation_id=row["operation_id"],
            stage_head_id=resolved[0].object_id,
            now=now,
        )

    def _stage_receipt(self, conn, row: dict) -> None:
        """Admit the receipt + membership Links; mark the lineage verified."""
        now = self._archive.clock()
        verification = next(
            (
                entry["verification"]
                for entry in reversed(row["purged"])
                if entry.get("store") == "verification"
            ),
            None,
        )
        if verification is None:
            raise ErasureError(
                f"operation {row['operation_id']} has no verification record"
            )
        suppression_commitment = row["suppression_set"]
        if suppression_commitment is None:
            raise ErasureError(
                f"operation {row['operation_id']} has no canonical "
                "suppression set descriptor; the receipt cannot commit to "
                "the suppression lineage (spec 12.7)"
            )
        receipt_spec = receipts.build_receipt_record_spec(
            schemas=self._archive.schemas,
            operation_id=row["operation_id"],
            decision_id=row["decision_id"],
            profile=row["assurance"],
            targets=row["plans"],
            verification=verification,
            worker_id=row["actor"]["recorded_by"],
            authority=row["actor"]["authority"],
            completed_at=now,
            suppression_commitment=suppression_commitment,
        )
        receipt_id = generate_id("record")
        receipt_spec["object_id"] = receipt_id
        marker = self._stage_marker_spec(row, "receipt", now)
        link_resolved = [
            receipts.resolve_membership_link(
                link_id=generate_id("link"),
                receipt_id=receipt_id,
                target_id=plan["object_id"],
                worker_id=row["actor"]["recorded_by"],
                authority=row["actor"]["authority"],
                recorded_at=now,
                catalog=self._archive.catalog,
                registries=self._archive.registries,
                schemas=self._archive.schemas,
                salt_fn=self._archive._salt_fn,
            )
            for plan in row["plans"]
        ]
        # Receipt, terminal lineage marker, and membership Links land in
        # one canonical commit, atomically with the operation row below.
        self._commit_canonical(conn, [marker, receipt_spec], link_resolved)
        operations.record_receipt(
            conn, operation_id=row["operation_id"], receipt_id=receipt_id, now=now
        )
        # Backfill receipt linkage on the suppression projection rows (the
        # verify-stage rebuild ran before the receipt existed).
        from ccf.erasure import suppression_set

        suppression_set.mark_receipt(
            conn,
            archive_id=row["archive_id"],
            set_record_id=suppression_commitment["suppression_set_record_id"],
            receipt_id=receipt_id,
            operation_id=row["operation_id"],
            authorized_producers=row["authorized_producers"],
        )

    # ------------------------------------------------------------------
    # Multi-subject media (spec 3.9)
    # ------------------------------------------------------------------

    def plan_media_decision(
        self,
        *,
        blob_id: str,
        subject_ids: list[str],
        restrict_pending_review: bool = False,
        reviewed_replacement_blob_id: str | None = None,
    ) -> dict:
        """The multi-subject media decision shape (whole-blob only)."""
        return decide_multi_subject(
            blob_id=blob_id,
            subject_ids=subject_ids,
            restrict_pending_review=restrict_pending_review,
            reviewed_replacement_blob_id=reviewed_replacement_blob_id,
        )
