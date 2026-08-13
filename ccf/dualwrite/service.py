"""The CCF dual-write service: bootstrap once, then mirror captures.

On first enable the service creates the archive (genesis commit) and
admits the founding Records (policy, person, runtime, device credential)
with deterministic, archive-derived IDs; on later starts it reopens the
archive and verifies those Records are present (fail closed otherwise).

``mirror_capture`` converts one persisted legacy capture — source,
optional session/run, raw artifact (Blob + artifact + Links), and any
prompt-security findings — through ``ccf.thothmap`` and admits it as one
signed producer batch. Re-mirroring an unchanged capture is a no-op:
origin tuples already present in the archive are skipped before anything
is signed, so idempotent re-runs never produce conflicts or duplicates.

Admission-time failures (conflicts, partial batches, schema/DB errors)
raise :class:`DualWriteError`; the caller (``core.capture_lifecycle``)
logs and ledgers them without letting them touch the legacy write.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ccf.archive import Archive
from ccf.catalog import SemanticCatalog
from ccf.credentials import DeviceCredential, device_credential_structural_payload
from ccf.db import migrate_ccf_store, open_ccf_connection
from ccf.keys import generate_signing_key
from ccf.producer import Producer
from ccf.registry import PinnedRegistries
from ccf.schemas import SchemaSet
from ccf.thothmap import artifacts as thothmap_artifacts
from ccf.thothmap import findings as thothmap_findings
from ccf.thothmap import sessions as thothmap_sessions
from ccf.thothmap import sources as thothmap_sources
from ccf.thothmap.context import MapContext, MappedSubmissions

from ccf.dualwrite.config import DualWriteSettings
from ccf.dualwrite.conventions import (
    FINDING_REVISION,
    SESSION_REVISION,
    bootstrap_ids,
    finding_origin_native_id,
    findings_from_metadata,
    run_native_id,
    source_record_id,
)
from ccf.dualwrite.ledger import append_error

logger = logging.getLogger(__name__)

_OK_ADMISSION_STATUSES = {"admitted", "existing"}


class DualWriteError(RuntimeError):
    """Raised when a dual-write bootstrap or mirror cannot complete safely."""


class CcfDualWriteService:
    """Mirrors legacy capture artifacts into the local CCF archive."""

    def __init__(
        self,
        *,
        settings: DualWriteSettings,
        archive: Archive,
        producer: Producer,
        ctx: MapContext,
    ) -> None:
        self.settings = settings
        self.archive = archive
        self.producer = producer
        self.ctx = ctx
        self._ids = bootstrap_ids(archive.archive_id)

    # ------------------------------------------------------------------
    # Construction / bootstrap
    # ------------------------------------------------------------------

    @classmethod
    def create_or_open(cls, settings: DualWriteSettings, *, clock=None) -> "CcfDualWriteService":
        """Open the dual-write archive, running first-enable bootstrap.

        Bootstrap generates the Ed25519 key pairs at the configured paths
        when absent, runs schema migrations, creates the archive genesis
        when the store is empty, and admits the founding Records. All key
        material stays at the operator-configured paths; nothing is
        written anywhere else.
        """
        if not settings.enabled or settings.store is None:
            raise DualWriteError("dual-write service requires enabled settings")
        if settings.package_root is None:
            raise DualWriteError("dual-write settings lack a package root")
        store = settings.store
        kwargs = {"clock": clock} if clock is not None else {}

        archive_key_path = Path(store.archive_key_path)
        device_key_path = Path(store.device_key_path)
        archive_key_path.parent.mkdir(parents=True, exist_ok=True)
        device_key_path.parent.mkdir(parents=True, exist_ok=True)
        if not archive_key_path.is_file():
            generate_signing_key(archive_key_path)
        if not device_key_path.is_file():
            generate_signing_key(device_key_path)

        migrate_ccf_store(store)
        with open_ccf_connection(store) as conn:
            row = conn.execute("SELECT archive_id FROM archive").fetchone()

        if row is None:
            archive = Archive.create(
                store,
                package_root=settings.package_root,
                archive_key_path=archive_key_path,
                **kwargs,
            )
            ids = bootstrap_ids(archive.archive_id)
            cls._admit_founding_records(archive, ids, device_key_path)
        else:
            archive = Archive.open(
                store,
                package_root=settings.package_root,
                archive_key_path=archive_key_path,
                **kwargs,
            )
            ids = bootstrap_ids(archive.archive_id)
            for label in ("person_id", "runtime_id", "credential_record_id"):
                if archive.get_object(ids[label]) is None:
                    raise DualWriteError(
                        f"archive {archive.archive_id} exists but its dual-write "
                        f"bootstrap record {ids[label]} is missing; refusing to "
                        "mirror into an archive this service did not bootstrap"
                    )
            credential = DeviceCredential.load(
                device_key_path,
                credential_id=ids["credential_id"],
                key_id=ids["device_key_id"],
            )
            admitted = archive.get_object(ids["credential_record_id"])
            admitted_key = (
                admitted["compartments"]["structural"]["envelope"]["content"]
                ["structural_payload"]["signing_key"]["public_key"]
            )
            if admitted_key != credential.public_key_b64url:
                raise DualWriteError(
                    f"device key at {device_key_path} does not match the admitted "
                    f"credential {ids['credential_id']}: the archive was "
                    "bootstrapped with different key material. Restore the "
                    "original keys, or roll back (drop the CCF schema) and "
                    "re-bootstrap."
                )

        catalog = SemanticCatalog.load(settings.package_root)
        credential = DeviceCredential.load(
            device_key_path,
            credential_id=ids["credential_id"],
            key_id=ids["device_key_id"],
        )
        producer = Producer(
            settings=store,
            producer_id=ids["runtime_id"],
            credential=credential,
            catalog=catalog,
            registries=PinnedRegistries.load(settings.package_root, catalog),
            schemas=SchemaSet.load(settings.package_root),
            **kwargs,
        )
        ctx = MapContext(
            person_id=ids["person_id"],
            policy_hint=ids["policy_lineage_id"],
        )
        return cls(settings=settings, archive=archive, producer=producer, ctx=ctx)

    @staticmethod
    def _admit_founding_records(
        archive: Archive, ids: dict[str, str], device_key_path: Path
    ) -> None:
        """Admit policy/person/runtime/device-credential bootstrap Records."""
        from ccf.objects import now_timestamp

        ts = now_timestamp()
        person_id = ids["person_id"]
        runtime_id = ids["runtime_id"]
        policy_lineage_id = ids["policy_lineage_id"]

        def authority(basis, asserted_by, accepted_by=None):
            return {
                "basis": basis,
                "asserted_by": asserted_by,
                "accepted_by": accepted_by,
            }

        def privacy(classes=None, subjects=None):
            subjects = subjects or []
            return {
                "data_subjects": subjects,
                "data_classes": classes or [],
                "consent_refs": [],
                "legal_basis_refs": [],
                "subject_coverage": "complete" if subjects else "unknown",
            }

        archive.admit_bootstrap(
            [
                {
                    "type": "governance.policy",
                    "object_id": ids["policy_record_id"],
                    "recorded_by": runtime_id,
                    "recorded_at": ts,
                    "person_id": person_id,
                    "authority": authority(
                        "explicit_authorization", person_id, person_id
                    ),
                    "privacy": privacy(["identity_data"]),
                    "policy_hint": policy_lineage_id,
                    "lineage": {
                        "lineage_id": policy_lineage_id,
                        "previous_head_id": None,
                        "transition": "create",
                        "valid_from": ts,
                        "expires_at": None,
                    },
                    "payload": {
                        "profile": "ccf.policy/0.1.2-rc1",
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
                    "object_id": person_id,
                    "recorded_by": runtime_id,
                    "recorded_at": ts,
                    "person_id": person_id,
                    "perspective_id": person_id,
                    "authority": authority(
                        "first_person_statement", person_id, person_id
                    ),
                    "privacy": privacy(
                        ["identity_data"],
                        [
                            {
                                "person_id": person_id,
                                "role": "archive_principal",
                                "identity_state_at_write": "verified",
                            }
                        ],
                    ),
                    "policy_hint": policy_lineage_id,
                    "payload": {
                        "kind": "human",
                        "display_name": "Thoth Operator",
                        "aliases": [],
                        "identity_anchors": [],
                        "extensions": {},
                    },
                },
                {
                    "type": "core.runtime",
                    "object_id": runtime_id,
                    "recorded_by": runtime_id,
                    "recorded_at": ts,
                    "person_id": person_id,
                    "authority": authority("runtime_import", runtime_id),
                    "privacy": privacy(),
                    "policy_hint": policy_lineage_id,
                    "payload": {
                        "kind": "backend",
                        "name": "thoth-dualwrite",
                        "version": "0.1.2-rc1",
                        "instance_id": "thoth-dualwrite",
                        "capabilities": ["capture", "sync"],
                        "operator_id": person_id,
                        "extensions": {},
                    },
                },
                {
                    "type": "core.device_credential",
                    "object_id": ids["credential_record_id"],
                    "recorded_by": runtime_id,
                    "recorded_at": ts,
                    "authority": authority(
                        "explicit_authorization", person_id, person_id
                    ),
                    "privacy": privacy(),
                    "policy_hint": policy_lineage_id,
                    "semantic": False,
                    "structural_payload": device_credential_structural_payload(
                        DeviceCredential.load(
                            device_key_path,
                            credential_id=ids["credential_id"],
                            key_id=ids["device_key_id"],
                        ),
                        subject_id=runtime_id,
                        issuer_key_id=ids["archive_key_id"],
                        scopes=["capture", "sync", "derive"],
                        valid_from=ts,
                    ),
                    "lineage": {
                        "lineage_id": ids["credential_lineage_id"],
                        "previous_head_id": None,
                        "transition": "issue",
                        "valid_from": ts,
                        "expires_at": None,
                    },
                    "payload": {},
                },
            ]
        )

    # ------------------------------------------------------------------
    # Mirroring
    # ------------------------------------------------------------------

    def mirror_capture(
        self,
        *,
        source: dict,
        session: dict | None,
        raw_ref: dict,
        data: bytes,
        findings_metadata: dict | None = None,
    ) -> dict:
        """Mirror one persisted legacy capture into the CCF archive.

        ``source`` carries the CaptureSource fields (``source_id``,
        ``source_name``, ``source_type``, ...); ``session`` the capture
        session fields plus ``session_id`` (or None); ``raw_ref`` the
        RawArtifactRef fields and ``data`` the raw file bytes.
        ``findings_metadata`` is the queue payload's
        ``normalized_metadata`` block; findings are parsed with the same
        rules as the legacy event store.

        Already-mirrored origin tuples are skipped (idempotent re-run).
        Returns a receipt describing what was admitted; raises
        :class:`DualWriteError` on any admission failure.
        """
        thoth_source_id = _required(source.get("source_id"), "source.source_id")
        source_ccf_id = source_record_id(self.archive.archive_id, thoth_source_id)
        origins = self._origin_index(source_ccf_id)

        parts = MappedSubmissions()
        objects: dict[str, object] = {"source_id": source_ccf_id}

        if self.archive.get_object(source_ccf_id) is None:
            mapped = thothmap_sources.source_submission(
                self.producer,
                self.ctx,
                source,
                trust_class="unknown",
                object_id=source_ccf_id,
            )
            parts.extend(mapped)
            objects["source_admitted"] = True

        session_ccf_id = self._mirror_session(parts, origins, source, session, objects)

        artifact_ccf_id, blob_ccf_id = self._mirror_media(
            parts, origins, source_ccf_id, session_ccf_id, raw_ref, data, objects
        )

        self._mirror_findings(
            parts,
            origins,
            source_ccf_id,
            evidence=[artifact_ccf_id, blob_ccf_id],
            findings_metadata=findings_metadata,
            objects=objects,
        )

        if not parts.records and not parts.links and not parts.blobs:
            return {
                "status": "existing",
                "archive_id": self.archive.archive_id,
                "objects": objects,
                "admissions": [],
            }

        batch = self.producer.create_batch(
            records=parts.records,
            links=parts.links,
            blobs=parts.blobs,
            blob_data=parts.blob_data or None,
        )
        result = self.archive.admit_batch(batch, blob_bytes=parts.blob_data or None)
        bad = [
            admission
            for admission in result.get("admissions", [])
            if admission.get("status") not in _OK_ADMISSION_STATUSES
        ]
        if result.get("status") != "accepted" or bad:
            raise DualWriteError(
                f"dual-write admission failed for source {thoth_source_id}: "
                f"batch status {result.get('status')!r}, rejections: {bad[:3]}"
            )
        return {
            "status": "accepted",
            "archive_id": self.archive.archive_id,
            "batch_id": batch["batch_id"],
            "commit_sequence": result.get("commit_sequence"),
            "objects": objects,
            "admissions": result.get("admissions", []),
        }

    def _mirror_session(
        self,
        parts: MappedSubmissions,
        origins: dict,
        source: dict,
        session: dict | None,
        objects: dict,
    ) -> str | None:
        if session is None:
            return None
        session_id = _required(session.get("session_id"), "session.session_id")
        source_ccf_id = objects["source_id"]

        session_ccf_id = origins.get((session_id, SESSION_REVISION, "record"))
        if session_ccf_id is None:
            mapped = thothmap_sessions.session_submission(
                self.producer,
                self.ctx,
                session,
                source_ccf_id=source_ccf_id,
                revision=SESSION_REVISION,
            )
            session_ccf_id = mapped.records[0]["id"]
            parts.extend(mapped)
            objects["session_admitted"] = True
        objects["session_id"] = session_ccf_id

        run_key = (run_native_id(session_id), SESSION_REVISION, "record")
        if run_key not in origins:
            ended = session.get("ended_at")
            mapped = thothmap_sessions.run_submission(
                self.producer,
                self.ctx,
                {
                    "run_id": run_native_id(session_id),
                    "status": "completed" if ended else "running",
                    "started_at": session.get("started_at"),
                    "finished_at": ended,
                    "connector_name": source.get("collector"),
                },
                source_ccf_id=source_ccf_id,
                revision=SESSION_REVISION,
            )
            parts.extend(mapped)
            objects["run_id"] = mapped.records[0]["id"]
            objects["run_admitted"] = True
        return session_ccf_id

    def _mirror_media(
        self,
        parts: MappedSubmissions,
        origins: dict,
        source_ccf_id: str,
        session_ccf_id: str | None,
        raw_ref: dict,
        data: bytes,
        objects: dict,
    ) -> tuple[str, str]:
        raw_ref_id = _required(raw_ref.get("raw_ref_id"), "raw_ref.raw_ref_id")
        sha256 = _required(raw_ref.get("sha256"), "raw_ref.sha256")
        artifact_key = (raw_ref_id, sha256, "record")
        blob_key = (raw_ref_id, sha256, "blob")
        artifact_ccf_id = origins.get(artifact_key)
        blob_ccf_id = origins.get(blob_key)

        if (artifact_ccf_id is None) != (blob_ccf_id is None):
            raise DualWriteError(
                f"partial media mirror for raw_ref {raw_ref_id}: "
                f"artifact {'present' if artifact_ccf_id else 'missing'}, "
                f"blob {'present' if blob_ccf_id else 'missing'}"
            )
        if artifact_ccf_id is None:
            mapped = thothmap_artifacts.media_submissions(
                self.producer,
                self.ctx,
                raw_ref,
                data=data,
                source_ccf_id=source_ccf_id,
                session_ccf_id=session_ccf_id,
                revision=sha256,
            )
            artifact_ccf_id = mapped.records[0]["id"]
            blob_ccf_id = mapped.blobs[0]["id"]
            parts.extend(mapped)
            objects["media_admitted"] = True
        objects["artifact_id"] = artifact_ccf_id
        objects["blob_id"] = blob_ccf_id
        return artifact_ccf_id, blob_ccf_id

    def _mirror_findings(
        self,
        parts: MappedSubmissions,
        origins: dict,
        source_ccf_id: str,
        *,
        evidence: list[str],
        findings_metadata: dict | None,
        objects: dict,
    ) -> None:
        admitted: list[str] = []
        for finding in findings_from_metadata(findings_metadata or {}):
            native_id = finding_origin_native_id(finding)
            if (native_id, FINDING_REVISION, "record") in origins:
                continue
            mapped = thothmap_findings.finding_submissions(
                self.producer,
                self.ctx,
                finding,
                source_ccf_id=source_ccf_id,
                evidence_ccf_ids=[ref for ref in evidence if ref],
                revision=FINDING_REVISION,
            )
            parts.extend(mapped)
            admitted.append(mapped.records[0]["id"])
        if admitted:
            objects["finding_ids"] = admitted

    # ------------------------------------------------------------------
    # Error ledger
    # ------------------------------------------------------------------

    def record_error(self, context: dict, exc: BaseException) -> None:
        """Ledger one mirror failure (after logging it loudly)."""
        logger.exception("CCF dual-write mirror failed: %s", context)
        append_error(
            self.settings.error_log_path,
            {
                "kind": "mirror_failure",
                "archive_id": self.archive.archive_id,
                "context": context,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _origin_index(self, source_ccf_id: str) -> dict[tuple[str, str, str], str]:
        """{(native_id, revision, object_kind): object_id} for one source."""
        with open_ccf_connection(self.settings.store) as conn:
            rows = conn.execute(
                """
                SELECT native_id, revision, object_kind, object_id
                FROM origin_index
                WHERE archive_id = %s AND source_id = %s
                """,
                (self.archive.archive_id, source_ccf_id),
            ).fetchall()
        return {
            (native_id, revision, kind): object_id
            for native_id, revision, kind, object_id in rows
        }


def _required(value, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise DualWriteError(f"dual-write mirror requires non-empty {field}")
    return value
