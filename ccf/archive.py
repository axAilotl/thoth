"""The CCF archive facade: genesis, bootstrap, admission, verification.

An Archive owns one archive ID, one epoch, and one Ed25519 signing key
loaded from an explicit path (fail closed). :meth:`Archive.create` runs the
schema migrations and writes the genesis ``integrity.commit`` (sequence 0)
pinning the archive/epoch IDs, hash and signature profiles, signer key,
semantic-catalog root, and active profiles (spec section 7.2).

``admit_bootstrap`` is the operator path for the archive's own founding
Records (policy, person, runtime, device credential) — the same shape the
vendored ``thoth-capture`` example commits at sequence 1. Bootstrap Records
are signed into the journal by the archive key and carry no producer
evidence, which keeps them distinguishable from producer-admitted objects.
"""

from __future__ import annotations

from pathlib import Path

from ccf.admission import (
    ResolvedObject,
    _make_envelope,
    _make_header,
    admit_producer_batch,
    commit_objects,
    load_archive,
    lock_archive_head,
)
from ccf.catalog import SemanticCatalog
from ccf.db import CcfPostgresSettings, migrate_ccf_store, open_ccf_connection
from ccf.ids import generate_id, parse_id
from ccf.journal import verify_chain
from ccf.keys import load_signing_key
from ccf.objects import now_timestamp, validate_timestamp
from ccf.registry import PinnedRegistries
from ccf.schemas import SchemaSet

DEFAULT_ACTIVE_PROFILES = [
    "ccf-core-0.1.2",
    "ccf-local-sync-0.1.2",
    "ccf-continuity-pack-0.1.2",
]


class ArchiveError(RuntimeError):
    """Raised when archive creation or opening cannot proceed safely."""


class Archive:
    """One canonical CCF archive over a Postgres operational envelope."""

    def __init__(
        self,
        *,
        settings: CcfPostgresSettings,
        archive_id: str,
        catalog: SemanticCatalog,
        registries: PinnedRegistries,
        schemas: SchemaSet,
        signer,
        clock=now_timestamp,
        salt_fn=None,
        package_root: str | Path | None = None,
    ) -> None:
        if parse_id(archive_id).kind != "archive":
            raise ArchiveError(f"archive_id must be an archive URN: {archive_id!r}")
        self._settings = settings
        self.archive_id = archive_id
        self.catalog = catalog
        self.registries = registries
        self.schemas = schemas
        self._signer = signer
        self.clock = clock
        self.package_root = Path(package_root) if package_root is not None else None
        from ccf.objects import new_salt

        self._salt_fn = salt_fn or new_salt

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        settings: CcfPostgresSettings,
        *,
        package_root: str | Path,
        archive_key_path: str | Path,
        active_profiles: list[str] | None = None,
        clock=now_timestamp,
        salt_fn=None,
    ) -> "Archive":
        """Migrate the schema and create a new archive with its genesis commit.

        Fails closed if the archive store already contains an archive row —
        genesis runs exactly once per archive database.
        """
        migrate_ccf_store(settings)
        catalog = SemanticCatalog.load(package_root)
        registries = PinnedRegistries.load(package_root, catalog)
        schemas = SchemaSet.load(package_root)
        signer = load_signing_key(archive_key_path)

        archive_id = generate_id("archive")
        epoch_id = generate_id("lineage")
        erasure_domain_id = generate_id("lineage")
        signer_key_id = generate_id("key")
        created_at = clock()
        profiles = list(active_profiles or DEFAULT_ACTIVE_PROFILES)

        archive = cls(
            settings=settings,
            archive_id=archive_id,
            catalog=catalog,
            registries=registries,
            schemas=schemas,
            signer=signer,
            clock=clock,
            salt_fn=salt_fn,
            package_root=package_root,
        )
        from ccf.journal import build_commit_record

        with open_ccf_connection(settings) as conn:
            with conn.transaction():
                existing = conn.execute("SELECT COUNT(*) FROM archive").fetchone()[0]
                if existing:
                    raise ArchiveError(
                        "CCF archive store is not empty; refusing to re-run genesis"
                    )
                genesis = build_commit_record(
                    commit_record_id=generate_id("record"),
                    archive_id=archive_id,
                    epoch_id=epoch_id,
                    sequence=0,
                    parent_commit_hash=None,
                    members=[],
                    signer=signer,
                    signer_key_id=signer_key_id,
                    semantic_catalog_root=catalog.root,
                    active_profiles=profiles,
                    committed_at=created_at,
                    catalog=catalog,
                    registries=registries,
                    salt_fn=archive._salt_fn,
                )
                conn.execute(
                    """
                    INSERT INTO archive (
                        archive_id, epoch_id, genesis_commit_hash, hash_profile,
                        signature_profile, semantic_catalog_root, active_profiles,
                        signer_key_id, erasure_domain_id, created_at
                    ) VALUES (%s, %s, %s, 'ccf-jcs-sha256-v2', 'ed25519-jcs-v1',
                              %s, %s, %s, %s, %s)
                    """,
                    (
                        archive_id,
                        epoch_id,
                        genesis.commit_hash,
                        catalog.root,
                        _jsonb(profiles),
                        signer_key_id,
                        erasure_domain_id,
                        created_at,
                    ),
                )
                _insert_commit_record(conn, archive_id, genesis, created_at)
                conn.execute(
                    """
                    INSERT INTO commit_journal (
                        archive_id, sequence, commit_record_id, parent_commit_hash,
                        commit_hash, batch_merkle_root, member_count, signer_key_id,
                        semantic_catalog_root, committed_at
                    ) VALUES (%s, 0, %s, NULL, %s, %s, 0, %s, %s, %s)
                    """,
                    (
                        archive_id,
                        genesis.record_id,
                        genesis.commit_hash,
                        genesis.merkle_root,
                        signer_key_id,
                        catalog.root,
                        created_at,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO archive_head (
                        archive_id, sequence, commit_record_id, commit_hash,
                        semantic_catalog_root, signer_key_id, updated_at
                    ) VALUES (%s, 0, %s, %s, %s, %s, %s)
                    """,
                    (
                        archive_id,
                        genesis.record_id,
                        genesis.commit_hash,
                        catalog.root,
                        signer_key_id,
                        created_at,
                    ),
                )
        return archive

    @classmethod
    def open(
        cls,
        settings: CcfPostgresSettings,
        *,
        package_root: str | Path,
        archive_key_path: str | Path,
        clock=now_timestamp,
        salt_fn=None,
    ) -> "Archive":
        """Open the single existing archive; fail closed if absent."""
        catalog = SemanticCatalog.load(package_root)
        registries = PinnedRegistries.load(package_root, catalog)
        schemas = SchemaSet.load(package_root)
        signer = load_signing_key(archive_key_path)
        with open_ccf_connection(settings) as conn:
            row = conn.execute("SELECT archive_id FROM archive").fetchone()
            if row is None:
                raise ArchiveError("no CCF archive exists; run Archive.create first")
        return cls(
            settings=settings,
            archive_id=row[0],
            catalog=catalog,
            registries=registries,
            schemas=schemas,
            signer=signer,
            clock=clock,
            salt_fn=salt_fn,
            package_root=package_root,
        )

    # ------------------------------------------------------------------
    # Admission
    # ------------------------------------------------------------------

    def admit_batch(
        self, batch: dict, *, blob_bytes: dict[str, bytes] | None = None
    ) -> dict:
        """Admit one signed producer batch; returns a batch-result document.

        The whole admission — validation, object writes, signed commit, head
        advancement, and the spool receipt — is one serialized transaction.
        A crash rolls it back entirely; replaying the batch is idempotent.

        The suppression-after-erasure check (spec 12.7) runs inside this
        transaction; when suppression entries exist but no suppression key
        is configured, admission fails closed.
        """
        from ccf.erasure import suppression

        suppression_key = suppression.load_suppression_key(self._settings)
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                archive = load_archive(conn, self.archive_id)
                return admit_producer_batch(
                    conn,
                    archive=archive,
                    batch=batch,
                    catalog=self.catalog,
                    registries=self.registries,
                    schemas=self.schemas,
                    signer=self._signer,
                    clock=self.clock,
                    blob_bytes=blob_bytes,
                    salt_fn=self._salt_fn,
                    suppression_key=suppression_key,
                )

    def admit_bootstrap(self, records: list[dict]) -> dict:
        """Commit operator-created bootstrap Records (policy, person, runtime,
        device credential, ...), signed by the archive key.

        Each entry: ``type``, ``payload``, ``recorded_by``, ``recorded_at``,
        ``authority``, ``privacy``; optional ``person_id``,
        ``perspective_id``, ``policy_hint``, ``lineage``,
        ``structural_payload``, ``semantic`` (default True), ``object_id``.
        Bootstrap Records carry no producer evidence or claimed block.
        """
        committed_at = self.clock()
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                archive = load_archive(conn, self.archive_id)
                head = lock_archive_head(conn, self.archive_id)
                lineage_heads = {}
                resolved: list[ResolvedObject] = []
                for spec_record in records:
                    resolved.append(
                        self._resolve_bootstrap_record(
                            conn, archive, spec_record, lineage_heads
                        )
                    )
                sequence, commit_hash = commit_objects(
                    conn,
                    archive=archive,
                    head=head,
                    objects=resolved,
                    catalog=self.catalog,
                    registries=self.registries,
                    signer=self._signer,
                    committed_at=committed_at,
                    salt_fn=self._salt_fn,
                )
        return {
            "archive_id": self.archive_id,
            "commit_sequence": str(sequence),
            "commit_hash": commit_hash,
            "admitted": [obj.object_id for obj in resolved],
        }

    def _resolve_bootstrap_record(
        self, conn, archive: dict, spec_record: dict, lineage_heads: dict
    ) -> ResolvedObject:
        from ccf.admission import _resolve_policy_ref
        from ccf.lineage import (
            LineageDeclarationError,
            check_state_transition,
            declare_lineage,
            load_lineage_heads,
        )

        if not lineage_heads:
            lineage_heads.update(load_lineage_heads(conn, self.archive_id))

        entry = self.registries.type_entry(spec_record["type"])
        object_id = spec_record.get("object_id") or generate_id("record")
        parse_id(object_id)

        # Registry-declared authority classes apply to operator admission
        # too (spec 5.5); the archive actor satisfies archive-scoped classes.
        from ccf.governance.authority import check_required_authority

        authority_reason = check_required_authority(
            entry.get("required_authority"),
            claim=spec_record.get("authority"),
            recorded_by=spec_record["recorded_by"],
            admitted_by_archive=True,
            registries=self.registries,
        )
        if authority_reason is not None:
            raise ArchiveError(authority_reason)

        # Resolve the policy reference against pre-transition lineage state.
        policy_ref = None
        if spec_record.get("policy_hint") is not None:
            policy_ref = _resolve_policy_ref(
                conn,
                lineage_heads,
                spec_record["policy_hint"],
                archive["semantic_catalog_root"],
            )

        lineage_update = None
        lineage_block = spec_record.get("lineage")
        if lineage_block is not None:
            try:
                declared = declare_lineage(
                    {**spec_record, "lineage": lineage_block},
                    type_entry=entry,
                    registries=self.registries,
                )
            except LineageDeclarationError as exc:
                raise ArchiveError(str(exc)) from exc
            machine_id, block = declared
            machine = self.registries.state_machine(machine_id)
            current = lineage_heads.get(block["lineage_id"])
            if block["previous_head_id"] is None:
                if current is not None:
                    raise ArchiveError(
                        f"bootstrap lineage {block['lineage_id']} already exists"
                    )
                reason = check_state_transition(
                    machine, current_state=None, transition=block["transition"]
                )
            else:
                if current is None or current["head_record_id"] != block["previous_head_id"]:
                    raise ArchiveError(
                        f"bootstrap lineage {block['lineage_id']} predecessor mismatch"
                    )
                reason = check_state_transition(
                    machine,
                    current_state=current["state"],
                    transition=block["transition"],
                )
            if reason is not None:
                raise ArchiveError(f"bootstrap lineage transition invalid: {reason}")
            lineage_update = (block["lineage_id"], block["transition"])

        validate_timestamp(spec_record["recorded_at"])
        structural_content = {
            "type": spec_record["type"],
            "type_version": 1,
            "type_visibility": "clear",
            "schema_digest": self.catalog.schema_digest(entry["semantic_schema_id"]),
            "registry_entry_digest": self.registries.entry_digest(entry),
            "retention_profile": entry["retention_profile"],
            "structural_payload": spec_record.get("structural_payload", {}),
            "extensions": {},
        }
        if lineage_block is not None:
            structural_content["lineage"] = lineage_block

        semantic_content = None
        if spec_record.get("semantic", True):
            semantic_content = {
                "recorded_by": spec_record["recorded_by"],
                "recorded_at": spec_record["recorded_at"],
                "authority": spec_record["authority"],
                "payload": spec_record["payload"],
                "extensions": spec_record.get("extensions", {}),
            }
            if spec_record.get("person_id") is not None:
                semantic_content["person_id"] = spec_record["person_id"]
            if spec_record.get("perspective_id") is not None:
                semantic_content["perspective_id"] = spec_record["perspective_id"]
            if spec_record.get("privacy") is not None:
                semantic_content["privacy"] = spec_record["privacy"]
            if policy_ref is not None:
                semantic_content["policy_ref"] = policy_ref
            self.schemas.validate(
                "urn:ccf:schema:0.1.2:objects.record-semantic-content",
                semantic_content,
                what="bootstrap semantic content",
            )
        self.schemas.validate(
            "urn:ccf:schema:0.1.2:objects.record-structural-content",
            structural_content,
            what="bootstrap structural content",
        )

        structural = _make_envelope("record", "structural", structural_content, self._salt_fn)
        semantic_env = (
            _make_envelope("record", "semantic", semantic_content, self._salt_fn)
            if semantic_content is not None
            else None
        )
        header = _make_header("record", object_id, structural, semantic_env)
        if lineage_update is not None:
            lineage_heads[lineage_update[0]] = {
                "head_record_id": object_id,
                "head_record_hash": header["object_hash"],
                "state": lineage_update[1],
            }
        return ResolvedObject(
            object_kind="record",
            object_id=object_id,
            header=header,
            structural=structural,
            semantic=semantic_env,
            submission_hash=None,
            origin=None,
            lineage_update=lineage_update,
            blob_data=None,
        )

    # ------------------------------------------------------------------
    # Projections
    # ------------------------------------------------------------------

    @property
    def projections(self):
        """Projection service entrypoint (spec 10): rebuilds and reads."""
        from ccf.projections.service import ProjectionService

        return ProjectionService(settings=self._settings, archive_id=self.archive_id)

    # ------------------------------------------------------------------
    # Governance (spec section 9)
    # ------------------------------------------------------------------

    @property
    def settings(self) -> CcfPostgresSettings:
        """The store settings this archive was opened with."""
        return self._settings

    @property
    def signer(self):
        """The archive signing key (internal sync/merge use only)."""
        return self._signer

    def sync(self):
        """The sync-and-packs service bound to this archive (spec 6.7, 11)."""
        from ccf.sync.service import SyncService

        return SyncService(self)

    def governance(self):
        """The contextual-authorization engine bound to this archive."""
        from ccf.governance.engine import GovernanceEngine

        return GovernanceEngine.from_archive(self)

    # ------------------------------------------------------------------
    # Erasure (spec sections 3.6-3.10, 12.7)
    # ------------------------------------------------------------------

    def erasure(self, *, wiki_staging_dir=None):
        """The erasure saga facade bound to this archive.

        ``wiki_staging_dir`` names the generated-plaintext wiki staging
        directory the destroy/verify stages purge and rebuild; omit it
        only when no wiki projection is in use.
        """
        from ccf.erasure.service import ErasureService

        return ErasureService.from_archive(self, wiki_staging_dir=wiki_staging_dir)

    # ------------------------------------------------------------------
    # Verification and inspection
    # ------------------------------------------------------------------

    def verify_chain(self, *, trusted_genesis_hash: str | None = None) -> dict:
        """Verify the journal from genesis through the head (spec 7.4)."""
        with open_ccf_connection(self._settings) as conn:
            return verify_chain(
                conn,
                archive_id=self.archive_id,
                expected_genesis_hash=trusted_genesis_hash,
            )

    def head(self) -> dict:
        with open_ccf_connection(self._settings) as conn:
            row = conn.execute(
                """
                SELECT sequence, commit_record_id, commit_hash FROM archive_head
                WHERE archive_id = %s
                """,
                (self.archive_id,),
            ).fetchone()
            if row is None:
                raise ArchiveError(f"archive {self.archive_id} has no head")
            return {
                "archive_id": self.archive_id,
                "sequence": str(int(row[0])),
                "commit_record_id": row[1],
                "commit_hash": row[2],
            }

    def find_origin_object(
        self,
        source_id: str,
        native_id: str,
        revision: str,
        object_kind: str,
    ) -> str | None:
        """Object ID admitted under one origin tuple, or ``None``.

        Producer-side idempotency probe (spec 6.5): importers use it to
        skip already-admitted source-native items instead of resubmitting
        them under fresh object IDs.
        """
        parse_id(source_id)
        result = self.find_origin_objects([(source_id, native_id, revision, object_kind)])
        return result.get((source_id, native_id, revision, object_kind))

    def find_origin_objects(
        self, probes: list[tuple[str, str, str, str]]
    ) -> dict[tuple[str, str, str, str], str]:
        """Bulk form of :meth:`find_origin_object` over one connection.

        Importers probe whole batches at once: one query per chunk instead
        of two connections per file, which matters at corpus scale.
        """
        if not probes:
            return {}
        sources = [probe[0] for probe in probes]
        natives = [probe[1] for probe in probes]
        revisions = [probe[2] for probe in probes]
        kinds = [probe[3] for probe in probes]
        with open_ccf_connection(self._settings) as conn:
            rows = conn.execute(
                """
                SELECT t.source_id, t.native_id, t.revision, t.object_kind,
                       o.object_id
                FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[])
                     AS t(source_id, native_id, revision, object_kind)
                JOIN origin_index o
                  ON o.archive_id = %s
                 AND o.source_id = t.source_id
                 AND o.native_id = t.native_id
                 AND o.revision = t.revision
                 AND o.object_kind = t.object_kind
                """,
                (sources, natives, revisions, kinds, self.archive_id),
            ).fetchall()
        return {
            (source_id, native_id, revision, kind): object_id
            for source_id, native_id, revision, kind, object_id in rows
        }

    def get_object(self, object_id: str) -> dict | None:
        """Portable view of one admitted object (header + compartments)."""
        parse_id(object_id)
        with open_ccf_connection(self._settings) as conn:
            header_row = conn.execute(
                """
                SELECT object_kind, structural_commitment, semantic_commitment,
                       object_hash
                FROM object_header WHERE id = %s
                """,
                (object_id,),
            ).fetchone()
            if header_row is None:
                return None
            result = {
                "id": object_id,
                "object_kind": header_row[0],
                "header": {
                    "spec": "ccf/0.1.2",
                    "object_kind": header_row[0],
                    "id": object_id,
                    "hash_profile": "ccf-jcs-sha256-v2",
                    "structural_commitment": header_row[1],
                    "semantic_commitment": header_row[2],
                    "object_hash": header_row[3],
                },
                "admission": None,
                "compartments": {},
            }
            admission_row = conn.execute(
                """
                SELECT commit_sequence, commit_position, admitted_at FROM admission
                WHERE archive_id = %s AND object_id = %s
                """,
                (self.archive_id, object_id),
            ).fetchone()
            if admission_row is not None:
                result["admission"] = {
                    "commit_sequence": str(int(admission_row[0])),
                    "commit_position": int(admission_row[1]),
                    "admitted_at": admission_row[2],
                }
            for compartment, state, fmt, salt, content in conn.execute(
                """
                SELECT compartment, state, format, salt, plaintext_json
                FROM compartment WHERE object_id = %s
                """,
                (object_id,),
            ).fetchall():
                from ccf.hashing import encode_b64url

                result["compartments"][compartment] = {
                    "state": state,
                    "envelope": (
                        {
                            "format": fmt,
                            "salt": encode_b64url(bytes(salt)),
                            "content": content,
                        }
                        if state == "plaintext"
                        else None
                    ),
                }
            return result


def _insert_commit_record(conn, archive_id: str, commit, committed_at: str) -> None:
    """Persist a commit Record's header and structural compartment."""
    from ccf.hashing import decode_b64url

    conn.execute(
        """
        INSERT INTO object_header (
            id, archive_id, object_kind, spec, hash_profile,
            structural_commitment, semantic_commitment, object_hash,
            submission_hash
        ) VALUES (%s, %s, 'record', 'ccf/0.1.2', 'ccf-jcs-sha256-v2',
                  %s, NULL, %s, NULL)
        """,
        (
            commit.record_id,
            archive_id,
            commit.header["structural_commitment"],
            commit.header["object_hash"],
        ),
    )
    conn.execute(
        """
        INSERT INTO compartment (
            object_id, compartment, state, format, salt, plaintext_json, updated_at
        ) VALUES (%s, 'structural', 'plaintext', %s, %s, %s, %s)
        """,
        (
            commit.record_id,
            commit.structural_envelope["format"],
            decode_b64url(commit.structural_envelope["salt"]),
            _jsonb(commit.structural_envelope["content"]),
            committed_at,
        ),
    )


def _jsonb(value):
    from psycopg.types.json import Jsonb

    return Jsonb(value)
