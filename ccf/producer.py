"""Local-first CCF producer (spec sections 2.1, 4.6-4.7, 6.1-6.3).

A Producer owns one Thoth runtime/device credential, generates canonical
object IDs before batch construction, builds signed producer batches on its
own append-only chain (sequence, previous batch hash, catalog root,
credential ID, batch hash, Ed25519 signature), and durably spools every
batch so retries are stable across restarts and network loss.

Producer claims stay producer-controlled; the archive resolves them
separately at admission (spec section 5.2). Objects in spooled-but-uncommitted
batches are exposed only as provisional (spec section 6.3).
"""

from __future__ import annotations

from ccf.catalog import SemanticCatalog
from ccf.credentials import DeviceCredential
from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.hashing import (
    blob_content_commitment,
    producer_batch_hash,
    producer_batch_signing_digest,
    sign_digest,
    submission_hash,
)
from ccf.ids import generate_id, parse_id
from ccf.objects import new_salt, now_timestamp
from ccf.registry import PinnedRegistries
from ccf.schemas import SchemaSet
from ccf import spool


class ProducerError(RuntimeError):
    """Raised when a producer operation cannot be completed safely."""


BATCH_FORMAT = "ccf.producer-batch/0.1.2-rc1"
SIGNATURE_PROFILE = "ed25519-jcs-v1"


class Producer:
    """One signed-batch chain for one runtime/device credential."""

    def __init__(
        self,
        *,
        settings: CcfPostgresSettings,
        producer_id: str,
        credential: DeviceCredential,
        catalog: SemanticCatalog,
        registries: PinnedRegistries,
        schemas: SchemaSet,
        clock=now_timestamp,
    ) -> None:
        if parse_id(producer_id).kind != "record":
            raise ProducerError(f"producer_id must be a Record URN: {producer_id!r}")
        if parse_id(credential.credential_id).kind != "credential":
            raise ProducerError(
                f"credential_id must be a credential URN: {credential.credential_id!r}"
            )
        self._settings = settings
        self.producer_id = producer_id
        self.credential = credential
        self.catalog = catalog
        self.registries = registries
        self.schemas = schemas
        self.clock = clock

    # ------------------------------------------------------------------
    # Submission construction (IDs generated before batch construction)
    # ------------------------------------------------------------------

    def new_record(
        self,
        *,
        type: str,
        claims: dict,
        payload: dict,
        recorded_by: str | None = None,
        recorded_at: str | None = None,
        occurred_at: dict | None = None,
        origin: dict | None = None,
        lineage: dict | None = None,
        type_visibility: str = "clear",
        retention_profile_hint: str | None = None,
        type_version: int = 1,
        object_id: str | None = None,
    ) -> dict:
        entry = self.registries.type_entry(type, type_version)
        submission = {
            "submission_kind": "record",
            "id": object_id or generate_id("record"),
            "type": type,
            "type_version": type_version,
            "type_visibility": type_visibility,
            "retention_profile_hint": retention_profile_hint
            or entry["retention_profile"],
            "recorded_by": recorded_by or self.producer_id,
            "recorded_at": recorded_at or self.clock(),
            "claims": claims,
            "payload": payload,
            "extensions": {},
        }
        if occurred_at is not None:
            submission["occurred_at"] = occurred_at
        if origin is not None:
            submission["origin"] = origin
        if lineage is not None:
            submission["lineage"] = lineage
        self.schemas.validate(
            "urn:ccf:schema:0.1.2-rc1:submissions.record", submission, what="record submission"
        )
        return submission

    def new_link(
        self,
        *,
        type: str,
        from_id: str,
        to_id: str,
        claims: dict,
        selector: dict | None = None,
        payload: dict | None = None,
        recorded_by: str | None = None,
        recorded_at: str | None = None,
        type_visibility: str = "clear",
        retention_profile_hint: str | None = None,
        type_version: int = 1,
        link_id: str | None = None,
    ) -> dict:
        entry = self.registries.link_entry(type, type_version)
        submission = {
            "submission_kind": "link",
            "id": link_id or generate_id("link"),
            "type": type,
            "type_version": type_version,
            "type_visibility": type_visibility,
            "retention_profile_hint": retention_profile_hint
            or entry["retention_profile"],
            "from_id": from_id,
            "to_id": to_id,
            "recorded_by": recorded_by or self.producer_id,
            "recorded_at": recorded_at or self.clock(),
            "claims": claims,
            "payload": payload if payload is not None else {},
            "extensions": {},
        }
        if selector is not None:
            submission["selector"] = selector
        self.schemas.validate(
            "urn:ccf:schema:0.1.2-rc1:submissions.link", submission, what="link submission"
        )
        return submission

    def new_blob(
        self,
        *,
        data: bytes,
        media_type: str,
        claims: dict,
        origin: dict | None = None,
        retention_profile_hint: str | None = None,
        blob_id: str | None = None,
        content_salt: str | None = None,
    ) -> tuple[dict, bytes]:
        """Build a Blob submission for ``data``; returns (submission, data).

        The caller retains the bytes for the transfer manifest; the archive
        verifies them against the declared salted content commitment at
        admission (spec section 4.4).
        """
        salt = content_salt or new_salt()
        submission = {
            "submission_kind": "blob",
            "id": blob_id or generate_id("blob"),
            "retention_profile_hint": retention_profile_hint
            or self.registries.blob_entry["retention_profile"],
            "media_type": media_type,
            "byte_length": str(len(data)),
            "content_salt": salt,
            "content_commitment": blob_content_commitment(salt, data),
            "content_profile": "ccf-blob-content-v2",
            "claims": claims,
            "extensions": {},
        }
        if origin is not None:
            submission["origin"] = origin
        self.schemas.validate(
            "urn:ccf:schema:0.1.2-rc1:submissions.blob", submission, what="blob submission"
        )
        return submission, data

    # ------------------------------------------------------------------
    # Signed batch chain + durable spool
    # ------------------------------------------------------------------

    def create_batch(
        self,
        *,
        records: list[dict] | None = None,
        links: list[dict] | None = None,
        blobs: list[dict] | None = None,
        blob_transfers: list[dict] | None = None,
        blob_data: dict[str, bytes] | None = None,
    ) -> dict:
        """Sign and durably spool the next batch on this producer's chain.

        The spool insert and the producer-head advance commit atomically,
        so a crash either leaves the batch fully spooled (replayable) or
        not spooled at all (safe to rebuild and re-sign). ``blob_data``
        maps Blob submission IDs to their raw bytes; the bytes are verified
        against each submission's declared length and salted content
        commitment, then spooled durably next to the batch so they survive
        restart and feed resumable transfer (spec sections 6.7, 11.4).
        """
        records = list(records or [])
        links = list(links or [])
        blobs = list(blobs or [])
        blob_data = dict(blob_data or {})
        if not records and not links and not blobs:
            raise ProducerError("refusing to create an empty producer batch")
        blob_ids = {sub["id"] for sub in blobs}
        unknown = set(blob_data) - blob_ids
        if unknown:
            raise ProducerError(
                f"blob_data IDs without a blob submission: {sorted(unknown)}"
            )
        for sub in blobs:
            data = blob_data.get(sub["id"])
            if data is None:
                continue
            if str(len(data)) != sub["byte_length"]:
                raise ProducerError(
                    f"blob {sub['id']} byte_length {sub['byte_length']} != "
                    f"spooled {len(data)}"
                )
            if blob_content_commitment(sub["content_salt"], data) != (
                sub["content_commitment"]
            ):
                raise ProducerError(
                    f"blob {sub['id']} bytes do not match its content commitment"
                )

        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                head = spool.lock_producer_head(conn, self.producer_id)
                if head is None:
                    sequence = 1
                    previous_hash = None
                else:
                    if head["credential_id"] != self.credential.credential_id:
                        raise ProducerError(
                            "producer head was advanced by a different credential: "
                            f"{head['credential_id']}"
                        )
                    sequence = head["producer_sequence"] + 1
                    previous_hash = head["batch_hash"]

                batch = {
                    "format": BATCH_FORMAT,
                    "batch_id": generate_id("batch"),
                    "producer_id": self.producer_id,
                    "producer_sequence": str(sequence),
                    "previous_batch_hash": previous_hash,
                    "credential_id": self.credential.credential_id,
                    "created_at": self.clock(),
                    "semantic_catalog_root": self.catalog.root,
                    "records": records,
                    "links": links,
                    "blobs": blobs,
                    "blob_transfers": list(blob_transfers or []),
                    "extensions": {},
                    "signature_profile": SIGNATURE_PROFILE,
                }
                batch["batch_hash"] = producer_batch_hash(batch)
                batch["signature"] = _encode_signature(
                    sign_digest(
                        self.credential.private_key,
                        producer_batch_signing_digest(batch["batch_hash"]),
                    )
                )
                self.schemas.validate(
                    "urn:ccf:schema:0.1.2-rc1:sync.producer-batch",
                    batch,
                    what="producer batch",
                )
                spool.spool_batch(conn, batch, spooled_at=self.clock())
                if blob_data:
                    spool.spool_blob_payloads(
                        conn, batch["batch_id"], blob_data, spooled_at=self.clock()
                    )
        return batch

    def spooled_blob_bytes(self, batch_id: str) -> dict[str, bytes]:
        """Reload durably spooled Blob bytes for one batch (post-restart)."""
        with open_ccf_connection(self._settings) as conn:
            return spool.load_blob_payloads(conn, batch_id)

    # ------------------------------------------------------------------
    # Provisional state and replay
    # ------------------------------------------------------------------

    def provisional_objects(self) -> list[dict]:
        """Locally created, uncommitted objects — always marked provisional."""
        with open_ccf_connection(self._settings) as conn:
            return spool.provisional_objects(conn, self.producer_id)

    def pending_batches(self) -> list[dict]:
        """Spooled batches that have no terminal archive result yet."""
        with open_ccf_connection(self._settings) as conn:
            return spool.pending_batches(conn, self.producer_id)

    def sync_pending(self, archive, *, blob_bytes: dict[str, bytes] | None = None) -> list[dict]:
        """Push every pending batch through an Archive, in chain order.

        Replaying an already-answered batch returns the stored result; the
        archive's idempotency rules make re-admission of a committed batch
        a no-op. When ``blob_bytes`` is not given, durably spooled Blob
        payloads for each pending batch are attached automatically, so
        bytes survive restarts between spool and sync. Returns one batch
        result per pending batch.
        """
        results = []
        for batch in self.pending_batches():
            effective = blob_bytes
            if effective is None and batch["blobs"]:
                effective = self.spooled_blob_bytes(batch["batch_id"])
            results.append(archive.admit_batch(batch, blob_bytes=effective))
        return results


def submission_hashes(batch: dict) -> dict[str, str]:
    """Stable submission hash for every object in a batch."""
    hashes: dict[str, str] = {}
    for kind in ("records", "links", "blobs"):
        for submission in batch[kind]:
            hashes[submission["id"]] = submission_hash(submission)
    return hashes


def _encode_signature(signature: bytes) -> str:
    from ccf.hashing import encode_b64url

    return encode_b64url(signature)
