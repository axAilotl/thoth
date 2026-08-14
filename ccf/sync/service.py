"""SyncService: the Archive-facing sync entrypoint (spec 6.7, 11).

Bound to an :class:`ccf.archive.Archive` via ``Archive.sync``; owns head
exchange, mindpack export, delta-pack build/apply, and mindpack import
dispatch:

- foreign archive ID → foreign merge (11.3);
- same head → no-op;
- same identity, clean extension → append the new segment like a delta;
- same identity, divergent chains → explicit fork: both heads preserved
  as a custody row, no invented winner (11.7).

Restore into an empty store is a module function,
:func:`ccf.sync.restore.restore_mindpack`.
"""

from __future__ import annotations

from pathlib import Path

from ccf.db import open_ccf_connection
from ccf.sync.chunks import SIDECAR_SUFFIX, load_sidecar, verify_file
from ccf.sync.delta import apply_delta_pack, build_delta_pack
from ccf.sync.export import export_mindpack
from ccf.sync.heads import build_sync_head, negotiate
from ccf.sync.merge import merge_mindpack
from ccf.sync.packio import MANIFEST_COMPLETENESS_MISMATCH, PackError
from ccf.sync.restore import (
    VerifiedMindpack,
    append_pack_commits,
    insert_pack_objects,
    verify_mindpack,
)
from ccf.sync.verify import PackVerificationError


class ForkRecord:
    """An explicit fork: two preserved heads, no invented winner (11.7)."""

    def __init__(self, *, local_head: dict, foreign_head: dict) -> None:
        self.local_head = local_head
        self.foreign_head = foreign_head

    def to_dict(self) -> dict:
        return {
            "status": "fork",
            "heads": [self.local_head, self.foreign_head],
        }


class SyncService:
    """Sync and packs bound to one Archive instance."""

    def __init__(self, archive) -> None:
        self._archive = archive

    # ------------------------------------------------------------------
    # Head exchange (6.7)
    # ------------------------------------------------------------------

    def sync_head(self) -> dict:
        """This archive's sync-head document (archive + producer heads)."""
        with open_ccf_connection(self._archive.settings) as conn:
            return build_sync_head(
                conn, archive_id=self._archive.archive_id, schemas=self._archive.schemas
            )

    def negotiate(self, remote_head: dict) -> dict:
        """Negotiate a remote sync head against the local one."""
        return negotiate(self.sync_head(), remote_head)

    # ------------------------------------------------------------------
    # Mindpack export (11.1, 11.5)
    # ------------------------------------------------------------------

    def export_mindpack(
        self,
        out_dir: str | Path,
        *,
        mode: str = "restore",
        external_dependencies: list[dict] | None = None,
    ) -> dict:
        """Export the whole archive as a mindpack directory; returns manifest."""
        with open_ccf_connection(self._archive.settings) as conn:
            return export_mindpack(
                conn,
                archive_id=self._archive.archive_id,
                package_root=self._package_root(),
                out_dir=out_dir,
                schemas=self._archive.schemas,
                clock=self._archive.clock,
                mode=mode,
                external_dependencies=external_dependencies,
            )

    # ------------------------------------------------------------------
    # Delta packs (11.4)
    # ------------------------------------------------------------------

    def build_delta_pack(
        self,
        from_sequence: int,
        out_file: str | Path,
        *,
        through_sequence: int | None = None,
        chunk_size: int | None = None,
    ) -> dict:
        """Build a compressed delta pack + chunk sidecar; returns manifest."""
        with open_ccf_connection(self._archive.settings) as conn:
            if through_sequence is None:
                through_sequence = int(self._archive.head()["sequence"])
            return build_delta_pack(
                conn,
                archive_id=self._archive.archive_id,
                from_sequence=from_sequence,
                through_sequence=through_sequence,
                out_file=out_file,
                schemas=self._archive.schemas,
                clock=self._archive.clock,
                chunk_size=chunk_size,
            )

    def apply_delta_pack(self, pack_path: str | Path, *, allow_partial: bool = False) -> dict:
        """Apply a delta pack after verifying its chunk sidecar and contents."""
        pack_path = Path(pack_path)
        sidecar_path = pack_path.with_name(pack_path.name + SIDECAR_SUFFIX)
        if sidecar_path.is_file():
            verify_file(pack_path, load_sidecar(sidecar_path))
        with open_ccf_connection(self._archive.settings) as conn:
            with conn.transaction():
                return apply_delta_pack(
                    conn,
                    archive_id=self._archive.archive_id,
                    pack_path=pack_path,
                    schemas=self._archive.schemas,
                    allow_partial=allow_partial,
                    clock=self._archive.clock,
                )

    # ------------------------------------------------------------------
    # Mindpack import: merge / extend / fork (11.2-11.4, 11.7)
    # ------------------------------------------------------------------

    def import_mindpack(self, pack_path: str | Path, *, allow_partial: bool = False) -> dict:
        """Import a mindpack into this (non-empty) archive's store.

        Dispatches on identity and chain relationship; fails closed on
        incomplete packs unless ``allow_partial`` is set.
        """
        pack = verify_mindpack(
            pack_path,
            package_root=self._package_root(),
            allow_partial=allow_partial,
            # Reconstruct missing signed members before deciding dispatch;
            # the unsigned manifest must never select a permissive path.
            allow_missing_member_objects=True,
            operation="import",
            destination_archive_id=self._archive.archive_id,
        )
        foreign = pack.chain["archive_id"] != self._archive.archive_id
        if not foreign and pack.inventory.missing_member_ids:
            raise PackVerificationError(
                "same-archive import is missing signed member objects",
                reason=MANIFEST_COMPLETENESS_MISMATCH,
            )
        manifest = pack.manifest
        archive_id = self._archive.archive_id
        if foreign:
            return self._merge(pack)

        local_head = self._archive.head()
        local_sequence = int(local_head["sequence"])
        if manifest["head_commit_hash"] == local_head["commit_hash"]:
            return {"status": "equal", "head_commit_hash": local_head["commit_hash"]}

        pack_commits = {int(c["sequence"]): c for c in pack.commits}
        if local_sequence < int(manifest["head_sequence"]) and local_sequence in pack_commits:
            if pack_commits[local_sequence]["commit_hash"] == local_head["commit_hash"]:
                return self._extend(pack, local_sequence)
        return self._fork(pack, local_head)

    def _merge(self, pack: VerifiedMindpack) -> dict:
        with open_ccf_connection(self._archive.settings) as conn:
            with conn.transaction():
                return merge_mindpack(
                    conn,
                    pack=pack,
                    destination_archive_id=self._archive.archive_id,
                    catalog=self._archive.catalog,
                    registries=self._archive.registries,
                    signer=self._archive.signer,
                    clock=self._archive.clock,
                    salt_fn=self._archive._salt_fn,
                )

    def _extend(self, pack: VerifiedMindpack, local_sequence: int) -> dict:
        """Append a verified same-identity extension (mindpack as delta)."""
        archive_id = self._archive.archive_id
        with open_ccf_connection(self._archive.settings) as conn:
            with conn.transaction():
                inserted, skipped = insert_pack_objects(
                    conn, archive_id, pack, updated_at=self._archive.clock(),
                    skip_existing=True,
                )
                append_pack_commits(
                    conn,
                    archive_id,
                    pack.commits,
                    pack.members,
                    start_sequence=local_sequence + 1,
                )
                from ccf.journal import verify_chain

                verification = verify_chain(conn, archive_id=archive_id)
        return {
            "status": "extended",
            "archive_id": archive_id,
            "objects_inserted": len(inserted),
            "objects_skipped": len(skipped),
            "head_commit_hash": verification["head_commit_hash"],
            "head_sequence": verification["head_sequence"],
            "partial": pack.partial,
            "verification": verification,
        }

    def _fork(self, pack: VerifiedMindpack, local_head: dict) -> dict:
        """Preserve a divergent same-identity chain as an explicit fork."""
        manifest = pack.manifest
        archive_id = self._archive.archive_id
        foreign_head = {
            "sequence": manifest["head_sequence"],
            "commit_hash": manifest["head_commit_hash"],
        }
        with open_ccf_connection(self._archive.settings) as conn:
            with conn.transaction():
                # Preserve foreign objects as evidence; keep both heads.
                insert_pack_objects(
                    conn, archive_id, pack, updated_at=self._archive.clock(),
                    skip_existing=True,
                )
                conn.execute(
                    """
                    INSERT INTO foreign_custody (
                        archive_id, source_archive_id, pack_id,
                        genesis_commit_hash, head_commit_hash, head_sequence,
                        commits_json, received_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (archive_id, source_archive_id, head_commit_hash)
                    DO NOTHING
                    """,
                    (
                        archive_id,
                        manifest["archive_id"],
                        manifest["pack_id"],
                        manifest["genesis_commit_hash"],
                        manifest["head_commit_hash"],
                        int(manifest["head_sequence"]),
                        _jsonb(list(pack.commits)),
                        self._archive.clock(),
                    ),
                )
        fork = ForkRecord(local_head=local_head, foreign_head=foreign_head)
        result = fork.to_dict()
        result["archive_id"] = archive_id
        return result

    # ------------------------------------------------------------------
    # Forks / custody inspection
    # ------------------------------------------------------------------

    def forks(self) -> list[dict]:
        """All preserved foreign custody proofs, forks included."""
        with open_ccf_connection(self._archive.settings) as conn:
            return [
                {
                    "source_archive_id": row[0],
                    "pack_id": row[1],
                    "genesis_commit_hash": row[2],
                    "head_commit_hash": row[3],
                    "head_sequence": str(int(row[4])),
                    "received_at": row[5],
                }
                for row in conn.execute(
                    """
                    SELECT source_archive_id, pack_id, genesis_commit_hash,
                           head_commit_hash, head_sequence, received_at
                    FROM foreign_custody WHERE archive_id = %s
                    ORDER BY source_archive_id, head_commit_hash
                    """,
                    (self._archive.archive_id,),
                ).fetchall()
            ]

    def _package_root(self) -> Path:
        return self._archive.package_root


def _jsonb(value):
    from psycopg.types.json import Jsonb

    return Jsonb(value)
