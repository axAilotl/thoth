"""Foreign merge (spec section 11.3).

A foreign merge preserves the source archive's portable objects and
custody proofs, then re-admits the objects into the destination archive
under the destination's own admission order and policy:

- portable IDs and object hashes are unchanged (never remapped, spec 2.4);
- the source commit chain is preserved as a foreign-custody proof row;
- source commit Records are imported as ordinary destination members;
- only the destination's own new commit is excluded from its member root
  (handled by ``commit_objects``);
- imported lineage assertions only *create* destination lineage heads —
  they never move an existing destination head, so destination overlays
  may tighten but never silently widen imported policy.
"""

from __future__ import annotations

from ccf.admission import ResolvedObject, commit_objects, load_archive, lock_archive_head
from ccf.objects import now_timestamp
from ccf.sync.packio import PackError
from ccf.sync.restore import VerifiedMindpack


class MergeError(PackError):
    """Raised when a foreign merge cannot proceed safely."""


def merge_mindpack(
    conn,
    *,
    pack: VerifiedMindpack,
    destination_archive_id: str,
    catalog,
    registries,
    signer,
    clock=now_timestamp,
    salt_fn=None,
) -> dict:
    """Merge a verified foreign mindpack into the destination archive.

    Caller holds the connection; the whole merge is one transaction:
    object inserts, the custody proof, and the destination commit.
    """
    from ccf.objects import new_salt as default_salt

    salt_fn = salt_fn or default_salt
    manifest = pack.manifest
    source_archive_id = manifest["archive_id"]
    if source_archive_id == destination_archive_id:
        raise MergeError(
            "same-identity import is restore/delta/fork, not foreign merge"
        )
    archive = load_archive(conn, destination_archive_id)
    head = lock_archive_head(conn, destination_archive_id)

    # Order objects by source admission order (journal order), which keeps
    # reference targets ahead of referrers in destination member order.
    ordered_ids: list[str] = []
    members_by_sequence: dict[int, list[dict]] = {}
    for member in pack.members:
        members_by_sequence.setdefault(int(member["commit_sequence"]), []).append(member)
    for commit in pack.commits:
        sequence = int(commit["sequence"])
        for member in sorted(
            members_by_sequence.get(sequence, []),
            key=lambda m: int(m["commit_position"]),
        ):
            ordered_ids.append(member["object_id"])
        ordered_ids.append(commit["record_id"])  # source commit Record as evidence

    existing_lineages = {
        row[0]
        for row in conn.execute(
            "SELECT lineage_id FROM lineage_head WHERE archive_id = %s",
            (destination_archive_id,),
        ).fetchall()
    }
    # Lineages this merge creates are merge-owned: later imported
    # transitions keep advancing them within the merge transaction.

    # Objects the source declared withheld or erased: an absent compartment
    # must be recorded as unavailable, never dropped — the destination
    # header keeps the compartment commitment, and chain verification
    # fails closed when the row is simply missing. The exact availability
    # state is preserved per compartment (0.1.2-rc1): erased stays erased,
    # withheld stays withheld.
    unavailable_ids = set(manifest.get("withheld", [])) | set(
        manifest.get("erased", [])
    )
    erased_ids = set(manifest.get("erased", []))
    # Per-compartment declarations (0.1.2-rc1 manifest) take precedence
    # over the object-level lists.
    declared_states: dict[tuple[str, str], str] = {}
    for entry in manifest.get("compartment_availability", []):
        if entry["availability"] in ("withheld", "erased"):
            declared_states[(entry["object_id"], entry["compartment"])] = entry[
                "availability"
            ]
    unavailable_compartments: list[tuple[str, str, str]] = []

    resolved: list[ResolvedObject] = []
    skipped_existing: list[str] = []
    not_included: list[str] = []
    held_overlays: list[str] = []
    seen: set[str] = set()
    for object_id in ordered_ids:
        if object_id in seen:
            continue
        seen.add(object_id)
        obj = pack.objects.get(object_id)
        if obj is None:
            # Partial pack (explicitly allowed): this member is not
            # imported; the completeness report lists the dangling refs.
            not_included.append(object_id)
            continue
        existing = conn.execute(
            "SELECT object_hash FROM object_header WHERE id = %s", (object_id,)
        ).fetchone()
        if existing is not None:
            if existing[0] != obj.header["object_hash"]:
                raise MergeError(
                    f"hard ID collision at {object_id}: same ID, different content"
                )
            skipped_existing.append(object_id)
            continue
        for compartment, envelope in (
            ("structural", obj.structural),
            ("semantic", obj.semantic),
        ):
            if envelope is not None:
                continue
            if obj.header.get(f"{compartment}_commitment") is None:
                continue  # object legitimately lacks this compartment
            if object_id not in unavailable_ids:
                raise MergeError(
                    f"object {object_id} has no {compartment} compartment and is "
                    "not declared withheld/erased; refusing to fabricate state"
                )
            if compartment == "structural":
                raise MergeError(
                    f"object {object_id} has no structural compartment; "
                    "withheld source material cannot be re-admitted"
                )
            state = declared_states.get(
                (object_id, compartment),
                "erased" if object_id in erased_ids else "withheld",
            )
            unavailable_compartments.append((object_id, compartment, state))
        lineage_update = None
        lineage_block = obj.structural["content"].get("lineage")
        if lineage_block is not None:
            lineage_id = lineage_block["lineage_id"]
            if lineage_id in existing_lineages:
                # Destination already has this lineage; hold the imported
                # assertion instead of silently moving the head (11.3).
                held_overlays.append(lineage_id)
            else:
                lineage_update = (lineage_id, lineage_block["transition"])
        resolved.append(
            ResolvedObject(
                object_kind=obj.object_kind,
                object_id=object_id,
                header=obj.header,
                structural=obj.structural,
                semantic=obj.semantic,
                submission_hash=None,
                origin=None,
                lineage_update=lineage_update,
                blob_data=pack.blob_data.get(object_id),
            )
        )

    received_at = clock()
    commit_sequence: int | None = None
    commit_hash: str | None = None
    if resolved:
        commit_sequence, commit_hash = commit_objects(
            conn,
            archive=archive,
            head=head,
            objects=resolved,
            catalog=catalog,
            registries=registries,
            signer=signer,
            committed_at=received_at,
            salt_fn=salt_fn,
        )
    # Record the unavailable state of declared-withheld/erased compartments
    # (headers exist once commit_objects ran; same transaction), preserving
    # the exact source state.
    for object_id, compartment, state in unavailable_compartments:
        conn.execute(
            """
            INSERT INTO compartment (
                object_id, compartment, state, updated_at
            ) VALUES (%s, %s, %s, %s)
            """,
            (object_id, compartment, state, received_at),
        )
    # Blob content is a third compartment outside the compartment table:
    # commit_objects wrote it 'withheld' when the pack carries no bytes;
    # mirror the source's erased state exactly (salt destroyed at erasure).
    for obj in resolved:
        if obj.object_kind != "blob" or obj.blob_data is not None:
            continue
        content_state = declared_states.get((obj.object_id, "blob_content"))
        if content_state is None and obj.object_id in erased_ids:
            content_state = "erased"
        if content_state == "erased":
            conn.execute(
                """
                UPDATE blob_content
                SET state = 'erased', content_salt = NULL, updated_at = %s
                WHERE blob_id = %s AND state = 'withheld'
                """,
                (received_at, obj.object_id),
            )

    # Custody proof: the source chain, preserved as foreign evidence.
    commits_with_members = []
    for commit in pack.commits:
        sequence = int(commit["sequence"])
        commits_with_members.append(
            dict(
                commit,
                members=sorted(
                    members_by_sequence.get(sequence, []),
                    key=lambda m: int(m["commit_position"]),
                ),
            )
        )
    conn.execute(
        """
        INSERT INTO foreign_custody (
            archive_id, source_archive_id, pack_id, genesis_commit_hash,
            head_commit_hash, head_sequence, commits_json, received_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (archive_id, source_archive_id, head_commit_hash) DO NOTHING
        """,
        (
            destination_archive_id,
            source_archive_id,
            manifest["pack_id"],
            manifest["genesis_commit_hash"],
            manifest["head_commit_hash"],
            int(manifest["head_sequence"]),
            _jsonb(commits_with_members),
            received_at,
        ),
    )

    return {
        "status": "merged",
        "source_archive_id": source_archive_id,
        "destination_archive_id": destination_archive_id,
        "admitted": [obj.object_id for obj in resolved],
        "skipped_existing": skipped_existing,
        "not_included": not_included,
        "lineage_overlays_held": sorted(set(held_overlays)),
        "commit_sequence": str(commit_sequence) if commit_sequence is not None else None,
        "commit_hash": commit_hash,
        "custody_proof": f"{source_archive_id}:{manifest['head_commit_hash']}",
        "partial": pack.partial,
        "completeness": pack.completeness.to_dict(),
    }


def _jsonb(value):
    from psycopg.types.json import Jsonb

    return Jsonb(value)
