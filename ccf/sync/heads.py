"""Sync-head exchange and range negotiation (spec section 6.7).

A sync-head document pins the archive head (sequence + commit hash), the
semantic-catalog root, and every known producer head (producer ID → batch
hash). Exchanging heads is the first sync step: the pair decides whether
one side is ahead (push/pull a delta pack over the missing commit range),
both are equal, the archives are foreign (foreign merge), or the chains
have diverged (an explicit fork — never silently resolved, spec 11.7).
"""

from __future__ import annotations

from ccf.sync.packio import PackError

SCHEMA_SYNC_HEAD = "urn:ccf:schema:0.1.2-rc1:sync.head"


class NegotiationError(PackError):
    """Raised when two heads cannot be reconciled into a sync plan."""


def build_sync_head(conn, *, archive_id: str, schemas) -> dict:
    """Build and schema-validate this archive's sync-head document."""
    archive = conn.execute(
        "SELECT epoch_id, semantic_catalog_root FROM archive WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if archive is None:
        raise NegotiationError(f"unknown archive: {archive_id}")
    head = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if head is None:
        raise NegotiationError(f"archive {archive_id} has no head")
    producer_heads = {
        row[0]: row[2]
        for row in conn.execute(
            "SELECT producer_id, producer_sequence, batch_hash FROM producer_head"
        ).fetchall()
    }
    document = {
        "archive_id": archive_id,
        "epoch_id": archive[0],
        "head_sequence": str(int(head[0])),
        "head_commit_hash": head[1],
        "semantic_catalog_root": archive[1],
        "producer_heads": producer_heads,
    }
    schemas.validate(SCHEMA_SYNC_HEAD, document, what="sync head")
    return document


def negotiate(local: dict, remote: dict) -> dict:
    """Negotiate two sync-head documents into a sync plan.

    Returns one of::

        {"relationship": "equal"}
        {"relationship": "pull", "commit_range": {"from_sequence", "through_sequence"}}
        {"relationship": "push", "commit_range": {...}}
        {"relationship": "foreign"}   # different archive IDs
        {"relationship": "fork"}      # same archive, divergent heads
        {"relationship": "unknown-ahead"/"unknown-behind"} with the range
            to request when only sequences differ and prefix hashes are
            unverified (the pack itself proves or disproves the prefix)
    """
    for side, document in (("local", local), ("remote", remote)):
        for field in ("archive_id", "epoch_id", "head_sequence", "head_commit_hash",
                      "semantic_catalog_root", "producer_heads"):
            if field not in document:
                raise NegotiationError(f"{side} sync head missing {field!r}")
    if local["archive_id"] != remote["archive_id"]:
        return {"relationship": "foreign"}
    if local["epoch_id"] != remote["epoch_id"]:
        raise NegotiationError(
            "same archive ID with different epochs cannot sync"
        )
    if local["semantic_catalog_root"] != remote["semantic_catalog_root"]:
        raise NegotiationError("semantic catalog root mismatch between heads")

    local_seq = int(local["head_sequence"])
    remote_seq = int(remote["head_sequence"])
    stale_producers = sorted(
        producer
        for producer, batch_hash in remote["producer_heads"].items()
        if local["producer_heads"].get(producer) != batch_hash
    )

    if local["head_commit_hash"] == remote["head_commit_hash"]:
        relationship = {"relationship": "equal"}
    elif local_seq == remote_seq:
        relationship = {"relationship": "fork"}
    elif local_seq < remote_seq:
        relationship = {
            "relationship": "pull",
            "commit_range": {
                "from_sequence": str(local_seq),
                "through_sequence": str(remote_seq),
            },
        }
    else:
        relationship = {
            "relationship": "push",
            "commit_range": {
                "from_sequence": str(remote_seq),
                "through_sequence": str(local_seq),
            },
        }
    relationship["stale_producers"] = stale_producers
    return relationship


def negotiate_blob_chunks(local_withheld: list[str], remote_available: list[str]) -> list[str]:
    """Blob IDs the local side still needs byte ranges for.

    ``local_withheld`` are locally known Blob IDs without bytes;
    ``remote_available`` are Blob IDs the remote can serve. The result is
    the intersection, in deterministic order.
    """
    return sorted(set(local_withheld) & set(remote_available))
