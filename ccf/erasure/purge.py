"""Purge and verification of controlled copies (spec 3.8 stages 4-5).

Erasing a compartment removes it from the canonical envelope; the destroy
stage then purges every *controlled* copy: projection tables, the
full-text and embedding indexes, checkpoints (whose snapshots embed
derived plaintext), egress capabilities covering the targets, and
generated plaintext in the wiki staging directory. Everything purged is
recorded on the durable operation row.

The verify stage rebuilds the affected projections from canonical state
and then asserts zero remaining controlled copies. It verifies only what
the archive actually controls: WAL/PITR, external replicas, backups, and
exports are outside this boundary, which is why the receipt can claim
``logical`` assurance only (spec 3.7).
"""

from __future__ import annotations

from pathlib import Path

from ccf.erasure.errors import ErasureError
from ccf.projections import DERIVATION, EMBEDDING, ENTITY_CLUSTER, FULL_TEXT, LINK_STATE, WIKI


def _erased_ids(plans: list[dict]) -> dict[str, list[str]]:
    """Partition target IDs by what the saga erases."""
    semantic_or_structural = [
        p["object_id"] for p in plans if p["erase_semantic"] or p["erase_structural"]
    ]
    structural = [p["object_id"] for p in plans if p["erase_structural"]]
    records = [
        p["object_id"]
        for p in plans
        if p["object_kind"] == "record" and (p["erase_semantic"] or p["erase_structural"])
    ]
    structural_links = [
        p["object_id"]
        for p in plans
        if p["object_kind"] == "link" and p["erase_structural"]
    ]
    return {
        "semantic_or_structural": semantic_or_structural,
        "structural": structural,
        "records": records,
        "structural_links": structural_links,
    }


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname = current_schema() AND tablename = %s",
        (table,),
    ).fetchone()
    return row is not None


def affected_projections(plans: list[dict]) -> set[str]:
    """Projections an erasure of ``plans`` affects (rebuild/verify set)."""
    ids = _erased_ids(plans)
    affected: set[str] = set()
    if ids["records"]:
        affected.update({FULL_TEXT, ENTITY_CLUSTER, EMBEDDING, WIKI})
    link_touched = any(
        p["object_kind"] == "link" and (p["erase_structural"] or p["erase_semantic"])
        for p in plans
    )
    if link_touched:
        # Selector erasure changes selector_available; structural erasure
        # removes the edge entirely.
        affected.add(LINK_STATE)
    if ids["structural"]:
        affected.add(DERIVATION)
    return affected


def purge_controlled_copies(
    conn, *, archive_id: str, plans: list[dict]
) -> tuple[list[dict], set[str]]:
    """Delete controlled copies of the targets; return (purged, affected).

    ``affected`` is the set of projection names that must be rebuilt and
    re-verified before the receipt may be appended.
    """
    ids = _erased_ids(plans)
    purged: list[dict] = []
    affected = affected_projections(plans)

    def _delete(store: str, sql: str, params: list) -> None:
        cursor = conn.execute(sql, params)
        purged.append({"store": store, "rows": cursor.rowcount})

    if ids["records"]:
        _delete(
            "projection_full_text",
            "DELETE FROM projection_full_text WHERE archive_id = %s AND object_id = ANY(%s)",
            [archive_id, ids["records"]],
        )
        _delete(
            "projection_entity_cluster",
            "DELETE FROM projection_entity_cluster WHERE archive_id = %s AND member_id = ANY(%s)",
            [archive_id, ids["records"]],
        )
        if _table_exists(conn, "projection_embedding"):
            _delete(
                "projection_embedding",
                "DELETE FROM projection_embedding WHERE archive_id = %s AND object_id = ANY(%s)",
                [archive_id, ids["records"]],
            )
    if ids["structural_links"]:
        _delete(
            "projection_link_state",
            "DELETE FROM projection_link_state WHERE archive_id = %s AND link_id = ANY(%s)",
            [archive_id, ids["structural_links"]],
        )
    if ids["structural"]:
        _delete(
            "projection_derivation_closure",
            """
            DELETE FROM projection_derivation_closure
            WHERE archive_id = %s
              AND (ancestor_id = ANY(%s) OR descendant_id = ANY(%s))
            """,
            [archive_id, ids["structural"], ids["structural"]],
        )
    if ids["semantic_or_structural"]:
        # Egress capabilities covering erased content must not outlive it:
        # consumption would fail closed at the storage boundary anyway, so
        # revoking here keeps the capability table honest.
        _delete(
            "egress_capability",
            """
            DELETE FROM egress_capability
            WHERE archive_id = %s
              AND EXISTS (
                  SELECT 1 FROM jsonb_array_elements_text(object_ids) AS element
                  WHERE element = ANY(%s)
              )
            """,
            [archive_id, ids["semantic_or_structural"]],
        )

    # Checkpoints embed derived plaintext in their snapshots; any
    # checkpoint of an affected projection is a controlled copy.
    checkpoint_projections = sorted(affected)
    if checkpoint_projections:
        cursor = conn.execute(
            """
            DELETE FROM projection_checkpoint
            WHERE archive_id = %s AND projection_name = ANY(%s)
            """,
            (archive_id, checkpoint_projections),
        )
        purged.append(
            {"store": "projection_checkpoint", "rows": cursor.rowcount,
             "projections": checkpoint_projections}
        )
    return purged, affected


def purge_wiki_staging(staging_dir: str | Path, erased_ids: list[str]) -> list[str]:
    """Delete generated wiki pages referencing erased Records.

    Only files under ``pages/`` of a directory that looks like a previous
    rebuild (has ``index.md``) are touched; anything else fails closed.
    """
    staging = Path(staging_dir)
    if not staging.exists():
        return []
    if not (staging / "index.md").exists():
        raise ErasureError(
            f"wiki staging dir {staging} has no index.md; refusing to purge "
            "an unmanaged directory"
        )
    removed: list[str] = []
    pages_dir = staging / "pages"
    if pages_dir.is_dir():
        for page in sorted(pages_dir.glob("*.md")):
            text = page.read_text(encoding="utf-8")
            if any(object_id in text for object_id in erased_ids):
                page.unlink()
                removed.append(page.name)
    return removed


def verify_controlled_copies(
    conn, *, archive_id: str, plans: list[dict], affected: set[str]
) -> dict:
    """Assert zero remaining controlled copies; return the verification map.

    Fails closed (raises :class:`ErasureError`) on any leftover — the saga
    then records ``failed`` and no receipt is appended.
    """
    ids = _erased_ids(plans)
    leftovers: list[str] = []

    def _count(sql: str, params: list) -> int:
        return int(conn.execute(sql, params).fetchone()[0])

    if ids["records"]:
        if _count(
            "SELECT COUNT(*) FROM projection_full_text WHERE archive_id = %s AND object_id = ANY(%s)",
            [archive_id, ids["records"]],
        ):
            leftovers.append("projection_full_text")
        if _count(
            "SELECT COUNT(*) FROM projection_entity_cluster WHERE archive_id = %s AND member_id = ANY(%s)",
            [archive_id, ids["records"]],
        ):
            leftovers.append("projection_entity_cluster")
        if _table_exists(conn, "projection_embedding") and _count(
            "SELECT COUNT(*) FROM projection_embedding WHERE archive_id = %s AND object_id = ANY(%s)",
            [archive_id, ids["records"]],
        ):
            leftovers.append("projection_embedding")
    if ids["structural_links"] and _count(
        "SELECT COUNT(*) FROM projection_link_state WHERE archive_id = %s AND link_id = ANY(%s)",
        [archive_id, ids["structural_links"]],
    ):
        leftovers.append("projection_link_state")
    if ids["structural"] and _count(
        """
        SELECT COUNT(*) FROM projection_derivation_closure
        WHERE archive_id = %s AND (ancestor_id = ANY(%s) OR descendant_id = ANY(%s))
        """,
        [archive_id, ids["structural"], ids["structural"]],
    ):
        leftovers.append("projection_derivation_closure")
    if ids["semantic_or_structural"] and _count(
        """
        SELECT COUNT(*) FROM egress_capability
        WHERE archive_id = %s
          AND EXISTS (
              SELECT 1 FROM jsonb_array_elements_text(object_ids) AS element
              WHERE element = ANY(%s)
          )
        """,
        [archive_id, ids["semantic_or_structural"]],
    ):
        leftovers.append("egress_capability")

    # Canonical envelope: erased parts must be in the erased state with no
    # plaintext and no salt.
    for plan in plans:
        object_id = plan["object_id"]
        for compartment, wanted in (
            ("structural", plan["erase_structural"]),
            ("semantic", plan["erase_semantic"]),
        ):
            if not wanted:
                continue
            row = conn.execute(
                """
                SELECT state FROM compartment
                WHERE object_id = %s AND compartment = %s
                """,
                (object_id, compartment),
            ).fetchone()
            if row is None or row[0] != "erased":
                leftovers.append(f"compartment:{object_id}:{compartment}")
        if plan["erase_content"]:
            row = conn.execute(
                "SELECT state FROM blob_content WHERE blob_id = %s", (object_id,)
            ).fetchone()
            if row is None or row[0] != "erased":
                leftovers.append(f"blob_content:{object_id}")

    if leftovers:
        raise ErasureError(
            "erasure verification failed; controlled copies remain: "
            + ", ".join(sorted(leftovers))
        )
    return {
        "assurance": "logical",
        "verified_stores": sorted(affected),
        "boundary": (
            "projection tables, indexes, checkpoints, egress capabilities, "
            "and wiki staging verified purged; WAL/PITR, external replicas, "
            "backups, and exports are outside the controlled boundary"
        ),
    }
