"""Projection service: the operational entrypoint for projection work.

Owns connection handling for every projection module so callers (Archive,
CLI, background workers) never juggle raw connections:

    archive = Archive.open(...)
    archive.projections.rebuild_all()
    archive.projections.rebuild_wiki("staging/")
    results = archive.projections.search_text("quarterly report")

Reads that serve consequential results should pin a snapshot first
(spec 10.6)::

    pin = archive.projections.pin_snapshot()
    rows = archive.projections.entity_clusters(pin=pin)
"""

from __future__ import annotations

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.projections import checkpoints, consistency, links, wiki
from ccf.projections.consistency import SnapshotPin


class ProjectionService:
    """One archive's projection facade over the CCF envelope."""

    def __init__(self, *, settings: CcfPostgresSettings, archive_id: str) -> None:
        self._settings = settings
        self.archive_id = archive_id

    # -- maintenance -----------------------------------------------------

    def rebuild(self, name: str) -> int:
        """Rebuild one projection from canonical state; returns row count."""
        from ccf.projections.rebuild import rebuild_projection

        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                return rebuild_projection(conn, archive_id=self.archive_id, name=name)

    def rebuild_all(self) -> dict[str, int]:
        """Rebuild every canonically rebuildable projection."""
        from ccf.projections.rebuild import rebuild_all

        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                return rebuild_all(conn, archive_id=self.archive_id)

    def rebuild_wiki(self, staging_dir) -> dict:
        """Regenerate wiki markdown into a staging directory."""
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                return wiki.rebuild_wiki(conn, self.archive_id, staging_dir)

    # -- consistency (spec 10.6) -----------------------------------------

    def pin_snapshot(self) -> SnapshotPin:
        """Pin one archive head + one dependency-generation vector."""
        with open_ccf_connection(self._settings) as conn:
            return consistency.pin_snapshot(conn, self.archive_id)

    # -- reads ------------------------------------------------------------

    def link_state(self, link_id: str) -> dict | None:
        with open_ccf_connection(self._settings) as conn:
            return links.get_link_state(conn, self.archive_id, link_id)

    def closure_pairs(self) -> dict:
        from ccf.projections import derivation

        with open_ccf_connection(self._settings) as conn:
            return derivation.closure_pairs(conn, self.archive_id)

    def entity_clusters(self, *, pin: SnapshotPin | None = None) -> dict:
        from ccf.projections import ENTITY_CLUSTER, entities

        with open_ccf_connection(self._settings) as conn:
            if pin is not None:
                consistency.require_current(conn, pin, ENTITY_CLUSTER)
            return entities.clusters(conn, self.archive_id)

    def search_text(self, query: str, *, limit: int = 50) -> list[dict]:
        from ccf.projections import fulltext

        with open_ccf_connection(self._settings) as conn:
            return fulltext.search(conn, self.archive_id, query, limit=limit)

    # -- embeddings --------------------------------------------------------

    def put_embedding(self, *, object_id: str, model_id: str, vector) -> None:
        from ccf.projections import vectors

        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                vectors.put_embedding(
                    conn,
                    archive_id=self.archive_id,
                    object_id=object_id,
                    model_id=model_id,
                    vector=vector,
                )

    def nearest(self, *, model_id: str, query_vector, limit: int = 10) -> list[dict]:
        from ccf.projections import vectors

        with open_ccf_connection(self._settings) as conn:
            return vectors.nearest(
                conn,
                archive_id=self.archive_id,
                model_id=model_id,
                query_vector=query_vector,
                limit=limit,
            )

    # -- checkpoints (spec 10.5) -------------------------------------------

    def save_checkpoint(self, projection_name: str) -> dict:
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                return checkpoints.save_checkpoint(
                    conn, archive_id=self.archive_id, projection_name=projection_name
                )

    def recover(self, projection_name: str) -> dict:
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                return checkpoints.recover(
                    conn, archive_id=self.archive_id, projection_name=projection_name
                )
