"""CCF projections (spec sections 1.4 and 10).

Projections are disposable, rebuildable derivatives of canonical state.
This package owns:

- :mod:`ccf.projections.schema` — additive migration 0002 (all
  ``projection_*`` tables plus fence/invalidation/checkpoint machinery);
- :mod:`ccf.projections.invalidation` — generation fences, the
  invalidation queue, the synchronous admission hook, and the fast-path
  usability check (spec 10.4);
- :mod:`ccf.projections.links` — current Link state from disposition
  lineage heads (spec 8.4);
- :mod:`ccf.projections.derivation` — active ``derived_from`` graph:
  recursive-CTE correctness baseline plus closure-table acceleration
  (spec 8.6, 10.3);
- :mod:`ccf.projections.entities` — entity clusters from resolution
  Records and active ``same_as`` Links (spec 8.5);
- :mod:`ccf.projections.fulltext` — tsvector search over available
  semantic text (spec 10.1);
- :mod:`ccf.projections.vectors` — pgvector storage/query for
  caller-supplied embeddings (spec 10.1);
- :mod:`ccf.projections.wiki` — wiki/knowledge-base rebuild from canonical
  state into a staging directory;
- :mod:`ccf.projections.checkpoints` — projection checkpoints and
  recovery (spec 10.5);
- :mod:`ccf.projections.consistency` — pinned head + generation vector
  for consequential cross-projection reads (spec 10.6).

Rebuild protocol: each projection module exposes ``rebuild(conn,
archive_id)``; :func:`ccf.projections.rebuild.rebuild_all` recomputes
every projection from canonical state. No projection stores a human
decision — destroying them all loses nothing (spec 8.7).
"""

from __future__ import annotations

#: Projection names used as fence namespaces (``projection.<name>``) and as
#: ``projection_invalidation.projection_name`` values. Kept distinct from
#: the governance stream's ``governance.*`` fence namespaces.
LINK_STATE = "link_state"
DERIVATION = "derivation"
ENTITY_CLUSTER = "entity_cluster"
FULL_TEXT = "full_text"
EMBEDDING = "embedding"
WIKI = "wiki"

#: Every rebuildable projection, in dependency order.
PROJECTION_NAMES: tuple[str, ...] = (
    LINK_STATE,
    DERIVATION,
    ENTITY_CLUSTER,
    FULL_TEXT,
    EMBEDDING,
    WIKI,
)

#: Projections with Postgres tables rebuildable by ``rebuild_all``.
#: ``wiki`` rebuilds into a staging directory, not a table.
TABLE_PROJECTIONS: tuple[str, ...] = (
    LINK_STATE,
    DERIVATION,
    ENTITY_CLUSTER,
    FULL_TEXT,
    EMBEDDING,
)
