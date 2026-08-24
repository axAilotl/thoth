"""CCF dual-write mirror (checklist section 10a).

Config-gated mirror of legacy Thoth captures into a local CCF archive.
The legacy stores (SQLite ``metadata_db`` + ``knowledge_vault`` files,
optionally the Postgres capture event store) stay authoritative; this
package only *mirrors* persisted capture artifacts into CCF through the
canonical producer -> signed batch -> admission path, reusing the
``ccf.thothmap`` converters.

Gating: ``database.ccf_archive.enabled`` + ``database.ccf_archive.dual_write``
must both be true. When off, legacy paths never import this package (see
``core.ccf_dualwrite.dual_write_requested``). Contradictory config
(``dual_write`` without ``enabled``) or missing key paths/DSN fail closed
in :func:`ccf.dualwrite.config.resolve_dual_write_settings`. The phase-2
converter families (``ccf.dualwrite.families``: transcripts, semantic
entities/assertions, review decisions, wiki projections) are each gated
behind their own ``mirror_*`` flag under ``database.ccf_archive`` for
incremental rollout; each family mirrors as its own batch and fails open
(ledgered) without blocking the others.

Rollback: set ``database.ccf_archive.dual_write`` (and ``enabled``) to
``false`` — the mirror stops being constructed and legacy behavior is
byte-identical to before — then drop the CCF Postgres schema
(``DROP SCHEMA <database.ccf_archive.schema> CASCADE``; default ``ccf``).
Nothing in the legacy SQLite/knowledge_vault stores references the mirror,
so no legacy cleanup is required.
"""

from ccf.dualwrite.config import DualWriteSettings, resolve_dual_write_settings
from ccf.dualwrite.ledger import append_error, read_errors
from ccf.dualwrite.service import CcfDualWriteService, DualWriteError

__all__ = [
    "CcfDualWriteService",
    "DualWriteError",
    "DualWriteSettings",
    "append_error",
    "read_errors",
    "resolve_dual_write_settings",
]
