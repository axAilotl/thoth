"""CCF sync and packs (spec sections 6.7-6.8, 11).

- :mod:`ccf.sync.heads` — sync-head exchange and range negotiation;
- :mod:`ccf.sync.export` — mindpack export (11.1, 11.5);
- :mod:`ccf.sync.restore` — restore/replica into an empty store (11.2);
- :mod:`ccf.sync.merge` — foreign merge with custody proofs (11.3);
- :mod:`ccf.sync.delta` — compressed, resumable delta packs (11.4);
- :mod:`ccf.sync.chunks` / :mod:`ccf.sync.transport` — verified chunk
  digests and byte-range resume over file/USB or HTTP;
- :mod:`ccf.sync.completeness` — reference completeness (2.5);
- :mod:`ccf.sync.manifest` — manifest cross-checks against the inventory
  derived from verified pack contents (11.5);
- :mod:`ccf.sync.service` — ``Archive.sync()`` facade (merge/fork dispatch).
"""

from ccf.sync.heads import build_sync_head, negotiate, negotiate_blob_chunks
from ccf.sync.restore import restore_mindpack, verify_mindpack

__all__ = [
    "build_sync_head",
    "negotiate",
    "negotiate_blob_chunks",
    "restore_mindpack",
    "verify_mindpack",
]
