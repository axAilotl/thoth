"""Thoth-to-CCF concept mapping (checklist section 4).

Converters from existing Thoth data shapes to CCF producer submissions,
one module per domain:

- ``sources``     capture source -> ``core.source``
- ``sessions``    capture session -> ``core.session``; connector run -> ``process.run``
- ``artifacts``   raw files/media -> Blob + ``experience.artifact`` + ``has_blob``
- ``transcripts`` transcripts -> ``experience.utterance`` derived from source media
- ``findings``    security scans -> ``security.finding`` with exact evidence
- ``semantic``    canonical entities -> ``semantic.entity``; memory candidates
                  -> candidate ``semantic.assertion`` Records
- ``review``      human review -> ``governance.review_decision`` + accepted successor
- ``wiki``        wiki pages -> rebuildable projections (never source replacements)

All converters take plain-dict snapshots of Thoth records and a
:class:`ccf.producer.Producer`; nothing here reads Thoth databases or the
filesystem, and nothing here is called by collectors/processors yet — the
dual-write phase is a thin wiring step on top of these functions.
"""

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    ccf_timestamp,
    claims,
    combine,
    data_subject,
    occurred_at,
    origin,
)

__all__ = [
    "MapContext",
    "MappedSubmissions",
    "ThothMapError",
    "ccf_timestamp",
    "claims",
    "combine",
    "data_subject",
    "occurred_at",
    "origin",
]
