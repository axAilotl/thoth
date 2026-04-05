"""Compatibility wrapper for archivist retrieval selection."""

from __future__ import annotations

import asyncio

from .archivist_retrieval.models import ArchivistCandidate, ArchivistSelectionResult
from .archivist_retrieval.service import select_archivist_candidates_async


def select_archivist_candidates(
    topic,
    *,
    config,
    layout=None,
    db=None,
    llm_interface=None,
) -> ArchivistSelectionResult:
    """Synchronously select archivist candidates for non-async call sites."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            select_archivist_candidates_async(
                topic,
                config=config,
                layout=layout,
                db=db,
                llm_interface=llm_interface,
            )
        )
    raise RuntimeError(
        "select_archivist_candidates() cannot be used inside an active event loop; "
        "use select_archivist_candidates_async() instead"
    )


__all__ = [
    "ArchivistCandidate",
    "ArchivistSelectionResult",
    "select_archivist_candidates",
]
