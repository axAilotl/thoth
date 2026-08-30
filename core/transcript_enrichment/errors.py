"""Domain errors for transcript enrichment."""

from __future__ import annotations


class TranscriptEnrichmentError(RuntimeError):
    """Raised when transcript enrichment cannot complete without fabrication."""
