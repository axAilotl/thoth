"""Transcript enrichment package.

Public imports are re-exported here so callers can continue to use the focused
facade ``core.transcript_enrichment`` without reaching into submodules.
"""

from .errors import TranscriptEnrichmentError
from .identity import ProcessorIdentity, TranscriptIdentityError
from .models import TranscriptDerivative, TranscriptEnrichmentResult
from .outputs import TranscriptOutput
from .protocols import (
    LocalTranscriptNormalizer,
    TranscriptClassifier,
    TranscriptNormalizer,
    TranscriptSummarizer,
)
from .request import ProcessingMode, ProcessingRequest, TranscriptRequestError
from .runtime import apply_derivatives_to_artifact
from .service import TranscriptEnrichmentService
from .storage import TranscriptStorageError
from .cache_state import TranscriptCacheError

__all__ = [
    "LocalTranscriptNormalizer",
    "ProcessingMode",
    "ProcessingRequest",
    "ProcessorIdentity",
    "TranscriptCacheError",
    "TranscriptClassifier",
    "TranscriptDerivative",
    "TranscriptEnrichmentError",
    "TranscriptEnrichmentResult",
    "TranscriptEnrichmentService",
    "TranscriptIdentityError",
    "TranscriptNormalizer",
    "TranscriptOutput",
    "TranscriptRequestError",
    "TranscriptStorageError",
    "TranscriptSummarizer",
    "apply_derivatives_to_artifact",
]
