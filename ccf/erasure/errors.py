"""Erasure error types."""


class ErasureError(RuntimeError):
    """Raised when an erasure operation cannot proceed safely."""


class RetentionViolation(ErasureError):
    """A requested erasure exceeds the registry-declared retention profile."""


class SuppressionKeyError(ErasureError):
    """The suppression store key is missing, unreadable, or malformed."""


class SuppressionProjectionError(ErasureError):
    """The suppression lookup projection drifted from canonical state.

    Raised when projection rows are missing, extra, or no longer match the
    canonical ``lineage.suppression_set`` lineage (spec 12.7): deletion or
    tampering is detected and must be repaired by rebuilding the projection
    from canonical state, never silently tolerated.
    """
