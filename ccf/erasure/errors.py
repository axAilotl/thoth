"""Erasure error types."""


class ErasureError(RuntimeError):
    """Raised when an erasure operation cannot proceed safely."""


class RetentionViolation(ErasureError):
    """A requested erasure exceeds the registry-declared retention profile."""


class SuppressionKeyError(ErasureError):
    """The suppression store key is missing, unreadable, or malformed."""
