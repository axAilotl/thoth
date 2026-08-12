"""Governance errors (fail closed: governance errors deny, never allow)."""

from __future__ import annotations


class GovernanceError(RuntimeError):
    """Raised when a governance operation cannot proceed safely."""


class CapabilityError(GovernanceError):
    """Raised when an egress capability is unknown, stale, or exhausted."""
