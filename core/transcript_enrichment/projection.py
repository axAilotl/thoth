"""Rebuild projection files from cached derivatives without processor calls."""

from __future__ import annotations

from pathlib import Path

from .models import TranscriptDerivative
from .storage import _atomic_write_text, resolve_derivative_path


def rebuild_projections_from_derivatives(
    *,
    vault_root: Path,
    derivatives: tuple[TranscriptDerivative, ...],
) -> tuple[TranscriptDerivative, ...]:
    """Recreate projection files from cached derivatives at their stored paths.

    The derivative objects are returned unchanged: their versions, paths,
    ``created_at``, cache keys, and content commitments are preserved. The file
    bytes written are exactly ``derivative.content``.
    """
    for derivative in derivatives:
        absolute_path = resolve_derivative_path(derivative.path, vault_root)
        _atomic_write_text(absolute_path, derivative.content)

    return derivatives
