"""Processing request enum, strict payload validation, and task-local ContextVar.

The processing request is the single source of truth for what a queue-driven
transcript run should do. It is carried as a dedicated JSON object in the queue
payload and propagated to the enrichment service through a task-local
``ContextVar`` so that ``dispatch_artifact(artifact)`` keeps its public
signature while remaining concurrency-safe.
"""

from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterator

from .outputs import TranscriptOutput


class ProcessingMode(str, Enum):
    """How the runtime should use cached transcript derivatives.

    - ``REUSE``: use cached derivatives when valid; compute only on cache miss.
    - ``REBUILD_PROJECTION``: reuse cached content to rewrite projection files
      and refresh the search index without calling processors. Fails closed if
      no valid cache entry exists.
    - ``RECOMPUTE``: ignore cache and call processors to produce a new
      derivative version.
    """

    REUSE = "reuse"
    REBUILD_PROJECTION = "rebuild_projection"
    RECOMPUTE = "recompute"


class TranscriptRequestError(ValueError):
    """Raised when a processing request is malformed."""


@dataclass(frozen=True)
class ProcessingRequest:
    """Schema-validated processing directive carried in the queue payload.

    The request is always a JSON object. ``mode`` controls cache behavior;
    ``outputs`` declares the durable outputs that must be produced. The mode
    never implies an output set, and the output set never implies a mode.
    """

    mode: ProcessingMode = ProcessingMode.REUSE
    outputs: tuple[TranscriptOutput, ...] = (TranscriptOutput.TRANSCRIPT,)

    @classmethod
    def default(cls) -> "ProcessingRequest":
        return cls(ProcessingMode.REUSE, (TranscriptOutput.TRANSCRIPT,))

    @classmethod
    def from_payload(cls, value: Any) -> "ProcessingRequest":
        """Parse a processing_request value from the queue payload.

        Malformed values raise ``TranscriptRequestError`` so the queue entry is
        rejected rather than silently falling back.
        """
        if value is None:
            return cls.default()
        if not isinstance(value, dict):
            raise TranscriptRequestError(
                "processing_request must be a JSON object"
            )

        allowed = {"mode", "outputs"}
        extra = set(value.keys()) - allowed
        if extra:
            raise TranscriptRequestError(
                f"processing_request has unknown keys: {sorted(extra)}"
            )

        mode = _parse_mode(value.get("mode", ProcessingMode.REUSE.value))
        outputs = _parse_outputs(value.get("outputs"))
        return cls(mode, outputs)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "outputs": [output.value for output in self.outputs],
        }


def _parse_mode(value: Any) -> ProcessingMode:
    if value is None:
        return ProcessingMode.REUSE
    if not isinstance(value, str):
        raise TranscriptRequestError("processing_request.mode must be a string")
    try:
        return ProcessingMode(value)
    except ValueError as exc:
        raise TranscriptRequestError(
            f"unknown processing_request.mode: {value!r}"
        ) from exc


def _parse_outputs(value: Any) -> tuple[TranscriptOutput, ...]:
    if value is None:
        return (TranscriptOutput.TRANSCRIPT,)
    if not isinstance(value, list):
        raise TranscriptRequestError("processing_request.outputs must be a list")
    if not value:
        raise TranscriptRequestError(
            "processing_request.outputs must contain at least one output"
        )

    seen: set[str] = set()
    parsed: list[TranscriptOutput] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise TranscriptRequestError(
                f"processing_request.outputs[{index}] must be a string"
            )
        if item not in TranscriptOutput.values():
            raise TranscriptRequestError(
                f"unknown processing_request.outputs[{index}]: {item!r}"
            )
        if item in seen:
            raise TranscriptRequestError(
                f"duplicate output in processing_request.outputs: {item!r}"
            )
        seen.add(item)
        parsed.append(TranscriptOutput(item))

    return tuple(parsed)


_current_processing_request: contextvars.ContextVar[
    ProcessingRequest | None
] = contextvars.ContextVar("transcript_processing_request", default=None)


def current_processing_request() -> ProcessingRequest | None:
    """Return the processing request for the current async task, if any."""
    return _current_processing_request.get()


@contextlib.contextmanager
def processing_request_scope(request: ProcessingRequest) -> Iterator[None]:
    """Set ``request`` as the current task-local request for the duration of the block."""
    token = _current_processing_request.set(request)
    try:
        yield
    finally:
        _current_processing_request.reset(token)
