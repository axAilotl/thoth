"""Dataclasses for transcript enrichment results and derivatives."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from core.artifacts import DerivedOutput
from core.time_utils import utc_now_iso

from .identity import ProcessorIdentity, TranscriptIdentityError
from .request import ProcessingMode


class TranscriptDerivativeError(ValueError):
    """Raised when a derivative is structurally invalid or has been tampered with."""


_OUTPUT_MEDIA_TYPES = {
    "transcript": "text/markdown",
    "summary": "text/markdown",
    "classification": "application/json",
}

_VALID_OUTPUT_TYPES = set(_OUTPUT_MEDIA_TYPES)
_VERSION_RE = re.compile(r"^v[1-9][0-9]*$")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TranscriptDerivativeError(f"{name} is required and must be a non-blank string")
    return value


def _require_sha256(value: Any, name: str) -> str:
    text = _require_string(value, name)
    if not _SHA256_RE.match(text):
        raise TranscriptDerivativeError(f"{name} must be a 64-character hex SHA-256: {text!r}")
    return text


def _require_choice(value: Any, name: str, choices: set[str]) -> str:
    text = _require_string(value, name)
    if text not in choices:
        raise TranscriptDerivativeError(
            f"{name} must be one of {sorted(choices)}, got {text!r}"
        )
    return text


@dataclass(frozen=True)
class TranscriptDerivative:
    """One reusable derivative object produced from a transcript."""

    output_type: str
    path: str
    content: str
    media_type: str
    version: str
    cache_key: str
    source_hash: str
    content_sha256: str
    processor_identity: ProcessorIdentity
    created_at: str = field(default_factory=utc_now_iso)

    def __post_init__(self) -> None:
        _require_choice(self.output_type, "output_type", _VALID_OUTPUT_TYPES)
        _require_string(self.path, "path")
        if not isinstance(self.content, str):
            raise TranscriptDerivativeError("content is required")
        expected_media = _OUTPUT_MEDIA_TYPES[self.output_type]
        if self.media_type != expected_media:
            raise TranscriptDerivativeError(
                f"media_type for {self.output_type} must be {expected_media}, got {self.media_type!r}"
            )
        if not _VERSION_RE.match(self.version):
            raise TranscriptDerivativeError(
                f"version must match v<N>, got {self.version!r}"
            )
        _require_sha256(self.cache_key, "cache_key")
        _require_sha256(self.source_hash, "source_hash")
        _require_sha256(self.content_sha256, "content_sha256")
        _require_string(self.created_at, "created_at")
        expected = _sha256_text(self.content)
        if self.content_sha256 != expected:
            raise TranscriptDerivativeError(
                f"content_sha256 mismatch: expected {expected}, got {self.content_sha256}"
            )

    def to_derived_output(self) -> DerivedOutput:
        return DerivedOutput(
            output_type=self.output_type,
            path=self.path,
            media_type=self.media_type,
            created_at=self.created_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "output_type": self.output_type,
            "path": self.path,
            "media_type": self.media_type,
            "content": self.content,
            "version": self.version,
            "cache_key": self.cache_key,
            "source_hash": self.source_hash,
            "content_sha256": self.content_sha256,
            "processor_identity": self.processor_identity.to_dict(),
            "created_at": self.created_at,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TranscriptDerivative":
        if not isinstance(value, Mapping):
            raise TranscriptDerivativeError("derivative value must be a mapping")
        output_type = _require_choice(
            value.get("output_type"), "output_type", _VALID_OUTPUT_TYPES
        )
        path = _require_string(value.get("path"), "path")
        content = value.get("content")
        if not isinstance(content, str):
            raise TranscriptDerivativeError("content is required and must be a string")
        media_type = _require_choice(
            value.get("media_type"), "media_type", {_OUTPUT_MEDIA_TYPES[output_type]}
        )
        version = _require_string(value.get("version"), "version")
        if not _VERSION_RE.match(version):
            raise TranscriptDerivativeError(
                f"version must match v<N>, got {version!r}"
            )
        cache_key = _require_sha256(value.get("cache_key"), "cache_key")
        source_hash = _require_sha256(value.get("source_hash"), "source_hash")
        content_sha256 = _require_sha256(value.get("content_sha256"), "content_sha256")
        created_at = _require_string(value.get("created_at"), "created_at")
        expected = _sha256_text(content)
        if content_sha256 != expected:
            raise TranscriptDerivativeError(
                f"content_sha256 mismatch: expected {expected}, got {content_sha256}"
            )
        processor_identity = value.get("processor_identity")
        if not isinstance(processor_identity, Mapping):
            raise TranscriptDerivativeError("processor_identity is required")
        try:
            identity = ProcessorIdentity.from_mapping(processor_identity)
        except TranscriptIdentityError as exc:
            raise TranscriptDerivativeError(f"invalid processor_identity: {exc}") from exc

        return cls(
            output_type=output_type,
            path=path,
            content=content,
            media_type=media_type,
            version=version,
            cache_key=cache_key,
            source_hash=source_hash,
            content_sha256=content_sha256,
            processor_identity=identity,
            created_at=created_at,
        )


@dataclass(frozen=True)
class TranscriptEnrichmentResult:
    """Summary returned to the ingestion runtime."""

    artifact_id: str
    source_hash: str
    cache_key: str
    version: str
    mode: ProcessingMode
    cache_hit: bool
    rerun_requested: bool
    derivatives: tuple[TranscriptDerivative, ...]
    indexed: bool
    source_path: str | None

    def derivative_paths(self) -> dict[str, str]:
        return {
            derivative.output_type: derivative.path
            for derivative in self.derivatives
        }

    def details(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "source_hash": self.source_hash,
            "cache_key": self.cache_key,
            "version": self.version,
            "mode": self.mode.value,
            "cache_hit": self.cache_hit,
            "rerun_requested": self.rerun_requested,
            "derivatives": self.derivative_paths(),
            "indexed": self.indexed,
        }
