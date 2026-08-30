"""Durable cache state for transcript derivatives."""

from __future__ import annotations

import re
from typing import Any, Mapping

from core.metadata_db import MetadataDB

from .identity import ProcessorIdentity, TranscriptIdentityError
from .models import TranscriptDerivative, TranscriptDerivativeError
from .outputs import TranscriptOutput


class TranscriptCacheError(RuntimeError):
    """Raised when cached state cannot be trusted or verified."""


_CACHE_STATE_PREFIX = "transcript_derivative_cache"
_VERSION_RE = re.compile(r"^v[1-9][0-9]*$")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")

_CACHE_STATE_KEYS = {
    "artifact_id",
    "source_hash",
    "origin_hash",
    "cache_key",
    "version",
    "processor_identities",
    "derivatives",
}

_DERIVATIVE_KEYS = {
    "output_type",
    "path",
    "content",
    "media_type",
    "version",
    "cache_key",
    "source_hash",
    "content_sha256",
    "processor_identity",
    "created_at",
}


def _require_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TranscriptCacheError(f"cache state missing {name}")
    return value


def _require_sha256(value: Any, name: str) -> str:
    text = _require_string(value, name)
    if not _SHA256_RE.match(text):
        raise TranscriptCacheError(f"cache state {name} is not a valid SHA-256: {text!r}")
    return text


def _cache_state_key(cache_key: str) -> str:
    return f"{_CACHE_STATE_PREFIX}:{cache_key}"


def load_cache_state(
    db: MetadataDB,
    cache_key: str,
) -> dict[str, Any] | None:
    state = db.get_automation_state(_cache_state_key(cache_key))
    if not isinstance(state, dict):
        return None
    return state


def persist_cache_state(
    db: MetadataDB,
    cache_key: str,
    state: Mapping[str, Any],
) -> None:
    db.upsert_automation_state(
        _cache_state_key(cache_key),
        dict(state),
    )


def next_version(state: dict[str, Any] | None) -> str:
    if state is None:
        return "v1"
    previous = state.get("version")
    if not isinstance(previous, str) or not _VERSION_RE.match(previous):
        raise TranscriptCacheError(
            f"cached version is malformed: {previous!r}"
        )
    number = int(previous.lstrip("v"))
    return f"v{number + 1}"


def _validate_outer_state(
    state: dict[str, Any],
    *,
    expected_source_hash: str,
    expected_origin_hash: str,
    expected_cache_key: str,
    expected_artifact_id: str,
    expected_outputs: tuple[TranscriptOutput, ...],
) -> str:
    extra_keys = set(state.keys()) - _CACHE_STATE_KEYS
    if extra_keys:
        raise TranscriptCacheError(
            f"cache state has unknown top-level keys: {sorted(extra_keys)}"
        )

    source_hash = _require_sha256(state.get("source_hash"), "source_hash")
    origin_hash = _require_sha256(state.get("origin_hash"), "origin_hash")
    cache_key = _require_sha256(state.get("cache_key"), "cache_key")
    version = _require_string(state.get("version"), "version")
    if not _VERSION_RE.match(version):
        raise TranscriptCacheError(f"cache state version is malformed: {version!r}")

    if source_hash != expected_source_hash:
        raise TranscriptCacheError("cached source commitment does not match")
    if origin_hash != expected_origin_hash:
        raise TranscriptCacheError("cached origin commitment does not match")
    if cache_key != expected_cache_key:
        raise TranscriptCacheError("cached cache key does not match")

    artifact_id = _require_string(state.get("artifact_id"), "artifact_id")
    if artifact_id != expected_artifact_id:
        raise TranscriptCacheError(
            f"cached artifact_id {artifact_id!r} does not match {expected_artifact_id!r}"
        )

    if not isinstance(state.get("processor_identities"), dict):
        raise TranscriptCacheError("cache state missing processor_identities")
    _validate_processor_identities(state["processor_identities"])

    raw_derivatives = state.get("derivatives")
    if not isinstance(raw_derivatives, list) or not raw_derivatives:
        raise TranscriptCacheError("cached derivatives are missing or empty")

    expected_types = {"transcript"}
    if TranscriptOutput.SUMMARY in expected_outputs:
        expected_types.add("summary")
    if TranscriptOutput.CLASSIFICATION in expected_outputs:
        expected_types.add("classification")
    found_types: set[str] = set()
    for raw in raw_derivatives:
        output_type = raw.get("output_type") if isinstance(raw, dict) else None
        if not isinstance(output_type, str):
            raise TranscriptCacheError("cached derivative missing output_type")
        if output_type in found_types:
            raise TranscriptCacheError(f"duplicate output_type in cache: {output_type}")
        found_types.add(output_type)
    if found_types != expected_types:
        raise TranscriptCacheError(
            f"cached output types {sorted(found_types)} do not match expected {sorted(expected_types)}"
        )

    return version


def _validate_processor_identities(value: dict[str, Any]) -> None:
    if set(value.keys()) != {"normalizer", "summarizer", "classifier"}:
        raise TranscriptCacheError(
            "processor_identities must contain exactly normalizer, summarizer, and classifier"
        )
    normalizer = value["normalizer"]
    if not isinstance(normalizer, dict):
        raise TranscriptCacheError("normalizer identity is required")
    try:
        ProcessorIdentity.from_mapping(normalizer)
    except TranscriptIdentityError as exc:
        raise TranscriptCacheError(f"invalid normalizer identity: {exc}") from exc

    for name in ("summarizer", "classifier"):
        identity = value[name]
        if identity is None:
            continue
        if not isinstance(identity, dict):
            raise TranscriptCacheError(f"{name} identity must be an object or null")
        try:
            ProcessorIdentity.from_mapping(identity)
        except TranscriptIdentityError as exc:
            raise TranscriptCacheError(f"invalid {name} identity: {exc}") from exc


def _cross_check_processor_identities(
    processor_identities: dict[str, Any],
    derivatives: tuple[TranscriptDerivative, ...],
) -> None:
    """Ensure state-level identities match the derivative-level identities."""
    by_type = {d.output_type: d for d in derivatives}

    transcript = by_type.get("transcript")
    if transcript is not None:
        if processor_identities["normalizer"] != transcript.processor_identity.to_dict():
            raise TranscriptCacheError(
                "state normalizer identity does not match transcript derivative identity"
            )

    summary = by_type.get("summary")
    state_summarizer = processor_identities.get("summarizer")
    if summary is not None:
        if state_summarizer is None:
            raise TranscriptCacheError(
                "summary derivative present but state summarizer identity is null"
            )
        if state_summarizer != summary.processor_identity.to_dict():
            raise TranscriptCacheError(
                "state summarizer identity does not match summary derivative identity"
            )
    elif state_summarizer is not None:
        raise TranscriptCacheError(
            "state summarizer identity present but no summary derivative"
        )

    classification = by_type.get("classification")
    state_classifier = processor_identities.get("classifier")
    if classification is not None:
        if state_classifier is None:
            raise TranscriptCacheError(
                "classification derivative present but state classifier identity is null"
            )
        if state_classifier != classification.processor_identity.to_dict():
            raise TranscriptCacheError(
                "state classifier identity does not match classification derivative identity"
            )
    elif state_classifier is not None:
        raise TranscriptCacheError(
            "state classifier identity present but no classification derivative"
        )


def derivatives_from_cache_state(
    state: dict[str, Any],
    *,
    expected_source_hash: str,
    expected_origin_hash: str,
    expected_cache_key: str,
    expected_artifact_id: str,
    expected_outputs: tuple[TranscriptOutput, ...],
) -> tuple[TranscriptDerivative, ...]:
    """Rematerialize derivatives from cache state, verifying commitments.

    Raises ``TranscriptCacheError`` if the stored commitments do not match the
    current artifact/processor commitment or if any derivative is malformed or
    has been tampered with.
    """
    version = _validate_outer_state(
        state,
        expected_source_hash=expected_source_hash,
        expected_origin_hash=expected_origin_hash,
        expected_cache_key=expected_cache_key,
        expected_artifact_id=expected_artifact_id,
        expected_outputs=expected_outputs,
    )

    loaded: list[TranscriptDerivative] = []
    for index, raw in enumerate(state["derivatives"]):
        if not isinstance(raw, dict):
            raise TranscriptCacheError(
                f"cached derivative at index {index} is not an object"
            )
        extra_keys = set(raw.keys()) - _DERIVATIVE_KEYS
        if extra_keys:
            raise TranscriptCacheError(
                f"derivative {index} has unknown keys: {sorted(extra_keys)}"
            )
        try:
            derivative = TranscriptDerivative.from_mapping(raw)
        except (TranscriptDerivativeError, TypeError) as exc:
            raise TranscriptCacheError(
                f"cached derivative at index {index} is invalid: {exc}"
            ) from exc

        if derivative.source_hash != expected_source_hash:
            raise TranscriptCacheError(
                f"derivative {index} source_hash does not match cache state"
            )
        if derivative.cache_key != expected_cache_key:
            raise TranscriptCacheError(
                f"derivative {index} cache_key does not match cache state"
            )
        if derivative.version != version:
            raise TranscriptCacheError(
                f"derivative {index} version does not match cache state"
            )
        loaded.append(derivative)

    _cross_check_processor_identities(
        state["processor_identities"], tuple(loaded)
    )

    return tuple(loaded)


def build_cache_state(
    *,
    artifact_id: str,
    source_hash: str,
    origin_hash: str,
    cache_key: str,
    version: str,
    normalizer_identity: ProcessorIdentity,
    summarizer_identity: ProcessorIdentity | None,
    classifier_identity: ProcessorIdentity | None,
    derivatives: tuple[TranscriptDerivative, ...],
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "source_hash": source_hash,
        "origin_hash": origin_hash,
        "cache_key": cache_key,
        "version": version,
        "processor_identities": {
            "normalizer": normalizer_identity.to_dict(),
            "summarizer": summarizer_identity.to_dict()
            if summarizer_identity
            else None,
            "classifier": classifier_identity.to_dict()
            if classifier_identity
            else None,
        },
        "derivatives": [derivative.to_dict() for derivative in derivatives],
    }
