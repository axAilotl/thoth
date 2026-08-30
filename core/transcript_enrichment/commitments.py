"""Canonical source/origin commitments and cache keys for transcript derivatives.

Commitments are built from canonical JSON with explicit field boundaries so
that two different completed recordings can never share a cache key, even if
their transcript text is identical.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Protocol

from core.artifacts import TranscriptArtifact

from .identity import (
    ProcessorIdentity,
    source_provided_classification_identity,
    source_provided_summary_identity,
)
from .outputs import TranscriptOutput


def _canonical_json(value: Any) -> str:
    """Stable JSON serialization for commitment material."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda obj: obj.to_dict() if hasattr(obj, "to_dict") else str(obj),
    )


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source_identity_commitment(artifact: TranscriptArtifact) -> dict[str, Any]:
    identity = artifact.source_identity
    if identity is None:
        return {"source_name": None, "source_type": None, "native_id": None}
    return {
        "source_name": identity.source_name,
        "source_type": identity.source_type,
        "native_id": identity.native_id,
        "uri": identity.uri,
        "account": identity.account,
        "collector": identity.collector,
    }


def _raw_payload_commitment(artifact: TranscriptArtifact) -> dict[str, Any]:
    ref = artifact.raw_payload
    if ref is None:
        return {
            "path": None,
            "content_key": None,
            "media_type": None,
            "sha256": None,
            "size_bytes": None,
            "immutable": None,
        }
    return {
        "path": ref.path,
        "content_key": ref.content_key,
        "media_type": ref.media_type,
        "sha256": ref.sha256,
        "size_bytes": ref.size_bytes,
        "immutable": ref.immutable,
    }


def _provenance_commitment(artifact: TranscriptArtifact) -> dict[str, Any]:
    provenance = artifact.provenance
    if provenance is None:
        return {
            "source_identity": None,
            "captured_at": None,
            "ingested_at": None,
            "collector": None,
            "queue_id": None,
            "raw_payload": None,
            "evidence_paths": None,
        }
    return {
        "source_identity": provenance.source_identity.to_dict(),
        "captured_at": provenance.captured_at,
        "ingested_at": provenance.ingested_at,
        "collector": provenance.collector,
        "queue_id": provenance.queue_id,
        "raw_payload": provenance.raw_payload.to_dict()
        if provenance.raw_payload
        else None,
        "evidence_paths": list(provenance.evidence_paths),
    }


def source_commitment(artifact: TranscriptArtifact) -> str:
    """Commitment over transcript inputs that affect derivative outputs."""
    return _sha256_text(
        _canonical_json(
            {
                "raw_transcript": artifact.raw_transcript or "",
                "processed_transcript": artifact.processed_transcript or "",
                "summary": artifact.summary,
                "tags": sorted(set(artifact.tags or [])),
                "language": artifact.language,
                "speaker": artifact.speaker,
                "session_id": artifact.session_id,
                "device_id": artifact.device_id,
            }
        )
    )


def origin_commitment(artifact: TranscriptArtifact) -> str:
    """Commitment over stable origin/source identity and provenance."""
    return _sha256_text(
        _canonical_json(
            {
                "artifact_id": artifact.id,
                "source_identity": _source_identity_commitment(artifact),
                "provenance": _provenance_commitment(artifact),
                "raw_payload": _raw_payload_commitment(artifact),
            }
        )
    )


def _processor_identity_for_cache(
    processor: Protocol | None,
) -> dict[str, Any] | None:
    if processor is None:
        return None
    identity = processor.identity()
    return identity.to_dict()


def _summary_identity_for_cache(
    artifact: TranscriptArtifact,
    summarizer: Protocol | None,
) -> dict[str, Any] | None:
    if (artifact.summary or "").strip():
        return source_provided_summary_identity().to_dict()
    if summarizer is not None:
        return summarizer.identity().to_dict()
    return None


def _classification_identity_for_cache(
    artifact: TranscriptArtifact,
    classifier: Protocol | None,
) -> dict[str, Any] | None:
    source_tags = [tag for tag in (artifact.tags or []) if str(tag).strip()]
    if source_tags:
        return source_provided_classification_identity().to_dict()
    if classifier is not None:
        return classifier.identity().to_dict()
    return None


def cache_key(
    artifact: TranscriptArtifact,
    normalizer: Protocol,
    outputs: tuple[TranscriptOutput, ...],
    summarizer: Protocol | None,
    classifier: Protocol | None,
) -> str:
    """Cache key covering source, origin, requested outputs, and processors.

    The cache identity includes every output-affecting input: the immutable
    source commitment, the stable origin/source identity, the requested output
    set, and the explicit identity of each processor that contributes to those
    outputs. When the source already provides a summary or tags, the honest
    source-provided identity is used; an injected but unused processor does not
    change the cache key. The processing mode is intentionally excluded.
    """
    return _sha256_text(
        _canonical_json(
            {
                "source_commitment": source_commitment(artifact),
                "origin_commitment": origin_commitment(artifact),
                "outputs": sorted(output.value for output in outputs),
                "processor_identities": {
                    "normalizer": _processor_identity_for_cache(normalizer),
                    "summarizer": _summary_identity_for_cache(artifact, summarizer)
                    if TranscriptOutput.SUMMARY in outputs
                    else None,
                    "classifier": _classification_identity_for_cache(artifact, classifier)
                    if TranscriptOutput.CLASSIFICATION in outputs
                    else None,
                },
            }
        )
    )
