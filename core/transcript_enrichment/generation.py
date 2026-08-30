"""Create transcript derivatives from processors or source-provided values."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from core.artifacts import TranscriptArtifact

from .errors import TranscriptEnrichmentError
from .identity import (
    ProcessorIdentity,
    source_provided_classification_identity,
    source_provided_summary_identity,
)
from .models import TranscriptDerivative, _sha256_text
from .outputs import TranscriptOutput
from .rendering import render_classification_json, render_summary_markdown, render_transcript_markdown
from .storage import _atomic_write_text, derivative_paths_for_artifact


def generate_derivatives(
    artifact: TranscriptArtifact,
    *,
    vault_root: Path,
    version: str,
    cache_key: str,
    source_hash: str,
    outputs: tuple[TranscriptOutput, ...],
    source_path: str | None,
    normalizer: Protocol,
    summarizer: Protocol | None,
    classifier: Protocol | None,
) -> tuple[TranscriptDerivative, ...]:
    """Generate the requested derivative set.

    Each ``TranscriptDerivative.content`` is the exact durable file content.
    Source-provided summary or tags are materialized as derivatives with an
    honest ``source_provided`` processor identity and without calling a
    processor. A requested expensive output that is absent from the source and
    has no injected processor fails closed.
    """
    source_text = artifact.raw_transcript or artifact.processed_transcript or ""
    normalized_text = normalizer.normalize(source_text, artifact)
    title = artifact.title or artifact.transcript_id or artifact.id or "transcript"
    paths = derivative_paths_for_artifact(artifact.id, cache_key, version, vault_root)

    derivatives: list[TranscriptDerivative] = []

    transcript_content = render_transcript_markdown(
        title=title,
        normalized_text=normalized_text,
        source_path=source_path,
        version=version,
        cache_key=cache_key,
    )
    _atomic_write_text(paths["transcript"], transcript_content)
    derivatives.append(
        TranscriptDerivative(
            output_type="transcript",
            path=paths["transcript"].relative_to(vault_root).as_posix(),
            content=transcript_content,
            media_type="text/markdown",
            version=version,
            cache_key=cache_key,
            source_hash=source_hash,
            content_sha256=_sha256_text(transcript_content),
            processor_identity=normalizer.identity(),
        )
    )

    summary_text = ""
    summary_identity: ProcessorIdentity | None = None
    if TranscriptOutput.SUMMARY in outputs:
        source_summary = (artifact.summary or "").strip()
        if source_summary:
            summary_text = artifact.summary or ""
            summary_identity = source_provided_summary_identity()
        elif summarizer is not None:
            summary_text = summarizer.summarize(normalized_text, artifact)
            summary_identity = summarizer.identity()
        else:
            raise TranscriptEnrichmentError(
                "summary output requested but no summarizer injected and source has no summary"
            )

        summary_content = render_summary_markdown(
            title=title,
            summary=summary_text,
            tags=[],
            source_path=source_path,
            version=version,
            cache_key=cache_key,
        )
        _atomic_write_text(paths["summary"], summary_content)
        derivatives.append(
            TranscriptDerivative(
                output_type="summary",
                path=paths["summary"].relative_to(vault_root).as_posix(),
                content=summary_content,
                media_type="text/markdown",
                version=version,
                cache_key=cache_key,
                source_hash=source_hash,
                content_sha256=_sha256_text(summary_content),
                processor_identity=summary_identity,
            )
        )

    if TranscriptOutput.CLASSIFICATION in outputs:
        source_tags = [tag.strip() for tag in (artifact.tags or []) if tag.strip()]
        if source_tags:
            tags = source_tags
            classification_identity = source_provided_classification_identity()
        elif classifier is not None:
            tags = classifier.classify(normalized_text, artifact)
            classification_identity = classifier.identity()
        else:
            raise TranscriptEnrichmentError(
                "classification output requested but no classifier injected and source has no tags"
            )

        classification_content = render_classification_json(
            title=title,
            tags=tags,
            summary=summary_text,
            normalized_length=len(normalized_text),
            source_hash=source_hash,
            cache_key=cache_key,
            version=version,
            processor_identity=classification_identity,
        )
        _atomic_write_text(paths["classification"], classification_content)
        derivatives.append(
            TranscriptDerivative(
                output_type="classification",
                path=paths["classification"].relative_to(vault_root).as_posix(),
                content=classification_content,
                media_type="application/json",
                version=version,
                cache_key=cache_key,
                source_hash=source_hash,
                content_sha256=_sha256_text(classification_content),
                processor_identity=classification_identity,
            )
        )

    return tuple(derivatives)
