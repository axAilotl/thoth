"""Runtime helpers for applying transcript derivatives to artifacts."""

from __future__ import annotations

from core.artifacts import ArtifactRelationship, TranscriptArtifact

from .models import TranscriptDerivative, TranscriptEnrichmentResult
from .rendering import classification_tags_from_content, extract_normalized_transcript_text, extract_summary_text


def _existing_derivative_paths(
    artifact: TranscriptArtifact,
) -> set[str]:
    """Return the set of derivative paths already recorded on the artifact."""
    paths: set[str] = set()
    for output in artifact.derived_outputs:
        if hasattr(output, "path"):
            paths.add(str(output.path))
    for output_type, path in artifact.output_paths.items():
        if path:
            paths.add(str(path))
    return paths


def _existing_relationship_signature(
    artifact: TranscriptArtifact,
) -> set[tuple[str, str, str]]:
    """Return signatures of existing derived_from relationships."""
    signatures: set[tuple[str, str, str]] = set()
    for relationship in artifact.relationships:
        if relationship.relationship_type != "derived_from":
            continue
        meta = relationship.metadata or {}
        signatures.add(
            (
                str(relationship.target_id),
                str(meta.get("derivative_type") or ""),
                str(meta.get("derivative_path") or ""),
            )
        )
    return signatures


def apply_derivatives_to_artifact(
    artifact: TranscriptArtifact,
    result: TranscriptEnrichmentResult,
) -> None:
    """Mutate the artifact to expose generated derivatives and source links.

    The source raw transcript is not overwritten. Derivative paths are added as
    both ``derived_outputs`` and ``output_paths``; a ``derived_from``
    relationship links every derivative back to the immutable source.

    A source-provided summary or processed transcript is preserved. Generated
    values are only copied to the artifact fields when the source did not
    already supply them, so source truth is never overwritten.

    Calling this function multiple times with the same derivative path is
    idempotent: existing ``derived_outputs`` and ``relationships`` entries are
    not duplicated.
    """
    existing_paths = _existing_derivative_paths(artifact)
    existing_signatures = _existing_relationship_signature(artifact)
    source_path = result.source_path or artifact.transcript_path

    new_outputs: list = []
    new_relationships: list = []

    for derivative in result.derivatives:
        if derivative.path in existing_paths:
            continue
        new_outputs.append(derivative.to_derived_output())

        signature = (
            artifact.id,
            derivative.output_type,
            derivative.path,
        )
        if signature in existing_signatures:
            continue
        new_relationships.append(
            ArtifactRelationship(
                relationship_type="derived_from",
                target_id=artifact.id,
                target_type="transcript",
                source_evidence=source_path,
                metadata={
                    "derivative_type": derivative.output_type,
                    "derivative_path": derivative.path,
                    "version": derivative.version,
                    "cache_key": derivative.cache_key,
                    "source_hash": derivative.source_hash,
                    "processor_identity": derivative.processor_identity.to_dict(),
                },
            )
        )

    if new_outputs:
        artifact.derived_outputs = tuple(list(artifact.derived_outputs) + new_outputs)
    for derivative in result.derivatives:
        artifact.output_paths[derivative.output_type] = derivative.path

    # Preserve source-provided values; only fill missing artifact fields.
    transcript_derivative = next(
        (d for d in result.derivatives if d.output_type == "transcript"), None
    )
    if transcript_derivative is not None and not artifact.processed_transcript:
        artifact.processed_transcript = extract_normalized_transcript_text(
            transcript_derivative.content
        )

    summary_derivative = next(
        (d for d in result.derivatives if d.output_type == "summary"), None
    )
    if summary_derivative is not None and not artifact.summary:
        artifact.summary = extract_summary_text(summary_derivative.content)

    classification_derivative = next(
        (d for d in result.derivatives if d.output_type == "classification"), None
    )
    if classification_derivative is not None:
        tags = classification_tags_from_content(classification_derivative.content)
        artifact.tags = list(dict.fromkeys([*artifact.tags, *tags]))

    if new_relationships:
        artifact.relationships = tuple(list(artifact.relationships) + new_relationships)
