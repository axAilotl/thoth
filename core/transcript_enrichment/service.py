"""Transcript enrichment service coordinator."""

from __future__ import annotations

from typing import Protocol

from core.artifacts import TranscriptArtifact
from core.config import Config, config
from core.metadata_db import MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout

from .cache_state import (
    TranscriptCacheError,
    build_cache_state,
    derivatives_from_cache_state,
    load_cache_state,
    next_version,
    persist_cache_state,
)
from .commitments import cache_key, origin_commitment, source_commitment
from .errors import TranscriptEnrichmentError
from .generation import generate_derivatives
from .identity import ProcessorIdentity
from .indexing import index_derivatives
from .models import TranscriptDerivative, TranscriptEnrichmentResult
from .projection import rebuild_projections_from_derivatives
from .protocols import LocalTranscriptNormalizer, TranscriptNormalizer
from .outputs import TranscriptOutput
from .request import ProcessingMode, ProcessingRequest
from .storage import verify_derivative_files_valid


def _identity_from_derivatives(
    derivatives: tuple[TranscriptDerivative, ...], output_type: str
) -> ProcessorIdentity | None:
    derivative = next(
        (d for d in derivatives if d.output_type == output_type), None
    )
    return derivative.processor_identity if derivative is not None else None


class TranscriptEnrichmentService:
    """Orchestrates normalization and optional summary/classification of transcripts.

    The normalizer is injected; the default production normalizer is local and
    makes no model/provider calls. Summarizer and classifier are optional and
    are only invoked when a requested output requires them. Source-provided
    summaries and tags are materialized as derivatives without calling a
    processor and are marked with an honest source-provided identity.
    """

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        normalizer: TranscriptNormalizer | None = None,
        summarizer: Protocol | None = None,
        classifier: Protocol | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()
        self.normalizer = normalizer or LocalTranscriptNormalizer()
        self.summarizer = summarizer
        self.classifier = classifier

    def enrich(
        self,
        artifact: TranscriptArtifact,
        request: ProcessingRequest | None = None,
    ) -> TranscriptEnrichmentResult:
        """Normalize, optionally summarize/classify, cache, and index one transcript."""
        request = request or ProcessingRequest.default()
        if not isinstance(artifact, TranscriptArtifact):
            raise TranscriptEnrichmentError(
                f"expected TranscriptArtifact, got {type(artifact).__name__}"
            )

        outputs = request.outputs
        source_hash = source_commitment(artifact)
        origin_hash = origin_commitment(artifact)
        key = cache_key(
            artifact,
            self.normalizer,
            outputs,
            self.summarizer,
            self.classifier,
        )
        cached_state = load_cache_state(self.db, key)

        if request.mode == ProcessingMode.RECOMPUTE:
            return self._compute(
                artifact,
                mode=request.mode,
                outputs=outputs,
                source_hash=source_hash,
                origin_hash=origin_hash,
                cache_key=key,
                cached_state=cached_state,
            )

        if cached_state is not None:
            derivatives = derivatives_from_cache_state(
                cached_state,
                expected_source_hash=source_hash,
                expected_origin_hash=origin_hash,
                expected_cache_key=key,
                expected_artifact_id=artifact.id,
                expected_outputs=outputs,
            )
            if request.mode == ProcessingMode.REBUILD_PROJECTION:
                derivatives = rebuild_projections_from_derivatives(
                    vault_root=self.layout.vault_root,
                    derivatives=derivatives,
                )
            else:
                verify_derivative_files_valid(
                    derivatives, self.layout.vault_root
                )
            self._index_artifact(artifact, derivatives)
            return TranscriptEnrichmentResult(
                artifact_id=artifact.id,
                source_hash=source_hash,
                cache_key=key,
                version=str(cached_state["version"]),
                mode=request.mode,
                cache_hit=True,
                rerun_requested=False,
                derivatives=derivatives,
                indexed=True,
                source_path=self._source_path_for_artifact(artifact),
            )

        if request.mode == ProcessingMode.REBUILD_PROJECTION:
            raise TranscriptEnrichmentError(
                "cannot rebuild projection: no valid cached derivatives"
            )

        return self._compute(
            artifact,
            mode=request.mode,
            outputs=outputs,
            source_hash=source_hash,
            origin_hash=origin_hash,
            cache_key=key,
            cached_state=cached_state,
        )

    def _compute(
        self,
        artifact: TranscriptArtifact,
        *,
        mode: ProcessingMode,
        outputs: tuple[TranscriptOutput, ...],
        source_hash: str,
        origin_hash: str,
        cache_key: str,
        cached_state: dict | None,
    ) -> TranscriptEnrichmentResult:
        version = next_version(cached_state)
        derivatives = generate_derivatives(
            artifact,
            vault_root=self.layout.vault_root,
            version=version,
            cache_key=cache_key,
            source_hash=source_hash,
            outputs=outputs,
            source_path=self._source_path_for_artifact(artifact),
            normalizer=self.normalizer,
            summarizer=self.summarizer,
            classifier=self.classifier,
        )
        persist_cache_state(
            self.db,
            cache_key,
            build_cache_state(
                artifact_id=artifact.id,
                source_hash=source_hash,
                origin_hash=origin_hash,
                cache_key=cache_key,
                version=version,
                normalizer_identity=_identity_from_derivatives(
                    derivatives, "transcript"
                ) or self.normalizer.identity(),
                summarizer_identity=_identity_from_derivatives(
                    derivatives, "summary"
                ),
                classifier_identity=_identity_from_derivatives(
                    derivatives, "classification"
                ),
                derivatives=derivatives,
            ),
        )
        self._index_artifact(artifact, derivatives)
        return TranscriptEnrichmentResult(
            artifact_id=artifact.id,
            source_hash=source_hash,
            cache_key=cache_key,
            version=version,
            mode=mode,
            cache_hit=False,
            rerun_requested=(mode == ProcessingMode.RECOMPUTE),
            derivatives=derivatives,
            indexed=True,
            source_path=self._source_path_for_artifact(artifact),
        )

    def _source_path_for_artifact(self, artifact: TranscriptArtifact) -> str | None:
        if artifact.transcript_path:
            return str(artifact.transcript_path)
        if artifact.raw_payload and artifact.raw_payload.path:
            return str(artifact.raw_payload.path)
        if artifact.custom_metadata.get("raw_payload_path"):
            return str(artifact.custom_metadata["raw_payload_path"])
        return None

    def _index_artifact(
        self,
        artifact: TranscriptArtifact,
        derivatives: tuple[TranscriptDerivative, ...],
    ) -> None:
        index_derivatives(
            artifact,
            vault_root=self.layout.vault_root,
            db=self.db,
            derivatives=derivatives,
        )
