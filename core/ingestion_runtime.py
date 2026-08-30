"""Shared runtime for queued knowledge-artifact processing.

This module is intentionally narrow: it materializes queued artifacts,
dispatches them to existing processors, and provides a single bookmark
processing path that the API can use for live captures.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .artifacts import (
    KnowledgeArtifact,
    MarkdownArtifact,
    PaperArtifact,
    RepositoryArtifact,
    TranscriptArtifact,
    TweetArtifact,
    VideoArtifact,
    WebClipperArtifact,
)
from .artifact_classification import (
    ArtifactClassifier,
    ClassificationResult,
    RoutingAction,
)
from .bounded_workers import map_bounded, resolve_worker_concurrency
from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id
from .canonical_identity import CanonicalArtifactIdentity, CanonicalIdentityService
from .config import Config, config
from .data_models import Tweet
from .metadata_db import (
    INGESTION_REVIEW_STATUSES,
    IngestionQueueEntry,
    MetadataDB,
    get_metadata_db,
)
from .path_layout import PathLayout, build_path_layout
from .runtime_composition import validate_metadata_db_matches_layout
from .prompt_security import prompt_security_requires_review
from .transcript_enrichment import (
    LocalTranscriptNormalizer,
    ProcessingRequest,
    TranscriptEnrichmentService,
    apply_derivatives_to_artifact,
)
from .transcript_enrichment.request import (
    current_processing_request,
    processing_request_scope,
)
from .translation_companion import EnglishCompanionPublisher, TranslationCompanionResult
from .wiki_updater import CompiledWikiUpdater

logger = logging.getLogger(__name__)


class IngestionRuntimeError(RuntimeError):
    """Base error for shared artifact-runtime failures."""


class UnsupportedArtifactTypeError(IngestionRuntimeError, ValueError):
    """Raised when a queue entry declares an unsupported artifact type."""


class ClassificationReviewRequired(IngestionRuntimeError):
    """Raised when an artifact needs human review before routing."""

    def __init__(self, classification: "ClassificationResult") -> None:
        self.classification = classification
        super().__init__(
            f"artifact classification review required: {classification.reasons[0]}"
        )


@dataclass(frozen=True)
class IngestionDispatchResult:
    """Summary of a single artifact dispatch."""

    artifact_id: str
    artifact_type: str
    source: str
    status: str
    processed_at: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BookmarkDispatchResult:
    """Summary of bookmark processing through the shared tweet runtime."""

    tweet_id: str
    tweet_count: int
    cache_file: str | None
    url_mapping_count: int
    pipeline_result: Any
    processed_at: str


def _now_iso() -> str:
    return datetime.now().isoformat()


def _json_loads_maybe(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception as exc:
            raise IngestionRuntimeError("Queue payload contained invalid JSON") from exc
    return value


def _capabilities_from_queue(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    payload = _json_loads_maybe(value)
    if not isinstance(payload, list):
        raise IngestionRuntimeError("Queue capabilities_json must decode to a list")
    return tuple(str(item) for item in payload if str(item).strip())


def _reviewable_artifact_error(exc: Exception) -> bool:
    return isinstance(exc, (IngestionRuntimeError, ValueError, TypeError))


def _review_category_for_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "invalid json" in message or "decode" in message:
        return "malformed_payload"
    if "missing" in message:
        return "incomplete_payload"
    if "unsupported" in message:
        return "unsupported_artifact"
    if "security review" in message:
        return "security_policy"
    return "runtime_validation"


class KnowledgeArtifactRuntime:
    """Shared runtime for bookmark and ingestion queue processing."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        transcript_enrichment_service: TranscriptEnrichmentService | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()
        validate_metadata_db_matches_layout(self.db, self.layout)
        self.layout.ensure_directories()
        self._pipeline = None
        self._wiki_updater = None
        self._companion_publisher = None
        self._canonical_identity_service = None
        self._worker_health: dict[str, Any] = {
            "healthy": True,
            "last_error": None,
            "last_error_at": None,
            "consecutive_failures": 0,
        }
        self._transcript_enrichment_service = transcript_enrichment_service

    @property
    def pipeline(self):
        if self._pipeline is None:
            from processors.pipeline_processor import PipelineProcessor

            self._pipeline = PipelineProcessor(vault_path=str(self.layout.vault_root))
        return self._pipeline

    @property
    def wiki_updater(self) -> CompiledWikiUpdater:
        if self._wiki_updater is None:
            self._wiki_updater = CompiledWikiUpdater(
                self.config,
                layout=self.layout,
                db=self.db,
            )
        return self._wiki_updater

    @property
    def companion_publisher(self) -> EnglishCompanionPublisher:
        if self._companion_publisher is None:
            self._companion_publisher = EnglishCompanionPublisher(
                self.config,
                layout=self.layout,
                db=self.db,
            )
        return self._companion_publisher

    @property
    def canonical_identity_service(self) -> CanonicalIdentityService:
        if self._canonical_identity_service is None:
            self._canonical_identity_service = CanonicalIdentityService(self.db)
        return self._canonical_identity_service

    @property
    def transcript_enrichment_service(self) -> TranscriptEnrichmentService:
        if self._transcript_enrichment_service is None:
            self._transcript_enrichment_service = TranscriptEnrichmentService(
                self.config,
                layout=self.layout,
                db=self.db,
                normalizer=LocalTranscriptNormalizer(),
            )
        return self._transcript_enrichment_service

    def materialize_artifact(self, entry: IngestionQueueEntry) -> KnowledgeArtifact:
        """Convert a queue row into a typed artifact."""
        payload = _json_loads_maybe(entry.payload_json)
        if not isinstance(payload, dict):
            raise IngestionRuntimeError("Queue payload must decode to an object")

        artifact_type = str(entry.artifact_type).strip().lower()
        if artifact_type == "tweet":
            artifact = TweetArtifact.from_queue_payload(payload)
        elif artifact_type == "paper":
            artifact = PaperArtifact.from_queue_payload(payload)
        elif artifact_type == "repository":
            artifact = RepositoryArtifact.from_queue_payload(payload)
        elif artifact_type == "web_clipper":
            artifact = WebClipperArtifact.from_queue_payload(payload)
        elif artifact_type == "markdown":
            artifact = MarkdownArtifact.from_queue_payload(payload)
        elif artifact_type == "video":
            artifact = VideoArtifact.from_queue_payload(payload)
        elif artifact_type == "transcript":
            artifact = TranscriptArtifact.from_queue_payload(payload)
        else:
            raise UnsupportedArtifactTypeError(
                f"Unsupported ingestion artifact type: {entry.artifact_type}"
            )

        capabilities = _capabilities_from_queue(entry.capabilities_json)
        artifact = artifact.apply_queue_context(
            queue_id=entry.artifact_id,
            queue_source=entry.source,
            queue_created_at=entry.created_at,
            capabilities=capabilities,
            payload=payload,
        )
        return artifact

    def _sync_wiki_for_artifact(
        self,
        artifact: KnowledgeArtifact,
        *,
        dispatch_details: dict[str, Any] | None = None,
    ) -> None:
        updater = self.wiki_updater
        if updater.supports_artifact(artifact):
            updater.update_from_artifact(
                artifact,
                dispatch_details=dispatch_details,
            )
            return
        updater.prune_legacy_tweet_pages()

    async def process_pending_ingestions_once(
        self,
        *,
        limit: int | None = None,
        concurrency: int | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> list[IngestionDispatchResult]:
        """Process all due ingestion rows once."""
        entries = self.db.get_pending_ingestions(limit=limit)
        if not entries:
            return []

        worker_count = (
            self._ingestion_worker_concurrency()
            if concurrency is None
            else resolve_worker_concurrency(
                concurrency,
                setting_name="processing.ingestion.concurrent_workers",
            )
        )
        if worker_count <= 1:
            results: list[IngestionDispatchResult] = []
            for entry in entries:
                if cancel_event is not None and cancel_event.is_set():
                    break
                results.append(await self.process_ingestion_entry(entry))
            return results

        return await map_bounded(
            entries,
            self.process_ingestion_entry,
            concurrency=worker_count,
            cancel_event=cancel_event,
        )

    async def run_background(
        self,
        shutdown_event: asyncio.Event,
        *,
        poll_interval_seconds: float = 5.0,
        batch_size: int = 25,
    ) -> None:
        """Poll the ingestion queue until shutdown.

        Queue read and processing failures are recorded in ``worker_health`` so
        they surface as unhealthy work instead of being mistaken for idle time.
        """
        while not shutdown_event.is_set():
            try:
                results = await self.process_pending_ingestions_once(
                    limit=batch_size,
                    concurrency=self._ingestion_worker_concurrency(),
                    cancel_event=shutdown_event,
                )
                # Any successful poll (including an empty queue) recovers health.
                self._record_worker_success()
                if results:
                    continue
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=poll_interval_seconds
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Ingestion worker iteration failed: %s", exc)
                self._record_worker_failure(exc)
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=poll_interval_seconds
                    )
                except asyncio.TimeoutError:
                    continue

    def _record_worker_failure(self, exc: Exception) -> None:
        """Update health state after a failed worker iteration."""
        self._worker_health["healthy"] = False
        self._worker_health["last_error"] = f"{exc.__class__.__name__}: {exc}"
        self._worker_health["last_error_at"] = _now_iso()
        self._worker_health["consecutive_failures"] += 1

    def _record_worker_success(self) -> None:
        """Reset health state after a successful worker iteration."""
        self._worker_health["healthy"] = True
        self._worker_health["last_error"] = None
        self._worker_health["last_error_at"] = None
        self._worker_health["consecutive_failures"] = 0

    @property
    def worker_health(self) -> dict[str, Any]:
        """Return the current worker health snapshot."""
        return dict(self._worker_health)

    def _ingestion_worker_concurrency(self) -> int:
        return resolve_worker_concurrency(
            self.config.get("processing.ingestion.concurrent_workers", 1),
            default=1,
            setting_name="processing.ingestion.concurrent_workers",
        )

    async def process_ingestion_entry(
        self, entry: IngestionQueueEntry
    ) -> IngestionDispatchResult:
        """Process a single ingestion queue row."""
        if entry.status in INGESTION_REVIEW_STATUSES:
            raise IngestionRuntimeError(
                f"Ingestion artifact {entry.artifact_id} requires security review "
                "or operator review"
            )
        try:
            processing_request = self._extract_processing_request(entry)
            artifact = self.materialize_artifact(entry)
            if prompt_security_requires_review(artifact.normalized_metadata):
                raise IngestionRuntimeError(
                    f"Ingestion artifact {entry.artifact_id} requires security review"
                )
            classification = self._classify_artifact(artifact)
            if classification is not None and classification.action == RoutingAction.REVIEW:
                raise ClassificationReviewRequired(classification)
            canonical_identity = self._canonicalize_artifact(
                artifact,
                artifact_type=entry.artifact_type,
            )
        except Exception as exc:
            if _reviewable_artifact_error(exc):
                classification = getattr(exc, "classification", None)
                return self._route_entry_to_review(
                    entry,
                    exc,
                    stage="classification" if classification else "materialize",
                    classification=classification,
                )
            raise
        self.db.mark_ingestion_processing(entry.artifact_id)

        try:
            with processing_request_scope(processing_request):
                result = await self.dispatch_artifact(artifact)
            if canonical_identity is not None:
                result.details.setdefault("canonical_id", canonical_identity.canonical_id)
                result.details.setdefault(
                    "canonical_entity_type",
                    canonical_identity.entity_type,
                )
            if classification is not None:
                result.details.setdefault(
                    "routing_projection_id", classification.projection_id
                )
            self._sync_wiki_for_artifact(
                artifact,
                dispatch_details=result.details,
            )
            self.db.mark_ingestion_processed(entry.artifact_id)
            return result
        except asyncio.CancelledError:
            self.db.mark_ingestion_failed(
                entry.artifact_id,
                "processing cancelled before completion",
            )
            raise
        except Exception as exc:
            if _reviewable_artifact_error(exc):
                return self._route_entry_to_review(entry, exc, stage="dispatch")
            failure = self.db.mark_ingestion_failed(entry.artifact_id, str(exc))
            if failure and failure.status == "pending" and failure.next_attempt_at:
                logger.info(
                    "Requeued ingestion artifact %s after failure: %s",
                    entry.artifact_id,
                    exc,
                )
            raise

    def _route_entry_to_review(
        self,
        entry: IngestionQueueEntry,
        exc: Exception,
        *,
        stage: str,
        classification: ClassificationResult | None = None,
    ) -> IngestionDispatchResult:
        error = f"artifact review required: {exc}"
        metadata: dict[str, Any] = {"stage": stage}
        if classification is not None:
            metadata["classification"] = classification.to_review_event()
        updated = self.db.mark_ingestion_review_required(
            entry.artifact_id,
            category=_review_category_for_error(exc),
            reason=str(exc),
            error=error,
            error_type=exc.__class__.__name__,
            metadata=metadata,
        )
        status = updated.status if updated else "needs_review"
        details: dict[str, Any] = {
            "review_required": True,
            "stage": stage,
            "error": str(exc),
            "error_type": exc.__class__.__name__,
        }
        if classification is not None:
            details["classification"] = classification.to_review_event()
        return IngestionDispatchResult(
            artifact_id=entry.artifact_id,
            artifact_type=entry.artifact_type,
            source=entry.source,
            status=status,
            processed_at=_now_iso(),
            details=details,
        )

    def _classify_artifact(
        self,
        artifact: KnowledgeArtifact,
    ) -> ClassificationResult | None:
        """Classify an artifact when the classification feature is enabled."""
        if not self.config.get("classification.enabled", False):
            return None
        from .classification_review import ClassificationReviewService

        review_service = ClassificationReviewService(self.db, config=self.config)
        policy = review_service.get_active_policy()
        classifier = ArtifactClassifier(policy)
        return classifier.classify(artifact, artifact_type=entry.artifact_type)

    def _canonicalize_artifact(
        self,
        artifact: KnowledgeArtifact,
        *,
        artifact_type: str,
    ) -> CanonicalArtifactIdentity | None:
        return self.canonical_identity_service.canonicalize_artifact(
            artifact,
            artifact_type=artifact_type,
        )

    async def dispatch_artifact(self, artifact: KnowledgeArtifact) -> IngestionDispatchResult:
        """Dispatch a typed artifact to the existing processors."""
        if isinstance(artifact, TweetArtifact):
            return await self._process_tweet_artifact(artifact)
        if isinstance(artifact, PaperArtifact):
            return await self._process_paper_artifact(artifact)
        if isinstance(artifact, RepositoryArtifact):
            return await self._process_repository_artifact(artifact)
        if isinstance(artifact, WebClipperArtifact):
            return await self._process_web_clipper_artifact(artifact)
        if isinstance(artifact, MarkdownArtifact):
            return await self._process_markdown_artifact(artifact)
        if isinstance(artifact, VideoArtifact):
            return await self._process_video_artifact(artifact)
        if isinstance(artifact, TranscriptArtifact):
            return await self._process_transcript_artifact(artifact)

        raise UnsupportedArtifactTypeError(
            f"Unsupported artifact class: {artifact.__class__.__name__}"
        )

    async def process_bookmark_payload(
        self,
        bookmark_data: Mapping[str, Any],
        *,
        resume: bool = True,
        rerun_llm: bool = False,
        llm_only: bool = False,
        dry_run: bool = False,
    ) -> BookmarkDispatchResult:
        """Run bookmark capture through the shared tweet pipeline."""
        normalized = normalize_bookmark_payload(bookmark_data)
        tweet_id = validate_tweet_id(normalized.get("tweet_id"))
        artifact = TweetArtifact.from_bookmark_payload(normalized)

        from processors.cache_loader import CacheLoader
        from core.graphql_cache import maybe_cleanup_graphql_cache

        tweets: list[Tweet] = []
        cache_loader = CacheLoader()
        cache_file = None

        enhanced_map = cache_loader.load_cached_enhancements([tweet_id])
        if tweet_id in enhanced_map:
            tweets.append(enhanced_map[tweet_id])
            cache_dir = self.layout.cache_root
            for candidate in cache_dir.glob(f"tweet_{tweet_id}_*.json"):
                cache_file = candidate
                break
        else:
            cache_filename = normalized.get("graphql_cache_file")
            if cache_filename:
                cache_file = self.layout.cache_root / str(cache_filename)
                if cache_file.exists():
                    enhanced_tweet = cache_loader._load_tweet_from_cache(
                        cache_file, tweet_id
                    )
                    if enhanced_tweet:
                        tweets.append(enhanced_tweet)

        if tweets and getattr(tweets[0], "is_self_thread", False) and cache_file:
            thread_tweets = cache_loader.extract_all_thread_tweets_from_cache(cache_file)
            if len(thread_tweets) > 1:
                tweets = thread_tweets

        if not tweets:
            tweets.append(artifact.to_tweet_model())

        url_mappings: dict[str, str] = {}
        for tw in tweets:
            for mapping in getattr(tw, "url_mappings", []) or []:
                short_url = getattr(mapping, "short_url", None)
                expanded_url = getattr(mapping, "expanded_url", None)
                if short_url and expanded_url and short_url != expanded_url:
                    url_mappings[short_url] = expanded_url

        pipeline_result = await self.pipeline.process_tweets_pipeline(
            tweets,
            url_mappings=url_mappings or None,
            resume=resume,
            rerun_llm=rerun_llm,
            llm_only=llm_only,
            dry_run=dry_run,
        )

        if not dry_run:
            maybe_cleanup_graphql_cache(tweets, pipeline_result, logger=logger)
            self._sync_wiki_for_artifact(
                artifact,
                dispatch_details={
                    "tweet_count": len(tweets),
                    "cache_file": str(cache_file) if cache_file else None,
                    "url_mapping_count": len(url_mappings),
                },
            )

        return BookmarkDispatchResult(
            tweet_id=tweet_id,
            tweet_count=len(tweets),
            cache_file=str(cache_file) if cache_file else None,
            url_mapping_count=len(url_mappings),
            pipeline_result=pipeline_result,
            processed_at=_now_iso(),
        )

    async def publish_english_companion(
        self,
        artifact: WebClipperArtifact,
        *,
        dry_run: bool = False,
    ) -> TranslationCompanionResult:
        if not isinstance(artifact, WebClipperArtifact):
            raise IngestionRuntimeError(
                f"English companion publication only supports Web Clipper artifacts, got {artifact.__class__.__name__}"
            )
        return await self.companion_publisher.publish_web_clipper_artifact(
            artifact,
            dry_run=dry_run,
        )

    async def _process_tweet_artifact(
        self, artifact: TweetArtifact
    ) -> IngestionDispatchResult:
        """Process a tweet artifact through the shared tweet pipeline."""
        bookmark_payload = artifact.to_dict()
        bookmark_payload["tweet_id"] = artifact.id
        bookmark_payload["tweet_data"] = artifact.custom_metadata.get("tweet_data", {})
        result = await self.process_bookmark_payload(bookmark_payload)
        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="tweet",
            source=artifact.source_type,
            status="processed",
            processed_at=result.processed_at,
            details={
                "tweet_count": result.tweet_count,
                "cache_file": result.cache_file,
                "url_mapping_count": result.url_mapping_count,
            },
        )

    async def _process_paper_artifact(
        self, artifact: PaperArtifact
    ) -> IngestionDispatchResult:
        """Process a paper artifact by downloading and indexing the PDF."""
        from processors.arxiv_processor_v2 import ArXivProcessorV2
        from core.research_graph import (
            ResearchGraphService,
            build_research_metadata_provider,
        )

        research_graph = ResearchGraphService(
            self.db,
            metadata_provider=build_research_metadata_provider(self.config),
        )

        if not artifact.pdf_url:
            if artifact.arxiv_id:
                artifact.pdf_url = f"https://arxiv.org/pdf/{artifact.arxiv_id}.pdf"
            else:
                graph_result = research_graph.record_paper_artifact(
                    artifact,
                    discovery_source=artifact.source_type,
                )
                return IngestionDispatchResult(
                    artifact_id=artifact.id,
                    artifact_type="paper",
                    source=artifact.source_type,
                    status="skipped",
                    processed_at=_now_iso(),
                    details={
                        "reason": "missing_pdf_url",
                        "research_graph": graph_result,
                    },
                )
        processor = ArXivProcessorV2(output_dir=str(self.layout.vault_root))
        try:
            document = await asyncio.to_thread(
                processor.download_document,
                artifact.pdf_url,
                artifact.id,
                True,
            )
        except Exception as exc:
            if artifact.source_type == "research_graph":
                graph_result = research_graph.record_paper_artifact(
                    artifact,
                    discovery_source=artifact.source_type,
                )
                return IngestionDispatchResult(
                    artifact_id=artifact.id,
                    artifact_type="paper",
                    source=artifact.source_type,
                    status="skipped",
                    processed_at=_now_iso(),
                    details={
                        "reason": f"download_failed: {exc}",
                        "pdf_url": artifact.pdf_url,
                        "research_graph": graph_result,
                    },
                )
            raise

        pdf_paths = []
        if document and getattr(document, "filename", None):
            pdf_path = self.layout.vault_root / "papers" / str(document.filename)
            if pdf_path.exists():
                pdf_paths.append(pdf_path)
                artifact.output_paths["pdf"] = pdf_path.relative_to(
                    self.layout.vault_root
                ).as_posix()

        graph_result = research_graph.record_paper_artifact(
            artifact,
            discovery_source=artifact.source_type,
            pdf_paths=pdf_paths,
        )

        if not document:
            return IngestionDispatchResult(
                artifact_id=artifact.id,
                artifact_type="paper",
                source=artifact.source_type,
                status="skipped",
                processed_at=_now_iso(),
                details={
                    "reason": "download_skipped",
                    "pdf_url": artifact.pdf_url,
                    "research_graph": graph_result,
                },
            )

        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="paper",
            source=artifact.source_type,
            status="processed" if getattr(document, "downloaded", False) else "skipped",
            processed_at=_now_iso(),
            details={
                "filename": getattr(document, "filename", None),
                "downloaded": getattr(document, "downloaded", False),
                "pdf_url": artifact.pdf_url,
                "research_graph": graph_result,
            },
        )

    async def _process_repository_artifact(
        self, artifact: RepositoryArtifact
    ) -> IngestionDispatchResult:
        """Process a repository artifact via the existing repo processors."""
        repo_source = str(artifact.source_type or "").strip().lower()
        raw_payload = _json_loads_maybe(artifact.raw_content)
        if not isinstance(raw_payload, dict):
            raw_payload = {}

        if repo_source == "github":
            from processors.github_stars_processor import GitHubRepo, GitHubStarsProcessor

            processor = GitHubStarsProcessor(
                vault_path=str(self.layout.vault_root),
                metadata_db=self.db,
            )
            repo = GitHubRepo.from_api_response(raw_payload)
            processed = await processor._process_single_repo(repo, resume=True)
            return IngestionDispatchResult(
                artifact_id=artifact.id,
                artifact_type="repository",
                source="github",
                status="processed" if processed else "skipped",
                processed_at=_now_iso(),
                details={
                    "repo_name": repo.full_name,
                    "stargazers_count": repo.stargazers_count,
                },
            )

        if repo_source == "huggingface":
            from processors.huggingface_likes_processor import (
                HuggingFaceLikesProcessor,
                HuggingFaceRepo,
            )

            processor = HuggingFaceLikesProcessor(
                vault_path=str(self.layout.vault_root),
                metadata_db=self.db,
                cache_dir=self.layout.cache_root / "huggingface_hub",
            )
            repo = HuggingFaceRepo(
                id=str(raw_payload.get("id") or artifact.repo_name or artifact.id),
                name=str(raw_payload.get("name") or artifact.repo_name or artifact.id).split("/")[-1],
                full_name=str(raw_payload.get("full_name") or artifact.repo_name or artifact.id),
                description=raw_payload.get("description"),
                html_url=str(
                    raw_payload.get("html_url")
                    or f"https://huggingface.co/{artifact.repo_name or artifact.id}"
                ),
                likes=int(raw_payload.get("likes", artifact.stars or 0) or 0),
                downloads=int(raw_payload.get("downloads", 0) or 0),
                repo_type=str(raw_payload.get("repo_type") or "model"),
                tags=list(raw_payload.get("tags") or artifact.topics or []),
                created_at=raw_payload.get("created_at"),
                updated_at=raw_payload.get("updated_at") or raw_payload.get("last_modified"),
                license=raw_payload.get("license"),
                library=raw_payload.get("library") or raw_payload.get("library_name"),
            )
            processed = await processor._process_single_repo(repo, resume=True)
            return IngestionDispatchResult(
                artifact_id=artifact.id,
                artifact_type="repository",
                source="huggingface",
                status="processed" if processed else "skipped",
                processed_at=_now_iso(),
                details={
                    "repo_name": repo.full_name,
                    "likes": repo.likes,
                    "repo_type": repo.repo_type,
                },
            )

        raise UnsupportedArtifactTypeError(
            f"Unsupported repository artifact source: {artifact.source_type}"
        )

    async def _process_web_clipper_artifact(
        self, artifact: WebClipperArtifact
    ) -> IngestionDispatchResult:
        """Process a Web Clipper artifact through the shared wiki path."""
        if artifact.file_type != "note":
            raise IngestionRuntimeError(
                f"Unsupported Web Clipper artifact type: {artifact.file_type}"
            )

        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="web_clipper",
            source=artifact.source_type,
            status="processed",
            processed_at=_now_iso(),
            details={
                "title": artifact.title,
                "source_path": artifact.source_path,
                "source_relative_path": artifact.source_relative_path,
                "source_url": artifact.source_url,
                "source_language": artifact.source_language,
                "file_type": artifact.file_type,
            },
        )

    async def _process_markdown_artifact(
        self, artifact: MarkdownArtifact
    ) -> IngestionDispatchResult:
        """Record imported markdown as capture-only evidence."""
        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="markdown",
            source=artifact.source_type,
            status="skipped",
            processed_at=_now_iso(),
            details={
                "reason": "capture_only",
                "title": artifact.title,
                "source_path": artifact.source_path,
                "source_relative_path": artifact.source_relative_path,
            },
        )

    async def _process_video_artifact(
        self, artifact: VideoArtifact
    ) -> IngestionDispatchResult:
        """Process a video artifact already collected by a connector."""
        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="video",
            source=artifact.source_type,
            status="processed",
            processed_at=_now_iso(),
            details={
                "video_id": artifact.video_id,
                "title": artifact.title,
                "source_url": artifact.source_url,
                "archive_path": artifact.archive_path,
                "transcript_artifact_id": artifact.transcript_artifact_id,
            },
        )

    async def _process_transcript_artifact(
        self,
        artifact: TranscriptArtifact,
    ) -> IngestionDispatchResult:
        """Process a transcript artifact through injected enrichment.

        Produces normalized transcript and optional summary/classification
        derivatives, caches them by exact source commitment plus processor
        identities, indexes the full text, and links every derivative back to
        the immutable source. AI outputs are only produced when requested and
        a processor is injected; otherwise source-provided values are used.
        """
        request = current_processing_request()
        service = self.transcript_enrichment_service
        result = service.enrich(
            artifact,
            request=request,
        )
        apply_derivatives_to_artifact(artifact, result)
        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="transcript",
            source=artifact.source_type,
            status="processed",
            processed_at=_now_iso(),
            details={
                "transcript_id": artifact.transcript_id,
                "video_id": artifact.video_id,
                "title": artifact.title,
                "transcript_path": artifact.transcript_path,
                "has_raw_transcript": bool(artifact.raw_transcript),
                "has_processed_transcript": bool(artifact.processed_transcript),
                "cache_hit": result.cache_hit,
                "rerun_requested": result.rerun_requested,
                "mode": result.mode.value,
                "version": result.version,
                "cache_key": result.cache_key,
                "source_hash": result.source_hash,
                "derivatives": result.derivative_paths(),
                "indexed": result.indexed,
            },
        )

    def _extract_processing_request(
        self, entry: IngestionQueueEntry
    ) -> ProcessingRequest:
        """Parse the schema-validated processing_request from the queue payload."""
        payload = _json_loads_maybe(entry.payload_json)
        if not isinstance(payload, dict):
            return ProcessingRequest.default()
        return ProcessingRequest.from_payload(payload.get("processing_request"))


_shared_runtime: KnowledgeArtifactRuntime | None = None


def clear_knowledge_artifact_runtime() -> None:
    """Release the process-global knowledge runtime during composition teardown."""
    global _shared_runtime
    _shared_runtime = None


def _runtime_configs_compatible(
    existing: KnowledgeArtifactRuntime,
    runtime_config: Config | None,
    layout: PathLayout | None,
    db: MetadataDB | None,
) -> bool:
    """Check whether explicit arguments point at the same runtime as the singleton."""
    if db is not None and db is not existing.db:
        return False
    if runtime_config is not None and runtime_config is not existing.config:
        if runtime_config.data != existing.config.data:
            return False
    if layout is not None and layout is not existing.layout:
        if layout != existing.layout:
            return False
    return True


def get_knowledge_artifact_runtime(
    runtime_config: Config | None = None,
    *,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
) -> KnowledgeArtifactRuntime:
    """Return the singleton runtime used by CLI and API entrypoints.

    The first call may provide explicit configuration, layout, and database.
    Subsequent calls must use the same instance (no arguments) or match the
    existing singleton exactly; mismatched arguments raise a hard failure so
    workers and API endpoints never silently use a different runtime.
    """
    global _shared_runtime
    if _shared_runtime is None:
        _shared_runtime = KnowledgeArtifactRuntime(
            runtime_config or config, layout=layout, db=db
        )
        return _shared_runtime

    if runtime_config is None and layout is None and db is None:
        return _shared_runtime

    if not _runtime_configs_compatible(_shared_runtime, runtime_config, layout, db):
        raise RuntimeError(
            "Mismatched knowledge artifact runtime requested: "
            f"existing db={_shared_runtime.db.db_path}, "
            f"requested layout db={layout.database_path if layout else None}"
        )
    return _shared_runtime


def get_knowledge_artifact_runtime_health() -> dict[str, Any]:
    """Return the singleton runtime's worker health.

    An uninitialized runtime is explicitly unhealthy so callers cannot mistake
    "not yet started" for "healthy".
    """
    if _shared_runtime is None:
        return {
            "healthy": False,
            "state": "uninitialized",
            "reason": "Knowledge artifact runtime has not been initialized",
            "last_error": None,
            "last_error_at": None,
            "consecutive_failures": 0,
        }
    return _shared_runtime.worker_health
