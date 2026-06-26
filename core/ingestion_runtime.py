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
from typing import Any, Mapping

from .artifacts import (
    KnowledgeArtifact,
    PaperArtifact,
    RepositoryArtifact,
    TranscriptArtifact,
    TweetArtifact,
    VideoArtifact,
    WebClipperArtifact,
)
from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id
from .config import Config, config
from .data_models import Tweet
from .metadata_db import (
    INGESTION_REVIEW_STATUSES,
    IngestionQueueEntry,
    MetadataDB,
    get_metadata_db,
)
from .path_layout import PathLayout, build_path_layout
from .prompt_security import prompt_security_requires_review
from .translation_companion import EnglishCompanionPublisher, TranslationCompanionResult
from .wiki_updater import CompiledWikiUpdater

logger = logging.getLogger(__name__)


class IngestionRuntimeError(RuntimeError):
    """Base error for shared artifact-runtime failures."""


class UnsupportedArtifactTypeError(IngestionRuntimeError, ValueError):
    """Raised when a queue entry declares an unsupported artifact type."""


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
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()
        self._pipeline = None
        self._wiki_updater = None
        self._companion_publisher = None

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
        elif artifact_type == "video":
            artifact = VideoArtifact.from_queue_payload(payload)
        elif artifact_type == "transcript":
            artifact = TranscriptArtifact.from_queue_payload(payload)
        else:
            raise UnsupportedArtifactTypeError(
                f"Unsupported ingestion artifact type: {entry.artifact_type}"
            )

        return artifact.apply_queue_context(
            queue_id=entry.artifact_id,
            queue_source=entry.source,
            queue_created_at=entry.created_at,
            capabilities=_capabilities_from_queue(entry.capabilities_json),
            payload=payload,
        )

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
        self, *, limit: int | None = None
    ) -> list[IngestionDispatchResult]:
        """Process all due ingestion rows once."""
        entries = self.db.get_pending_ingestions(limit=limit)
        results: list[IngestionDispatchResult] = []
        for entry in entries:
            results.append(await self.process_ingestion_entry(entry))
        return results

    async def run_background(
        self,
        shutdown_event: asyncio.Event,
        *,
        poll_interval_seconds: float = 5.0,
        batch_size: int = 25,
    ) -> None:
        """Poll the ingestion queue until shutdown."""
        while not shutdown_event.is_set():
            try:
                results = await self.process_pending_ingestions_once(limit=batch_size)
                if not results:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=poll_interval_seconds
                    )
                    continue
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Ingestion worker iteration failed: %s", exc)
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=poll_interval_seconds
                    )
                except asyncio.TimeoutError:
                    continue

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
            artifact = self.materialize_artifact(entry)
            if prompt_security_requires_review(artifact.normalized_metadata):
                raise IngestionRuntimeError(
                    f"Ingestion artifact {entry.artifact_id} requires security review"
                )
        except Exception as exc:
            if _reviewable_artifact_error(exc):
                return self._route_entry_to_review(entry, exc, stage="materialize")
            raise
        self.db.mark_ingestion_processing(entry.artifact_id)

        try:
            result = await self.dispatch_artifact(artifact)
            self._sync_wiki_for_artifact(
                artifact,
                dispatch_details=result.details,
            )
            self.db.mark_ingestion_processed(entry.artifact_id)
            return result
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
    ) -> IngestionDispatchResult:
        error = f"artifact review required: {exc}"
        updated = self.db.mark_ingestion_review_required(
            entry.artifact_id,
            category=_review_category_for_error(exc),
            reason=str(exc),
            error=error,
            error_type=exc.__class__.__name__,
            metadata={"stage": stage},
        )
        status = updated.status if updated else "needs_review"
        return IngestionDispatchResult(
            artifact_id=entry.artifact_id,
            artifact_type=entry.artifact_type,
            source=entry.source,
            status=status,
            processed_at=_now_iso(),
            details={
                "review_required": True,
                "stage": stage,
                "error": str(exc),
                "error_type": exc.__class__.__name__,
            },
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
        self, artifact: TranscriptArtifact
    ) -> IngestionDispatchResult:
        """Process a transcript artifact already normalized by a connector."""
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
            },
        )


_shared_runtime: KnowledgeArtifactRuntime | None = None


def get_knowledge_artifact_runtime(
    runtime_config: Config | None = None,
    *,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
) -> KnowledgeArtifactRuntime:
    """Return the singleton runtime used by CLI and API entrypoints."""
    global _shared_runtime
    if (
        _shared_runtime is None
        or runtime_config is not None
        or layout is not None
        or db is not None
    ):
        _shared_runtime = KnowledgeArtifactRuntime(
            runtime_config or config, layout=layout, db=db
        )
    return _shared_runtime
