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
    TweetArtifact,
    WebClipperArtifact,
)
from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id
from .config import Config, config
from .data_models import Tweet
from .metadata_db import IngestionQueueEntry, MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout
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
            return TweetArtifact.from_queue_payload(payload)
        if artifact_type == "paper":
            return PaperArtifact.from_queue_payload(payload)
        if artifact_type == "repository":
            return RepositoryArtifact.from_queue_payload(payload)
        if artifact_type == "web_clipper":
            return WebClipperArtifact.from_queue_payload(payload)

        raise UnsupportedArtifactTypeError(
            f"Unsupported ingestion artifact type: {entry.artifact_type}"
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
        artifact = self.materialize_artifact(entry)
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
            failure = self.db.mark_ingestion_failed(entry.artifact_id, str(exc))
            if failure and failure.status == "pending" and failure.next_attempt_at:
                logger.info(
                    "Requeued ingestion artifact %s after failure: %s",
                    entry.artifact_id,
                    exc,
                )
            raise

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

        if not artifact.pdf_url:
            raise IngestionRuntimeError(
                f"Paper artifact {artifact.id} is missing pdf_url"
            )

        processor = ArXivProcessorV2(output_dir=str(self.layout.vault_root))
        document = await asyncio.to_thread(
            processor.download_document,
            artifact.pdf_url,
            artifact.id,
            True,
        )
        if not document:
            raise IngestionRuntimeError(
                f"Failed to process paper artifact {artifact.id}"
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

            processor = GitHubStarsProcessor(vault_path=str(self.layout.vault_root))
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

            processor = HuggingFaceLikesProcessor(vault_path=str(self.layout.vault_root))
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
