"""
Pipeline Processor - Single-pass processing pipeline
Processes all tweet enhancements in one pass instead of multiple loops
"""

import asyncio
import logging
from collections import Counter
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

from core.data_models import Tweet, ProcessingStats
from core.config import config
from core.path_layout import resolve_vault_root
from core.pipeline_registry import pipeline_registry
from .url_processor import URLProcessor
from .media_processor import MediaProcessor
from .document_factory import DocumentFactory
from .llm_processor import LLMProcessor
from .async_llm_processor import AsyncProcessingConfig, AsyncLLMProcessor
from .content_processor import ContentProcessor
from .thread_processor import ThreadProcessor
from .youtube_processor import YouTubeProcessor
from .transcription_processor import TranscriptionProcessor

logger = logging.getLogger(__name__)


@dataclass
class StageTimings:
    """Per-stage timing statistics"""

    url_expansion: float = 0.0
    media_download: float = 0.0
    documents: float = 0.0
    transcripts: float = 0.0
    youtube_metadata: float = 0.0
    youtube_transcripts: float = 0.0
    llm_processing: float = 0.0
    file_save: float = 0.0
    thread_processing: float = 0.0
    total_pipeline: float = 0.0


@dataclass
class StageSkipCounts:
    """Per-stage skip statistics"""

    media_skipped: int = 0
    arxiv_skipped: int = 0
    pdf_skipped: int = 0
    readme_skipped: int = 0
    youtube_skipped: int = 0
    transcript_skipped: int = 0
    llm_skipped: int = 0


@dataclass
class PipelineStats:
    """Comprehensive pipeline statistics with enhanced observability"""

    total_tweets: int = 0
    processed_tweets: int = 0
    skipped_tweets: int = 0
    failed_tweets: int = 0

    # Enhancement statistics
    url_expansions: int = 0
    media_downloads: int = 0
    arxiv_downloads: int = 0
    pdf_downloads: int = 0
    readme_downloads: int = 0
    youtube_videos: int = 0
    video_transcripts: int = 0
    llm_enhancements: int = 0
    youtube_transcript_attempts: int = 0
    youtube_transcript_success: int = 0
    youtube_transcript_failures: int = 0

    # Output statistics
    tweet_files: ProcessingStats = field(default_factory=ProcessingStats)
    thread_files: ProcessingStats = field(default_factory=ProcessingStats)

    # Enhanced observability
    timings: StageTimings = field(default_factory=StageTimings)
    skip_counts: StageSkipCounts = field(default_factory=StageSkipCounts)
    cache_hits: Dict[str, int] = field(default_factory=dict)

    def add_enhancement(self, enhancement_type: str):
        """Add an enhancement count"""
        if hasattr(self, enhancement_type):
            current_value = getattr(self, enhancement_type)
            setattr(self, enhancement_type, current_value + 1)

    def print_summary(self):
        """Print comprehensive pipeline summary with enhanced observability"""
        print(f"\n📊 Single-Pass Pipeline Results:")
        print(
            f"📈 Tweets: {self.processed_tweets}/{self.total_tweets} processed, {self.skipped_tweets} skipped, {self.failed_tweets} failed"
        )

        print(f"\n🔧 Enhancements Applied:")
        print(f"   🔗 URL Expansions: {self.url_expansions}")
        print(f"   📸 Media Downloads: {self.media_downloads}")
        print(f"   📄 ArXiv Papers: {self.arxiv_downloads}")
        print(f"   📄 PDF Documents: {self.pdf_downloads}")
        print(f"   📂 Repository READMEs: {self.readme_downloads}")
        print(f"   📺 YouTube Videos: {self.youtube_videos}")
        print(f"   🎤 Video Transcripts: {self.video_transcripts}")
        print(f"   🤖 LLM Enhancements: {self.llm_enhancements}")

        print(f"\n📝 Output Files:")
        print(
            f"   Tweet Files: ✅ {self.tweet_files.created} created, 🔄 {self.tweet_files.updated} updated, ⏭️ {self.tweet_files.skipped} skipped"
        )
        print(
            f"   Thread Files: ✅ {self.thread_files.created} created, 🔄 {self.thread_files.updated} updated, ⏭️ {self.thread_files.skipped} skipped"
        )

        # Enhanced observability
        if self.timings.total_pipeline > 0:
            print(f"\n⏱️ Stage Timings:")
            print(f"   Total Pipeline: {self.timings.total_pipeline:.2f}s")
            print(f"   URL Expansion: {self.timings.url_expansion:.2f}s")
            print(f"   Media Download: {self.timings.media_download:.2f}s")
            print(f"   Documents: {self.timings.documents:.2f}s")
            print(f"   YouTube Metadata: {self.timings.youtube_metadata:.2f}s")
            print(f"   YouTube Transcripts: {self.timings.youtube_transcripts:.2f}s")
            print(f"   Other Transcripts: {self.timings.transcripts:.2f}s")
            print(f"   LLM Processing: {self.timings.llm_processing:.2f}s")
            print(f"   File Save: {self.timings.file_save:.2f}s")
            print(f"   Thread Processing: {self.timings.thread_processing:.2f}s")

        if self.youtube_transcript_attempts:
            print(f"\n🎬 YouTube Transcript Health:")
            print(f"   Attempts: {self.youtube_transcript_attempts}")
            print(f"   Completed: {self.youtube_transcript_success}")
            print(f"   Failures: {self.youtube_transcript_failures}")

        total_skips = (
            self.skip_counts.media_skipped
            + self.skip_counts.arxiv_skipped
            + self.skip_counts.pdf_skipped
            + self.skip_counts.readme_skipped
            + self.skip_counts.youtube_skipped
            + self.skip_counts.transcript_skipped
            + self.skip_counts.llm_skipped
        )
        if total_skips > 0:
            print(f"\n⏭️ Skip Statistics:")
            print(f"   Media: {self.skip_counts.media_skipped}")
            print(f"   ArXiv: {self.skip_counts.arxiv_skipped}")
            print(f"   PDFs: {self.skip_counts.pdf_skipped}")
            print(f"   READMEs: {self.skip_counts.readme_skipped}")
            print(f"   YouTube: {self.skip_counts.youtube_skipped}")
            print(f"   Transcripts: {self.skip_counts.transcript_skipped}")
            print(f"   LLM: {self.skip_counts.llm_skipped}")

        if self.cache_hits:
            print(f"\n💾 Cache Performance:")
            for cache_type, hits in self.cache_hits.items():
                print(f"   {cache_type}: {hits} hits")


class PipelineProcessor:
    """Single-pass pipeline processor that applies all enhancements to tweets in one iteration"""

    def __init__(self, vault_path: str = None):
        self.vault_path = resolve_vault_root(config, override=vault_path)

        # Initialize all processors
        self.url_processor = URLProcessor()
        self.media_processor = MediaProcessor()
        self.document_factory = DocumentFactory(vault_path)
        self.youtube_processor = YouTubeProcessor(vault_path)
        self.transcript_processor = TranscriptionProcessor(vault_path)
        # Use enhanced async LLM processor with configuration-driven concurrency
        async_settings = config.get("processing.llm_async", {}) or {}
        async_config = AsyncProcessingConfig(
            max_concurrent_requests=async_settings.get("max_concurrent_requests", 8),
            max_concurrent_batches=async_settings.get("max_concurrent_batches", 2),
            rate_limit_delay=async_settings.get("rate_limit_delay", 0.05),
            request_timeout=async_settings.get("request_timeout", 20.0),
            retry_attempts=async_settings.get(
                "retry_attempts", AsyncProcessingConfig.retry_attempts
            ),
            retry_delay=async_settings.get(
                "retry_delay", AsyncProcessingConfig.retry_delay
            ),
        )
        self.llm_processor = AsyncLLMProcessor(async_config)
        self.content_processor = ContentProcessor(vault_path)
        self.thread_processor = ThreadProcessor(vault_path)

        # Track progress
        self.stats = PipelineStats()

    async def process_tweets_pipeline(
        self,
        tweets: List[Tweet],
        url_mappings: Optional[List] = None,
        resume: bool = True,
        batch_size: int = 10,
        rerun_llm: bool = False,
        llm_only: bool = False,
        dry_run: bool = False,
    ) -> PipelineStats:
        """Process all tweets through the complete pipeline in a single pass"""
        import time

        pipeline_start = time.time()

        # Reset stats for each run so repeated executions start clean
        self.stats = PipelineStats()
        self.stats.total_tweets = len(tweets)
        logger.info(f"🚀 Starting single-pass pipeline for {len(tweets)} tweets")

        resume_non_llm = resume
        resume_llm = not rerun_llm
        resume_files = resume if not llm_only else False
        if llm_only:
            resume_llm = False

        if dry_run:
            self._simulate_pipeline(
                tweets,
                url_mappings,
                resume_non_llm,
                resume_llm,
                resume_files,
                llm_only,
            )
            return self.stats

        # Apply URL expansions first (affects all tweets)
        stage_start = time.time()
        if url_mappings and pipeline_registry.is_enabled("url_expansion"):
            logger.debug("🔗 [PIPE] URL expansion stage start")
            url_stats = self.url_processor.apply_url_expansions(tweets, url_mappings)
            self.stats.url_expansions = url_stats.updated
            logger.info(f"🔗 Applied {url_stats.updated} URL expansions")
        elif not pipeline_registry.is_enabled("url_expansion"):
            logger.info("🔗 URL expansion disabled in pipeline configuration")
        self.stats.timings.url_expansion = time.time() - stage_start

        # Mark thread tweets early to optimize LLM processing
        self._mark_thread_tweets(tweets)

        # Process tweets in batches with incremental file saving
        for i in range(0, len(tweets), batch_size):
            batch = tweets[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(tweets) + batch_size - 1) // batch_size

            logger.info(
                f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} tweets)"
            )
            logger.debug("📸 [PIPE] Media stage start")
            await self._process_tweet_batch(batch, resume_non_llm, resume_llm, llm_only)

            # Save files for this batch immediately
            save_start = time.time()
            logger.debug("💾 [PIPE] Save stage start")
            logger.info(f"💾 Saving files for batch {batch_num}/{total_batches}")
            await self._save_batch_files(batch, resume_files)
            self.stats.timings.file_save += time.time() - save_start

        # Process and save threads for all tweets at the end
        stage_start = time.time()
        logger.debug("🧵 [PIPE] Thread stage start")
        await self._process_and_save_threads(tweets, resume_files, resume_llm)
        self.stats.timings.thread_processing = time.time() - stage_start

        # Record total pipeline time
        self.stats.timings.total_pipeline = time.time() - pipeline_start

        # Update metadata database if enabled
        if config.get("database.enabled", False):
            await self._update_metadata_database(tweets)

        logger.info(
            f"✅ Pipeline complete: {self.stats.processed_tweets}/{self.stats.total_tweets} tweets processed in {self.stats.timings.total_pipeline:.2f}s"
        )
        return self.stats

    def _simulate_pipeline(
        self,
        tweets: List[Tweet],
        url_mappings: Optional[List],
        resume_non_llm: bool,
        resume_llm: bool,
        resume_files: bool,
        llm_only: bool,
    ) -> None:
        """Plan the pipeline without performing downloads or writes."""

        from core.filename_utils import get_filename_normalizer

        normalizer = get_filename_normalizer()
        stage_counts: Counter[str] = Counter()
        tweet_plans: List[Dict[str, Any]] = []
        notes: List[str] = []

        url_expansion_enabled = bool(url_mappings) and pipeline_registry.is_enabled(
            "url_expansion"
        )
        if url_mappings and not pipeline_registry.is_enabled("url_expansion"):
            notes.append("URL expansion disabled via configuration")

        if llm_only:
            notes.append("llm-only mode: markdown regeneration is skipped")

        youtube_stage_enabled = pipeline_registry.is_enabled(
            "transcripts.youtube_videos"
        )
        transcripts_stage_enabled = pipeline_registry.is_enabled(
            "transcripts.twitter_videos"
        )
        if tweets:
            if not youtube_stage_enabled and any(
                self._extract_youtube_urls_from_tweet(tweet) for tweet in tweets
            ):
                notes.append(
                    "YouTube processing disabled; embeds/transcripts will be skipped"
                )
            if not transcripts_stage_enabled and any(
                getattr(tweet, "media_items", None) for tweet in tweets
            ):
                notes.append("Twitter video transcription disabled in configuration")

        threads: Dict[str, List[Tweet]] = {}
        if tweets:
            threads = self.thread_processor.detect_threads_from_tweets(tweets)

        for tweet in tweets:
            entry: Dict[str, Any] = {
                "tweet_id": tweet.id,
                "screen_name": getattr(tweet, "screen_name", ""),
                "stages": [],
            }

            if url_expansion_enabled and getattr(tweet, "url_mappings", None):
                stage_counts["url_expansions"] += 1
                entry["stages"].append("url_expansion")

            if not llm_only:
                pending_media = 0
                if tweet.media_items:
                    for media_item in tweet.media_items:
                        already_downloaded = getattr(media_item, "downloaded", False)
                        if resume_non_llm and already_downloaded:
                            continue
                        pending_media += 1
                if pending_media:
                    stage_counts["media_downloads"] += 1
                    entry["stages"].append("media_download")

                if pipeline_registry.is_enabled("documents.arxiv_papers"):
                    arxiv_urls = (
                        self.document_factory.arxiv_processor.extract_urls_from_tweet(
                            tweet
                        )
                    )
                    need_arxiv = bool(arxiv_urls)
                    if resume_non_llm and getattr(tweet, "arxiv_papers", None):
                        if all(
                            getattr(paper, "downloaded", False)
                            for paper in tweet.arxiv_papers
                        ):
                            need_arxiv = False
                    if need_arxiv:
                        stage_counts["arxiv_downloads"] += 1
                        entry["stages"].append("arxiv_documents")

                if pipeline_registry.is_enabled("documents.general_pdfs"):
                    pdf_urls = (
                        self.document_factory.pdf_processor.extract_urls_from_tweet(
                            tweet
                        )
                    )
                    need_pdf = bool(pdf_urls)
                    if resume_non_llm and getattr(tweet, "pdf_links", None):
                        if all(
                            getattr(link, "downloaded", False)
                            for link in tweet.pdf_links
                        ):
                            need_pdf = False
                    if need_pdf:
                        stage_counts["pdf_downloads"] += 1
                        entry["stages"].append("pdf_documents")

                readme_urls = []
                if pipeline_registry.any_enabled(
                    ["documents.github_readmes", "documents.huggingface_readmes"]
                ):
                    readme_urls = (
                        self.document_factory.readme_processor.extract_urls_from_tweet(
                            tweet
                        )
                    )
                need_readme = bool(readme_urls)
                if resume_non_llm and getattr(tweet, "repo_links", None):
                    if all(
                        getattr(link, "downloaded", False) for link in tweet.repo_links
                    ):
                        need_readme = False
                if need_readme:
                    stage_counts["readme_downloads"] += 1
                    entry["stages"].append("readme_documents")

                if self._should_process_youtube(tweet, resume_non_llm):
                    youtube_urls = self._extract_youtube_urls_from_tweet(tweet)
                    stage_counts["youtube_videos"] += len(youtube_urls)
                    entry["stages"].append("youtube")

                if self._should_process_transcripts(tweet, resume_llm):
                    stage_counts["video_transcripts"] += 1
                    entry["stages"].append("video_transcripts")

                if not tweet.is_self_thread:
                    tweet_filename = normalizer.generate_tweet_filename(
                        tweet.id, tweet.screen_name
                    )
                    tweet_path = self.content_processor.tweets_dir / tweet_filename
                    should_write_tweet = not resume_files or not tweet_path.exists()
                    if should_write_tweet:
                        stage_counts["tweet_markdown"] += 1
                        entry["stages"].append("tweet_markdown")
                else:
                    entry.setdefault("notes", []).append(
                        "part of thread; markdown handled separately"
                    )

            if self._should_process_llm(tweet, resume_llm):
                stage_counts["llm_enhancements"] += 1
                entry["stages"].append("llm")

            tweet_plans.append(entry)

        thread_updates: List[str] = []
        if threads and not llm_only:
            for thread_id, thread_tweets in threads.items():
                if len(thread_tweets) < 2:
                    continue
                first_tweet = thread_tweets[0]
                thread_filename = normalizer.generate_thread_filename(
                    thread_id, first_tweet.screen_name
                )
                thread_path = self.thread_processor.threads_dir / thread_filename
                should_write_thread = not resume_files or not thread_path.exists()
                if should_write_thread:
                    stage_counts["thread_markdown"] += 1
                    thread_updates.append(thread_id)
                    for entry in tweet_plans:
                        if entry["tweet_id"] == first_tweet.id:
                            entry["stages"].append("thread_markdown")
                            break

        self.stats.url_expansions = stage_counts["url_expansions"]
        self.stats.media_downloads = stage_counts["media_downloads"]
        self.stats.arxiv_downloads = stage_counts["arxiv_downloads"]
        self.stats.pdf_downloads = stage_counts["pdf_downloads"]
        self.stats.readme_downloads = stage_counts["readme_downloads"]
        self.stats.youtube_videos = stage_counts["youtube_videos"]
        self.stats.video_transcripts = stage_counts["video_transcripts"]
        self.stats.llm_enhancements = stage_counts["llm_enhancements"]
        self.stats.tweet_files.updated = stage_counts["tweet_markdown"]
        self.stats.thread_files.updated = stage_counts["thread_markdown"]
        self.stats.skipped_tweets = len(tweets)

        self.stats.extras.setdefault("dry_run", {})
        self.stats.extras["dry_run"].update(
            {
                "stage_counts": dict(stage_counts),
                "tweets": tweet_plans,
                "thread_updates": thread_updates,
                "notes": notes,
            }
        )

        logger.info(
            "🧪 Dry run: planned %s tweet(s) without performing downloads",
            len(tweets),
        )

    async def _process_tweet_batch(
        self,
        tweets: List[Tweet],
        resume_non_llm: bool,
        resume_llm: bool,
        llm_only: bool,
    ):
        """Process a batch of tweets with all enhancements"""
        # Create async tasks for each tweet in the batch
        tasks = []
        for tweet in tweets:
            task = self._process_single_tweet(
                tweet, resume_non_llm, resume_llm, llm_only
            )
            tasks.append(task)

        # Process batch concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Update statistics
        for result in results:
            if isinstance(result, Exception):
                self.stats.failed_tweets += 1
                logger.error(f"Tweet processing failed: {result}")
            else:
                self.stats.processed_tweets += 1

    async def _process_single_tweet(
        self, tweet: Tweet, resume_non_llm: bool, resume_llm: bool, llm_only: bool
    ) -> bool:
        """Process a single tweet with all enhancements"""
        try:
            import time

            enhanced = False
            resume_transcripts = resume_llm

            if not llm_only:
                # Media processing
                if self._should_process_media(tweet, resume_non_llm):
                    stage_start = time.time()
                    media_result = await self._process_media_for_tweet(
                        tweet, resume_non_llm
                    )
                    self.stats.timings.media_download += time.time() - stage_start
                    if media_result:
                        self.stats.add_enhancement("media_downloads")
                        enhanced = True
                else:
                    self._track_skip("media")

                # Document processing (ArXiv, PDFs, READMEs) via DocumentFactory
                stage_start = time.time()
                doc_result, doc_attempts = await self._process_documents_unified(
                    tweet, resume_non_llm
                )
                self.stats.timings.documents += time.time() - stage_start
                if doc_result:
                    if doc_result.get("arxiv"):
                        self.stats.add_enhancement("arxiv_downloads")
                        enhanced = True
                    if doc_result.get("pdf"):
                        self.stats.add_enhancement("pdf_downloads")
                        enhanced = True
                    if doc_result.get("readme"):
                        self.stats.add_enhancement("readme_downloads")
                        enhanced = True
                        if pipeline_registry.is_enabled(
                            "llm_processing.readme_summaries"
                        ):
                            try:
                                if (
                                    hasattr(tweet, "repo_links")
                                    and tweet.repo_links
                                    and hasattr(self, "llm_processor")
                                    and self.llm_processor
                                ):
                                    for repo_link in tweet.repo_links:
                                        if getattr(repo_link, "llm_summary", None):
                                            continue
                                        if resume_non_llm:
                                            reused = self._reuse_readme_summary(
                                                repo_link
                                            )
                                            if reused:
                                                repo_link.llm_summary = reused
                                                logger.debug(
                                                    f"♻️ Reused README summary for {repo_link.repo_name}"
                                                )
                                                continue
                                        logger.debug(
                                            f"🤖 [PIPE] README LLM for {repo_link.repo_name}"
                                        )
                                        summary = await self.llm_processor._generate_readme_summary(
                                            repo_link
                                        )
                                        if summary:
                                            repo_link.llm_summary = summary
                            except Exception as e:
                                logger.debug(f"README LLM on unified path failed: {e}")
                        else:
                            logger.debug(
                                "README LLM stage disabled; skipping summary generation"
                            )
                # Track skips for attempted but unprocessed document types
                for doc_type, attempted in doc_attempts.items():
                    if not attempted:
                        continue
                    has_result = bool(doc_result.get(doc_type)) if doc_result else False
                    if not has_result:
                        self._track_skip(doc_type)

            # YouTube processing (metadata + transcript formatting)
            if (
                self._should_process_youtube(tweet, resume_non_llm)
                or not resume_transcripts
            ):
                youtube_result = await self._process_youtube_for_tweet(
                    tweet, resume_non_llm, resume_transcripts
                )
                if youtube_result:
                    self.stats.add_enhancement("youtube_videos")
                    enhanced = True
                else:
                    self._track_skip("youtube")
            else:
                self._track_skip("youtube")

            # Twitter video transcript processing (Deepgram/Whisper)
            if self._should_process_transcripts(tweet, resume_transcripts):
                stage_start = time.time()
                transcript_result = await self._process_transcripts_for_tweet(
                    tweet, resume_transcripts
                )
                self.stats.timings.transcripts += time.time() - stage_start
                if transcript_result:
                    self.stats.add_enhancement("video_transcripts")
                    enhanced = True
            else:
                self._track_skip("transcript")

            # LLM processing (async)
            if self._should_process_llm(tweet, resume_llm):
                stage_start = time.time()
                llm_result = await self._process_llm_for_tweet(tweet)
                self.stats.timings.llm_processing += time.time() - stage_start
                if llm_result:
                    self.stats.add_enhancement("llm_enhancements")
                    enhanced = True
            else:
                self._track_skip("llm")

            return enhanced

        except Exception as e:
            logger.error(f"Error processing tweet {tweet.id}: {e}")
            raise

    def _should_process_media(self, tweet: Tweet, resume: bool) -> bool:
        """Check if tweet needs media processing"""
        if not pipeline_registry.is_enabled("media_download"):
            return False
        if not tweet.media_items:
            return False
        if resume and all(
            hasattr(m, "filename") and m.filename for m in tweet.media_items
        ):
            return False
        return True

    def _should_process_llm(self, tweet: Tweet, resume: bool) -> bool:
        """Check if tweet needs LLM processing"""
        if not self.llm_processor.is_enabled():
            return False

        # Check if any LLM processing stages are enabled
        llm_stages_enabled = pipeline_registry.any_enabled(
            [
                "llm_processing.tweet_tags",
                "llm_processing.tweet_summaries",
                "llm_processing.alt_text",
            ]
        )
        if not llm_stages_enabled:
            return False

        # Skip LLM processing for individual thread tweets - only process the compiled thread
        if hasattr(tweet, "is_thread_tweet") and tweet.is_thread_tweet:
            logger.debug(
                f"Skipping LLM processing for thread tweet {tweet.id} - will process compiled thread instead"
            )
            return False

        # Always process if not resuming
        if not resume:
            return True

        from core.filename_utils import get_filename_normalizer

        normalizer = get_filename_normalizer()
        tweet_filename = normalizer.generate_tweet_filename(tweet.id, tweet.screen_name)
        tweet_file = self.vault_path / "tweets" / tweet_filename
        if self.llm_processor.has_llm_sections_in_markdown(tweet_file):
            logger.debug(
                f"Skipping LLM processing for {tweet.id} - markdown already has LLM sections"
            )
            return False

        # Check in-memory LLM enhancements
        if hasattr(tweet, "llm_tags") and tweet.llm_tags:
            return False

        return True

    def _should_process_youtube(self, tweet: Tweet, resume: bool) -> bool:
        """Determine if YouTube processing should run for this tweet"""
        if not pipeline_registry.is_enabled("transcripts.youtube_videos"):
            return False

        embeddings_enabled = config.get("youtube.enable_embeddings", False)
        transcripts_enabled = config.get("youtube.enable_transcripts", False)
        if not embeddings_enabled and not transcripts_enabled:
            return False

        youtube_urls = self._extract_youtube_urls_from_tweet(tweet)
        if not youtube_urls:
            return False

        if resume and getattr(tweet, "youtube_videos", None):
            return False

        if resume:
            video_ids = [
                self.youtube_processor.extract_video_id(url)
                for url in youtube_urls
            ]
            durable_video_ids = [video_id for video_id in video_ids if video_id]
            if durable_video_ids and all(
                self.youtube_processor.has_existing_transcript(video_id)
                for video_id in durable_video_ids
            ):
                logger.debug(
                    "Skipping YouTube processing for tweet %s - durable transcript markdown already exists",
                    tweet.id,
                )
                return False

        return True

    def _extract_youtube_urls_from_tweet(self, tweet: Tweet) -> List[str]:
        """Extract unique YouTube URLs from tweet text and URL mappings"""
        youtube_urls: List[str] = []

        if tweet.full_text:
            youtube_urls.extend(
                self.youtube_processor.extract_youtube_urls(tweet.full_text)
            )

        if hasattr(tweet, "url_mappings") and tweet.url_mappings:
            for mapping in tweet.url_mappings:
                expanded = (mapping.expanded_url or "").lower()
                if "youtube.com" in expanded or "youtu.be" in expanded:
                    youtube_urls.append(mapping.expanded_url)

        # Ensure uniqueness
        seen = set()
        unique_urls: List[str] = []
        for url in youtube_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        return unique_urls

    async def _process_youtube_for_tweet(
        self, tweet: Tweet, resume_metadata: bool, resume_transcripts: bool
    ) -> bool:
        """Process YouTube metadata/transcripts for a single tweet"""
        try:
            youtube_urls = self._extract_youtube_urls_from_tweet(tweet)
            if not youtube_urls:
                return False

            stats = await self.youtube_processor.process_youtube_urls(
                youtube_urls,
                resume_metadata=resume_metadata,
                resume_transcripts=resume_transcripts,
                source_label=f"tweet {tweet.id} by @{tweet.screen_name}",
            )
            if stats.updated > 0:
                extras = stats.extras or {}
                tweet.youtube_videos = extras.get("videos", [])
                self.stats.timings.youtube_metadata += extras.get(
                    "metadata_seconds", 0.0
                )
                self.stats.timings.youtube_transcripts += extras.get(
                    "transcript_seconds", 0.0
                )
                self.stats.youtube_transcript_attempts += extras.get(
                    "transcript_attempts", 0
                )
                self.stats.youtube_transcript_success += extras.get(
                    "transcript_completed", 0
                )
                self.stats.youtube_transcript_failures += extras.get(
                    "transcript_failed", 0
                )
                return True

        except Exception as e:
            logger.error(f"YouTube processing failed for tweet {tweet.id}: {e}")

        return False

    async def _process_media_for_tweet(self, tweet: Tweet, resume: bool) -> bool:
        """Process media for a single tweet"""
        try:
            if not tweet.media_items:
                return False

            # Use unified media processor which handles thumbnails and video files
            stats = await asyncio.to_thread(
                self.media_processor.process_media, [tweet], resume=resume
            )
            # Treat any update as enhanced
            return stats.updated > 0

        except Exception as e:
            logger.error(f"Media processing failed for tweet {tweet.id}: {e}")

        return False

    def _should_process_transcripts(self, tweet: Tweet, resume: bool) -> bool:
        """Check if tweet needs transcript processing"""
        if not pipeline_registry.is_enabled("transcripts.twitter_videos"):
            return False
        # Enable transcripts if any backend is enabled
        if not (
            config.get("whisper.enabled", False)
            or config.get("deepgram.enabled", False)
        ):
            return False

        # Check if tweet has videos
        if not tweet.media_items:
            return False

        videos = [
            media
            for media in tweet.media_items
            if media.media_type in ["video", "animated_gif"] and media.video_filename
        ]

        if not videos:
            return False

        # Check if transcript already exists
        if resume:
            from core.filename_utils import get_filename_normalizer

            normalizer = get_filename_normalizer()
            transcript_filename = normalizer.generate_twitter_transcript_filename(
                tweet.id, tweet.screen_name
            )
            transcript_file = self.vault_path / "transcripts" / transcript_filename
            if transcript_file.exists():
                return False

        return True

    async def _process_transcripts_for_tweet(self, tweet: Tweet, resume: bool) -> bool:
        """Process video transcripts for a single tweet"""
        try:
            if not self.transcript_processor.is_enabled():
                return False

            # Find videos in this tweet
            videos = [
                media
                for media in tweet.media_items
                if media.media_type in ["video", "animated_gif"]
                and media.video_filename
            ]

            if not videos:
                return False

            # Process each video
            for video in videos:
                if await self.transcript_processor._process_video_transcript(
                    tweet, video
                ):
                    return True

            return False

        except Exception as e:
            logger.error(f"Transcript processing failed for tweet {tweet.id}: {e}")
            return False

    async def _process_llm_for_tweet(self, tweet: Tweet) -> bool:
        """Process LLM enhancements for a single tweet using enhanced async processor"""
        try:
            # Use the enhanced async LLM processor's single tweet processing
            enhanced = await self.llm_processor._process_single_tweet_enhanced(tweet)
            return enhanced

        except Exception as e:
            logger.error(f"LLM processing failed for tweet {tweet.id}: {e}")

        return False

    async def _generate_markdown_files(self, tweets: List[Tweet], resume: bool):
        """Generate final markdown files for tweets and threads"""
        logger.info("📝 Generating markdown files...")

        # Generate individual tweet files
        tweet_stats = self.content_processor.process_tweets(tweets, resume=resume)
        self.stats.tweet_files = tweet_stats

        # Generate thread files
        threads = self.thread_processor.detect_threads_from_tweets(tweets)
        if threads:
            thread_stats = self.thread_processor.process_threads(threads, resume=resume)
            self.stats.thread_files = thread_stats

        logger.info(
            f"📝 Generated {tweet_stats.created + tweet_stats.updated} tweet files and {self.stats.thread_files.created + self.stats.thread_files.updated} thread files"
        )

    async def _save_batch_files(self, batch_tweets: List[Tweet], resume: bool):
        """Save markdown files for a batch of tweets immediately"""
        try:
            # Generate individual tweet files for this batch only
            tweet_stats = self.content_processor.process_tweets(
                batch_tweets, resume=resume
            )

            # Update overall stats
            self.stats.tweet_files.created += tweet_stats.created
            self.stats.tweet_files.updated += tweet_stats.updated
            self.stats.tweet_files.skipped += tweet_stats.skipped

            logger.debug(
                f"📝 Batch saved: {tweet_stats.created} created, {tweet_stats.updated} updated, {tweet_stats.skipped} skipped"
            )

        except Exception as e:
            logger.error(f"Error saving batch files: {e}")

    async def _process_and_save_threads(
        self, tweets: List[Tweet], resume_non_llm: bool, resume_llm: bool
    ):
        """Process thread detection and save thread files"""
        logger.info("🧵 Processing and saving threads...")

        # Detect threads
        threads = self.thread_processor.detect_threads_from_tweets(tweets)
        if not threads:
            logger.info("🧵 No threads detected")
            return

        logger.info(f"🧵 Detected {len(threads)} threads")

        # Process threads with LLM if enabled
        thread_llm_stats = await self.llm_processor.process_threads(
            threads, resume=resume_llm
        )
        logger.info(f"🧵 Thread LLM processing: {thread_llm_stats.updated} processed")

        # Save thread files
        thread_stats = self.thread_processor.process_threads(
            threads, resume=resume_non_llm
        )
        self.stats.thread_files = thread_stats

        logger.info(
            f"🧵 Thread files: {thread_stats.created} created, {thread_stats.updated} updated, {thread_stats.skipped} skipped"
        )

    def _mark_thread_tweets(self, tweets: List[Tweet]):
        """Mark individual tweets that are part of threads to optimize LLM processing"""
        try:
            # Detect threads - returns Dict[str, List[Tweet]]
            threads_dict = self.thread_processor.detect_threads_from_tweets(tweets)
            if not threads_dict:
                return

            # Mark all tweets that are part of any thread
            thread_tweet_ids = set()
            for thread_id, thread_tweets in threads_dict.items():
                for tweet in thread_tweets:
                    thread_tweet_ids.add(tweet.id)

            # Mark tweets in our list
            marked_count = 0
            for tweet in tweets:
                if tweet.id in thread_tweet_ids:
                    tweet.is_thread_tweet = True
                    marked_count += 1

            logger.info(
                f"🧵 Marked {marked_count} tweets as thread tweets to optimize LLM processing"
            )

        except Exception as e:
            logger.error(f"Error marking thread tweets: {e}")
            import traceback

            logger.error(f"Full traceback: {traceback.format_exc()}")

    async def _process_documents_unified(
        self, tweet: Tweet, resume: bool
    ) -> Tuple[Dict[str, Any], Dict[str, bool]]:
        """Process all document types for a tweet using DocumentFactory.

        Returns a tuple of (results, attempts) where attempts maps document type to
        whether processing was attempted for that type.
        """
        try:
            arxiv_enabled = pipeline_registry.is_enabled("documents.arxiv_papers")
            pdf_enabled = pipeline_registry.is_enabled("documents.general_pdfs")
            readme_enabled = pipeline_registry.any_enabled(
                ["documents.github_readmes", "documents.huggingface_readmes"]
            )

            arxiv_urls = (
                self.document_factory.arxiv_processor.extract_urls_from_tweet(tweet)
                if arxiv_enabled
                else []
            )
            pdf_urls = (
                self.document_factory.pdf_processor.extract_urls_from_tweet(tweet)
                if pdf_enabled
                else []
            )
            readme_urls = (
                self.document_factory.readme_processor.extract_urls_from_tweet(tweet)
                if readme_enabled
                else []
            )

            attempts = {
                "arxiv": bool(arxiv_urls),
                "pdf": bool(pdf_urls),
                "readme": bool(readme_urls),
            }

            if not any(attempts.values()):
                return {}, attempts

            results = await self.document_factory.process_single_tweet_async(
                tweet, resume
            )

            filtered_results: Dict[str, Any] = {}
            if attempts["arxiv"] and results.get("arxiv"):
                filtered_results["arxiv"] = results["arxiv"]

            if attempts["pdf"] and results.get("pdf"):
                filtered_results["pdf"] = results["pdf"]

            if attempts["readme"] and results.get("readme"):
                filtered_readmes = []
                for readme in results["readme"]:
                    if (
                        readme.platform == "github"
                        and pipeline_registry.is_enabled("documents.github_readmes")
                    ) or (
                        readme.platform == "huggingface"
                        and pipeline_registry.is_enabled(
                            "documents.huggingface_readmes"
                        )
                    ):
                        filtered_readmes.append(readme)

                if filtered_readmes:
                    filtered_results["readme"] = filtered_readmes

            return filtered_results, attempts

        except Exception as e:
            logger.error(
                f"Unified document processing failed for tweet {tweet.id}: {e}"
            )
            return {}, {"arxiv": False, "pdf": False, "readme": False}

    def _reuse_readme_summary(self, repo_link) -> Optional[str]:
        """Attempt to reuse README summary from stars directory if present."""
        try:
            safe_name = (
                repo_link.repo_name.replace("/", "_")
                if getattr(repo_link, "repo_name", None)
                else "unknown"
            )
            if getattr(repo_link, "platform", "github") == "github":
                summary_file = self.vault_path / "stars" / f"github_{safe_name}_summary.md"
            elif getattr(repo_link, "platform", "") == "huggingface":
                summary_file = self.vault_path / "stars" / f"hf_{safe_name}_summary.md"
            else:
                return None

            if not summary_file.exists():
                return None

            content = summary_file.read_text(encoding="utf-8")
            marker = "## 🤖 AI Summary"
            if marker not in content:
                return None

            start = content.find(marker) + len(marker)
            # Skip newline following marker
            while start < len(content) and content[start] in ("\n", "\r"):
                start += 1
            end = content.find("\n\n", start)
            if end == -1:
                end = content.find("\n#", start)
            if end == -1:
                end = len(content)
            summary = content[start:end].strip()
            return summary or None

        except Exception as e:
            logger.debug(
                f"Could not reuse README summary for {getattr(repo_link, 'repo_name', 'unknown')}: {e}"
            )
            return None

    async def _update_metadata_database(self, tweets: List[Tweet]):
        """Update metadata database with tweet and processing information"""
        try:
            from core.metadata_db import get_metadata_db, TweetMetadata
            from datetime import datetime

            db = get_metadata_db()
            current_time = datetime.now().isoformat()

            for tweet in tweets:
                # Create tweet metadata
                tweet_meta = TweetMetadata(
                    tweet_id=tweet.id,
                    screen_name=tweet.screen_name,
                    created_at=tweet.created_at,
                    is_thread_tweet=getattr(tweet, "is_thread_tweet", False),
                    thread_id=getattr(tweet, "thread_id", None),
                    last_processed_at=current_time,
                    content_hash=self._generate_content_hash(tweet),
                )

                # Set file path if tweet file was created
                from core.filename_utils import get_filename_normalizer

                normalizer = get_filename_normalizer()
                tweet_filename = normalizer.generate_tweet_filename(
                    tweet.id, tweet.screen_name
                )
                tweet_file_path = self.vault_path / "tweets" / tweet_filename
                if tweet_file_path.exists():
                    tweet_meta.file_path = str(
                        tweet_file_path.relative_to(self.vault_path)
                    )

                # Upsert tweet metadata
                db.upsert_tweet(tweet_meta)

            logger.debug(f"Updated metadata database with {len(tweets)} tweets")

        except Exception as e:
            logger.warning(f"Failed to update metadata database: {e}")

    def _generate_content_hash(self, tweet: Tweet) -> str:
        """Generate content hash for tweet"""
        import hashlib

        content = f"{tweet.id}|{tweet.full_text or ''}|{tweet.created_at}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def _track_skip(self, skip_type: str):
        """Track skip statistics"""
        skip_mapping = {
            "media": "media_skipped",
            "arxiv": "arxiv_skipped",
            "pdf": "pdf_skipped",
            "readme": "readme_skipped",
            "youtube": "youtube_skipped",
            "transcript": "transcript_skipped",
            "llm": "llm_skipped",
        }

        if skip_type in skip_mapping:
            attr_name = skip_mapping[skip_type]
            current_value = getattr(self.stats.skip_counts, attr_name)
            setattr(self.stats.skip_counts, attr_name, current_value + 1)
