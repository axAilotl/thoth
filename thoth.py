#!/usr/bin/env python3
"""
Thoth - Modular Twitter Knowledge Management System
Main CLI interface using the new modular architecture
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Any

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from core import (
    ArchivistCompiler,
    Tweet,
    config,
    build_path_layout,
    ensure_wiki_scaffold,
    WikiLintRunner,
    WikiQueryRunner,
)
from core.archivist_benchmark import benchmark_archivist_topics
from core.graphql_cache import maybe_cleanup_graphql_cache
from processors import URLProcessor, CacheLoader, VideoUpdater
from processors.pipeline_processor import PipelineProcessor
from processors.async_llm_processor import AsyncLLMProcessor, AsyncProcessingConfig
from processors.github_stars_processor import GitHubStarsProcessor
from processors.huggingface_likes_processor import HuggingFaceLikesProcessor
from processors.youtube_processor import YouTubeProcessor
from processors.transcription_processor import TranscriptionProcessor
from core.download_tracker import get_download_tracker


logger = logging.getLogger(__name__)


def load_cached_data(limit: int = None, verbose: bool = False):
    """Load tweets and URL mappings from cached GraphQL data"""
    cache_loader = CacheLoader()

    # Load cached GraphQL data
    tweets = cache_loader.load_tweets_from_cache_files(limit=limit)
    if verbose:
        print(f"📂 Loaded {len(tweets)} tweets from cache")

    # Extract URL mappings from cached tweet data
    url_mappings = {}
    if tweets:
        for tweet in tweets:
            if tweet.url_mappings:
                for url_mapping in tweet.url_mappings:
                    url_mappings[url_mapping.short_url] = url_mapping.expanded_url

    return tweets, url_mappings


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # File handler - write into the canonical runtime log path.
    log_file = build_path_layout(config).log_file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Reduce noise from other libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def has_cache_file(tweet_id: str) -> bool:
    """Check if a GraphQL cache file exists for the given tweet ID"""
    cache_dir = build_path_layout(config).cache_root
    if not cache_dir.exists():
        return False

    # Look for any cache file with this tweet ID (pattern: tweet_{id}_{timestamp}.json)
    cache_files = list(cache_dir.glob(f"tweet_{tweet_id}_*.json"))
    return len(cache_files) > 0


def load_bookmarks(
    bookmarks_file: str, limit: int = None, skip_cached: bool = True
) -> List[Tweet]:
    """Load bookmarks from JSON file and convert to Tweet objects

    Args:
        bookmarks_file: Path to the bookmarks JSON file
        limit: Maximum number of bookmarks to process
        skip_cached: If True, skip tweets that already have GraphQL cache files
    """
    try:
        with open(bookmarks_file, "r", encoding="utf-8") as f:
            raw_bookmarks = json.load(f)

        print(f"📚 Loaded {len(raw_bookmarks)} bookmarks from {bookmarks_file}")

        if limit:
            raw_bookmarks = raw_bookmarks[:limit]
            print(f"🔢 Limited to {limit} bookmarks for processing")

        # Convert to Tweet objects and optionally skip cached ones
        tweets = []
        skipped_cached = 0

        for item in raw_bookmarks:
            try:
                # Check if this tweet already has a cache file
                tweet_id = item.get("id")
                if skip_cached and tweet_id and has_cache_file(tweet_id):
                    skipped_cached += 1
                    continue

                tweet = Tweet.from_dict(item)
                tweets.append(tweet)
            except Exception as e:
                logging.debug(f"Skipping invalid bookmark: {e}")
                continue

        print(f"✅ Converted {len(tweets)} valid bookmarks to Tweet objects")
        if skip_cached and skipped_cached > 0:
            print(
                f"⏭️  Skipped {skipped_cached} tweets that already have GraphQL cache files"
            )

        return tweets

    except Exception as e:
        print(f"❌ Error loading bookmarks: {e}")
        return []


def _ensure_pipeline_defaults(args):
    """Normalize argparse namespace for pipeline execution."""
    if not hasattr(args, "batch_size") or args.batch_size is None:
        args.batch_size = config.get("pipeline.batch_size", 10) or 10
    if not hasattr(args, "tweet_ids"):
        args.tweet_ids = None
    if not hasattr(args, "use_cache"):
        args.use_cache = False
    if not hasattr(args, "dry_run"):
        args.dry_run = False
    return args


async def cmd_process_pipeline(args):
    """Process tweets using single-pass pipeline (optimized)"""
    print("🚀 Starting Single-Pass Pipeline Processing")

    if getattr(args, "dry_run", False) and not args.use_cache:
        print("❌ --dry-run currently requires --use-cache to read cached GraphQL data")
        return

    # Fast path: specific tweet IDs from cache only (avoid scanning entire cache)
    if args.use_cache and getattr(args, "tweet_ids", None):
        ids = [tid.strip() for tid in args.tweet_ids.split(",") if tid.strip()]
        cache_loader = CacheLoader()
        tweets: List[Tweet] = []
        url_mappings: Dict[str, str] = {}
        seen = set()
        cache_dir = build_path_layout(config).cache_root
        for tid in ids:
            cache_file = next(iter(cache_dir.glob(f"tweet_{tid}_*.json")), None)
            if not cache_file:
                continue
            # Expand full thread from the single cache file when present
            thread_tweets = cache_loader.extract_all_thread_tweets_from_cache(
                cache_file
            )
            if thread_tweets:
                for t in thread_tweets:
                    if t.id in seen:
                        continue
                    tweets.append(t)
                    seen.add(t.id)
                    # build URL mappings incrementally
                    if t.url_mappings:
                        for m in t.url_mappings:
                            url_mappings[m.short_url] = m.expanded_url
                continue
            # Fallback to one tweet
            t = cache_loader._load_tweet_from_cache(cache_file, tid)
            if t and t.id not in seen:
                tweets.append(t)
                seen.add(t.id)
                if t.url_mappings:
                    for m in t.url_mappings:
                        url_mappings[m.short_url] = m.expanded_url
    else:
        # Regular paths
        if args.use_cache:
            tweets, url_mappings = load_cached_data(
                args.limit, verbose=getattr(args, "verbose", False)
            )
        else:
            tweets = load_bookmarks(
                args.bookmarks,
                args.limit,
                skip_cached=getattr(args, "skip_cached", True),
            )
            url_mappings = None

            # Load GraphQL data for URL mappings if not using cache
            if tweets:
                cache_loader = CacheLoader()
                graphql_data = cache_loader.load_graphql_cache(tweets)
                if graphql_data:
                    url_processor = URLProcessor()
                    url_mappings = url_processor.extract_urls_from_graphql(graphql_data)

    if not tweets:
        print("❌ No tweets to process")
        return

    # Use single-pass pipeline processor
    pipeline = PipelineProcessor()
    rerun_llm = bool(
        getattr(args, "rerun_llm", False) or getattr(args, "llm_only", False)
    )
    stats = await pipeline.process_tweets_pipeline(
        tweets,
        url_mappings,
        resume=getattr(args, "resume", True),
        batch_size=getattr(args, "batch_size", 10),
        rerun_llm=rerun_llm,
        llm_only=getattr(args, "llm_only", False),
        dry_run=getattr(args, "dry_run", False),
    )
    if getattr(args, "dry_run", False):
        plan = stats.extras.get("dry_run", {}) if stats else {}
        stage_counts = plan.get("stage_counts", {})
        print("\n🧪 Dry Run Summary (no files written):")
        if stage_counts:
            print("Planned enhancements:")
            for stage, count in stage_counts.items():
                print(f"   - {stage}: {count}")
        tweet_plan = plan.get("tweets", [])
        if tweet_plan:
            max_preview = min(10, len(tweet_plan))
            print(f"\nTweet preview ({max_preview}/{len(tweet_plan)}):")
            for entry in tweet_plan[:max_preview]:
                stages = ", ".join(entry.get("stages", [])) or "none"
                print(
                    f"   • {entry['tweet_id']} (@{entry.get('screen_name', 'unknown')}): {stages}"
                )
            if len(tweet_plan) > max_preview:
                print(f"   … {len(tweet_plan) - max_preview} more tweets")
        if plan.get("notes"):
            print("\nNotes:")
            for note in plan["notes"]:
                print(f"   - {note}")
        print("\nRe-run without --dry-run to execute the pipeline.")
    else:
        maybe_cleanup_graphql_cache(tweets, stats, logger=logger)
        # Print comprehensive results
        stats.print_summary()


async def cmd_async_test(args):
    """Test enhanced async LLM processing with performance monitoring"""
    print("🚀 Testing Enhanced Async LLM Processing")

    # Load data
    tweets, _ = load_cached_data(args.limit, verbose=True)

    if not tweets:
        print("❌ No tweets to process")
        return

    # Configure async processing
    async_config = AsyncProcessingConfig(
        max_concurrent_requests=args.concurrent,
        max_concurrent_batches=2,
        rate_limit_delay=0.01,
        request_timeout=args.timeout,
        retry_attempts=2,
    )

    # Create enhanced async processor
    async_processor = AsyncLLMProcessor(async_config)

    if not async_processor.is_enabled():
        print("❌ LLM processing not available")
        return

    # Progress callback function
    async def progress_callback(processed: int, total: int, progress: float):
        print(f"⏳ Progress: {processed}/{total} ({progress*100:.1f}%)")

    print(
        f"🎯 Processing {len(tweets)} tweets with {args.concurrent} concurrent requests"
    )
    print(f"⏱️ Timeout: {args.timeout}s per request")

    # Measure processing time
    import time

    start_time = time.time()

    # Process with progress reporting
    stats = await async_processor.process_tweets_with_progress(
        tweets,
        resume=False,  # Process all tweets for testing
        progress_callback=progress_callback,
    )

    end_time = time.time()
    elapsed = end_time - start_time

    # Get processing statistics
    proc_stats = await async_processor.get_processing_stats()

    # Print comprehensive results
    print(f"\n📊 Async Processing Results:")
    print(f"⏱️ Total time: {elapsed:.2f}s")
    print(f"📈 Throughput: {len(tweets)/elapsed:.2f} tweets/second")
    print(f"✅ Processed: {stats.updated}")
    print(f"⏭️ Skipped: {stats.skipped}")
    print(f"❌ Errors: {stats.errors}")

    print(f"\n🔧 Processing Configuration:")
    print(f"   Max concurrent: {proc_stats['max_concurrent']}")
    print(f"   Max batches: {proc_stats['max_batches']}")
    print(f"   Semaphore available: {proc_stats['semaphore_available']}")
    print(f"   Rate limiter available: {proc_stats['rate_limiter_available']}")


async def cmd_github_stars(args):
    """Fetch and process GitHub starred repositories"""
    print("⭐ Fetching GitHub Starred Repositories")

    try:
        # Create GitHub stars processor
        stars_processor = GitHubStarsProcessor()

        print(
            f"🎯 Processing {'all' if not args.limit else args.limit} starred repositories"
        )
        print(f"📂 Summaries will be saved to: knowledge_vault/stars/")
        print(f"📄 READMEs will be saved to: knowledge_vault/repos/")

        # Measure processing time
        import time

        start_time = time.time()

        # Fetch and process repositories
        stats = await stars_processor.fetch_and_process_starred_repos(
            limit=args.limit, resume=args.resume
        )

        end_time = time.time()
        elapsed = end_time - start_time

        # Print results
        print(f"\n📊 GitHub Stars Processing Results:")
        print(f"⏱️ Total time: {elapsed:.2f}s")
        print(f"✅ Processed: {stats.updated}")
        print(f"⏭️ Skipped: {stats.skipped}")
        print(f"❌ Errors: {stats.errors}")
        print(f"📈 Total: {stats.total_processed}")

        if stats.total_processed > 0:
            print(f"📈 Throughput: {stats.total_processed/elapsed:.2f} repos/second")

        print(f"\n📁 Files created:")
        print(f"   📋 Summaries: knowledge_vault/stars/*_summary.md")
        print(f"   📄 READMEs: knowledge_vault/repos/*_README.md")
        print(f"   📇 Index: knowledge_vault/stars/starred_repos_index.json")

    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure GITHUB_API environment variable is set in .env file")
    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"GitHub stars processing failed: {e}")


async def cmd_huggingface_likes(args):
    """Fetch and process HuggingFace liked repositories"""
    print("🤗 Fetching HuggingFace Liked Repositories")

    try:
        # Create HuggingFace likes processor
        hf_processor = HuggingFaceLikesProcessor()

        print(
            f"🎯 Processing {'all' if not args.limit else args.limit} liked repositories"
        )
        print(f"📂 Summaries will be saved to: knowledge_vault/stars/")
        print(f"📄 READMEs will be saved to: knowledge_vault/repos/")

        # Show what types will be included
        types = []
        if getattr(args, "include_models", True):
            types.append("models 🤖")
        if getattr(args, "include_datasets", True):
            types.append("datasets 📊")
        if getattr(args, "include_spaces", True):
            types.append("spaces 🚀")
        print(f"📦 Including: {', '.join(types)}")

        # Measure processing time
        import time

        start_time = time.time()

        # Fetch and process repositories
        stats = await hf_processor.fetch_and_process_liked_repos(
            limit=args.limit,
            resume=args.resume,
            include_models=getattr(args, "include_models", True),
            include_datasets=getattr(args, "include_datasets", True),
            include_spaces=getattr(args, "include_spaces", True),
        )

        end_time = time.time()
        elapsed = end_time - start_time

        # Print results
        print(f"\n📊 HuggingFace Likes Processing Results:")
        print(f"⏱️ Total time: {elapsed:.2f}s")
        print(f"✅ Processed: {stats.updated}")
        print(f"⏭️ Skipped: {stats.skipped}")
        print(f"❌ Errors: {stats.errors}")
        print(f"📈 Total: {stats.total_processed}")

        if stats.total_processed > 0:
            print(f"📈 Throughput: {stats.total_processed/elapsed:.2f} repos/second")

        print(f"\n📁 Files created:")
        print(f"   📋 Summaries: knowledge_vault/stars/hf_*_summary.md")
        print(f"   📄 READMEs: knowledge_vault/repos/hf_*_README.md")
        print(f"   📇 Index: knowledge_vault/stars/huggingface_liked_repos_index.json")

    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure HF_USER environment variable is set in .env file")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Install HuggingFace Hub: pip install huggingface_hub")
    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"HuggingFace likes processing failed: {e}")


async def cmd_youtube(args):
    """Post-process existing tweets for YouTube videos"""
    print("📺 Post-Processing Tweets for YouTube Videos")

    try:
        # Validate configuration
        if not config.validate_and_warn():
            print("⚠️ Configuration issues detected")

        # Override config settings based on arguments
        if hasattr(args, "embeddings"):
            config.set("youtube.enable_embeddings", args.embeddings)
        if hasattr(args, "transcripts"):
            config.set("youtube.enable_transcripts", args.transcripts)

        # Load existing tweets from cache
        tweets, url_mappings = load_cached_data(limit=args.limit, verbose=False)

        if not tweets:
            print("❌ No cached tweets found. Run 'python thoth.py x-api-sync' first.")
            return

        print(f"📚 Loaded {len(tweets)} tweets from cache")
        print(
            f"🎯 Processing {'all tweets' if not args.limit else f'first {args.limit} tweets'} for YouTube videos"
        )
        print(
            f"📺 Embeddings: {'enabled' if config.get('youtube.enable_embeddings', True) else 'disabled'}"
        )
        print(
            f"📄 Transcripts: {'enabled' if config.get('youtube.enable_transcripts', True) else 'disabled'}"
        )
        print(f"📂 Transcripts will be saved to: knowledge_vault/transcripts/")

        # Measure processing time
        import time

        start_time = time.time()

        # Initialize YouTube processor
        youtube_processor = YouTubeProcessor()

        # Count tweets with YouTube URLs (check both text and URL mappings)
        youtube_tweets = []
        for tweet in tweets:
            youtube_urls = []

            # Check tweet text
            if tweet.full_text:
                youtube_urls.extend(
                    youtube_processor.extract_youtube_urls(tweet.full_text)
                )

            # Check URL mappings for expanded YouTube URLs
            if hasattr(tweet, "url_mappings") and tweet.url_mappings:
                for mapping in tweet.url_mappings:
                    expanded_url = mapping.expanded_url.lower()
                    if "youtube.com" in expanded_url or "youtu.be" in expanded_url:
                        youtube_urls.append(mapping.expanded_url)

            youtube_urls = list(set(youtube_urls))  # Remove duplicates
            if youtube_urls:
                youtube_tweets.append((tweet, youtube_urls))

        print(f"🎥 Found {len(youtube_tweets)} tweets with YouTube videos")

        if not youtube_tweets:
            print("✅ No YouTube videos found in tweets")
            return

        # Process YouTube videos
        total_processed = 0
        total_videos = 0
        total_errors = 0

        for tweet, youtube_urls in youtube_tweets:
            try:
                stats = await youtube_processor.process_youtube_urls(
                    youtube_urls,
                    resume_metadata=args.resume,
                    resume_transcripts=args.resume,
                    source_label=f"tweet {tweet.id} by @{tweet.screen_name}",
                )
                total_processed += 1
                total_videos += stats.updated
                total_errors += stats.errors

                # Attach video info to tweet for potential markdown regeneration
                if stats.updated > 0:
                    tweet.youtube_videos = (
                        stats.extras.get("videos", []) if stats.extras else []
                    )

                print(
                    f"✅ Processed tweet {tweet.id}: {stats.updated} videos, {stats.errors} errors"
                )

            except Exception as e:
                total_errors += 1
                print(f"❌ Error processing tweet {tweet.id}: {e}")
                logger.error(f"YouTube processing failed for tweet {tweet.id}: {e}")

        end_time = time.time()
        elapsed = end_time - start_time

        # Print results
        print(f"\n📊 YouTube Post-Processing Results:")
        print(f"⏱️ Total time: {elapsed:.2f}s")
        print(f"🐦 Tweets processed: {total_processed}")
        print(f"📺 Videos processed: {total_videos}")
        print(f"❌ Errors: {total_errors}")

        if total_processed > 0:
            print(f"📈 Throughput: {total_processed/elapsed:.2f} tweets/second")

        print(f"\n📁 Files created:")
        print(f"   📄 Video transcripts: knowledge_vault/transcripts/youtube_*.md")

        if config.get("youtube.enable_embeddings", True):
            print("\n💡 To regenerate tweet markdown files with YouTube embeds, run:")
            print("   python thoth.py process --use-cache --no-resume")

    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"YouTube post-processing failed: {e}")


async def cmd_update_videos(args):
    """Update existing tweets and threads with video content"""
    print("🎬 Updating Videos in Existing Content")

    try:
        # Create video updater
        updater = VideoUpdater()

        # Show current statistics
        print("📊 Current video statistics:")
        stats = updater.get_video_statistics()
        print(f"   📁 Cache files: {stats['total_cache_files']}")
        print(
            f"   🎥 Estimated tweets with videos: {stats['total_tweets_with_videos']}"
        )
        print(
            f"   📹 Video files: {stats['total_video_files']} ({stats['video_files_size_mb']:.1f} MB)"
        )
        print(
            f"   🖼️ Thumbnail files: {stats['total_thumbnail_files']} ({stats['thumbnail_files_size_mb']:.1f} MB)"
        )

        if stats["total_tweets_with_videos"] == 0:
            print("✅ No tweets with videos found in cache")
            return

        # Determine what to update
        update_tweets = getattr(args, "tweets", True)
        update_threads = getattr(args, "threads", True)

        print(f"\n🎯 Update scope:")
        print(f"   📝 Tweets: {'enabled' if update_tweets else 'disabled'}")
        print(f"   🧵 Threads: {'enabled' if update_threads else 'disabled'}")
        print(f"   🔄 Resume: {'enabled' if args.resume else 'disabled'}")

        # Measure processing time
        import time

        start_time = time.time()

        # Run updates
        if update_tweets and update_threads:
            print(f"\n🔄 Updating all content...")
            update_stats = updater.update_all_videos(resume=args.resume)
        elif update_tweets:
            print(f"\n📝 Updating tweets only...")
            update_stats = updater.update_videos_in_tweets(resume=args.resume)
        elif update_threads:
            print(f"\n🧵 Updating threads only...")
            update_stats = updater.update_videos_in_threads(resume=args.resume)
        else:
            print("❌ No update scope selected")
            return

        end_time = time.time()
        elapsed = end_time - start_time

        # Show results
        print(f"\n📊 Video Update Results:")
        print(f"⏱️ Total time: {elapsed:.2f}s")
        print(f"📝 Files updated: {update_stats.created}")
        print(f"📹 Videos downloaded: {update_stats.updated}")
        print(f"⏭️ Skipped: {update_stats.skipped}")
        print(f"❌ Errors: {update_stats.errors}")

        if update_stats.created > 0:
            print(f"📈 Throughput: {update_stats.created/elapsed:.2f} files/second")

        # Show updated statistics
        print(f"\n📊 Updated video statistics:")
        new_stats = updater.get_video_statistics()
        print(
            f"   📹 Video files: {new_stats['total_video_files']} ({new_stats['video_files_size_mb']:.1f} MB)"
        )
        print(
            f"   🖼️ Thumbnail files: {new_stats['total_thumbnail_files']} ({new_stats['thumbnail_files_size_mb']:.1f} MB)"
        )

        print(f"\n✅ Video update complete!")
        print(
            f"💡 Videos now display as clickable thumbnails that open the video files"
        )

    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"Video update failed: {e}")


async def cmd_twitter_transcripts(args):
    """Process Twitter videos for transcripts using local Whisper"""
    print("🎤 Processing Twitter Video Transcripts")

    try:
        # Validate configuration
        if not config.validate_and_warn():
            print("⚠️ Configuration issues detected")

        # Load existing tweets from cache
        tweets, url_mappings = load_cached_data(limit=args.limit, verbose=False)

        if not tweets:
            print("❌ No cached tweets found. Run 'python thoth.py x-api-sync' first.")
            return

        print(f"📚 Loaded {len(tweets)} tweets from cache")
        print(
            f"🎯 Processing {'all tweets' if not args.limit else f'first {args.limit} tweets'} for video transcripts"
        )

        # Check if Whisper is enabled
        if not config.get("whisper.enabled", True) and not config.get(
            "deepgram.enabled", False
        ):
            print("❌ Whisper processing is disabled in config")
            return

        print(
            f"🎤 Whisper server: {config.get('whisper.base_url', 'http://localhost:11434/v1')}"
        )
        print(
            f"🎤 Model: {config.get('whisper.model', 'Systran/faster-distil-whisper-large-v3')}"
        )
        print(f"🎤 Min duration: {config.get('whisper.min_duration_seconds', 60)}s")
        print(f"📂 Transcripts will be saved to: knowledge_vault/transcripts/")

        # Measure processing time
        import time

        start_time = time.time()

        # Initialize transcript processor
        transcript_processor = TranscriptionProcessor()

        if not transcript_processor.is_enabled():
            print("❌ Twitter video transcript processor is not properly configured")
            print("   Check that Whisper server is running and ffmpeg is installed")
            return

        # Process tweets for video transcripts
        stats = await transcript_processor.process_tweets(tweets, resume=args.resume)

        # Calculate processing time
        elapsed_time = time.time() - start_time

        print(f"\n✅ Twitter video transcript processing complete!")
        print(f"   📊 Processed: {stats.total_processed} tweets")
        print(f"   🎤 Created: {stats.updated} transcripts")
        print(f"   ⏭️ Skipped: {stats.skipped} tweets")
        print(f"   ❌ Errors: {stats.errors} tweets")
        print(f"   ⏱️ Time: {elapsed_time:.1f}s")

        if stats.updated > 0:
            print(f"   📄 Video transcripts: knowledge_vault/transcripts/*.md")

    except Exception as e:
        logger.error(f"Twitter video transcript processing failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()


async def cmd_process(args):
    """Process tweets via the unified pipeline (legacy alias)."""
    print("🎯 Starting Markdown Processing (pipeline mode)")
    await cmd_process_pipeline(_ensure_pipeline_defaults(args))


def delete_tweet_artifacts(tweet_id: str, dry_run: bool = False) -> Dict[str, Any]:
    """Delete all artifacts associated with a tweet
    
    Args:
        tweet_id: The tweet ID to delete
        dry_run: If True, only report what would be deleted without actually deleting
        
    Returns:
        Dictionary with deletion statistics
    """
    stats = {
        "tweet_files": [],
        "thread_files": [],
        "media_files": [],
        "transcript_files": [],
        "cache_files": [],
        "pdf_files": [],
        "repo_files": [],
        "database_entries": 0,
        "errors": []
    }
    
    try:
        path_layout = build_path_layout(config)
        vault_dir = path_layout.vault_root
        cache_dir = path_layout.cache_root

        # Find and delete tweet markdown files
        tweet_pattern = f"tweets_{tweet_id}_*.md"
        tweets_dir = vault_dir / "tweets"
        if tweets_dir.exists():
            for tweet_file in tweets_dir.glob(tweet_pattern):
                stats["tweet_files"].append(str(tweet_file))
                if not dry_run:
                    try:
                        tweet_file.unlink()
                        logger.info(f"Deleted tweet file: {tweet_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {tweet_file}: {e}")
        
        # Find and delete thread files that include this tweet
        thread_pattern = f"thread_{tweet_id}_*.md"
        threads_dir = vault_dir / "threads"
        if threads_dir.exists():
            for thread_file in threads_dir.glob(thread_pattern):
                stats["thread_files"].append(str(thread_file))
                if not dry_run:
                    try:
                        thread_file.unlink()
                        logger.info(f"Deleted thread file: {thread_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {thread_file}: {e}")
        
        # Find and delete media files (images, videos) from both directories
        media_pattern = f"{tweet_id}_media_*"
        
        # Check images directory (handle absolute vs relative paths)
        images_path = config.get("paths.images_dir", "images")
        if Path(images_path).is_absolute():
            images_dir = Path(images_path)
        else:
            images_dir = vault_dir / images_path
        if images_dir.exists():
            for media_file in images_dir.glob(media_pattern):
                stats["media_files"].append(str(media_file))
                if not dry_run:
                    try:
                        media_file.unlink()
                        logger.info(f"Deleted image file: {media_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {media_file}: {e}")
        
        # Check videos directory (handle absolute vs relative paths)
        videos_path = config.get("paths.videos_dir", "videos")
        if Path(videos_path).is_absolute():
            videos_dir = Path(videos_path)
        else:
            videos_dir = vault_dir / videos_path
        if videos_dir.exists():
            for media_file in videos_dir.glob(media_pattern):
                stats["media_files"].append(str(media_file))
                if not dry_run:
                    try:
                        media_file.unlink()
                        logger.info(f"Deleted video file: {media_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {media_file}: {e}")
        
        # Also check legacy media directory for backward compatibility
        legacy_media_dir = vault_dir / "media"
        if legacy_media_dir.exists():
            for media_file in legacy_media_dir.glob(media_pattern):
                stats["media_files"].append(str(media_file))
                if not dry_run:
                    try:
                        media_file.unlink()
                        logger.info(f"Deleted legacy media file: {media_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {media_file}: {e}")
        
        # Find and delete transcript files
        transcript_pattern = f"*_{tweet_id}_*.md"
        transcripts_dir = vault_dir / "transcripts"
        if transcripts_dir.exists():
            for transcript_file in transcripts_dir.glob(transcript_pattern):
                stats["transcript_files"].append(str(transcript_file))
                if not dry_run:
                    try:
                        transcript_file.unlink()
                        logger.info(f"Deleted transcript file: {transcript_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {transcript_file}: {e}")
        
        # Find and delete GraphQL cache files
        cache_pattern = f"tweet_{tweet_id}_*.json"
        if cache_dir.exists():
            for cache_file in cache_dir.glob(cache_pattern):
                stats["cache_files"].append(str(cache_file))
                if not dry_run:
                    try:
                        cache_file.unlink()
                        logger.info(f"Deleted cache file: {cache_file}")
                    except Exception as e:
                        stats["errors"].append(f"Failed to delete {cache_file}: {e}")
        
        # Delete from database if enabled
        if config.get("database.enabled", False) and not dry_run:
            try:
                from core.metadata_db import get_metadata_db
                db = get_metadata_db()
                
                # Delete tweet metadata
                db.delete_tweet(tweet_id)
                
                # Delete from bookmark queue
                db.delete_bookmark_entry(tweet_id)
                
                # Delete associated downloads
                db.delete_downloads_for_context(f"tweet_{tweet_id}")
                
                # Delete LLM cache entries
                db.delete_llm_cache_for_context(tweet_id)
                
                stats["database_entries"] = 1
                logger.info(f"Deleted database entries for tweet {tweet_id}")
                
            except Exception as e:
                stats["errors"].append(f"Database deletion error: {e}")
        
        # Delete from realtime bookmarks file
        if not dry_run:
            try:
                realtime_file = path_layout.realtime_bookmarks_file
                    
                if realtime_file.exists():
                    with open(realtime_file, "r", encoding="utf-8") as f:
                        bookmarks = json.load(f)
                    
                    original_count = len(bookmarks)
                    bookmarks = [b for b in bookmarks if b.get("tweet_id") != tweet_id]
                    
                    if len(bookmarks) < original_count:
                        with open(realtime_file, "w", encoding="utf-8") as f:
                            json.dump(bookmarks, f, indent=2, ensure_ascii=False)
                        logger.info(f"Removed tweet {tweet_id} from realtime bookmarks")
                        
            except Exception as e:
                stats["errors"].append(f"Failed to update realtime bookmarks: {e}")
        
        return stats
        
    except Exception as e:
        stats["errors"].append(f"General error: {e}")
        return stats


async def cmd_delete(args):
    """Delete a tweet and all its associated artifacts"""
    tweet_id = args.tweet_id
    dry_run = args.dry_run
    
    print(f"🗑️ {'DRY RUN: ' if dry_run else ''}Deleting tweet {tweet_id}")
    
    stats = delete_tweet_artifacts(tweet_id, dry_run)
    
    # Print results
    total_files = (
        len(stats["tweet_files"]) + 
        len(stats["thread_files"]) + 
        len(stats["media_files"]) + 
        len(stats["transcript_files"]) +
        len(stats["cache_files"]) +
        len(stats["pdf_files"]) +
        len(stats["repo_files"])
    )
    
    if dry_run:
        print(f"\n📊 Would delete {total_files} files:")
    else:
        print(f"\n📊 Deleted {total_files} files:")
    
    if stats["tweet_files"]:
        print(f"   📄 Tweet files: {len(stats['tweet_files'])}")
    if stats["thread_files"]:
        print(f"   🧵 Thread files: {len(stats['thread_files'])}")
    if stats["media_files"]:
        print(f"   🖼️ Media files: {len(stats['media_files'])}")
    if stats["transcript_files"]:
        print(f"   📝 Transcript files: {len(stats['transcript_files'])}")
    if stats["cache_files"]:
        print(f"   💾 Cache files: {len(stats['cache_files'])}")
    if stats["database_entries"] and not dry_run:
        print(f"   🗄️ Database entries: {stats['database_entries']}")
    
    if args.verbose and total_files > 0:
        print("\nDeleted files:")
        for category, files in [
            ("Tweet", stats["tweet_files"]),
            ("Thread", stats["thread_files"]),
            ("Media", stats["media_files"]),
            ("Transcript", stats["transcript_files"]),
            ("Cache", stats["cache_files"])
        ]:
            for file in files[:5]:  # Show first 5 of each type
                print(f"   {category}: {Path(file).name}")
            if len(files) > 5:
                print(f"   ... and {len(files) - 5} more {category.lower()} files")
    
    if stats["errors"]:
        print(f"\n❌ Errors ({len(stats['errors'])}):")
        for error in stats["errors"][:5]:
            print(f"   {error}")
        if len(stats["errors"]) > 5:
            print(f"   ... and {len(stats['errors']) - 5} more errors")
    
    if dry_run:
        print("\n💡 Run without --dry-run to actually delete these files")


def cmd_stats(args):
    """Show statistics about cached data and processed files"""
    print("📊 Thoth Statistics")

    db = None
    if config.get("database.enabled", False):
        try:
            from core.metadata_db import get_metadata_db

            db = get_metadata_db()
        except Exception as exc:
            db = None
            print(f"⚠️  Metadata DB unavailable: {exc}")

    # GraphQL cache stats
    cache_dir = build_path_layout(config).cache_root
    if cache_dir.exists():
        cache_files = len(list(cache_dir.glob("tweet_*.json")))
        print(f"📡 GraphQL Cache: {cache_files} responses cached")

    # Knowledge vault & media stats (filesystem + DB when available)
    vault_dir = build_path_layout(config).vault_root
    if vault_dir.exists():
        tweets_dir = vault_dir / "tweets"
        threads_dir = vault_dir / "threads"
        tweet_files = len(list(tweets_dir.glob("*.md"))) if tweets_dir.exists() else 0
        thread_files = (
            len(list(threads_dir.glob("*.md"))) if threads_dir.exists() else 0
        )
        print(f"📚 Knowledge Vault (filesystem):")
        print(f"   📄 Tweet files: {tweet_files}")
        print(f"   🧵 Thread files: {thread_files}")

    if db:
        file_stats = db.get_file_stats()
        if file_stats:
            print(f"📚 Knowledge Vault (DB index):")
            print(f"   Total indexed files: {file_stats.get('total_files', 0):,}")
            print(f"   Total size: {file_stats.get('total_size_mb', 0)} MB")
            by_type = file_stats.get("by_type", {})
            if by_type:
                print("   By type:")
                for file_type, stats in by_type.items():
                    size_mb = round(
                        (stats.get("total_size_bytes", 0)) / (1024 * 1024), 2
                    )
                    print(
                        f"     {file_type:12} {stats.get('count', 0):,} files ({size_mb} MB)"
                    )

    # Check both new media directories and legacy
    vault_dir = Path(config.get("vault_dir", "knowledge_vault"))
    
    # Handle absolute vs relative paths for images
    images_path = config.get("images_dir", "images")
    if Path(images_path).is_absolute():
        images_dir = Path(images_path)
    else:
        images_dir = vault_dir / images_path
    
    # Handle absolute vs relative paths for videos
    videos_path = config.get("videos_dir", "videos")
    if Path(videos_path).is_absolute():
        videos_dir = Path(videos_path)
    else:
        videos_dir = vault_dir / videos_path
    
    # Legacy media is always relative to vault
    legacy_media_dir = vault_dir / config.get("media_dir", "media")
    
    image_files = len(list(images_dir.glob("*"))) if images_dir.exists() else 0
    video_files = len(list(videos_dir.glob("*"))) if videos_dir.exists() else 0
    legacy_media_files = len(list(legacy_media_dir.glob("*"))) if legacy_media_dir.exists() else 0
    
    total_media_files = image_files + video_files + legacy_media_files
    print(f"🖼️ Media Files: {total_media_files} total")
    if image_files > 0:
        print(f"   📸 Images: {image_files}")
    if video_files > 0:
        print(f"   🎬 Videos: {video_files}")
    if legacy_media_files > 0:
        print(f"   📁 Legacy media: {legacy_media_files}")

    if db:
        download_summary = db.get_download_summary()
        if download_summary and download_summary.get("total_entries"):
            print(f"📥 Downloads (DB):")
            print(
                f"   Total entries: {download_summary['total_entries']:,} ({download_summary['total_mb']} MB)"
            )
            for status, stats in download_summary.get("by_status", {}).items():
                print(f"   {status:>10}: {stats['count']:,} ({stats['total_mb']} MB)")
    else:
        download_tracker = get_download_tracker()
        download_stats = download_tracker.get_stats()
        if download_stats["total_tracked"] > 0:
            print(f"📥 Download Tracking:")
            print(f"   ✅ Successful: {download_stats['successful']}")
            print(f"   🚫 404 errors: {download_stats['404_errors']}")
            print(f"   ❌ Other errors: {download_stats['other_errors']}")
            print(f"   ⏳ Pending: {download_stats['pending']}")
            print(f"   📊 Total tracked: {download_stats['total_tracked']}")

    # Bookmarks file stats
    bookmarks_file = Path(args.bookmarks)
    if bookmarks_file.exists():
        try:
            with open(bookmarks_file, "r") as f:
                bookmarks = json.load(f)
            print(f"📊 Source Data: {len(bookmarks)} bookmarks in {args.bookmarks}")
        except:
            print(f"❌ Could not read {args.bookmarks}")

    if db:
        queue_counts = db.get_bookmark_queue_counts()
        if queue_counts:
            print(f"🗂️  Bookmark Queue:")
            print(f"   Pending: {queue_counts.get('pending', 0)}")
            print(f"   Processing: {queue_counts.get('processing', 0)}")
            print(f"   Processed: {queue_counts.get('processed', 0)}")
            print(f"   Failed: {queue_counts.get('failed', 0)}")

        llm_summary = db.get_llm_cache_stats()
        if llm_summary and llm_summary.get("total_entries"):
            print(f"🤖 LLM Cache:")
            print(f"   Total entries: {llm_summary['total_entries']:,}")
            if llm_summary.get("by_task"):
                print(f"   By task:")
                for task, count in llm_summary["by_task"].items():
                    print(f"     {task:15} {count:,}")
            if llm_summary.get("by_provider"):
                print(f"   By provider:")
                for provider, count in llm_summary["by_provider"].items():
                    print(f"     {provider:20} {count:,}")

        chunk_stats = db.get_transcript_chunk_stats()
        if chunk_stats and chunk_stats.get("total_contexts"):
            print(f"🎬 Transcript Chunk Cache:")
            print(f"   Contexts tracked: {chunk_stats['total_contexts']:,}")
            print(
                f"   Contexts with failures: {chunk_stats['contexts_with_failures']:,}"
            )
            print(
                f"   Contexts with fallback: {chunk_stats['contexts_with_fallback']:,}"
            )
            print(f"   Failed chunks: {chunk_stats['total_failed_chunks']:,}")
            details = chunk_stats.get("context_details", [])
            if details:
                print(f"   Recent failures:")
                for detail in details[:5]:
                    print(
                        f"     {detail['context_id']} -> processed {detail['chunks_processed']}/"
                        f"{detail['chunks_total']} chunks, failures: {detail['failed_count']}"
                        f" (fallback: {'Yes' if detail['fallback'] else 'No'})"
                    )


async def cmd_database(args):
    """Database management commands"""
    from core.metadata_db import get_metadata_db
    import json
    from datetime import datetime

    db = get_metadata_db()

    if args.db_action == "stats":
        print("📊 Database Statistics")
        print("=" * 50)

        stats = db.get_db_stats()
        print(f"Database: {stats['db_path']}")
        print(f"Size: {stats['db_size_mb']} MB ({stats['db_size_bytes']:,} bytes)")
        print(f"Total Records: {stats['total_records']:,}")

        print("\nTable Counts:")
        for table, count in stats["table_counts"].items():
            print(f"   {table:20} {count:,}")

        # File statistics
        file_stats = db.get_file_stats()
        if file_stats:
            print(f"\nFile Index Statistics:")
            print(f"   Total Files: {file_stats['total_files']:,}")
            print(f"   Total Size: {file_stats['total_size_mb']} MB")

            if file_stats["by_type"]:
                print("   By Type:")
                for file_type, type_stats in file_stats["by_type"].items():
                    size_mb = round(type_stats["total_size_bytes"] / (1024 * 1024), 2)
                    print(
                        f"     {file_type:12} {type_stats['count']:,} files ({size_mb} MB)"
                    )

    elif args.db_action == "vacuum":
        print("🧹 Vacuuming database...")
        success = db.vacuum()
        if success:
            print("✅ Database vacuumed successfully")
        else:
            print("❌ Database vacuum failed")

    elif args.db_action == "export":
        print("📤 Exporting database...")
        output_file = (
            args.output
            or f"thoth_db_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        # Export all data
        export_data = {
            "exported_at": datetime.now().isoformat(),
            "stats": db.get_db_stats(),
            "file_stats": db.get_file_stats(),
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Database exported to: {output_file}")

    else:
        print("❌ Unknown database action. Use: stats, vacuum, or export")


async def cmd_migrate_frontmatter(args):
    """Migrate existing markdown files to enhanced frontmatter format for Dataview"""
    import re
    import yaml
    import json
    import math
    from pathlib import Path
    from core.config import config

    path_layout = build_path_layout(config)
    vault_dir = path_layout.vault_root
    tweets_dir = vault_dir / "tweets"
    threads_dir = vault_dir / "threads"
    cache_dir = path_layout.cache_root

    dry_run = args.dry_run
    updated = 0
    skipped = 0
    errors = 0
    enhanced_count = 0

    # Build a cache of tweet_id -> graphql file
    graphql_cache = {}
    if cache_dir.exists():
        for cache_file in cache_dir.glob("tweet_*.json"):
            # Extract tweet_id from filename like tweet_1234567890_timestamp.json
            parts = cache_file.stem.split('_')
            if len(parts) >= 2:
                tweet_id = parts[1]
                # Keep the most recent cache file for each tweet
                if tweet_id not in graphql_cache:
                    graphql_cache[tweet_id] = cache_file
        print(f"Found {len(graphql_cache)} GraphQL cache files")

    def find_legacy_in_graphql(obj, depth=0):
        """Recursively find the legacy object with engagement metrics"""
        if depth > 15:
            return None
        if isinstance(obj, dict):
            if 'legacy' in obj and isinstance(obj.get('legacy'), dict):
                legacy = obj['legacy']
                if 'favorite_count' in legacy:
                    return legacy
            for v in obj.values():
                r = find_legacy_in_graphql(v, depth + 1)
                if r:
                    return r
        elif isinstance(obj, list):
            for item in obj:
                r = find_legacy_in_graphql(item, depth + 1)
                if r:
                    return r
        return None

    def get_engagement_from_cache(tweet_id: str) -> dict:
        """Get engagement metrics from GraphQL cache"""
        nonlocal enhanced_count
        if tweet_id not in graphql_cache:
            return {'likes': 0, 'retweets': 0, 'replies': 0}

        try:
            with open(graphql_cache[tweet_id], 'r', encoding='utf-8') as f:
                data = json.load(f)
            legacy = find_legacy_in_graphql(data)
            if legacy:
                enhanced_count += 1
                return {
                    'likes': legacy.get('favorite_count', 0) or 0,
                    'retweets': legacy.get('retweet_count', 0) or 0,
                    'replies': legacy.get('reply_count', 0) or 0,
                }
        except:
            pass
        return {'likes': 0, 'retweets': 0, 'replies': 0}

    def calculate_importance(likes, retweets, replies, has_paper, has_repo, has_youtube, is_thread, word_count):
        """Calculate importance score based on engagement and content"""
        score = 0.0
        if likes > 0:
            score += min(math.log10(likes + 1) * 10, 25)
        if retweets > 0:
            score += min(math.log10(retweets + 1) * 8, 15)
        if replies > 0:
            score += min(math.log10(replies + 1) * 5, 10)
        if has_paper: score += 15
        if has_repo: score += 10
        if has_youtube: score += 8
        if is_thread: score += 5
        if word_count > 200: score += 5
        return min(int(score), 100)

    def extract_tags_from_body(content: str) -> list:
        """Extract tags from ## Tags section"""
        lines = content.split('\n')
        in_tags = False
        tags = []
        for line in lines:
            if line.strip().startswith('## Tags'):
                in_tags = True
                continue
            if in_tags:
                if line.strip().startswith('## ') or line.strip().startswith('# '):
                    break
                found = re.findall(r'#([\w-]+)', line)
                tags.extend(found)
        return tags

    def migrate_file(filepath: Path) -> bool:
        nonlocal updated, skipped, errors

        try:
            content = filepath.read_text(encoding='utf-8')

            if not content.startswith('---'):
                skipped += 1
                return False

            end_idx = content.find('---', 3)
            if end_idx == -1:
                skipped += 1
                return False

            yaml_str = content[3:end_idx].strip()
            body = content[end_idx + 3:].strip()

            try:
                fm = yaml.safe_load(yaml_str) or {}
            except:
                errors += 1
                return False

            # Check if already migrated
            if 'importance' in fm and 'status' in fm:
                skipped += 1
                return False

            # Extract tags from body
            tags = extract_tags_from_body(body)
            author = fm.get('author', '')
            tags = [t for t in tags if t.lower() != author.lower()]

            # Detect content types from body
            has_paper = '## ArXiv' in body or 'arxiv.org' in body.lower()
            has_repo = '## Repository' in body or 'github.com' in body.lower() or 'huggingface.co' in body.lower()
            has_youtube = '## YouTube' in body or 'youtube.com' in body.lower()
            has_video = '![[' in body and '.mp4' in body
            has_images = '![[' in body and any(ext in body for ext in ['.jpg', '.png', '.gif'])
            has_pdf = '## PDF' in body or '.pdf]]' in body

            word_count = len(body.split())

            # Get engagement from GraphQL cache
            tweet_id = fm.get('id', '')
            engagement = get_engagement_from_cache(tweet_id)

            # Build new frontmatter
            new_fm = {
                'type': fm.get('type', 'tweet'),
                'id': tweet_id,
                'author': fm.get('author', ''),
                'created': fm.get('created_at', fm.get('created', '')),
                'processed': fm.get('processed_at', fm.get('processed', '')),
                'url': fm.get('url', ''),
                'likes': engagement['likes'],
                'retweets': engagement['retweets'],
                'replies': engagement['replies'],
                'has_paper': has_paper,
                'has_repo': has_repo,
                'has_video': has_video,
                'has_images': has_images,
                'has_youtube': has_youtube,
                'has_pdf': has_pdf,
                'is_thread': fm.get('type') == 'thread',
                'word_count': word_count,
                'status': 'unread',
                'enhanced': fm.get('enhanced', False),
            }

            if fm.get('thread_id'):
                new_fm['thread_id'] = fm['thread_id']
            if fm.get('tweet_count'):
                new_fm['tweet_count'] = fm['tweet_count']

            if tags:
                new_fm['tags'] = tags[:10]

            # Calculate importance with real engagement data
            new_fm['importance'] = calculate_importance(
                engagement['likes'], engagement['retweets'], engagement['replies'],
                has_paper, has_repo, has_youtube,
                new_fm.get('is_thread', False), word_count
            )

            # Generate YAML
            lines = ['---']
            for key, value in new_fm.items():
                if value is None:
                    continue
                elif isinstance(value, list):
                    if value:
                        formatted = [f'"{v}"' for v in value]
                        lines.append(f"{key}: [{', '.join(formatted)}]")
                elif isinstance(value, bool):
                    lines.append(f"{key}: {str(value).lower()}")
                elif isinstance(value, str):
                    escaped = value.replace('"', '\\"')
                    lines.append(f'{key}: "{escaped}"')
                else:
                    lines.append(f"{key}: {value}")
            lines.append('---')

            new_content = '\n'.join(lines) + '\n\n' + body

            if not dry_run:
                filepath.write_text(new_content, encoding='utf-8')

            updated += 1
            return True

        except Exception as e:
            errors += 1
            return False

    print(f"{'[DRY RUN] ' if dry_run else ''}Migrating frontmatter for Dataview...")

    if tweets_dir.exists():
        tweet_files = list(tweets_dir.glob("*.md"))
        print(f"\nProcessing {len(tweet_files)} tweets...")
        for i, f in enumerate(tweet_files):
            if i % 200 == 0 and i > 0:
                print(f"  {i}/{len(tweet_files)}...")
            migrate_file(f)

    if threads_dir.exists():
        thread_files = list(threads_dir.glob("*.md"))
        print(f"\nProcessing {len(thread_files)} threads...")
        for i, f in enumerate(thread_files):
            if i % 100 == 0 and i > 0:
                print(f"  {i}/{len(thread_files)}...")
            migrate_file(f)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Migration complete:")
    print(f"  ✅ Updated: {updated}")
    print(f"  📊 With engagement data from GraphQL: {enhanced_count}")
    print(f"  ⏭️  Skipped (already migrated): {skipped}")
    print(f"  ❌ Errors: {errors}")

    if not dry_run and updated > 0:
        print(f"\n💡 Run 'python thoth.py digest' to regenerate digests with new data")


async def cmd_digest(args):
    """Generate digest notes for content discovery"""
    from processors.digest_generator import DigestGenerator, send_ntfy_notification
    from datetime import datetime, timedelta

    generator = DigestGenerator()
    generated_files = []

    if args.digest_type == "weekly":
        print("📰 Generating Weekly Digest")

        # Calculate week range
        if args.week:
            # Parse week string like "2024-W52"
            try:
                year, week = args.week.split("-W")
                # Get first day of that week
                week_start = datetime.strptime(f"{year}-W{week}-1", "%Y-W%W-%w")
            except ValueError:
                print(f"❌ Invalid week format: {args.week}. Use YYYY-WNN (e.g., 2024-W52)")
                return
        else:
            # Current week
            today = datetime.now()
            week_start = today - timedelta(days=today.weekday())

        filepath = generator.generate_weekly_digest(week_start=week_start)
        generated_files.append(filepath)
        print(f"✅ Generated: {filepath}")

    elif args.digest_type == "inbox":
        print("📬 Generating Inbox View")
        filepath = generator.generate_inbox_view()
        generated_files.append(filepath)
        print(f"✅ Generated: {filepath}")

    elif args.digest_type == "dashboard":
        print("📊 Generating Discovery Dashboard")
        filepath = generator.generate_discovery_dashboard()
        generated_files.append(filepath)
        print(f"✅ Generated: {filepath}")

    elif args.digest_type == "all":
        print("📰 Generating All Digest Views")

        # Generate all types
        dashboard_path = generator.generate_discovery_dashboard()
        generated_files.append(dashboard_path)
        print(f"✅ Dashboard: {dashboard_path}")

        inbox_path = generator.generate_inbox_view()
        generated_files.append(inbox_path)
        print(f"✅ Inbox: {inbox_path}")

        weekly_path = generator.generate_weekly_digest()
        generated_files.append(weekly_path)
        print(f"✅ Weekly: {weekly_path}")

        print(f"\n📁 Digest files created in: {generator.digests_dir}")

    else:
        print(f"❌ Unknown digest type: {args.digest_type}")
        print("Available types: weekly, inbox, dashboard, all")
        return

    # Send notification if requested
    if getattr(args, "notify", False) and generated_files:
        file_names = [Path(f).name for f in generated_files]
        send_ntfy_notification(
            title="Thoth Digest Ready",
            message=f"Generated {len(generated_files)} digest files: {', '.join(file_names)}",
            topic=getattr(args, "ntfy_topic", "thoth")
        )
        print("📬 Notification sent")


async def cmd_arxiv(args):
    """Discover research papers from ArXiv."""
    from collectors.arxiv_collector import ArXivCollector
    
    collector = ArXivCollector()
    source = args.source or config.get("sources.arxiv.source", "api")
    feed_format = args.feed_format or config.get("sources.arxiv.feed_format", "rss")
    limit = args.limit or config.get("sources.arxiv.limit", 50)

    def normalize_items(raw_value):
        if isinstance(raw_value, list):
            return [str(item).strip() for item in raw_value if str(item).strip()]
        if isinstance(raw_value, str):
            return [item.strip() for item in raw_value.split(",") if item.strip()]
        return []

    if source == "rss":
        categories = normalize_items(args.categories) or normalize_items(
            config.get("sources.arxiv.categories", [])
        )
        if not categories:
            print("❌ RSS discovery requires --categories (example: cs.AI,cs.LG)")
            return

        print(
            f"📡 Scanning arXiv {feed_format.upper()} feed(s) for {len(categories)} categories..."
        )
        discovered = collector.scan_rss_feeds(
            categories,
            max_results=limit,
            feed_format=feed_format,
        )
        print(f"✅ Discovered {len(discovered)} new papers from arXiv feeds and added to ingestion queue.")
        return

    topics = normalize_items(args.topics) or normalize_items(
        config.get("sources.arxiv.topics", [])
    )
    if not topics:
        print("❌ API discovery requires --topics or sources.arxiv.topics in control.json")
        return

    print(f"🔍 Searching ArXiv for {len(topics)} topics...")
    discovered = collector.discover_papers(topics, max_results=limit)
    print(f"✅ Discovered {len(discovered)} new papers and added to ingestion queue.")


async def cmd_social(args):
    """Sync GitHub stars and HuggingFace likes."""
    from collectors.social_collector import SocialCollector
    
    collector = SocialCollector()
    github_user = args.github_user if args.github_user is not None else config.get("sources.github.username")
    hf_user = args.hf_user if args.hf_user is not None else config.get("sources.huggingface.username")
    limit = args.limit or config.get("sources.github.limit") or config.get("sources.huggingface.limit") or 50
    ran_any = False
    
    if config.get("sources.github.enabled", True):
        target = github_user or "authenticated account"
        print(f"⭐ Syncing GitHub stars for {target}...")
        gh_discovered = collector.discover_github_stars(github_user, limit=limit)
        print(f"✅ Added {len(gh_discovered)} new GitHub repositories to queue.")
        ran_any = True

    if config.get("sources.huggingface.enabled", True) and (hf_user or os.getenv("HF_USER")):
        print(f"🤗 Syncing HuggingFace likes for {hf_user or os.getenv('HF_USER')}...")
        hf_discovered = collector.discover_hf_likes(hf_user, limit=limit)
        print(f"✅ Added {len(hf_discovered)} new HuggingFace items to queue.")
        ran_any = True

    if not ran_any:
        print("❌ No social sources configured. Set sources.github / sources.huggingface in control.json or pass CLI flags.")


async def cmd_web_clipper(args):
    """Index explicit Web Clipper source directories."""
    from collectors.web_clipper_collector import WebClipperCollector
    from core.ingestion_runtime import get_knowledge_artifact_runtime
    from core.metadata_db import get_metadata_db

    if config.get("sources.web_clipper.enabled", True) is False:
        print("❌ Web Clipper collection is disabled in config")
        return

    layout = build_path_layout(config)
    db = get_metadata_db()
    collector = WebClipperCollector(
        config,
        layout=layout,
        db=db,
    )
    runtime = get_knowledge_artifact_runtime(
        config,
        layout=layout,
        db=db,
    )

    print("📎 Indexing configured Web Clipper source directories...")
    discovered = collector.collect()
    changed = sum(1 for record in discovered if record.is_new_or_changed)
    queued = sum(
        1
        for record in discovered
        if record.file_type == "note" and record.is_new_or_changed
    )
    staged = sum(
        1
        for record in discovered
        if record.file_type == "attachment" and record.is_new_or_changed
    )

    print(f"✅ Scanned {len(discovered)} files from {len(collector.contract.watch_dirs)} source directories.")
    print(f"   New or changed files: {changed}")
    print(f"   Notes queued for shared ingestion: {queued}")
    print(f"   Attachments staged: {staged}")
    translated = 0
    for record in discovered:
        if record.artifact is None or record.file_type != "note":
            continue
        result = await runtime.publish_english_companion(record.artifact)
        if result.status in {"created", "updated"}:
            translated += 1

    print(f"✅ Scanned {len(discovered)} files from {len(collector.contract.watch_dirs)} source directories.")
    print(f"   New or changed files: {changed}")
    print(f"   English companions: {translated}")
    print(f"   Notes queued for shared ingestion: {queued}")
    print(f"   Attachments staged: {staged}")
    print(f"   English companions: {translated}")


async def cmd_ingest_queue(args):
    """Process pending generalized ingestion queue entries."""
    from core.ingestion_runtime import get_knowledge_artifact_runtime
    from core.metadata_db import get_metadata_db

    layout = build_path_layout(config)
    runtime = get_knowledge_artifact_runtime(config, layout=layout, db=get_metadata_db())

    limit = getattr(args, "limit", None)
    print("📥 Processing ingestion queue...")
    results = await runtime.process_pending_ingestions_once(limit=limit)

    if not results:
        print("✅ No pending ingestion entries found")
        return

    processed = sum(1 for result in results if result.status == "processed")
    skipped = sum(1 for result in results if result.status == "skipped")
    print(f"✅ Processed {processed} entries, skipped {skipped}")
    for result in results:
        print(f"   {result.artifact_type}:{result.artifact_id} -> {result.status}")


async def cmd_archivist(args):
    """Compile archivist topic pages from configured source material."""
    topic_ids = None
    if getattr(args, "topics", None):
        topic_ids = [
            item.strip().lower()
            for item in str(args.topics).split(",")
            if item.strip()
        ]

    if bool(getattr(args, "benchmark", False)):
        print("🔎 Benchmarking archivist retrieval...")
        results = await benchmark_archivist_topics(
            config,
            project_root=Path.cwd(),
            topic_ids=topic_ids,
            limit=getattr(args, "limit", None),
        )
        if not results:
            print("✅ No archivist topics matched the requested scope")
            return

        for result in results:
            print(
                f"   - {result.topic_id}: mode={result.retrieval_mode} "
                f"candidates={result.candidate_count} indexed={result.indexed_count}"
            )
            if result.scanned_roots:
                print(f"     scanned: {', '.join(result.scanned_roots)}")
            if result.missing_roots:
                print(f"     missing: {', '.join(result.missing_roots)}")
            if result.source_type_counts:
                source_breakdown = ", ".join(
                    f"{source_type}={count}"
                    for source_type, count in sorted(result.source_type_counts.items())
                )
                print(f"     source_types: {source_breakdown}")
            if result.top_candidate_paths:
                for path in result.top_candidate_paths[: max(1, int(getattr(args, 'benchmark_top', 10) or 10))]:
                    print(f"     top: {path}")
        print(f"✅ Archivist benchmark complete: topics={len(results)}")
        return

    compiler = ArchivistCompiler(config, project_root=Path.cwd())

    print("📚 Running archivist topic compiler...")
    results = await compiler.run(
        topic_ids=topic_ids,
        force=bool(getattr(args, "force", False)),
        dry_run=bool(getattr(args, "dry_run", False)),
        limit=getattr(args, "limit", None),
    )
    if not results:
        print("✅ No archivist topics matched the requested scope")
        return

    for result in results:
        page = str(result.page_path) if result.page_path else "-"
        print(
            f"   - {result.topic_id}: {result.status} "
            f"(reason={result.reason}, sources={result.candidate_count}, page={page})"
        )

    compiled = sum(1 for result in results if result.status == "compiled")
    skipped = sum(1 for result in results if result.status == "skipped")
    dry_runs = sum(1 for result in results if result.status == "dry_run")
    print(
        f"✅ Archivist complete: compiled={compiled}, skipped={skipped}, dry_run={dry_runs}"
    )


async def cmd_wiki_query(args):
    """Search the compiled wiki and optionally write back a curated result."""
    layout = build_path_layout(config)
    runner = WikiQueryRunner(config, layout=layout)
    limit = max(1, int(getattr(args, "limit", 10) or 10))
    result = runner.search(args.query, limit=limit)

    print(f"🔎 Wiki query: {args.query}")
    print(f"✅ Matches: {len(result.hits)}")
    for hit in result.hits:
        matched_fields = ", ".join(hit.matched_fields) if hit.matched_fields else "none"
        print(f"   - {hit.title} [{hit.slug}] score={hit.score} fields={matched_fields}")

    if getattr(args, "write_back", False):
        selected_slugs = None
        if getattr(args, "selected_slugs", None):
            selected_slugs = [
                slug.strip()
                for slug in args.selected_slugs.split(",")
                if slug.strip()
            ]
        write_back = runner.curated_write_back(
            args.query,
            limit=limit,
            selected_slugs=selected_slugs,
            curated_notes=getattr(args, "curated_notes", None),
            curated_title=getattr(args, "title", None),
        )
        print(f"📝 Curated wiki page written: {write_back.page_path}")
        print(f"   Selected slugs: {', '.join(write_back.selected_slugs)}")


async def cmd_wiki_lint(args):
    """Run wiki health checks for contradictions, staleness, and orphans."""
    layout = build_path_layout(config)
    runner = WikiLintRunner(config, layout=layout)
    stale_after_days = int(getattr(args, "stale_after_days", 30) or 30)
    report = runner.lint(stale_after_days=stale_after_days)

    print("🧪 Wiki lint report")
    print(f"   Pages checked: {report.pages_checked}")
    print(f"   Issues found: {len(report.issues)}")
    for issue in report.issues:
        location = f" ({issue.page_path})" if issue.page_path else ""
        print(f"   - {issue.severity.upper()} {issue.code}{location}: {issue.message}")

    if report.has_errors:
        raise SystemExit(1)


async def cmd_x_api_sync(args):
    """Backfill bookmarks from the X API and process them immediately."""
    from thoth_api import run_x_api_bookmark_sync

    print("🔁 Backfilling bookmarks from X API...")
    result = await run_x_api_bookmark_sync(
        max_results=args.max_results,
        max_pages=args.max_pages,
        resume_from_checkpoint=not args.no_resume,
        process_immediately=True,
    )

    print(f"✅ User: {result['user_id']}")
    print(f"   Pages fetched: {result['pages_fetched']}")
    print(f"   Bookmarks emitted: {result['bookmarks_emitted']}")
    print(f"   Queued: {result['queued']}")
    print(f"   Processed immediately: {result['processed_immediately']}")
    if result.get("stopped_at_known_id"):
        print("   Stopped at the first already-seen bookmark")


async def cmd_migrate_filenames(args):
    """Filename migration command"""
    from core.filename_utils import get_filename_migrator

    migrator = get_filename_migrator()

    if args.analyze:
        print("🔍 Analyzing filename migration needs...")
        plan = migrator.create_migration_plan()

        print(f"\n📊 Migration Analysis:")
        print(f"   Total files to migrate: {plan['total_files']}")

        if plan["total_files"] == 0:
            print("   ✅ All filenames are already normalized")
            return

        for dir_type, dir_plan in plan["directories"].items():
            print(f"\n📁 {dir_type.title()} Directory:")
            print(f"   Files to migrate: {dir_plan['count']}")

            if hasattr(args, "verbose") and args.verbose:
                for old_name, new_name in list(dir_plan["migrations"].items())[:5]:
                    print(f"     {old_name} → {new_name}")
                if dir_plan["count"] > 5:
                    print(f"     ... and {dir_plan['count'] - 5} more")

        if plan["backlinks_to_update"]:
            print(
                f"\n🔗 Files with backlinks to update: {len(plan['backlinks_to_update'])}"
            )
            if hasattr(args, "verbose") and args.verbose:
                for ref_file in plan["backlinks_to_update"][:10]:
                    print(f"     {ref_file}")
                if len(plan["backlinks_to_update"]) > 10:
                    print(f"     ... and {len(plan['backlinks_to_update']) - 10} more")

    else:
        dry_run = args.dry_run
        print(f"🚀 {'DRY RUN: ' if dry_run else ''}Executing filename migration...")

        results = migrator.execute_migration(dry_run=dry_run)

        print(f"\n📊 Migration Results:")
        print(f"   Successful renames: {results['successful_renames']}")
        print(f"   Failed renames: {results['failed_renames']}")
        print(f"   Backlinks updated: {results['backlinks_updated']}")

        if results["errors"]:
            print(f"\n❌ Errors ({len(results['errors'])}):")
            for error in results["errors"][:5]:
                print(f"     {error}")
            if len(results["errors"]) > 5:
                print(f"     ... and {len(results['errors']) - 5} more")

        if dry_run and results["successful_renames"] > 0:
            print(f"\n💡 Run without --dry-run to execute the migration")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Thoth - Twitter Bookmark Knowledge Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s x-api-sync                  # Backfill bookmarks from the X API
  %(prog)s process --limit 100         # Process first 100 bookmarks to markdown
  %(prog)s update-videos               # Update existing content with video links
  %(prog)s web-clipper                 # Index configured Web Clipper source dirs
  %(prog)s stats                       # Show current statistics
        """,
    )

    # Global options
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--bookmarks",
        default=config.get("bookmarks_file"),
        help="Bookmarks JSON file (default: %(default)s)",
    )
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Process command
    process_parser = subparsers.add_parser("process", help="Process tweets to markdown")
    process_parser.add_argument(
        "--limit", type=int, help="Limit number of tweets to process"
    )
    process_parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached GraphQL data for processing",
    )
    process_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Don't skip already processed files",
    )
    process_parser.add_argument(
        "--no-skip-cached",
        dest="skip_cached",
        action="store_false",
        help="Don't skip tweets that already have GraphQL cache files",
    )
    process_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate pipeline actions using cached data (requires --use-cache)",
    )

    # Pipeline command (single-pass processing)
    pipeline_parser = subparsers.add_parser(
        "pipeline", help="Single-pass pipeline processing (optimized)"
    )
    pipeline_parser.add_argument(
        "--limit", type=int, help="Limit number of tweets to process"
    )
    pipeline_parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached GraphQL data for processing",
    )
    pipeline_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Re-run non-LLM stages even if outputs exist (LLM respects --rerun-llm)",
    )
    pipeline_parser.set_defaults(resume=True)
    pipeline_parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for concurrent processing (default: 10)",
    )
    pipeline_parser.add_argument(
        "--tweet-ids",
        type=str,
        help="Comma-separated tweet IDs to process (expands threads from cache)",
    )
    pipeline_parser.add_argument(
        "--rerun-llm",
        action="store_true",
        help="Force rerunning LLM enhancements regardless of resume mode",
    )
    pipeline_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate pipeline actions using cached data without writing files",
    )
    pipeline_parser.add_argument(
        "--llm-only",
        action="store_true",
        help="Skip downloads and rerun only LLM enhancement stages",
    )

    # Async test command (enhanced LLM processing)
    async_parser = subparsers.add_parser(
        "async-test", help="Test enhanced async LLM processing"
    )
    async_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Limit number of tweets to process (default: 5)",
    )
    async_parser.add_argument(
        "--concurrent", type=int, default=8, help="Max concurrent requests (default: 8)"
    )
    async_parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Request timeout in seconds (default: 20.0)",
    )

    # GitHub stars command
    stars_parser = subparsers.add_parser(
        "github-stars", help="Fetch and process GitHub starred repositories"
    )
    stars_parser.add_argument(
        "--limit", type=int, help="Limit number of repositories to process"
    )
    stars_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Don't skip already processed repositories",
    )

    # HuggingFace likes command
    hf_parser = subparsers.add_parser(
        "huggingface-likes", help="Fetch and process HuggingFace liked repositories"
    )
    hf_parser.add_argument(
        "--limit", type=int, help="Limit number of repositories to process"
    )
    hf_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Don't skip already processed repositories",
    )
    hf_parser.add_argument(
        "--no-models",
        dest="include_models",
        action="store_false",
        help="Don't include liked models",
    )
    hf_parser.add_argument(
        "--no-datasets",
        dest="include_datasets",
        action="store_false",
        help="Don't include liked datasets",
    )
    hf_parser.add_argument(
        "--no-spaces",
        dest="include_spaces",
        action="store_false",
        help="Don't include liked spaces",
    )

    # YouTube post-processing command
    youtube_parser = subparsers.add_parser(
        "youtube", help="Post-process existing tweets for YouTube videos"
    )
    youtube_parser.add_argument(
        "--limit", type=int, help="Limit number of tweets to process"
    )
    youtube_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Don't skip already processed YouTube videos",
    )
    youtube_parser.add_argument(
        "--no-embeddings",
        dest="embeddings",
        action="store_false",
        help="Don't generate video embeddings",
    )
    youtube_parser.add_argument(
        "--no-transcripts",
        dest="transcripts",
        action="store_false",
        help="Don't retrieve transcripts",
    )

    # Video update command
    video_parser = subparsers.add_parser(
        "update-videos", help="Update existing tweets/threads with video content"
    )
    video_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Don't skip already processed content",
    )
    video_parser.add_argument(
        "--no-tweets",
        dest="tweets",
        action="store_false",
        help="Don't update tweet files",
    )
    video_parser.add_argument(
        "--no-threads",
        dest="threads",
        action="store_false",
        help="Don't update thread files",
    )

    # Twitter transcripts command
    twitter_transcripts_parser = subparsers.add_parser(
        "twitter-transcripts",
        help="Process Twitter videos for transcripts using local Whisper",
    )
    twitter_transcripts_parser.add_argument(
        "--limit", type=int, help="Limit number of tweets to process"
    )
    twitter_transcripts_parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Don't skip already processed content",
    )
    twitter_transcripts_parser.add_argument(
        "--verbose", action="store_true", help="Verbose output"
    )

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics")

    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete a tweet and all its artifacts")
    delete_parser.add_argument("tweet_id", help="Tweet ID to delete")
    delete_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    # Database management command
    db_parser = subparsers.add_parser("db", help="Database management operations")
    db_subparsers = db_parser.add_subparsers(dest="db_action", help="Database actions")

    # Database stats
    db_stats_parser = db_subparsers.add_parser("stats", help="Show database statistics")

    # Database vacuum
    db_vacuum_parser = db_subparsers.add_parser(
        "vacuum", help="Vacuum database to reclaim space"
    )

    # Database export
    db_export_parser = db_subparsers.add_parser(
        "export", help="Export database to JSON"
    )
    db_export_parser.add_argument("--output", "-o", help="Output file path")

    # Filename migration command
    migrate_parser = subparsers.add_parser(
        "migrate-filenames", help="Migrate filenames to normalized format"
    )
    migrate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    migrate_parser.add_argument(
        "--analyze", action="store_true", help="Only analyze what needs migration"
    )

    # Frontmatter migration command
    migrate_fm_parser = subparsers.add_parser(
        "migrate-frontmatter", help="Migrate existing files to enhanced frontmatter for Dataview"
    )
    migrate_fm_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to files",
    )

    # Digest generation command
    digest_parser = subparsers.add_parser(
        "digest", help="Generate digest notes for content discovery in Obsidian"
    )
    digest_parser.add_argument(
        "digest_type",
        nargs="?",
        default="all",
        choices=["weekly", "inbox", "dashboard", "all"],
        help="Type of digest to generate (default: all)",
    )
    digest_parser.add_argument(
        "--week",
        type=str,
        help="Specific week for weekly digest (format: YYYY-WNN, e.g., 2024-W52)",
    )
    digest_parser.add_argument(
        "--notify",
        action="store_true",
        help="Send ntfy notification when digest is complete",
    )
    digest_parser.add_argument(
        "--ntfy-topic",
        type=str,
        default="thoth",
        help="ntfy topic to send notification to (default: thoth)",
    )

    # ArXiv command
    arxiv_parser = subparsers.add_parser("arxiv", help="ArXiv paper discovery")
    arxiv_parser.add_argument("--discover", action="store_true", help="Discover new papers")
    arxiv_parser.add_argument(
        "--source",
        choices=["api", "rss"],
        default=None,
        help="Discovery source: arXiv search API or category RSS feeds",
    )
    arxiv_parser.add_argument(
        "--topics", 
        type=str, 
        default=None,
        help="Comma-separated topics to search"
    )
    arxiv_parser.add_argument(
        "--categories",
        type=str,
        default=None,
        help="Comma-separated arXiv categories to scan when --source rss (e.g. cs.AI,cs.LG)",
    )
    arxiv_parser.add_argument(
        "--feed-format",
        choices=["rss", "atom"],
        default=None,
        help="Feed format to use when --source rss (default: rss)",
    )
    arxiv_parser.add_argument("--limit", type=int, default=None, help="Max results per topic")

    # Social command (GH/HF)
    social_parser = subparsers.add_parser("social", help="GitHub and HuggingFace integration")
    social_parser.add_argument("--sync", action="store_true", help="Sync stars and likes")
    social_parser.add_argument("--github-user", type=str, help="GitHub username")
    social_parser.add_argument("--hf-user", type=str, help="HuggingFace username")
    social_parser.add_argument("--limit", type=int, default=None, help="Max results per source")

    # X API backfill command
    x_api_parser = subparsers.add_parser(
        "x-api-sync",
        help="Backfill bookmarks from the X API and process them immediately",
    )
    x_api_parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Maximum bookmark results per page",
    )
    x_api_parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum pages to fetch",
    )
    x_api_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore the stored sync checkpoint and restart from the newest page",
    )

    # Web Clipper command
    web_clipper_parser = subparsers.add_parser(
        "web-clipper",
        help="Index files from the configured Web Clipper source directories",
    )

    archivist_parser = subparsers.add_parser(
        "archivist",
        help="Compile archivist topic pages from configured source material",
    )
    archivist_parser.add_argument(
        "--topics",
        type=str,
        default=None,
        help="Comma-separated archivist topic IDs to compile",
    )
    archivist_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of topics to evaluate",
    )
    archivist_parser.add_argument(
        "--force",
        action="store_true",
        help="Run selected topics even if the dirty check says they are up to date",
    )
    archivist_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate topics without calling the LLM or writing wiki pages",
    )
    archivist_parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Evaluate retrieval only and print candidate diagnostics without compiling pages",
    )
    archivist_parser.add_argument(
        "--benchmark-top",
        type=int,
        default=10,
        help="Number of top candidate paths to print per topic during --benchmark",
    )

    wiki_query_parser = subparsers.add_parser(
        "wiki-query",
        help="Search the compiled wiki and optionally write a curated result back",
    )
    wiki_query_parser.add_argument("query", help="Wiki search query")
    wiki_query_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of wiki matches to return",
    )
    wiki_query_parser.add_argument(
        "--write-back",
        action="store_true",
        help="Persist the curated query output into the wiki",
    )
    wiki_query_parser.add_argument(
        "--selected-slugs",
        type=str,
        default=None,
        help="Comma-separated wiki slugs to include in the write-back page",
    )
    wiki_query_parser.add_argument(
        "--curated-notes",
        type=str,
        default=None,
        help="Optional notes to include in the curated write-back page",
    )
    wiki_query_parser.add_argument(
        "--title",
        type=str,
        default=None,
        help="Optional custom title for the curated write-back page",
    )

    wiki_lint_parser = subparsers.add_parser(
        "wiki-lint",
        help="Run wiki health checks for contradictions, staleness, and orphan pages",
    )
    wiki_lint_parser.add_argument(
        "--stale-after-days",
        type=int,
        default=30,
        help="Warn when wiki pages have not been updated within this many days",
    )

    ingest_queue_parser = subparsers.add_parser(
        "ingest-queue",
        help="Process pending generalized ingestion queue entries",
    )
    ingest_queue_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of queue entries to process",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Validate configuration (allow offline-safe commands even if invalid)
    if not config.validate_and_warn():
        print("⚠️ Configuration validation failed. Check logs for details.")
        offline_safe = {
            "stats",
            "process",
            "pipeline",
            "update-videos",
            "youtube",
            "twitter-transcripts",
            "digest",
            "migrate-frontmatter",
            "arxiv",
            "social",
            "x-api-sync",
            "web-clipper",
            "wiki-query",
            "wiki-lint",
            "ingest-queue",
            "db",
        }
        if args.command not in offline_safe:  # Block only network-heavy commands
            print("❌ Cannot proceed with invalid configuration for this command")
            sys.exit(1)

    ensure_wiki_scaffold(config)

    # Default to stats if no command given
    if not args.command:
        args.command = "stats"

    # Run command
    try:
        if args.command == "process":
            asyncio.run(cmd_process(args))
        elif args.command == "pipeline":
            asyncio.run(cmd_process_pipeline(args))
        elif args.command == "async-test":
            asyncio.run(cmd_async_test(args))
        elif args.command == "github-stars":
            asyncio.run(cmd_github_stars(args))
        elif args.command == "huggingface-likes":
            asyncio.run(cmd_huggingface_likes(args))
        elif args.command == "youtube":
            asyncio.run(cmd_youtube(args))
        elif args.command == "update-videos":
            asyncio.run(cmd_update_videos(args))
        elif args.command == "twitter-transcripts":
            asyncio.run(cmd_twitter_transcripts(args))
        elif args.command == "stats":
            cmd_stats(args)
        elif args.command == "delete":
            asyncio.run(cmd_delete(args))
        elif args.command == "db":
            asyncio.run(cmd_database(args))
        elif args.command == "migrate-filenames":
            asyncio.run(cmd_migrate_filenames(args))
        elif args.command == "migrate-frontmatter":
            asyncio.run(cmd_migrate_frontmatter(args))
        elif args.command == "digest":
            asyncio.run(cmd_digest(args))
        elif args.command == "arxiv":
            asyncio.run(cmd_arxiv(args))
        elif args.command == "social":
            asyncio.run(cmd_social(args))
        elif args.command == "x-api-sync":
            asyncio.run(cmd_x_api_sync(args))
        elif args.command == "web-clipper":
            asyncio.run(cmd_web_clipper(args))
        elif args.command == "archivist":
            asyncio.run(cmd_archivist(args))
        elif args.command == "wiki-query":
            asyncio.run(cmd_wiki_query(args))
        elif args.command == "wiki-lint":
            asyncio.run(cmd_wiki_lint(args))
        elif args.command == "ingest-queue":
            asyncio.run(cmd_ingest_queue(args))
    except KeyboardInterrupt:
        print("\n❌ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
