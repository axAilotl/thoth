"""
LLM Processor - Handles AI-powered features for tweets and threads
Generates tags, summaries, and alt text using configured LLM providers
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Optional
from core.data_models import Tweet, ProcessingStats
from core.config import config
from core.path_layout import resolve_vault_relative_path, resolve_vault_root
from core.llm_interface import LLMInterface
from core.pipeline_registry import PipelineStage, pipeline_registry, register_pipeline_stages

logger = logging.getLogger(__name__)

PIPELINE_STAGES = (
    PipelineStage(
        name='llm_processing.tweet_tags',
        config_path='llm_processing.tweet_tags',
        description='Generate AI tags for individual tweets.',
        processor='LLMProcessor',
        capabilities=('llm', 'tags'),
        required_config=('processing.enable_llm_features', 'llm.tasks.tags.enabled'),
        config_keys=('processing.enable_llm_features', 'processing.batch_size', 'llm.tasks.tags.enabled')
    ),
    PipelineStage(
        name='llm_processing.tweet_summaries',
        config_path='llm_processing.tweet_summaries',
        description='Create AI summaries for tweets.',
        processor='LLMProcessor',
        capabilities=('llm', 'summary'),
        required_config=('processing.enable_llm_features', 'llm.tasks.summary.enabled'),
        config_keys=('processing.enable_llm_features', 'processing.summary_min_chars', 'llm.tasks.summary.enabled')
    ),
    PipelineStage(
        name='llm_processing.thread_summaries',
        config_path='llm_processing.thread_summaries',
        description='Summarize detected threads with LLM output.',
        processor='LLMProcessor',
        capabilities=('llm', 'summary', 'threads'),
        required_config=('processing.enable_llm_features', 'llm.tasks.summary.enabled'),
        config_keys=('processing.enable_llm_features', 'llm.tasks.summary.enabled')
    ),
    PipelineStage(
        name='llm_processing.alt_text',
        config_path='llm_processing.alt_text',
        description='Generate alt text for tweet media.',
        processor='LLMProcessor',
        capabilities=('llm', 'alt_text'),
        required_config=('processing.enable_llm_features', 'llm.tasks.alt_text.enabled'),
        config_keys=('processing.enable_llm_features', 'processing.alt_text_delay_seconds', 'llm.tasks.alt_text.enabled')
    ),
    PipelineStage(
        name='llm_processing.readme_summaries',
        config_path='llm_processing.readme_summaries',
        description='Summarize repository README files.',
        processor='LLMProcessor',
        capabilities=('llm', 'summary', 'readme'),
        required_config=('processing.enable_llm_features', 'llm.tasks.summary.enabled'),
        config_keys=('processing.enable_llm_features', 'processing.readme_summary_max_chars', 'llm.tasks.summary.enabled')
    )
)


register_pipeline_stages(*PIPELINE_STAGES)

class LLMProcessor:
    """Processes tweets and threads with LLM-powered features"""
    
    def __init__(self):
        self.llm_config = config.get('llm', {})
        self.processing_config = config.get('processing', {})
        self.vault_path = resolve_vault_root(config)
        self.llm_interface = None
        
        # Initialize LLM interface if enabled
        if self.processing_config.get('enable_llm_features', False):
            try:
                self.llm_interface = LLMInterface(self.llm_config)
                logger.info("LLM interface initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize LLM interface: {e}")
                self.llm_interface = None
    
    def is_enabled(self) -> bool:
        """Check if LLM processing is enabled and available"""
        return (
            self.processing_config.get('enable_llm_features', False) and 
            self.llm_interface is not None
        )
    
    async def process_tweets(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Process tweets with LLM features"""
        stats = ProcessingStats()
        
        if not self.is_enabled():
            logger.info("LLM processing disabled or unavailable")
            stats.skipped = len(tweets)
            return stats
        
        batch_size = self.processing_config.get('batch_size', 10)
        rate_limit_delay = self.processing_config.get('rate_limit_delay', 1.0)
        
        # Process tweets in batches to avoid rate limits
        for i in range(0, len(tweets), batch_size):
            batch = tweets[i:i + batch_size]
            batch_stats = await self._process_tweet_batch(batch, resume)
            
            stats.created += batch_stats.created
            stats.updated += batch_stats.updated
            stats.skipped += batch_stats.skipped
            stats.errors += batch_stats.errors
            stats.total_processed += batch_stats.total_processed
            
            # Rate limiting between batches
            if i + batch_size < len(tweets) and rate_limit_delay > 0:
                await asyncio.sleep(rate_limit_delay)
                
        
        logger.info(f"🤖 LLM processing complete: {stats.updated} processed, {stats.skipped} skipped")
        return stats
    
    async def _process_tweet_batch(self, tweets: List[Tweet], resume: bool) -> ProcessingStats:
        """Process a batch of tweets concurrently"""
        stats = ProcessingStats()
        
        # Create tasks for concurrent processing
        tasks = []
        for tweet in tweets:
            if resume and self._has_llm_features(tweet):
                stats.skipped += 1
                continue
            
            task = self._process_single_tweet(tweet)
            tasks.append(task)
        
        # Process all tasks concurrently
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Tweet processing error: {result}")
                    stats.errors += 1
                elif result:
                    stats.updated += 1
                else:
                    stats.skipped += 1
                stats.total_processed += 1
        
        return stats
    
    async def _process_single_tweet(self, tweet: Tweet) -> bool:
        """Process a single tweet with LLM features"""
        try:
            updated = False
            
            # Generate tags if enabled
            if self._is_task_enabled('tags'):
                tags = await self._generate_tags(tweet)
                if tags:
                    tweet.llm_tags = tags
                    updated = True
            
            # Generate summary for long tweets if enabled
            if self._is_task_enabled('summary') and self._should_summarize_tweet(tweet):
                summary = await self._generate_tweet_summary(tweet)
                if summary:
                    tweet.llm_summary = summary
                    updated = True
            
            # Generate alt text for media if enabled
            if self._is_task_enabled('alt_text') and tweet.media_items:
                alt_texts = await self._generate_alt_texts(tweet)
                if alt_texts:
                    # Add alt text to media items
                    for i, media_item in enumerate(tweet.media_items):
                        if i < len(alt_texts) and alt_texts[i]:
                            media_item.alt_text = alt_texts[i]
                            updated = True
            
            return updated
            
        except Exception as e:
            logger.error(f"Error processing tweet {tweet.id}: {e}")
            return False
    
    async def process_threads(self, threads: Dict[str, List[Tweet]], resume: bool = True) -> ProcessingStats:
        """Process threads with LLM features"""
        stats = ProcessingStats()
        
        if not self.is_enabled():
            logger.info("LLM processing disabled or unavailable")
            stats.skipped = len(threads)
            return stats
        
        for thread_id, thread_tweets in threads.items():
            try:
                if resume and self._thread_has_llm_features(thread_tweets):
                    stats.skipped += 1
                    continue
                
                updated = False
                
                # Generate thread summary if enabled
                if self._is_task_enabled('summary', stage='llm_processing.thread_summaries'):
                    summary = await self._generate_thread_summary(thread_tweets)
                    if summary:
                        # Store summary in first tweet
                        thread_tweets[0].thread_summary = summary
                        updated = True
                
                # Generate thread tags if enabled (after summary)
                if self._is_task_enabled('tags', stage='llm_processing.thread_summaries'):
                    tags = await self._generate_thread_tags(thread_tweets)
                    if tags:
                        thread_tweets[0].thread_tags = tags
                        updated = True
                
                # Generate alt text for all tweets in thread if enabled
                if self._is_task_enabled('alt_text'):
                    for tweet in thread_tweets:
                        if tweet.media_items and any(not hasattr(m, 'alt_text') or not m.alt_text for m in tweet.media_items):
                            alt_texts = await self._generate_alt_texts(tweet)
                            if alt_texts:
                                # Add alt text to media items
                                for i, media_item in enumerate(tweet.media_items):
                                    if i < len(alt_texts) and alt_texts[i]:
                                        media_item.alt_text = alt_texts[i]
                                        updated = True
                
                if updated:
                    stats.updated += 1
                else:
                    stats.skipped += 1
                
                stats.total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing thread {thread_id}: {e}")
                stats.errors += 1
        
        logger.info(f"🧵 Thread LLM processing complete: {stats.updated} processed, {stats.skipped} skipped")
        return stats
    
    async def process_readme_files(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Process README files with LLM summaries"""
        stats = ProcessingStats()
        
        if not self.is_enabled() or not self._is_task_enabled('summary', stage='llm_processing.readme_summaries'):
            logger.info("README LLM processing disabled or unavailable")
            return stats
        
        for tweet in tweets:
            try:
                if not hasattr(tweet, 'repo_links') or not tweet.repo_links:
                    stats.skipped += 1
                    continue
                
                updated = False
                for repo_link in tweet.repo_links:
                    if resume and hasattr(repo_link, 'llm_summary') and repo_link.llm_summary:
                        continue
                    
                    if repo_link.downloaded and repo_link.filename:
                        summary = await self._generate_readme_summary(repo_link)
                        if summary:
                            repo_link.llm_summary = summary
                            updated = True
                
                if updated:
                    stats.updated += 1
                else:
                    stats.skipped += 1
                
                stats.total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing README for tweet {tweet.id}: {e}")
                stats.errors += 1
        
        logger.info(f"📂 README LLM processing complete: {stats.updated} processed, {stats.skipped} skipped")
        return stats
    
    def _is_task_enabled(self, task: str, stage: Optional[str] = None) -> bool:
        """Check if a specific LLM task is enabled and its pipeline stage is active."""
        default_stage_map = {
            'tags': 'llm_processing.tweet_tags',
            'summary': 'llm_processing.tweet_summaries',
            'alt_text': 'llm_processing.alt_text',
            'transcript': 'llm_processing.transcript_formatting'
        }

        stage_name = stage or default_stage_map.get(task)
        if stage_name and not pipeline_registry.is_enabled(stage_name):
            return False

        return self.llm_config.get('tasks', {}).get(task, {}).get('enabled', False)
    
    def _has_llm_features(self, tweet: Tweet) -> bool:
        """Check if tweet already has LLM features processed"""
        if (
            (hasattr(tweet, 'llm_tags') and tweet.llm_tags) or
            (hasattr(tweet, 'llm_summary') and tweet.llm_summary) or
            (tweet.media_items and any(hasattr(m, 'alt_text') and m.alt_text for m in tweet.media_items))
        ):
            return True

        from core.filename_utils import get_filename_normalizer

        normalizer = get_filename_normalizer()
        tweet_filename = normalizer.generate_tweet_filename(tweet.id, tweet.screen_name)
        tweet_file = self.vault_path / "tweets" / tweet_filename
        return self.has_llm_sections_in_markdown(tweet_file)
    
    def _thread_has_llm_features(self, thread_tweets: List[Tweet]) -> bool:
        """Check if thread already has LLM features processed"""
        first_tweet = thread_tweets[0] if thread_tweets else None
        if not first_tweet:
            return False
        
        if (
            (hasattr(first_tweet, 'thread_summary') and first_tweet.thread_summary) or
            (hasattr(first_tweet, 'thread_tags') and first_tweet.thread_tags)
        ):
            return True

        thread_id = getattr(first_tweet, 'thread_id', None)
        screen_name = getattr(first_tweet, 'screen_name', '')
        if not thread_id or not screen_name:
            return False

        from core.filename_utils import get_filename_normalizer

        normalizer = get_filename_normalizer()
        thread_filename = normalizer.generate_thread_filename(thread_id, screen_name)
        thread_file = self.vault_path / "threads" / thread_filename
        return self.has_llm_sections_in_markdown(thread_file)

    def has_llm_sections_in_markdown(self, markdown_path: Path) -> bool:
        """Return True when markdown already contains durable LLM output sections."""
        if not markdown_path.exists() or not markdown_path.is_file():
            return False

        try:
            content = markdown_path.read_text(encoding="utf-8")
        except Exception:
            return False

        markers = (
            "## Summary",
            "## Tags",
            "## 📝 Summary",
            "## 🏷️ AI Tags",
        )
        return any(marker in content for marker in markers)
    
    def _should_summarize_tweet(self, tweet: Tweet) -> bool:
        """Determine if tweet should be summarized based on configured threshold"""
        if not tweet.full_text:
            return False
        threshold = config.get_processing_threshold('summary_min_chars', 512)
        try:
            threshold = int(threshold)
        except (TypeError, ValueError):
            threshold = 512
        return len(tweet.full_text) > threshold
    
    async def _generate_tags(self, tweet: Tweet) -> List[str]:
        """Generate tags for tweet content with caching"""
        try:
            content = self._get_tweet_content_for_tagging(tweet)
            if not content:
                return []
            
            from core.llm_cache import llm_cache
            route = self.llm_interface.resolve_task_route('tags')
            if not route:
                return []
            provider, model_id, _ = route
            cache_id = f"{provider}:{model_id}"

            cached_result = llm_cache.get(content, 'tags', cache_id)
            if cached_result:
                logger.debug(f"Using cached tags for tweet {tweet.id}")
                return cached_result.get('tags', [])
            
            # Generate new tags
            tags = await self.llm_interface.generate_tags(content)
            logger.debug(f"Generated {len(tags)} tags for tweet {tweet.id}")
            
            # Cache the result
            llm_cache.set(content, 'tags', {'tags': tags}, cache_id)
            # Persist to DB
            try:
                from core.metadata_db import get_metadata_db
                db = get_metadata_db()
                import json, hashlib
                content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
                cache_key = f"tags_{content_hash}"
                db.upsert_llm_cache(cache_key, 'tags', content_hash, json.dumps({'tags': tags}), cache_id)
            except Exception:
                pass
            
            return tags
            
        except Exception as e:
            logger.error(f"Tag generation error for tweet {tweet.id}: {e}")
            return []
    
    async def _generate_tweet_summary(self, tweet: Tweet) -> str:
        """Generate summary for long tweet with caching"""
        try:
            content = tweet.full_text or ""
            if not content:
                return ""
            
            # Check cache first
            from core.llm_cache import llm_cache
            route = self.llm_interface.resolve_task_route('summary')
            if not route:
                return ""
            provider, model_id, _ = route
            cache_id = f"{provider}:{model_id}"

            cached_result = llm_cache.get(content, 'tweet_summary', cache_id)
            if cached_result:
                logger.debug(f"Using cached summary for tweet {tweet.id}")
                return cached_result.get('summary', '')
            
            # Generate new summary
            summary = await self.llm_interface.summarize_content(content, "text")
            logger.debug(f"Generated summary for tweet {tweet.id}")
            
            # Cache the result
            llm_cache.set(content, 'tweet_summary', {'summary': summary}, cache_id)
            # Persist to DB
            try:
                from core.metadata_db import get_metadata_db
                db = get_metadata_db()
                import json, hashlib
                content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
                cache_key = f"tweet_summary_{content_hash}"
                db.upsert_llm_cache(cache_key, 'tweet_summary', content_hash, json.dumps({'summary': summary}), cache_id)
            except Exception:
                pass
            
            return summary
            
        except Exception as e:
            logger.error(f"Tweet summary generation error for tweet {tweet.id}: {e}")
            return ""
    
    async def _generate_thread_summary(self, thread_tweets: List[Tweet]) -> str:
        """Generate summary for thread"""
        try:
            logger.debug("🤖 [LLM] THREAD summary start")
            # Combine all tweet texts
            thread_content = []
            for i, tweet in enumerate(thread_tweets, 1):
                if tweet.full_text:
                    thread_content.append(f"Tweet {i}: {tweet.full_text}")
            
            if not thread_content:
                return ""
            
            content = "\n\n".join(thread_content)
            summary = await self.llm_interface.summarize_content(content, "thread")
            logger.debug(f"🤖 [LLM] THREAD summary ok ({len(thread_tweets)} tweets)")
            return summary
            
        except Exception as e:
            logger.error(f"Thread summary generation error: {e}")
            return ""
    
    async def _generate_readme_summary(self, repo_link) -> str:
        """Generate summary for README file"""
        try:
            logger.debug(f"🤖 [LLM] README summary start: {repo_link.filename}")
            # Load README content
            readme_path = self.vault_path / 'repos' / repo_link.filename
            
            if not readme_path.exists():
                logger.warning(f"README file not found: {readme_path}")
                return ""
            
            with open(readme_path, 'r', encoding='utf-8') as f:
                readme_content = f.read()
            
            if not readme_content.strip():
                return ""
            
            # Truncate if too long (most LLMs have token limits)
            max_chars = config.get_processing_threshold('readme_summary_max_chars', 8000)
            try:
                max_chars = int(max_chars)
            except (TypeError, ValueError):
                max_chars = 8000
            if max_chars > 0 and len(readme_content) > max_chars:
                readme_content = readme_content[:max_chars] + "\n\n[Content truncated...]"
            
            summary = await self.llm_interface.summarize_content(readme_content, "readme")
            logger.debug(f"🤖 [LLM] README summary ok: {repo_link.filename}")
            return summary
            
        except Exception as e:
            logger.error(f"README summary generation error for {repo_link.filename}: {e}")
            return ""
    
    async def _generate_alt_texts(self, tweet: Tweet) -> List[str]:
        """Generate alt text for tweet media"""
        try:
            alt_texts = []
            
            route = self.llm_interface.resolve_task_route('alt_text')
            if not route:
                return []
            provider, model_id, _ = route
            cache_id = f"{provider}:{model_id}"

            for media_item in tweet.media_items:
                if media_item.filename and media_item.media_type == 'photo':
                    images_dir = resolve_vault_relative_path(config, 'paths.images_dir')
                    media_path = images_dir / media_item.filename
                    
                    if media_path.exists():
                        logger.debug(f"Processing alt text for {media_item.filename}")
                        
                        # Check cache first (use filename as content key since it's based on file)
                        from core.llm_cache import llm_cache
                        cached_result = llm_cache.get(media_item.filename, 'alt_text', cache_id)
                        if cached_result:
                            alt_text = cached_result.get('alt_text', '')
                            logger.debug(f"Using cached alt text for {media_item.filename}")
                        else:
                            # Generate new alt text
                            alt_text = await self.llm_interface.generate_alt_text(str(media_path))
                            
                            # Cache the result if valid
                            if alt_text and not alt_text.startswith("Alt text"):
                                llm_cache.set(media_item.filename, 'alt_text', {'alt_text': alt_text}, cache_id)
                                # Persist alt text cache to DB (content hash based on filename)
                                try:
                                    from core.metadata_db import get_metadata_db
                                    db = get_metadata_db()
                                    import json, hashlib
                                    content_hash = hashlib.sha256(media_item.filename.encode('utf-8')).hexdigest()[:16]
                                    cache_key = f"alt_text_{content_hash}"
                                    db.upsert_llm_cache(cache_key, 'alt_text', content_hash, json.dumps({'alt_text': alt_text}), cache_id)
                                except Exception:
                                    pass
                        
                        # Only add if we got a real alt text, not an error message
                        if alt_text and not alt_text.startswith("Alt text"):
                            alt_texts.append(alt_text)
                            logger.info(f"✓ Alt text for {media_item.filename}: {alt_text[:50]}...")
                        else:
                            logger.warning(f"✗ Alt text failed for {media_item.filename}: {alt_text}")
                            alt_texts.append("")
                        
                        # Rate limiting for vision API calls (configurable)
                        delay = config.get_processing_threshold('alt_text_delay_seconds', 2.0)
                        try:
                            delay_value = float(delay)
                        except (TypeError, ValueError):
                            delay_value = 2.0
                        await asyncio.sleep(max(0.0, delay_value))
                    else:
                        logger.warning(f"Media file not found: {media_path}")
                        alt_texts.append("")
                else:
                    # Skip non-photo media or media without files
                    alt_texts.append("")
            
            return alt_texts
            
        except Exception as e:
            logger.error(f"Alt text generation error for tweet {tweet.id}: {e}")
            return []
    
    async def _generate_thread_tags(self, thread_tweets: List[Tweet]) -> List[str]:
        """Generate tags for thread content"""
        try:
            # Combine all tweet content for tagging
            content_parts = []
            
            for tweet in thread_tweets:
                if tweet.full_text:
                    content_parts.append(tweet.full_text)
                
                # Add ArXiv paper titles if present
                if hasattr(tweet, 'arxiv_papers') and tweet.arxiv_papers:
                    for paper in tweet.arxiv_papers:
                        if paper.title:
                            content_parts.append(f"ArXiv: {paper.title}")
                
                # Add repo names if present
                if hasattr(tweet, 'repo_links') and tweet.repo_links:
                    for repo in tweet.repo_links:
                        content_parts.append(f"{repo.platform.upper()}: {repo.repo_name}")
            
            if not content_parts:
                return []
            
            content = " | ".join(content_parts)
            tags = await self.llm_interface.generate_tags(content)
            logger.debug(f"Generated {len(tags)} tags for thread with {len(thread_tweets)} tweets")
            return tags
            
        except Exception as e:
            logger.error(f"Thread tag generation error: {e}")
            return []

    def _get_tweet_content_for_tagging(self, tweet: Tweet) -> str:
        """Get relevant tweet content for tag generation"""
        content_parts = []
        
        # Add main tweet text
        if tweet.full_text:
            content_parts.append(tweet.full_text)
        
        # Add ArXiv paper titles if present
        if hasattr(tweet, 'arxiv_papers') and tweet.arxiv_papers:
            for paper in tweet.arxiv_papers:
                if paper.title:
                    content_parts.append(f"ArXiv: {paper.title}")
        
        # Add repo names if present
        if hasattr(tweet, 'repo_links') and tweet.repo_links:
            for repo in tweet.repo_links:
                content_parts.append(f"{repo.platform.upper()}: {repo.repo_name}")
        
        return " | ".join(content_parts)
