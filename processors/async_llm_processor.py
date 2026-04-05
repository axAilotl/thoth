"""
Enhanced Async LLM Processor - Optimized asynchronous LLM processing
Improves performance with better concurrency control and connection pooling
"""

import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import time

from core.data_models import Tweet, ProcessingStats
from core.config import config
from core.llm_interface import LLMInterface
from core.llm_cache import llm_cache
from .llm_processor import LLMProcessor

logger = logging.getLogger(__name__)


@dataclass
class AsyncProcessingConfig:
    """Configuration for async processing"""
    max_concurrent_requests: int = 10
    max_concurrent_batches: int = 3
    rate_limit_delay: float = 0.1
    request_timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


class AsyncLLMProcessor(LLMProcessor):
    """Enhanced async LLM processor with better concurrency control"""
    
    def __init__(self, async_config: AsyncProcessingConfig = None):
        super().__init__()
        self.async_config = async_config or AsyncProcessingConfig()
        self._semaphore = asyncio.Semaphore(self.async_config.max_concurrent_requests)
        self._rate_limiter = asyncio.Semaphore(self.async_config.max_concurrent_batches)
    
    async def process_tweets_enhanced(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Enhanced tweet processing with improved concurrency"""
        stats = ProcessingStats()
        
        if not self.is_enabled():
            logger.info("LLM processing disabled or unavailable")
            stats.skipped = len(tweets)
            return stats
        
        # Filter tweets that need processing
        tweets_to_process = []
        for tweet in tweets:
            if resume and self._has_llm_features(tweet):
                stats.skipped += 1
            else:
                tweets_to_process.append(tweet)
        
        if not tweets_to_process:
            return stats
        
        # Create batches for concurrent processing
        batch_size = self.processing_config.get('batch_size', 10)
        batches = [tweets_to_process[i:i + batch_size] 
                  for i in range(0, len(tweets_to_process), batch_size)]
        
        logger.info(f"🚀 Processing {len(tweets_to_process)} tweets in {len(batches)} batches with enhanced async")
        
        # Process batches with controlled concurrency
        batch_tasks = []
        for i, batch in enumerate(batches):
            task = self._process_batch_with_rate_limit(batch, i)
            batch_tasks.append(task)
        
        # Wait for all batches to complete
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Aggregate results
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Batch processing failed: {result}")
                stats.errors += 1
            elif isinstance(result, ProcessingStats):
                stats.created += result.created
                stats.updated += result.updated
                stats.skipped += result.skipped
                stats.errors += result.errors
                stats.total_processed += result.total_processed
        
        logger.info(f"🤖 Enhanced LLM processing complete: {stats.updated} processed, {stats.skipped} skipped")
        return stats
    
    async def _process_batch_with_rate_limit(self, batch: List[Tweet], batch_num: int) -> ProcessingStats:
        """Process a batch with rate limiting"""
        async with self._rate_limiter:
            start_time = time.time()
            logger.debug(f"📦 Starting batch {batch_num + 1} ({len(batch)} tweets)")
            
            # Create tasks for all tweets in the batch
            tasks = []
            for tweet in batch:
                task = self._process_single_tweet_with_semaphore(tweet)
                tasks.append(task)
            
            # Process all tweets in the batch concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate batch statistics
            stats = ProcessingStats()
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Tweet processing error: {result}")
                    stats.errors += 1
                elif result:
                    stats.updated += 1
                else:
                    stats.skipped += 1
                stats.total_processed += 1
            
            elapsed = time.time() - start_time
            logger.debug(f"📦 Batch {batch_num + 1} completed in {elapsed:.2f}s")
            
            # Rate limiting delay
            if self.async_config.rate_limit_delay > 0:
                await asyncio.sleep(self.async_config.rate_limit_delay)
            
            return stats
    
    async def _process_single_tweet_with_semaphore(self, tweet: Tweet) -> bool:
        """Process single tweet with semaphore control"""
        async with self._semaphore:
            return await self._process_single_tweet_enhanced(tweet)
    
    async def _process_single_tweet_enhanced(self, tweet: Tweet) -> bool:
        """Enhanced single tweet processing with retries and better error handling"""
        for attempt in range(self.async_config.retry_attempts):
            try:
                return await asyncio.wait_for(
                    self._process_single_tweet_with_timeout(tweet),
                    timeout=self.async_config.request_timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Tweet {tweet.id} processing timeout (attempt {attempt + 1})")
                if attempt < self.async_config.retry_attempts - 1:
                    await asyncio.sleep(self.async_config.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Tweet {tweet.id} processing failed after {self.async_config.retry_attempts} attempts")
                    return False
            except Exception as e:
                logger.error(f"Tweet {tweet.id} processing error (attempt {attempt + 1}): {e}")
                if attempt < self.async_config.retry_attempts - 1:
                    await asyncio.sleep(self.async_config.retry_delay * (attempt + 1))
                else:
                    return False
        
        return False
    
    async def _process_single_tweet_with_timeout(self, tweet: Tweet) -> bool:
        """Process single tweet with all LLM features"""
        try:
            updated = False
            
            # Create tasks for all LLM operations (with cache lookups where possible)
            tasks = []
            task_types = []
            
            # Generate tags if enabled (with cache)
            if self._is_task_enabled('tags'):
                try:
                    cache_provider = self.llm_config.get('tasks', {}).get('tags', {}).get('provider', '')
                    cached_tags = llm_cache.get(self._get_tweet_content_for_tagging(tweet), 'tags', cache_provider)
                except Exception:
                    cached_tags = None
                if cached_tags:
                    tweet.llm_tags = cached_tags if isinstance(cached_tags, list) else cached_tags.get('tags', [])
                else:
                    tasks.append(self._generate_tags(tweet))
                    task_types.append('tags')
            
            # Generate summary for long tweets if enabled (with cache)
            if self._is_task_enabled('summary') and self._should_summarize_tweet(tweet):
                try:
                    cache_provider = self.llm_config.get('tasks', {}).get('summary', {}).get('provider', '')
                    cached_summary = llm_cache.get(tweet.full_text or '', 'summary', cache_provider)
                except Exception:
                    cached_summary = None
                if cached_summary:
                    tweet.llm_summary = cached_summary if isinstance(cached_summary, str) else cached_summary.get('summary', '')
                else:
                    tasks.append(self._generate_tweet_summary(tweet))
                    task_types.append('summary')
            
            # Generate alt text for media if enabled
            if self._is_task_enabled('alt_text') and tweet.media_items:
                tasks.append(self._generate_alt_texts(tweet))
                task_types.append('alt_text')
            
            if not tasks:
                return False
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Apply results to tweet (and write cache)
            for i, (result, task_type) in enumerate(zip(results, task_types)):
                if isinstance(result, Exception):
                    logger.error(f"Failed to generate {task_type} for tweet {tweet.id}: {result}")
                    continue
                
                if task_type == 'tags' and result:
                    tweet.llm_tags = result
                    try:
                        cache_provider = self.llm_config.get('tasks', {}).get('tags', {}).get('provider', '')
                        llm_cache.set(self._get_tweet_content_for_tagging(tweet), 'tags', result, cache_provider)
                    except Exception:
                        pass
                    updated = True
                elif task_type == 'summary' and result:
                    tweet.llm_summary = result
                    try:
                        cache_provider = self.llm_config.get('tasks', {}).get('summary', {}).get('provider', '')
                        llm_cache.set(tweet.full_text or '', 'summary', result, cache_provider)
                    except Exception:
                        pass
                    updated = True
                elif task_type == 'alt_text' and result:
                    # Apply alt text to media items
                    for j, media_item in enumerate(tweet.media_items):
                        if j < len(result) and result[j]:
                            media_item.alt_text = result[j]
                            updated = True
            
            return updated
            
        except Exception as e:
            logger.error(f"Error processing tweet {tweet.id}: {e}")
            raise
    
    async def process_tweets_with_progress(self, tweets: List[Tweet], resume: bool = True, 
                                         progress_callback: Optional[callable] = None) -> ProcessingStats:
        """Process tweets with progress reporting"""
        stats = ProcessingStats()
        
        if not self.is_enabled():
            logger.info("LLM processing disabled or unavailable")
            stats.skipped = len(tweets)
            return stats
        
        # Filter tweets that need processing
        tweets_to_process = []
        for tweet in tweets:
            if resume and self._has_llm_features(tweet):
                stats.skipped += 1
            else:
                tweets_to_process.append(tweet)
        
        total_tweets = len(tweets_to_process)
        if total_tweets == 0:
            return stats
        
        processed_count = 0
        batch_size = self.processing_config.get('batch_size', 10)
        
        # Process in batches with progress reporting
        for i in range(0, total_tweets, batch_size):
            batch = tweets_to_process[i:i + batch_size]
            batch_stats = await self._process_batch_with_rate_limit(batch, i // batch_size)
            
            # Aggregate stats
            stats.created += batch_stats.created
            stats.updated += batch_stats.updated
            stats.skipped += batch_stats.skipped
            stats.errors += batch_stats.errors
            stats.total_processed += batch_stats.total_processed
            
            processed_count += len(batch)
            
            # Report progress
            if progress_callback:
                progress = processed_count / total_tweets
                await progress_callback(processed_count, total_tweets, progress)
        
        return stats
    
    async def get_processing_stats(self) -> Dict[str, any]:
        """Get current processing statistics"""
        return {
            "semaphore_available": self._semaphore._value,
            "rate_limiter_available": self._rate_limiter._value,
            "max_concurrent": self.async_config.max_concurrent_requests,
            "max_batches": self.async_config.max_concurrent_batches,
            "is_enabled": self.is_enabled()
        }