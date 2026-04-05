"""
Video Updater - Retroactively update existing tweets/threads with video links
Downloads videos and updates markdown files to use thumbnail images with video links
"""

import logging
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from core.data_models import Tweet, ProcessingStats
from processors.media_processor import MediaProcessor
from processors.cache_loader import CacheLoader
from processors.content_processor import ContentProcessor
from processors.thread_processor import ThreadProcessor

logger = logging.getLogger(__name__)


class VideoUpdater:
    """Handles retroactive updates for video content in existing tweets and threads"""
    
    def __init__(self, vault_dir: str = 'knowledge_vault'):
        self.vault_dir = Path(vault_dir)
        self.media_processor = MediaProcessor()
        self.cache_loader = CacheLoader()
        self.content_processor = ContentProcessor(vault_dir)
        self.thread_processor = ThreadProcessor(vault_dir)
    
    def update_videos_in_tweets(self, resume: bool = True) -> ProcessingStats:
        """Update all existing tweet files to include video links and download videos"""
        stats = ProcessingStats()
        
        tweets_dir = self.vault_dir / 'tweets'
        if not tweets_dir.exists():
            logger.warning(f"Tweets directory not found: {tweets_dir}")
            return stats
        
        # Find all tweet markdown files
        tweet_files = list(tweets_dir.glob('*.md'))
        logger.info(f"Found {len(tweet_files)} tweet files to check for video updates")
        
        stats.extras.setdefault('media_downloads', 0)

        for tweet_file in tweet_files:
            try:
                # Extract tweet ID from filename
                tweet_id = self._extract_tweet_id_from_filename(tweet_file.name)
                if not tweet_id:
                    continue

                # Load cached GraphQL data for this tweet
                cached_tweets = self.cache_loader.load_cached_enhancements([tweet_id])
                if tweet_id not in cached_tweets:
                    logger.debug(f"No cached data for tweet {tweet_id}")
                    continue
                cached_tweet = cached_tweets[tweet_id]

                # Check if this tweet has videos
                has_videos = any(media.media_type in ['video', 'animated_gif'] for media in cached_tweet.media_items)
                if not has_videos:
                    continue

                logger.info(f"Updating tweet {tweet_id} with video content")

                # Download videos for this tweet
                video_stats = self.media_processor.process_media([cached_tweet], resume=resume)
                stats.errors += video_stats.errors
                stats.skipped += video_stats.skipped
                stats.extras['media_downloads'] += video_stats.updated

                # Regenerate the markdown file
                self._regenerate_tweet_file(cached_tweet, tweet_file)
                stats.updated += 1
                stats.total_processed += 1

            except Exception as e:
                logger.error(f"Error updating tweet file {tweet_file}: {e}")
                stats.errors += 1

        if stats.total_processed:
            logger.info(f"Video update complete: {stats.updated} files refreshed, {stats.errors} errors")
        else:
            logger.info("Video update complete: no tweet files required changes")
        
        return stats
    
    def update_videos_in_threads(self, resume: bool = True) -> ProcessingStats:
        """Update all existing thread files to include video links and download videos"""
        stats = ProcessingStats()
        
        threads_dir = self.vault_dir / 'threads'
        if not threads_dir.exists():
            logger.warning(f"Threads directory not found: {threads_dir}")
            return stats
        
        # Find all thread markdown files
        thread_files = list(threads_dir.glob('*.md'))
        logger.info(f"Found {len(thread_files)} thread files to check for video updates")
        
        stats.extras.setdefault('media_downloads', 0)

        for thread_file in thread_files:
            try:
                # Extract thread info from filename
                thread_info = self._extract_thread_info_from_filename(thread_file.name)
                if not thread_info:
                    continue
                
                # Load cached tweets for this specific thread only
                thread_tweets = self._load_thread_tweets(thread_info['thread_id'])
                
                if not thread_tweets:
                    logger.debug(f"No cached data for thread {thread_info['thread_id']}")
                    continue
                
                # Check if any tweets in this thread have videos
                has_videos = any(
                    any(media.media_type in ['video', 'animated_gif'] for media in tweet.media_items)
                    for tweet in thread_tweets
                )
                if not has_videos:
                    continue
                
                logger.info(f"Updating thread {thread_info['thread_id']} with video content")

                # Download videos for all tweets in thread
                video_stats = self.media_processor.process_media(thread_tweets, resume=resume)
                stats.errors += video_stats.errors
                stats.skipped += video_stats.skipped
                stats.extras['media_downloads'] += video_stats.updated

                # Regenerate the thread file
                self._regenerate_thread_file(thread_tweets, thread_file)
                stats.updated += 1
                stats.total_processed += 1

            except Exception as e:
                logger.error(f"Error updating thread file {thread_file}: {e}")
                stats.errors += 1

        if stats.total_processed:
            logger.info(f"Thread video update complete: {stats.updated} files refreshed, {stats.errors} errors")
        else:
            logger.info("Thread video update complete: no thread files required changes")

        return stats
    
    def update_all_videos(self, resume: bool = True) -> ProcessingStats:
        """Update all existing files (tweets and threads) with video content"""
        logger.info("Starting comprehensive video update...")
        
        # Update tweets
        tweet_stats = self.update_videos_in_tweets(resume=resume)

        # Update threads
        thread_stats = self.update_videos_in_threads(resume=resume)

        # Combine stats
        total_stats = ProcessingStats()
        total_stats.total_processed = tweet_stats.total_processed + thread_stats.total_processed
        total_stats.skipped = tweet_stats.skipped + thread_stats.skipped
        total_stats.updated = tweet_stats.updated + thread_stats.updated
        total_stats.errors = tweet_stats.errors + thread_stats.errors
        total_stats.extras['media_downloads'] = (
            tweet_stats.extras.get('media_downloads', 0)
            + thread_stats.extras.get('media_downloads', 0)
        )

        logger.info(
            "Comprehensive video update complete: %s files refreshed, %s errors (media downloads: %s)",
            total_stats.updated,
            total_stats.errors,
            total_stats.extras.get('media_downloads', 0),
        )
        return total_stats
    
    def _load_thread_tweets(self, thread_id: str) -> List[Tweet]:
        """Load tweets for a specific thread by checking cache files efficiently"""
        thread_tweets = []
        
        # Get list of cache files
        cache_files = list(self.cache_loader.cache_dir.glob('*.json'))
        
        for cache_file in cache_files:
            try:
                # Quick check if this cache file might contain the thread
                with open(cache_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Skip if no mention of the thread ID
                    if thread_id not in content:
                        continue
                
                # Load the cache file data
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract tweets from this cache file
                tweets = self.cache_loader.extract_all_thread_tweets_from_cache(cache_file)
                
                # Filter for tweets in this thread
                for tweet in tweets:
                    if tweet.thread_id == thread_id:
                        thread_tweets.append(tweet)
                        
            except Exception as e:
                logger.debug(f"Error reading cache file {cache_file}: {e}")
                continue
        
        return thread_tweets

    def _extract_tweet_id_from_filename(self, filename: str) -> Optional[str]:
        """Extract tweet ID from markdown filename"""
        # Expected format: tweet_1234567890_username.md
        if filename.startswith('tweet_') and filename.endswith('.md'):
            parts = filename.split('_')
            if len(parts) >= 2:
                return parts[1]
        return None
    
    def _extract_thread_info_from_filename(self, filename: str) -> Optional[Dict[str, str]]:
        """Extract thread info from markdown filename"""
        # Expected format: thread_1234567890_username.md
        if filename.startswith('thread_') and filename.endswith('.md'):
            parts = filename.split('_')
            if len(parts) >= 3:
                return {
                    'thread_id': parts[1],
                    'username': parts[2].replace('.md', '')
                }
        return None
    
    def _regenerate_tweet_file(self, tweet: Tweet, tweet_file: Path):
        """Regenerate a tweet markdown file with updated video content"""
        try:
            # Generate new content
            new_content = self.content_processor.create_tweet_markdown(tweet)
            
            # Write to file
            with open(tweet_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.debug(f"Regenerated tweet file: {tweet_file}")
            
        except Exception as e:
            logger.error(f"Error regenerating tweet file {tweet_file}: {e}")
            raise
    
    def _regenerate_thread_file(self, tweets: List[Tweet], thread_file: Path):
        """Regenerate a thread markdown file with updated video content"""
        try:
            # Extract thread ID from filename
            thread_id = self._extract_thread_info_from_filename(thread_file.name)['thread_id']
            
            # Generate new content using the correct method
            new_content = self.thread_processor.create_thread_markdown(thread_id, tweets)
            
            # Write to file
            with open(thread_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.debug(f"Regenerated thread file: {thread_file}")
            
        except Exception as e:
            logger.error(f"Error regenerating thread file {thread_file}: {e}")
            raise
    
    def get_video_statistics(self) -> Dict[str, Any]:
        """Get statistics about video content in the vault"""
        stats = {
            'total_tweets_with_videos': 0,
            'total_threads_with_videos': 0,
            'total_video_files': 0,
            'total_thumbnail_files': 0,
            'video_files_size_mb': 0,
            'thumbnail_files_size_mb': 0,
            'total_cache_files': 0,
            'cache_files_with_videos': 0
        }
        
        # Count cache files and check for videos directly from cache files
        if self.cache_loader.cache_dir.exists():
            cache_files = list(self.cache_loader.cache_dir.glob("tweet_*.json"))
            stats['total_cache_files'] = len(cache_files)
            
            # Sample cache files from different parts to estimate video content
            sample_size = min(50, len(cache_files))
            if len(cache_files) > sample_size:
                # Sample from beginning, middle, and end
                step = len(cache_files) // sample_size
                sampled_files = cache_files[::step][:sample_size]
            else:
                sampled_files = cache_files
            
            for cache_file in sampled_files:
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Quick check for video content in the JSON
                    if self._cache_file_has_videos(data):
                        stats['cache_files_with_videos'] += 1
                except Exception:
                    continue
            
            # Extrapolate based on sample
            if sample_size > 0:
                video_ratio = stats['cache_files_with_videos'] / sample_size
                stats['total_tweets_with_videos'] = int(len(cache_files) * video_ratio)
        
        # Count video and thumbnail files in both new directories and legacy media directory
        from core.config import config
        
        # Check new videos directory (handle absolute vs relative paths)
        videos_path = config.get('paths.videos_dir', 'videos')
        if Path(videos_path).is_absolute():
            videos_dir = Path(videos_path)
        else:
            videos_dir = self.vault_dir / videos_path
        if videos_dir.exists():
            for media_file in videos_dir.glob('*'):
                if media_file.is_file():
                    file_size = media_file.stat().st_size
                    if media_file.suffix == '.mp4':
                        stats['total_video_files'] += 1
                        stats['video_files_size_mb'] += file_size / (1024 * 1024)
                    elif '_thumb' in media_file.name:
                        stats['total_thumbnail_files'] += 1
                        stats['thumbnail_files_size_mb'] += file_size / (1024 * 1024)
        
        # Check new images directory for thumbnails (handle absolute vs relative paths)
        images_path = config.get('paths.images_dir', 'images')
        if Path(images_path).is_absolute():
            images_dir = Path(images_path)
        else:
            images_dir = self.vault_dir / images_path
        if images_dir.exists():
            for media_file in images_dir.glob('*'):
                if media_file.is_file():
                    file_size = media_file.stat().st_size
                    if '_thumb' in media_file.name:
                        stats['total_thumbnail_files'] += 1
                        stats['thumbnail_files_size_mb'] += file_size / (1024 * 1024)
        
        # Check legacy media directory for backward compatibility
        legacy_media_dir = self.vault_dir / 'media'
        if legacy_media_dir.exists():
            for media_file in legacy_media_dir.glob('*'):
                if media_file.is_file():
                    file_size = media_file.stat().st_size
                    if media_file.suffix == '.mp4':
                        stats['total_video_files'] += 1
                        stats['video_files_size_mb'] += file_size / (1024 * 1024)
                    elif '_thumb' in media_file.name:
                        stats['total_thumbnail_files'] += 1
                        stats['thumbnail_files_size_mb'] += file_size / (1024 * 1024)
        
        return stats
    
    def _cache_file_has_videos(self, data: Dict) -> bool:
        """Quick check if a cache file contains video content"""
        try:
            # Navigate to the tweet data structure
            instructions = data.get('data', {}).get('threaded_conversation_with_injections_v2', {}).get('instructions', [])
            
            for instruction in instructions:
                if instruction.get('type') == 'TimelineAddEntries':
                    entries = instruction.get('entries', [])
                    for entry in entries:
                        if entry.get('entryId', '').startswith('tweet-'):
                            tweet_data = entry.get('content', {}).get('itemContent', {}).get('tweet_results', {}).get('result', {})
                            legacy = tweet_data.get('legacy', {})
                            media_list = legacy.get('extended_entities', {}).get('media', [])
                            
                            # Check if any media is video
                            for media in media_list:
                                if media.get('type') in ['video', 'animated_gif']:
                                    return True
            return False
        except Exception:
            return False
