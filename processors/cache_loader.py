"""
Cache Loader - Loads and processes cached GraphQL data
Converts cached GraphQL responses into enhanced Tweet objects
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from core.data_models import Tweet
from core.config import config
from core.path_layout import build_path_layout

logger = logging.getLogger(__name__)


class CacheLoader:
    """Loads cached GraphQL data and converts to Tweet objects"""

    def __init__(self, cache_dir: str = None):
        self.cache_dir = Path(cache_dir) if cache_dir else build_path_layout(config).cache_root

    def load_cached_enhancements(self, tweet_ids: List[str]) -> Dict[str, Tweet]:
        """Load cached GraphQL data for specific tweet IDs"""
        enhanced_tweets = {}

        if not self.cache_dir.exists():
            logger.warning(f"Cache directory {self.cache_dir} does not exist")
            return enhanced_tweets

        # Get latest cache file for each tweet ID
        tweet_files = self._get_latest_cache_files()

        for tweet_id in tweet_ids:
            if tweet_id in tweet_files:
                try:
                    enhanced_tweet = self._load_tweet_from_cache(
                        tweet_files[tweet_id], tweet_id
                    )
                    if enhanced_tweet:
                        enhanced_tweets[tweet_id] = enhanced_tweet
                except Exception as e:
                    logger.error(f"Error loading cached data for tweet {tweet_id}: {e}")

        logger.info(f"📂 Loaded {len(enhanced_tweets)} enhanced tweets from cache")
        return enhanced_tweets

    def load_graphql_cache(self, tweets: List[Tweet]) -> Dict[str, Dict]:
        """Load raw GraphQL payloads for the provided tweets."""

        if not self.cache_dir.exists():
            logger.debug(
                f"Cache directory {self.cache_dir} does not exist; no GraphQL data loaded"
            )
            return {}

        latest_files = self._get_latest_cache_files()
        graphql_payloads: Dict[str, Dict] = {}

        for tweet in tweets:
            tweet_id = getattr(tweet, "id", None)
            if not tweet_id:
                continue

            cache_file = latest_files.get(tweet_id)
            if not cache_file:
                continue

            try:
                with open(cache_file, "r", encoding="utf-8") as cached_file:
                    graphql_payloads[tweet_id] = json.load(cached_file)
            except Exception as exc:
                logger.debug(f"Could not load GraphQL cache for {tweet_id}: {exc}")

        if graphql_payloads:
            logger.info(
                f"📡 Loaded GraphQL payloads for {len(graphql_payloads)} tweet(s)"
            )
        return graphql_payloads

    def _get_latest_cache_files(self) -> Dict[str, Path]:
        """Get the latest cache file for each tweet ID"""
        tweet_files = {}

        cache_files = list(self.cache_dir.glob("tweet_*.json"))
        logger.debug(f"Found {len(cache_files)} cache files")

        for cache_file in cache_files:
            try:
                filename = cache_file.stem
                parts = filename.split("_")
                logger.debug(f"Processing file: {filename}, parts: {parts}")

                if len(parts) >= 3 and parts[0] == "tweet":
                    tweet_id = parts[1]
                    timestamp = None
                    # Support both tweet_{id}_{YYYYmmddHHMMSS} and tweet_{id}_{YYYYmmdd}_{HHMMSS}
                    if len(parts) >= 4:
                        ts_str = f"{parts[2]}{parts[3]}"
                    else:
                        ts_str = parts[2]
                    try:
                        timestamp = int(ts_str)
                    except Exception:
                        # Fallback to file mtime if parsing fails
                        try:
                            timestamp = int(cache_file.stat().st_mtime)
                        except Exception:
                            timestamp = 0

                    logger.debug(f"Found tweet {tweet_id} with timestamp {timestamp}")

                    # Keep only the latest file for each tweet ID
                    if (
                        tweet_id not in tweet_files
                        or timestamp > tweet_files[tweet_id]["timestamp"]
                    ):
                        tweet_files[tweet_id] = {
                            "file": cache_file,
                            "timestamp": timestamp,
                        }
            except Exception as e:
                logger.debug(f"Invalid cache file {cache_file}: {e}")

        logger.info(f"Found {len(tweet_files)} unique tweet IDs in cache")

        # Return just the file paths
        return {tweet_id: info["file"] for tweet_id, info in tweet_files.items()}

    def _load_tweet_from_cache(
        self, cache_file: Path, target_tweet_id: str = None
    ) -> Optional[Tweet]:
        """Load and convert a single cached GraphQL response to Tweet object"""
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                graphql_data = json.load(f)

            # Extract the target tweet ID from filename if not provided
            if not target_tweet_id:
                filename = cache_file.stem
                parts = filename.split("_")
                if len(parts) >= 3 and parts[0] == "tweet":
                    target_tweet_id = parts[1]

            # Extract tweet data from GraphQL structure
            tweet_data = self._extract_tweet_from_graphql(graphql_data, target_tweet_id)
            if not tweet_data:
                return None

            # Convert to Tweet object
            enhanced_tweet = Tweet.from_graphql(tweet_data)
            setattr(enhanced_tweet, "_cache_file", str(cache_file))
            return enhanced_tweet

        except Exception as e:
            logger.error(f"Error loading tweet from cache file {cache_file}: {e}")
            return None

    def _extract_tweet_from_graphql(
        self, graphql_data: Dict, target_tweet_id: str = None
    ) -> Optional[Dict]:
        """Extract the specific tweet data from GraphQL response structure"""
        try:
            instructions = (
                graphql_data.get("data", {})
                .get("threaded_conversation_with_injections_v2", {})
                .get("instructions", [])
            )

            for instruction in instructions:
                if instruction.get("type") != "TimelineAddEntries":
                    continue

                for entry in instruction.get("entries", []):
                    # Try to extract from direct entry first
                    tweet_info, entry_metadata = (
                        self._extract_tweet_from_entry_with_metadata(entry)
                    )
                    if tweet_info and tweet_info.get("legacy", {}).get("id_str"):
                        tweet_id = tweet_info.get("legacy", {}).get("id_str")
                        # If we're looking for a specific tweet, only return that one
                        if target_tweet_id and tweet_id == target_tweet_id:
                            # Add entry-level metadata to tweet_info for from_graphql()
                            tweet_info.update(entry_metadata)
                            return tweet_info
                        # If no target specified, return the first valid tweet
                        elif not target_tweet_id:
                            # Add entry-level metadata to tweet_info for from_graphql()
                            tweet_info.update(entry_metadata)
                            return tweet_info

                    # If not found in direct entry, check items within entry
                    content = entry.get("content", {})
                    items = content.get("items", [])
                    for item in items:
                        item_entry = item.get("item", {})
                        tweet_info, entry_metadata = (
                            self._extract_tweet_from_entry_with_metadata(item_entry)
                        )
                        if tweet_info and tweet_info.get("legacy", {}).get("id_str"):
                            tweet_id = tweet_info.get("legacy", {}).get("id_str")
                            # If we're looking for a specific tweet, only return that one
                            if target_tweet_id and tweet_id == target_tweet_id:
                                # Add entry-level metadata to tweet_info for from_graphql()
                                tweet_info.update(entry_metadata)
                                return tweet_info
                            # If no target specified, return the first valid tweet
                            elif not target_tweet_id:
                                # Add entry-level metadata to tweet_info for from_graphql()
                                tweet_info.update(entry_metadata)
                                return tweet_info

            return None

        except Exception as e:
            logger.debug(f"Error extracting tweet from GraphQL: {e}")
            return None

    def _extract_tweet_from_entry(self, entry: Dict) -> Optional[Dict]:
        """Extract tweet info from timeline entry"""
        try:
            content = entry.get("content", {})
            item_content = content.get("itemContent", {})
            tweet_results = item_content.get("tweet_results", {})
            result = tweet_results.get("result", {})

            if not result or result.get("__typename") != "Tweet":
                return None

            legacy = result.get("legacy", {})
            if not legacy:
                return None

            return result  # Return the full result for Tweet.from_graphql()

        except Exception as e:
            logger.debug(f"Error extracting tweet from entry: {e}")
            return None

    def _extract_tweet_from_entry_with_metadata(
        self, entry: Dict
    ) -> tuple[Optional[Dict], Dict]:
        """Extract tweet info and entry-level metadata from timeline entry"""
        try:
            # Handle two different structures:
            # 1. Regular entries: entry.content.itemContent
            # 2. Items: entry.itemContent (item.item has itemContent directly)

            item_content = None
            if "itemContent" in entry:
                # Items structure: itemContent is directly in the entry
                item_content = entry.get("itemContent", {})
            else:
                # Regular entry structure: itemContent is under content
                content = entry.get("content", {})
                item_content = content.get("itemContent", {})

            if not item_content:
                return None, {}

            tweet_results = item_content.get("tweet_results", {})
            result = tweet_results.get("result", {})

            if not result or result.get("__typename") != "Tweet":
                return None, {}

            legacy = result.get("legacy", {})
            if not legacy:
                return None, {}

            # Extract entry-level metadata
            metadata = {
                "tweetDisplayType": item_content.get("tweetDisplayType"),
                "conversation_id": legacy.get("conversation_id_str"),
            }

            return result, metadata

        except Exception as e:
            logger.debug(f"Error extracting tweet from entry: {e}")
            return None, {}

    def extract_all_thread_tweets_from_cache(self, cache_file: Path) -> List[Tweet]:
        """Extract all SelfThread tweets from a GraphQL cache file"""
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                graphql_data = json.load(f)

            thread_tweets = []
            instructions = (
                graphql_data.get("data", {})
                .get("threaded_conversation_with_injections_v2", {})
                .get("instructions", [])
            )

            for instruction in instructions:
                if instruction.get("type") != "TimelineAddEntries":
                    continue

                for entry in instruction.get("entries", []):
                    # Extract from direct entry first
                    tweet_info, entry_metadata = (
                        self._extract_tweet_from_entry_with_metadata(entry)
                    )
                    if (
                        tweet_info
                        and tweet_info.get("legacy", {}).get("id_str")
                        and entry_metadata.get("tweetDisplayType") == "SelfThread"
                    ):
                        # Add entry-level metadata to tweet_info for from_graphql()
                        tweet_info.update(entry_metadata)
                        enhanced_tweet = Tweet.from_graphql(tweet_info)
                        setattr(enhanced_tweet, "_cache_file", str(cache_file))
                        thread_tweets.append(enhanced_tweet)

                    # Check items within entry for more SelfThread tweets
                    content = entry.get("content", {})
                    items = content.get("items", [])
                    for item in items:
                        item_entry = item.get("item", {})
                        tweet_info, entry_metadata = (
                            self._extract_tweet_from_entry_with_metadata(item_entry)
                        )
                        if (
                            tweet_info
                            and tweet_info.get("legacy", {}).get("id_str")
                            and entry_metadata.get("tweetDisplayType") == "SelfThread"
                        ):
                            # Add entry-level metadata to tweet_info for from_graphql()
                            tweet_info.update(entry_metadata)
                            enhanced_tweet = Tweet.from_graphql(tweet_info)
                            setattr(enhanced_tweet, "_cache_file", str(cache_file))
                            thread_tweets.append(enhanced_tweet)

            logger.debug(
                f"Extracted {len(thread_tweets)} SelfThread tweets from {cache_file.name}"
            )
            return thread_tweets

        except Exception as e:
            logger.error(
                f"Error extracting thread tweets from cache file {cache_file}: {e}"
            )
            return []

    def load_all_thread_tweets_from_cache(self, limit: int = None) -> List[Tweet]:
        """Load all SelfThread tweets from GraphQL cache files"""
        all_thread_tweets = []

        if not self.cache_dir.exists():
            logger.warning(f"Cache directory {self.cache_dir} does not exist")
            return all_thread_tweets

        # Get all GraphQL cache files, sorted by modification time (newest first)
        cache_files = sorted(
            self.cache_dir.glob("tweet_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        # Apply limit if specified
        if limit:
            cache_files = cache_files[:limit]
            logger.info(
                f"Processing {len(cache_files)} GraphQL cache files for threads (limited)"
            )
        else:
            logger.info(
                f"Processing {len(cache_files)} GraphQL cache files for threads"
            )

        for cache_file in cache_files:
            try:
                thread_tweets = self.extract_all_thread_tweets_from_cache(cache_file)
                all_thread_tweets.extend(thread_tweets)
            except Exception as e:
                logger.debug(f"Error processing cache file {cache_file}: {e}")

        logger.info(f"🧵 Loaded {len(all_thread_tweets)} SelfThread tweets from cache")
        return all_thread_tweets

    def load_tweets_from_cache_files(self, limit: int = None) -> List[Tweet]:
        """Load tweets from GraphQL cache files with optional limit"""
        all_tweets = []

        if not self.cache_dir.exists():
            logger.warning(f"Cache directory {self.cache_dir} does not exist")
            return all_tweets

        # Get all GraphQL cache files, sorted by modification time (newest first)
        cache_files = sorted(
            self.cache_dir.glob("tweet_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        # Apply limit if specified
        if limit:
            cache_files = cache_files[:limit]
            logger.info(f"Processing {len(cache_files)} GraphQL cache files (limited)")
        else:
            logger.info(f"Processing {len(cache_files)} GraphQL cache files")

        for cache_file in cache_files:
            try:
                # Extract main tweet from cache file
                tweet = self._load_tweet_from_cache(cache_file)
                if tweet:
                    all_tweets.append(tweet)

                # Also extract any thread tweets from this cache file
                thread_tweets = self.extract_all_thread_tweets_from_cache(cache_file)
                all_tweets.extend(thread_tweets)

            except Exception as e:
                logger.debug(f"Error processing cache file {cache_file}: {e}")

        # Remove duplicates by tweet ID
        seen_ids = set()
        unique_tweets = []
        for tweet in all_tweets:
            if tweet.id not in seen_ids:
                seen_ids.add(tweet.id)
                unique_tweets.append(tweet)

        logger.info(
            f"📂 Loaded {len(unique_tweets)} unique tweets from {len(cache_files)} cache files"
        )
        return unique_tweets

    def apply_cached_enhancements(self, original_tweets: List[Tweet]) -> List[Tweet]:
        """Apply cached GraphQL enhancements to original tweet objects"""
        # Get tweet IDs
        tweet_ids = [tweet.id for tweet in original_tweets]

        # Load enhancements from cache
        enhanced_tweets = self.load_cached_enhancements(tweet_ids)

        # Apply enhancements
        enhanced_count = 0
        for tweet in original_tweets:
            if tweet.id in enhanced_tweets:
                enhanced_tweet = enhanced_tweets[tweet.id]

                # Copy enhanced data
                tweet.display_type = enhanced_tweet.display_type
                tweet.thread_id = enhanced_tweet.thread_id
                tweet.is_self_thread = enhanced_tweet.is_self_thread
                if enhanced_tweet.full_text and len(enhanced_tweet.full_text) > len(
                    tweet.full_text or ""
                ):
                    tweet.full_text = enhanced_tweet.full_text

                tweet.media_items = enhanced_tweet.media_items
                tweet.url_mappings = enhanced_tweet.url_mappings
                tweet.extracted_urls = enhanced_tweet.extracted_urls
                tweet.enhanced = True

                enhanced_count += 1

        logger.info(
            f"🔧 Applied cached enhancements to {enhanced_count}/{len(original_tweets)} tweets"
        )
        return original_tweets

    def get_cache_statistics(self) -> Dict:
        """Get statistics about cached data"""
        if not self.cache_dir.exists():
            return {"total_cache_files": 0, "unique_tweets_cached": 0}

        cache_files = list(self.cache_dir.glob("tweet_*.json"))
        tweet_files = self._get_latest_cache_files()

        return {
            "total_cache_files": len(cache_files),
            "unique_tweets_cached": len(tweet_files),
            "cache_directory": str(self.cache_dir),
        }
