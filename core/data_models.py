"""
Data models for Thoth based on Twitter GraphQL responses
Clean models without legacy dependencies
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import json
import logging

logger = logging.getLogger(__name__)


def _extract_note_tweet_text(result: Dict[str, Any]) -> Optional[str]:
    """Return the full text from a note tweet if present."""
    try:
        note_tweet = result.get('note_tweet') or {}
        note_results = note_tweet.get('note_tweet_results') or {}
        note_result = note_results.get('result') or {}
        if not isinstance(note_result, dict):
            return None

        text = note_result.get('text')
        if text:
            return text

        # Some payloads nest the plain text under rich_text/richtext fields
        rich_text = note_result.get('rich_text') or note_result.get('richtext')
        if isinstance(rich_text, dict):
            for key in ('text', 'plain_text'):
                candidate = rich_text.get(key)
                if candidate:
                    return candidate
    except Exception as exc:
        logger.debug(f"Failed to extract note tweet text: {exc}")
    return None


def extract_full_text_from_result(result: Dict[str, Any]) -> str:
    """Determine the best available full text for a tweet result."""
    if not isinstance(result, dict):
        return ''

    note_text = _extract_note_tweet_text(result)
    if note_text:
        return note_text

    legacy = result.get('legacy', {}) or {}
    full_text = legacy.get('full_text') or legacy.get('text')
    if full_text:
        return full_text

    # Fallback: some responses provide display text via extended entities
    extended_tweet = result.get('extended_tweet', {}) if result else {}
    if isinstance(extended_tweet, dict):
        full_text = extended_tweet.get('full_text')
        if full_text:
            return full_text

    return ''


@dataclass
class MediaItem:
    """Represents a media item from a tweet"""
    media_id: str
    media_url: str
    media_type: str  # photo, video, animated_gif
    thumbnail_url: Optional[str] = None
    original_url: Optional[str] = None
    filename: Optional[str] = None
    downloaded: bool = False
    alt_text: Optional[str] = None
    video_url: Optional[str] = None  # For videos: actual video URL
    video_filename: Optional[str] = None  # For videos: video file name
    duration_millis: Optional[int] = None  # For videos: duration in milliseconds
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'media_id': self.media_id,
            'media_url': self.media_url,
            'media_type': self.media_type,
            'thumbnail_url': self.thumbnail_url,
            'original_url': self.original_url,
            'filename': self.filename,
            'downloaded': self.downloaded,
            'alt_text': self.alt_text,
            'video_url': self.video_url,
            'video_filename': self.video_filename,
            'duration_millis': self.duration_millis
        }


@dataclass
class URLMapping:
    """Represents a URL mapping from t.co to expanded URL"""
    short_url: str
    expanded_url: str
    display_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'short_url': self.short_url,
            'expanded_url': self.expanded_url,
            'display_url': self.display_url
        }


@dataclass
class Tweet:
    """Enhanced Tweet model based on GraphQL data"""
    id: str
    full_text: str
    created_at: str
    screen_name: str
    name: str
    
    # Engagement metrics
    favorite_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    
    # Enhanced data from GraphQL
    display_type: Optional[str] = None
    thread_id: Optional[str] = None
    is_self_thread: bool = False
    
    # Media and URLs
    media_items: List[MediaItem] = None
    url_mappings: List[URLMapping] = None
    extracted_urls: List[str] = None
    
    # Processing metadata  
    enhanced: bool = False
    processed_at: Optional[str] = None
    
    def __post_init__(self):
        if self.media_items is None:
            self.media_items = []
        if self.url_mappings is None:
            self.url_mappings = []
        if self.extracted_urls is None:
            self.extracted_urls = []
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Tweet':
        """Create Tweet from dictionary (bookmark JSON format)"""
        return cls(
            id=data.get('id', ''),
            full_text=data.get('full_text', ''),
            created_at=data.get('created_at', ''),
            screen_name=data.get('screen_name', ''),
            name=data.get('name', ''),
            favorite_count=data.get('favorite_count', 0),
            retweet_count=data.get('retweet_count', 0),
            reply_count=data.get('reply_count', 0)
        )
    
    @classmethod
    def from_graphql(cls, graphql_data: Dict[str, Any]) -> 'Tweet':
        """Create Tweet from GraphQL response data"""
        legacy = graphql_data.get('legacy', {})
        # User info is in core.user_results.result.core (not legacy)
        user_info = graphql_data.get('core', {}).get('user_results', {}).get('result', {}).get('core', {})
        
        full_text = extract_full_text_from_result(graphql_data)

        # Extract media items
        media_items = []
        legacy_media = legacy.get('extended_entities', {}).get('media', [])
        if not legacy_media:
            # Fallback: some payloads put video_info under entities.media
            legacy_media = legacy.get('entities', {}).get('media', [])
        try:
            logger.debug(f"🎥 [GQL] Media scan: count={len(legacy_media)} tweet={legacy.get('id_str','')}")
        except Exception:
            pass
        for media_data in legacy_media:
            raw_type = media_data.get('type', 'photo')
            has_video_info = bool(media_data.get('video_info'))
            # Some GraphQL payloads mark video thumbs as type 'photo' but include video_info
            media_type = raw_type if raw_type in ['photo', 'video', 'animated_gif'] else 'photo'
            if has_video_info and media_type == 'photo':
                media_type = 'video'

            thumbnail_url = media_data.get('media_url_https', '')  # Thumbnail or image URL
            video_url = None
            duration_millis = None

            # Parse variants if present regardless of reported type
            if has_video_info:
                video_info = media_data.get('video_info', {})
                variants = video_info.get('variants', [])
                duration_millis = video_info.get('duration_millis')
                try:
                    logger.debug(f"🎞️ [GQL] Variants: n={len(variants)} tweet={legacy.get('id_str','')} type={media_type}")
                except Exception:
                    pass
                # Prefer highest bitrate MP4
                mp4_variants = [v for v in variants if v.get('content_type') == 'video/mp4']
                if mp4_variants:
                    mp4_variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                    video_url = mp4_variants[0].get('url', '')
                    try:
                        logger.debug(f"✅ [GQL] MP4 chosen: bitrate={mp4_variants[0].get('bitrate')} url={video_url}")
                    except Exception:
                        pass
                else:
                    # Fallback: use HLS if no MP4 (some responses only provide m3u8)
                    hls = next((v for v in variants if v.get('content_type') == 'application/x-mpegURL'), None)
                    if hls:
                        video_url = hls.get('url', '')
                        try:
                            logger.debug(f"ℹ️ [GQL] HLS fallback: url={video_url}")
                        except Exception:
                            pass

            media_item = MediaItem(
                media_id=media_data.get('id_str', ''),
                media_url=thumbnail_url,
                media_type=media_type,
                thumbnail_url=thumbnail_url,
                video_url=video_url,
                original_url=media_data.get('url', ''),
                duration_millis=duration_millis
            )
            media_items.append(media_item)
        
        # Extract URL mappings
        url_mappings = []
        for url_data in legacy.get('entities', {}).get('urls', []):
            url_mapping = URLMapping(
                short_url=url_data.get('url', ''),
                expanded_url=url_data.get('expanded_url', ''),
                display_url=url_data.get('display_url', '')
            )
            url_mappings.append(url_mapping)
        
        # Note tweet entity sets may include extra URLs beyond legacy entities
        note_result = (
            (graphql_data.get('note_tweet') or {})
            .get('note_tweet_results', {})
            .get('result', {})
        )
        if isinstance(note_result, dict):
            note_entities = note_result.get('entity_set', {}) or {}
            for url_data in note_entities.get('urls', []):
                mapping = URLMapping(
                    short_url=url_data.get('url', ''),
                    expanded_url=url_data.get('expanded_url', ''),
                    display_url=url_data.get('display_url', '')
                )
                if mapping.expanded_url and not any(
                    existing.expanded_url == mapping.expanded_url for existing in url_mappings
                ):
                    url_mappings.append(mapping)

        return cls(
            id=legacy.get('id_str', ''),
            full_text=full_text,
            created_at=legacy.get('created_at', ''),
            screen_name=user_info.get('screen_name', ''),
            name=user_info.get('name', ''),
            favorite_count=legacy.get('favorite_count', 0),
            retweet_count=legacy.get('retweet_count', 0),
            reply_count=legacy.get('reply_count', 0),
            display_type=graphql_data.get('tweetDisplayType'),
            thread_id=graphql_data.get('conversation_id') if graphql_data.get('tweetDisplayType') == 'SelfThread' else None,
            is_self_thread=graphql_data.get('tweetDisplayType') == 'SelfThread',
            media_items=media_items,
            url_mappings=url_mappings,
            enhanced=True
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'id': self.id,
            'full_text': self.full_text,
            'created_at': self.created_at,
            'screen_name': self.screen_name,
            'name': self.name,
            'favorite_count': self.favorite_count,
            'retweet_count': self.retweet_count,
            'reply_count': self.reply_count,
            'display_type': self.display_type,
            'thread_id': self.thread_id,
            'is_self_thread': self.is_self_thread,
            'media_items': [item.to_dict() for item in self.media_items],
            'url_mappings': [mapping.to_dict() for mapping in self.url_mappings],
            'extracted_urls': self.extracted_urls,
            'enhanced': self.enhanced,
            'processed_at': self.processed_at
        }


@dataclass 
class ThreadInfo:
    """Information about a detected thread"""
    thread_id: str
    author: str
    tweet_count: int
    tweets: List[Tweet]
    created_at: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'thread_id': self.thread_id,
            'author': self.author,
            'tweet_count': self.tweet_count,
            'tweets': [tweet.to_dict() for tweet in self.tweets],
            'created_at': self.created_at
        }


@dataclass
class ProcessingStats:
    """Statistics for processing operations"""

    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    created: int = 0
    updated: int = 0
    errors: int = 0
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_processed': self.total_processed,
            'successful': self.successful,
            'failed': self.failed,
            'skipped': self.skipped,
            'created': self.created,
            'updated': self.updated,
            'errors': self.errors,
            'success_rate': (self.successful / self.total_processed * 100) if self.total_processed > 0 else 0,
            'extras': self.extras,
        }
