"""
TweetArtifact - Twitter/X bookmark knowledge entity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Mapping

from .base import KnowledgeArtifact
from ..data_models import MediaItem, URLMapping, Tweet


def _safe_int(value: Any) -> int:
    """Coerce a metrics value to int without inventing a number."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _media_items_from_tweet_data(tweet_data: Mapping[str, Any]) -> List[MediaItem]:
    """Build media items from an X API tweet payload's expanded media objects."""
    items: List[MediaItem] = []
    raw_media = tweet_data.get("media")
    if not isinstance(raw_media, (list, tuple)):
        return items
    for raw in raw_media:
        if not isinstance(raw, Mapping):
            continue
        media_key = str(raw.get("media_key") or raw.get("media_id") or "").strip()
        media_url = str(raw.get("url") or raw.get("preview_image_url") or "").strip()
        if not media_key and not media_url:
            continue
        alt_text = raw.get("alt_text")
        items.append(
            MediaItem(
                media_id=media_key or media_url,
                media_url=media_url,
                media_type=str(raw.get("type") or "photo").strip() or "photo",
                thumbnail_url=str(raw.get("preview_image_url") or "").strip() or None,
                original_url=media_url or None,
                alt_text=str(alt_text) if alt_text else None,
                duration_millis=_safe_int(raw.get("duration_ms")) or None,
            )
        )
    return items


def _links_from_tweet_entities(
    tweet_data: Mapping[str, Any],
) -> Tuple[List[URLMapping], List[str]]:
    """Recover t.co expansions and expanded URLs from an X API tweet payload.

    Paper and repository detection runs on expanded URLs; without these the
    pipeline only ever sees shorteners and misses the links behind them.
    """
    mappings: List[URLMapping] = []
    extracted: List[str] = []
    entities = tweet_data.get("entities")
    if not isinstance(entities, Mapping):
        return mappings, extracted
    for raw in entities.get("urls") or ():
        if not isinstance(raw, Mapping):
            continue
        short_url = str(raw.get("url") or "").strip()
        expanded_url = str(raw.get("expanded_url") or "").strip()
        if not expanded_url:
            continue
        if short_url and short_url != expanded_url:
            mappings.append(
                URLMapping(
                    short_url=short_url,
                    expanded_url=expanded_url,
                    display_url=str(raw.get("display_url") or "").strip() or None,
                )
            )
        if expanded_url not in extracted:
            extracted.append(expanded_url)
    return mappings, extracted


@dataclass
class TweetArtifact(KnowledgeArtifact):
    """Twitter/X bookmark with rich GraphQL data."""

    source_type: str = "twitter"
    capabilities: Tuple[str, ...] = (
        "media",
        "urls",
        "transcription",
        "llm_summary",
        "embedding",
    )

    # Twitter-specific fields
    screen_name: str = ""
    name: str = ""
    full_text: str = ""
    media_items: List[MediaItem] = field(default_factory=list)
    url_mappings: List[URLMapping] = field(default_factory=list)
    extracted_urls: List[str] = field(default_factory=list)
    engagement: Dict[str, int] = field(default_factory=dict)
    thread_id: Optional[str] = None
    is_self_thread: bool = False

    def __post_init__(self):
        super().__post_init__()
        if self.media_items is None:
            self.media_items = []
        if self.url_mappings is None:
            self.url_mappings = []
        if self.extracted_urls is None:
            self.extracted_urls = []
        if self.engagement is None:
            self.engagement = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = super().to_dict()
        data.update(
            {
                "screen_name": self.screen_name,
                "name": self.name,
                "full_text": self.full_text,
                "media_items": [item.to_dict() for item in self.media_items],
                "url_mappings": [mapping.to_dict() for mapping in self.url_mappings],
                "extracted_urls": self.extracted_urls,
                "engagement": self.engagement,
                "thread_id": self.thread_id,
                "is_self_thread": self.is_self_thread,
            }
        )
        return data

    @classmethod
    def from_bookmark_payload(cls, payload: Mapping[str, Any]) -> "TweetArtifact":
        """Build a tweet artifact from the canonical bookmark payload."""
        bookmark_payload = dict(payload)
        tweet_data = bookmark_payload.get("tweet_data")
        if not isinstance(tweet_data, dict):
            tweet_data = {}

        tweet_id = str(bookmark_payload.get("tweet_id") or bookmark_payload.get("id") or "").strip()
        if not tweet_id:
            raise ValueError("tweet artifact payload missing tweet_id")

        raw_content = bookmark_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(bookmark_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        base_fields = cls.base_fields_from_payload(bookmark_payload)
        metrics = tweet_data.get("public_metrics")
        if not isinstance(metrics, Mapping):
            metrics = {}
        favorite_count = _safe_int(metrics.get("like_count"))
        retweet_count = _safe_int(metrics.get("retweet_count"))
        reply_count = _safe_int(metrics.get("reply_count"))
        base_fields["custom_metadata"] = {
            **base_fields.get("custom_metadata", {}),
            "tweet_data": tweet_data,
            "graphql_cache_file": bookmark_payload.get("graphql_cache_file"),
            "has_graphql_response": bool(bookmark_payload.get("graphql_response")),
            "favorite_count": favorite_count,
            "retweet_count": retweet_count,
            "reply_count": reply_count,
        }
        media_items = _media_items_from_tweet_data(tweet_data)
        url_mappings, extracted_urls = _links_from_tweet_entities(tweet_data)

        return cls(
            id=tweet_id,
            source_type=str(bookmark_payload.get("source") or "browser_extension"),
            raw_content=raw_content,
            # The bookmark payload's timestamp is when the bookmark was captured,
            # not when the tweet was posted.  Prefer the tweet's own created_at so
            # capture time never masquerades as publication time.
            created_at=str(tweet_data.get("created_at") or bookmark_payload.get("timestamp") or ""),
            ingested_at=str(bookmark_payload.get("timestamp") or tweet_data.get("created_at") or ""),
            processing_status="pending",
            screen_name=str(tweet_data.get("author") or tweet_data.get("screen_name") or ""),
            name=str(tweet_data.get("author_name") or tweet_data.get("author") or tweet_data.get("name") or ""),
            full_text=str(tweet_data.get("text") or tweet_data.get("full_text") or ""),
            media_items=media_items,
            url_mappings=url_mappings,
            extracted_urls=extracted_urls,
            engagement={
                "favorite_count": favorite_count,
                "retweet_count": retweet_count,
                "reply_count": reply_count,
            },
            **base_fields,
        )

    @classmethod
    def from_queue_payload(cls, payload: Mapping[str, Any]) -> "TweetArtifact":
        """Build a tweet artifact from persisted queue payload data."""
        if "tweet_id" in payload or "tweet_data" in payload:
            return cls.from_bookmark_payload(payload)

        artifact_payload = dict(payload)
        tweet_id = str(artifact_payload.get("id") or artifact_payload.get("tweet_id") or "").strip()
        if not tweet_id:
            raise ValueError("tweet queue payload missing id")

        raw_content = artifact_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(artifact_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        engagement_payload = artifact_payload.get("engagement")
        if not isinstance(engagement_payload, Mapping):
            engagement_payload = {}
        favorite_count = int(
            artifact_payload.get(
                "favorite_count", engagement_payload.get("favorite_count", 0)
            )
            or 0
        )
        retweet_count = int(
            artifact_payload.get(
                "retweet_count", engagement_payload.get("retweet_count", 0)
            )
            or 0
        )
        reply_count = int(
            artifact_payload.get("reply_count", engagement_payload.get("reply_count", 0))
            or 0
        )
        extracted_urls = artifact_payload.get("extracted_urls")
        if not isinstance(extracted_urls, (list, tuple)):
            extracted_urls = []
        base_fields = cls.base_fields_from_payload(artifact_payload)
        base_fields["custom_metadata"] = {
            **base_fields.get("custom_metadata", {}),
            "favorite_count": favorite_count,
            "retweet_count": retweet_count,
            "reply_count": reply_count,
        }

        return cls(
            id=tweet_id,
            source_type=str(artifact_payload.get("source_type") or artifact_payload.get("source") or "twitter"),
            raw_content=raw_content,
            created_at=str(artifact_payload.get("created_at") or ""),
            ingested_at=str(artifact_payload.get("ingested_at") or artifact_payload.get("created_at") or ""),
            processing_status=str(artifact_payload.get("processing_status") or "pending"),
            screen_name=str(artifact_payload.get("screen_name") or ""),
            name=str(artifact_payload.get("name") or ""),
            full_text=str(artifact_payload.get("full_text") or ""),
            extracted_urls=[str(url) for url in extracted_urls],
            engagement={
                "favorite_count": favorite_count,
                "retweet_count": retweet_count,
                "reply_count": reply_count,
            },
            thread_id=artifact_payload.get("thread_id"),
            is_self_thread=bool(artifact_payload.get("is_self_thread", False)),
            **base_fields,
        )

    def to_tweet_model(self) -> Tweet:
        """Convert the artifact into the legacy Tweet model used by the pipeline."""
        metadata = self.custom_metadata if isinstance(self.custom_metadata, dict) else {}
        return Tweet(
            id=self.id,
            full_text=self.full_text,
            created_at=self.created_at or self.ingested_at or "",
            screen_name=self.screen_name,
            name=self.name,
            favorite_count=_safe_int(metadata.get("favorite_count")),
            retweet_count=_safe_int(metadata.get("retweet_count")),
            reply_count=_safe_int(metadata.get("reply_count")),
            media_items=list(self.media_items),
            url_mappings=list(self.url_mappings),
            extracted_urls=list(self.extracted_urls),
            thread_id=self.thread_id,
            is_self_thread=self.is_self_thread,
        )

    @classmethod
    def from_tweet_model(cls, tweet: Any) -> TweetArtifact:
        """
        Convert existing Tweet model to TweetArtifact.
        Provides backward compatibility during refactoring.
        """
        # We use Any for the tweet type to avoid circular imports 
        # while core.data_models still exists in its current form
        return cls(
            id=tweet.id,
            source_type="twitter",
            raw_content=json.dumps(tweet.to_dict()),
            created_at=tweet.created_at,
            screen_name=tweet.screen_name,
            name=tweet.name,
            full_text=tweet.full_text,
            media_items=tweet.media_items,
            url_mappings=tweet.url_mappings,
            extracted_urls=tweet.extracted_urls,
            engagement={
                "favorite_count": tweet.favorite_count,
                "retweet_count": tweet.retweet_count,
                "reply_count": tweet.reply_count,
            },
            thread_id=tweet.thread_id,
            is_self_thread=tweet.is_self_thread,
            processing_status="processed" if tweet.processed_at else "pending",
        )
