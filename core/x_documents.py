"""One X post shape for archive imports and managed bookmark sync.

Both paths build the ``_build_tweet_data`` dict shape, pass it through the
canonical bookmark contract and queue a ``TweetArtifact`` whose custom metadata
carries the document Cissa reads (title, body, source URI, time, ``x`` metadata).
Linked papers, videos and repositories are recorded as follow-on fetch
candidates only; nothing is fetched here.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from .artifacts.tweet import TweetArtifact
from .bookmark_contract import normalize_bookmark_payload, validate_tweet_id

STATUS_URL = "https://x.com/i/web/status/{tweet_id}"
KINDS = {"tweet": "Post", "like": "Liked post", "note": "Long post", "bookmark": "Bookmarked post"}
_FOLLOW_ON_HOSTS = {
    "arxiv.org": "arxiv", "www.arxiv.org": "arxiv", "export.arxiv.org": "arxiv",
    "youtube.com": "youtube", "www.youtube.com": "youtube", "m.youtube.com": "youtube", "youtu.be": "youtube",
    "github.com": "github", "www.github.com": "github",
}
_SHORT_HOSTS = {"t.co"}
_URL = re.compile(r"https?://[^\s<>\"]+")


def status_url(tweet_id: str) -> str:
    return STATUS_URL.format(tweet_id=validate_tweet_id(tweet_id))


def archive_time(value: Any) -> str | None:
    """X archive ``Fri Jan 09 15:12:21 +0000 2026`` or ISO text as UTC ISO."""
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError("invalid_x_time")
    try:
        parsed = datetime.strptime(value, "%a %b %d %H:%M:%S %z %Y")
    except ValueError:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("invalid_x_time")
    return parsed.astimezone(timezone.utc).isoformat()


def entity_urls(entities: Any) -> list[dict[str, str]]:
    """Short/expanded URL pairs from v1.1 (archive), v2 (API) or note-tweet entities."""
    if not isinstance(entities, Mapping):
        return []
    result = []
    for item in entities.get("urls") or []:
        if not isinstance(item, Mapping):
            continue
        short = item.get("url") or item.get("shortUrl")
        expanded = item.get("unwound_url") or item.get("expanded_url") or item.get("expandedUrl")
        if isinstance(expanded, str) and expanded:
            result.append({"url": str(short or expanded), "expanded_url": expanded})
    return result


def follow_on_candidates(urls: Iterable[str]) -> list[dict[str, str]]:
    """Linked items worth fetching later: arXiv, YouTube, GitHub, X posts, unresolved short links."""
    seen, result = set(), []
    for url in urls:
        try:
            host = (urlsplit(url).hostname or "").lower()
        except ValueError:
            continue
        kind = _FOLLOW_ON_HOSTS.get(host)
        if kind is None and host in {"x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"} and "/status/" in url:
            kind = "x_post"
        if kind is None and host in _SHORT_HOSTS:
            kind = "short_link"
        if kind and url not in seen:
            seen.add(url)
            result.append({"url": url, "kind": kind})
    return result


def build_tweet_data(*, tweet_id: str, text: str, created_at: str | None = None, author_id: str | None = None,
                     author_username: str | None = None, author_name: str | None = None,
                     entities: Mapping[str, Any] | None = None, **extra: Any) -> dict[str, Any]:
    """The ``x_api_bookmark_sync._build_tweet_data`` shape, from any X source."""
    return {
        "id": validate_tweet_id(tweet_id), "text": text, "full_text": text, "created_at": created_at,
        "author_id": author_id, "author_username": author_username, "author_name": author_name,
        "author": author_username or author_name or author_id,
        "conversation_id": extra.get("conversation_id"), "lang": extra.get("lang"),
        "public_metrics": extra.get("public_metrics"), "entities": dict(entities or {}),
        "attachments": extra.get("attachments"), "referenced_tweets": extra.get("referenced_tweets") or [],
        "media": extra.get("media") or [],
    }


def x_document(*, kind: str, tweet_data: Mapping[str, Any], artifact_id: str, source: str,
               observed_at: str, extra_metadata: Mapping[str, Any] | None = None,
               source_uri: str | None = None, search_only: bool = False) -> TweetArtifact:
    """A queueable tweet artifact carrying the Cissa document projection."""
    if kind not in KINDS:
        raise ValueError("invalid_x_document_kind")
    tweet_id = validate_tweet_id(tweet_data.get("id"))
    urls = entity_urls(tweet_data.get("entities"))
    text = str(tweet_data.get("full_text") or tweet_data.get("text") or "")
    for item in urls:
        if item["url"] != item["expanded_url"]:
            text = text.replace(item["url"], item["expanded_url"])
    linked = [item["expanded_url"] for item in urls] + [url for url in _URL.findall(text) if url not in
                                                        {item["expanded_url"] for item in urls}]
    uri = source_uri or status_url(tweet_id)
    candidates = follow_on_candidates(linked)
    if not text.strip() and source_uri is None:
        # A post known only by id (a like without text, a bookmark) is itself the follow-on.
        candidates.append({"url": uri, "kind": "x_post"})
    author = tweet_data.get("author_username")
    title = KINDS[kind] + (f" by @{author}" if author else "") + (f": {' '.join(text.split())[:80]}" if text.strip() else f" {tweet_id}")
    body = text.strip() or uri
    created = tweet_data.get("created_at")
    metadata = {"kind": kind, "tweet_id": tweet_id, "created_at": created, "author_id": tweet_data.get("author_id"),
                "author_username": author, "lang": tweet_data.get("lang"), "urls": linked,
                "follow_on": candidates, "media_count": len(tweet_data.get("media") or []), **dict(extra_metadata or {})}
    metadata = {key: value for key, value in metadata.items() if value not in (None, [], {})}
    if search_only:
        metadata["search_only"] = True
    payload = normalize_bookmark_payload({"tweet_id": tweet_id, "tweet_data": dict(tweet_data),
                                          "timestamp": created or observed_at, "source": source},
                                         default_source=source)
    payload["raw_content"] = json.dumps({"kind": kind, "tweet_data": tweet_data, "x": metadata},
                                        sort_keys=True, ensure_ascii=False)
    artifact = TweetArtifact.from_bookmark_payload(payload)
    artifact.id = artifact_id
    artifact.source_type = source
    artifact.ingested_at = observed_at
    projection = {"title": title[:1024], "body": body, "source_uri": uri,
                  "source_updated_at": created or observed_at, "x": metadata}
    if search_only:
        # Search-only bulk data (owner decision 2026-11-07) is never enriched and
        # keeps one copy of the text plus the metadata.
        # The text digest keeps the receipt hash bound to the text it serves.
        artifact.raw_content = json.dumps(
            {"kind": kind, "x": metadata, "text_sha256": hashlib.sha256(body.encode()).hexdigest()},
            sort_keys=True, ensure_ascii=False)
        artifact.full_text = ""
        artifact.custom_metadata = projection
    else:
        artifact.custom_metadata = {**artifact.custom_metadata, **projection}
    return artifact
