"""Owner-uploaded X archive data files; digest-bound, selected, never scanned.

Cissa sends the selected ``data/*.js`` files of an X archive (``window.YTD.<name>.partN
= [...]``) as protected ``history_exports``. Posts, likes and long-form note posts
become tweet artifacts with a Cissa document projection. Direct messages and
every other archive section are refused. Linked URLs are follow-on candidates only.

Owner decision 2026-11-07: archive items are searchable data only (lexical and
embedding index), never processed knowledge. Every document carries
``x.search_only: true`` so enrichment and memory/task extraction skip it, and no
per-item raw copy is written.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import re

from core.collection_result import CollectionCoverage, collection_limit
from core.connector_capture import ConnectorCaptureQueue
from core.x_documents import archive_time, build_tweet_data, entity_urls, x_document

# Selected file stem -> the YTD global it must declare. A large part may be
# re-sliced by the uploader as ``<stem>.<n>`` with the same global name.
SECTIONS = {"tweets": "tweets", "like": "like", "note-tweet": "note_tweet"}
FILE_PATTERN = re.compile(r"^(tweets|like|note-tweet)(-part[0-9]+)?(\.[0-9]+)?$")
_GLOBAL = re.compile(r"^\s*window\.YTD\.([a-z_]+)\.part([0-9]+)\s*=\s*")
SOURCE = "x_archive"


def parse_archive_file(content: str, file: str, *, max_bytes: int) -> tuple[str, list]:
    """Return (section, items) for one selected archive data file."""
    if not isinstance(content, str) or len(content.encode()) > max_bytes:
        raise ValueError("x_archive_file_budget_exceeded")
    match = FILE_PATTERN.fullmatch(file)
    if match is None:
        raise ValueError("x_archive_section_not_ingested")
    section = match.group(1)
    header = _GLOBAL.match(content)
    if header is None or header.group(1) != SECTIONS[section]:
        raise ValueError("x_archive_file_section_mismatch")
    items = json.loads(content[header.end():].strip().rstrip(";"))
    if not isinstance(items, list):
        raise ValueError("x_archive_file_not_a_list")
    return section, items


def _plain(text: str) -> str:
    return " ".join(re.sub(r"https?://t\.co/\S+", "", text or "").replace("…", "").split())


def match_notes(notes: list[dict], tweets: list[dict]) -> dict[str, str]:
    """note id -> tweet id for long posts whose short tweet is in the same run.

    The archive does not link them. A note matches the tweet created in the same
    second; several candidates are resolved by the longest shared text prefix.
    """
    by_second = defaultdict(list)
    for tweet in tweets:
        by_second[tweet["created_at"][:19]].append(tweet)
    result = {}
    for note in notes:
        candidates = by_second.get((note["created_at"] or "")[:19], [])
        if len(candidates) > 1:
            text = _plain(note["text"])
            scored = sorted(((len(_common_prefix(text, _plain(tweet["text"]))), tweet["id"]) for tweet in candidates), reverse=True)
            candidates = [tweet for tweet in candidates if tweet["id"] == scored[0][1]] if scored[0][0] and (
                len(scored) < 2 or scored[0][0] > scored[1][0]) else []
        if len(candidates) == 1:
            result[note["id"]] = candidates[0]["id"]
    return result


def _common_prefix(left: str, right: str) -> str:
    size = 0
    for a, b in zip(left, right):
        if a != b:
            break
        size += 1
    return left[:size]


def _tweet(item):
    tweet = item["tweet"]
    created = archive_time(tweet.get("created_at"))
    entities = tweet.get("entities") or {}
    media = (tweet.get("extended_entities") or entities).get("media") or []
    # Compact: only the URL entities are kept; media are counted, never copied.
    data = build_tweet_data(tweet_id=tweet.get("id_str") or tweet.get("id"), text=str(tweet.get("full_text") or ""),
                            created_at=created, entities={"urls": entity_urls(entities)}, lang=tweet.get("lang"),
                            media=[{"type": item.get("type")} for item in media if isinstance(item, dict)],
                            public_metrics={"retweet_count": int(tweet.get("retweet_count") or 0),
                                            "like_count": int(tweet.get("favorite_count") or 0)})
    extra = {"in_reply_to_status_id": tweet.get("in_reply_to_status_id_str"),
             "in_reply_to_screen_name": tweet.get("in_reply_to_screen_name"),
             "is_retweet": str(tweet.get("full_text") or "").startswith("RT @") or None}
    return {"id": data["id"], "created_at": created or "", "text": data["text"], "data": data, "extra": extra}


def _note(item):
    note = item["noteTweet"]
    core = note.get("core") or note.get("noteTweetResults") or {}
    text = core.get("text")
    if not isinstance(text, str):
        raise ValueError("x_archive_note_without_text")
    return {"id": str(note["noteTweetId"]), "created_at": archive_time(note.get("createdAt")) or "",
            "text": text, "entities": {"urls": core.get("urls") or (core.get("entitySet") or {}).get("urls") or []},
            "tweet_id": note.get("tweetId")}


class XArchiveCollector:
    def __init__(self, config, *, layout, db, capture_event_store=None, history_exports=None):
        self.config, self.layout, self.db = config, layout, db
        self.exports = history_exports or []
        self.queue = ConnectorCaptureQueue(config, layout=layout, db=db, capture_event_store=capture_event_store)
        self.coverage = CollectionCoverage(config)

    def collect(self):
        sources = self.config.get("sources.x_archive.sources")
        selected = self.config.get("managed.selected_resources")
        if not isinstance(sources, list) or not sources or not selected:
            raise ValueError("X archive files must be explicitly selected")
        resources = [f'x_archive:{item["archive_id"]}:{item["file"]}' for item in sources]
        if len(resources) != len(set(resources)) or set(resources) != set(selected):
            raise ValueError("X archive selection mismatch")
        exports = {item["resource_id"]: item["content"] for item in self.exports}
        if len(exports) != len(self.exports) or set(exports) != set(resources):
            raise ValueError("X archive uploads must match selected files")
        max_bytes = collection_limit(self.config, "max_x_archive_file_bytes")
        budget = int(collection_limit(self.config, "max_x_archive_documents"))
        parsed = {}
        for resource, source in zip(resources, sources):
            self.coverage.source(resource, time_basis="observation_time")
            try:
                self._validate(source, exports[resource])
                parsed[resource] = parse_archive_file(exports[resource], source["file"], max_bytes=max_bytes)
            except (ValueError, TypeError, KeyError):
                self.coverage.fail(resource, "x_archive_file_rejected")
        notes = [_safe(_note, item) for resource in parsed if parsed[resource][0] == "note-tweet"
                 for item in parsed[resource][1]]
        tweets = [_safe(_tweet, item) for resource in parsed if parsed[resource][0] == "tweets"
                  for item in parsed[resource][1]]
        merged = match_notes([note for note in notes if note], [tweet for tweet in tweets if tweet])
        merged_tweets = {tweet_id: note_id for note_id, tweet_id in merged.items()}
        tweet_index = {tweet["id"]: tweet for tweet in tweets if tweet}
        now = datetime.now(timezone.utc).isoformat()
        queued = 0
        with self.queue.lifecycle() as lifecycle:
            for resource in resources:
                if resource not in parsed:
                    continue
                section, items = parsed[resource]
                entry = self.coverage.source(resource, time_basis="observation_time")
                offset = int(self.coverage.cursor(resource).get("offset", 0))
                entry["observed_count"] = len(items)
                rejected = 0
                end = offset
                for index in range(offset, len(items)):
                    if queued >= budget:
                        break
                    end = index + 1
                    try:
                        artifact = self._document(section, items[index], merged, merged_tweets, tweet_index, now)
                    except (ValueError, TypeError, KeyError, AttributeError):
                        rejected += 1
                        continue
                    if artifact is None:
                        continue
                    self._queue(lifecycle, artifact, resource, now)
                    queued += 1
                    entry["queued_count"] += 1
                if rejected:
                    # One malformed record never stops the file; the run is partial.
                    self.coverage.fail(resource, "x_archive_item_rejected", partial=True)
                if end < len(items):
                    self.coverage.limited(resource, {"offset": end})
        return {**self.coverage.receipt(), "queued_count": queued}

    @staticmethod
    def _validate(source, content):
        if source.get("operation") != "upsert":
            raise ValueError("x_archive_upsert_only")
        for value in (source["archive_id"], source["file"]):
            if not isinstance(value, str) or not value or ":" in value:
                raise ValueError("invalid_x_archive_identity")
        if not isinstance(source.get("revision"), str) or not source["revision"]:
            raise ValueError("invalid_x_archive_identity")
        if hashlib.sha256(content.encode()).hexdigest() != source["content_sha256"]:
            raise ValueError("x_archive_digest_mismatch")

    @staticmethod
    def _document(section, item, merged, merged_tweets, tweet_index, now):
        if section == "tweets":
            tweet = _tweet(item)
            if tweet["id"] in merged_tweets:
                return None  # Its full text is the matching long post's document.
            return x_document(kind="tweet", tweet_data=tweet["data"], artifact_id=f'x:tweet:{tweet["id"]}',
                              source=SOURCE, observed_at=now, extra_metadata=tweet["extra"])
        if section == "note-tweet":
            note = _note(item)
            tweet = tweet_index.get(merged.get(note["id"]) or str(note.get("tweet_id") or ""))
            post_id = tweet["id"] if tweet else note.get("tweet_id")
            # A long post whose short post is not in this run keeps its own identity.
            base = tweet["data"] if tweet else {"id": post_id or note["id"]}
            urls = entity_urls(note["entities"]) + entity_urls(base.get("entities"))
            data = build_tweet_data(**{**base, "tweet_id": base["id"], "text": note["text"],
                                       "created_at": note["created_at"] or base.get("created_at"),
                                       "entities": {"urls": [{"url": u["url"], "expanded_url": u["expanded_url"]} for u in urls]}})
            return x_document(kind="note", tweet_data=data, artifact_id=f'x:note:{note["id"]}', source=SOURCE,
                              observed_at=now, source_uri=None if post_id else f'cissa://x/note/{note["id"]}',
                              extra_metadata={"note_tweet_id": note["id"], "post_matched": bool(post_id),
                                              **(tweet["extra"] if tweet else {})})
        like = item["like"]
        data = build_tweet_data(tweet_id=like["tweetId"], text=str(like.get("fullText") or ""))
        return x_document(kind="like", tweet_data=data, artifact_id=f'x:like:{data["id"]}', source=SOURCE,
                          observed_at=now)

    def _queue(self, lifecycle, artifact, resource, now):
        # Search-only data: no per-item raw evidence copy. The digest-bound
        # upload is the evidence; the queue row keeps the compact document.
        artifact.custom_metadata["x"]["search_only"] = True
        self.queue.queue_artifact(lifecycle, artifact, artifact_type="tweet",
            source={"source_name": SOURCE, "source_type": SOURCE, "native_source_id": resource,
                    "base_uri": artifact.custom_metadata["source_uri"]},
            session={"session_type": "x_archive_import", "native_session_id": resource},
            event={"event_type": "x_archive_item", "native_event_id": artifact.id, "captured_at": now,
                   "provenance": {"collector": SOURCE, "resource": resource},
                   "privacy": {"classification": "personal"}})


def _safe(parse, item):
    try:
        return parse(item)
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
