"""Public Reddit archive and Hacker News evidence, adapted from collect_ugc.py."""
from __future__ import annotations

from datetime import datetime
import html

from core.collection_result import collection_limit
from .research_capture import ResearchCapture, epoch_time


class DiscussionCollector(ResearchCapture):
    name = "ugc_discussion"

    def collect(self):
        start = int(datetime.fromisoformat(self.config.get("managed.window_start", self.settings.get("window_start")).replace("Z", "+00:00")).timestamp())
        end = int(datetime.fromisoformat(self.config.get("managed.window_end", self.settings.get("window_end")).replace("Z", "+00:00")).timestamp())
        if start >= end:
            raise ValueError("Discussion window must have start before end")
        with self.queue.lifecycle() as lifecycle:
            for subreddit in self.settings.get("reddit", []):
                self._reddit(lifecycle, subreddit, start, end)
            for query in self.settings.get("hn_queries", []):
                self._hn(lifecycle, query, start, end)
        return self.receipt()

    def _reddit(self, lifecycle, subreddit, start, end):
        name = subreddit["name"]
        source = f"reddit:{name}"
        summary = self.source(source)
        summary["limitations"] = ["Public archive; lag and missing/deleted content are possible"]
        cursor = self.coverage.cursor(source)
        if cursor.get("completed"):
            return
        before = int(cursor.get("before", end))
        page_size = min(int(self.settings["page_size"]), int(collection_limit(self.config, "max_items")))
        try:
            for _ in range(int(collection_limit(self.config, "max_pages"))):
                records = self.fetch("https://arctic-shift.photon-reddit.com/api/posts/search",
                    {"subreddit": name, "after": start - 1, "before": before, "limit": page_size, "sort": "desc"})["data"]
                for post in records:
                    summary["observed_count"] += 1
                    if sum(item["observed_count"] for item in self.coverage.sources.values()) > collection_limit(self.config, "max_items"):
                        self.coverage.limited(source, {"before": before})
                        return
                    content = post.get("title", "") + "\n\n" + post.get("selftext", "")
                    keywords = subreddit.get("keywords", [])
                    if keywords and not any(term.casefold() in content.casefold() for term in keywords):
                        continue
                    uri = "https://www.reddit.com" + post.get("permalink", f"/comments/{post['id']}")
                    self.capture(lifecycle, source=source, native_id=post["id"], uri=uri,
                        timestamp=epoch_time(post["created_utc"]), title=post.get("title", "Reddit post"), text=content, raw=post)
                    if post.get("num_comments") and self.settings.get("include_comments"):
                        self._comments(lifecycle, post, source)
                if len(records) < page_size:
                    return
                oldest = min(post["created_utc"] for post in records)
                if oldest + 1 >= before:
                    raise ValueError("pagination_stalled")
                before = oldest + 1
            self.coverage.limited(source, {"before": before})
        except Exception as exc:
            self.coverage.fail(source, type(exc).__name__)
            summary["cursor"] = {"before": before}

    def _comments(self, lifecycle, post, parent_source):
        source = f"reddit_comments:{post['id']}"
        # Comments inherit the selected subreddit grant, never a new arbitrary URL.
        summary = self.coverage.source(source)
        try:
            tree = self.fetch("https://arctic-shift.photon-reddit.com/api/comments/tree",
                {"link_id": post["id"], "limit": int(self.settings["comment_limit"])})
            comments = list(_comments(tree))
            for comment in comments[:int(self.settings["comment_limit"])]:
                summary["observed_count"] += 1
                self.capture(lifecycle, source=source, native_id=comment["id"],
                    uri=f"https://www.reddit.com/comments/{post['id']}/_/{comment['id']}",
                    timestamp=epoch_time(comment["created_utc"]), title=f"Reddit comment on {post.get('title', post['id'])}",
                    text=comment.get("body", ""), raw=comment)
            if len(comments) >= int(self.settings["comment_limit"]):
                self.coverage.limited(source)
                self.coverage.fail(parent_source, "comments_sample_limit", partial=True)
        except Exception as exc:
            self.coverage.fail(source, type(exc).__name__)
            self.coverage.fail(parent_source, "comments_failed", partial=True)

    def _hn(self, lifecycle, query, start, end):
        source = f"hn:{query}"
        summary = self.source(source)
        cursor = self.coverage.cursor(source)
        if cursor.get("completed"):
            return
        page = int(cursor.get("page", 0))
        try:
            for _ in range(int(collection_limit(self.config, "max_pages"))):
                data = self.fetch("https://hn.algolia.com/api/v1/search_by_date", {
                    "query": query, "numericFilters": f"created_at_i>={start},created_at_i<{end}",
                    "hitsPerPage": min(int(self.settings["page_size"]), int(collection_limit(self.config, "max_items"))), "page": page})
                for hit in data["hits"]:
                    summary["observed_count"] += 1
                    if sum(item["observed_count"] for item in self.coverage.sources.values()) > collection_limit(self.config, "max_items"):
                        self.coverage.limited(source, {"page": page})
                        return
                    text = html.unescape("\n".join(str(hit.get(key) or "") for key in ("url", "title", "story_title", "comment_text")))
                    raw = {key: value for key, value in hit.items() if key not in {"_highlightResult", "_snippetResult"}}
                    summary.setdefault("exact_content_matches", 0)
                    summary["exact_content_matches"] += int(query.casefold() in text.casefold())
                    self.capture(lifecycle, source=source, native_id=hit["objectID"],
                        uri=f"https://news.ycombinator.com/item?id={hit['objectID']}", timestamp=epoch_time(hit["created_at_i"]),
                        title=hit.get("title") or hit.get("story_title") or "HN comment", text=text, raw=raw)
                page += 1
                if page >= data["nbPages"]:
                    return
            self.coverage.limited(source, {"page": page})
        except Exception as exc:
            self.coverage.fail(source, type(exc).__name__)
            summary["cursor"] = {"page": page}


def _comments(value):
    if isinstance(value, dict):
        if "id" in value and "body" in value and "created_utc" in value:
            yield value
        for child in value.values():
            yield from _comments(child)
    elif isinstance(value, list):
        for child in value:
            yield from _comments(child)
