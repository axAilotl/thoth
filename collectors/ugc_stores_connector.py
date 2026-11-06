"""Configured Apple/Play review samples adapted from collect_ugc_stores.py."""
from __future__ import annotations
import json
import os
import re
import subprocess
import sys

from core.collection_result import collection_limit
from .research_capture import ResearchCapture


class StoreReviewCollector(ResearchCapture):
    name = "ugc_stores"

    def collect(self):
        with self.queue.lifecycle() as lifecycle:
            for app in self.settings["apps"]:
                self._app(lifecycle, app)
        return self.receipt()

    def _app(self, lifecycle, app):
        platform, app_id = app["platform"], app["app_id"]
        source = f"{platform}:{app_id}"
        summary = self.source(source, time_basis="review_updated_at")
        summary.update(coverage_kind="sample", limitations=["Store samples are not a census; timestamp may be review edit time"],
                       locale=f"{self.settings['country']}/{self.settings['language']}")
        if platform not in {"apple", "play"} or not re.fullmatch(r"[A-Za-z0-9_.-]+", app_id):
            raise ValueError("Invalid configured app identifier")
        if not re.fullmatch(r"[a-z]{2}", self.settings["country"]):
            raise ValueError("Invalid store country")
        cursor = self.coverage.cursor(source)
        if cursor.get("completed"):
            return
        page = int(cursor.get("page", 1))
        continuation = cursor.get("continuation")
        try:
            for _ in range(int(collection_limit(self.config, "max_pages"))):
                if platform == "apple":
                    if page > int(self.settings["apple_max_pages"]):
                        self.coverage.limited(source)
                        return
                    url = f"https://itunes.apple.com/{self.settings['country']}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"
                    raw = self.fetch(url)
                    records = raw["feed"].get("entry", [])
                    records = records if isinstance(records, list) else [records]
                    records = [row for row in records if "im:rating" in row]
                else:
                    raw = self._play_page(app_id, continuation)
                    records = raw["records"]
                page_hash = self.raw_page(source, page, raw)
                if not records:
                    if summary["observed_count"] == 0:
                        self.coverage.fail(source, "empty_or_unavailable_sample", partial=True)
                    return
                for record in records:
                    summary["observed_count"] += 1
                    if sum(item["observed_count"] for item in self.coverage.sources.values()) > collection_limit(self.config, "max_items"):
                        self.coverage.limited(source, {"page": page, "continuation": continuation})
                        return
                    if platform == "apple":
                        native_id, timestamp = record["id"]["label"], record["updated"]["label"]
                        title, content = record["title"]["label"], record["content"]["label"]
                        url = f"https://apps.apple.com/{self.settings['country']}/app/id{app_id}?see-all=reviews"
                    else:
                        native_id, timestamp = record["reviewId"], record["at"]
                        title, content = f"{app.get('name', app_id)} review", record["content"]
                        url = f"https://play.google.com/store/apps/details?id={app_id}&reviewId={native_id}"
                    self.capture(lifecycle, source=source, native_id=native_id, uri=url, timestamp=timestamp,
                        title=title, text=content, raw={"platform": platform, "app_id": app_id, "review": record})
                summary.setdefault("review_page_links", []).append({"review_ids": [row["id"]["label"] if platform == "apple" else row["reviewId"] for row in records], "page_hash": page_hash})
                if platform == "play":
                    continuation = raw["cursor"]
                    if continuation is None:
                        return
                page += 1
            self.coverage.limited(source, {"page": page, "continuation": continuation})
        except Exception as exc:
            self.coverage.fail(source, type(exc).__name__)
            summary["cursor"] = {"page": page, "continuation": continuation}

    def _play_page(self, app_id, cursor):
        request = {"app_id": app_id, "cursor": cursor, "language": self.settings["language"],
                   "country": self.settings["country"], "count": min(int(self.settings["play_page_size"]), int(collection_limit(self.config, "max_items"))),
                   "timeout_seconds": collection_limit(self.config, "request_timeout_seconds")}
        process = subprocess.run([sys.executable, "-m", "collectors.play_reviews_worker"],
            input=json.dumps(request), capture_output=True, text=True, check=False,
            timeout=collection_limit(self.config, "worker_timeout_seconds"),
            env={"PATH": os.defpath, "TZ": "UTC"})
        if process.returncode:
            raise RuntimeError("Play review worker failed")
        return json.loads(process.stdout)
