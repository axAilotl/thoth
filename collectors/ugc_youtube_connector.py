"""Public, dated YouTube metadata candidates; no media/transcript downloads."""
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import re
import subprocess
import sys

from core.collection_result import collection_limit
from .research_capture import ResearchCapture

PUBLIC_FIELDS = ("id", "title", "upload_date", "timestamp", "channel", "channel_url",
                 "description", "view_count", "comment_count", "duration", "webpage_url")


class YouTubeResearchCollector(ResearchCapture):
    name = "ugc_youtube"

    def collect(self):
        cache = {}
        with self.queue.lifecycle() as lifecycle:
            for query in self.settings["queries"]:
                source = f"youtube:{query}"
                summary = self.source(source, time_basis="upload_date")
                summary.update(coverage_kind="search_sample", limitations=["Search results are a sample; relevance and affiliate claims need review"])
                cursor = self.coverage.cursor(source)
                if cursor.get("completed"):
                    continue
                try:
                    size = min(int(self.settings["search_count"]), int(collection_limit(self.config, "max_items")))
                    found = self._metadata(f"ytsearch{size}:{query}", flat=True)
                    candidates = list(dict.fromkeys(str(row["id"]) for row in found.get("entries", []) if row and row.get("id")))
                    offset = int(cursor.get("offset", 0))
                    self.raw_page(source, "search", {"query": query, "video_ids": candidates})
                    for index, video_id in enumerate(candidates[offset:], start=offset):
                        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
                            raise ValueError("Invalid YouTube video ID")
                        if sum(row["observed_count"] for row in self.coverage.sources.values()) >= collection_limit(self.config, "max_items"):
                            self.coverage.limited(source, {"offset": index})
                            break
                        summary["observed_count"] += 1
                        uri = f"https://www.youtube.com/watch?v={video_id}"
                        if video_id not in cache:
                            data = self._metadata(uri)
                            cache[video_id] = {key: data.get(key) for key in PUBLIC_FIELDS}
                        public = cache[video_id]
                        self.raw_page(source, video_id, public)
                        if not public.get("upload_date"):
                            self.coverage.fail(source, "missing_upload_date", partial=True)
                            continue
                        timestamp = datetime.strptime(public["upload_date"], "%Y%m%d").replace(tzinfo=timezone.utc).isoformat()
                        self.capture(lifecycle, source=source, native_id=video_id, uri=uri, timestamp=timestamp,
                            title=public.get("title") or video_id, text=public.get("description") or "", raw=public)
                except Exception as exc:
                    self.coverage.fail(source, type(exc).__name__)
        return self.receipt()

    def _metadata(self, target, *, flat=False):
        command = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-cache-dir",
                   "--skip-download", "--dump-single-json", "--no-warnings",
                   "--socket-timeout", str(collection_limit(self.config, "request_timeout_seconds"))]
        if flat:
            command.append("--flat-playlist")
        command.extend(["--", target])
        process = subprocess.run(command, capture_output=True, text=True, check=False,
            timeout=collection_limit(self.config, "worker_timeout_seconds"),
            env={"PATH": os.defpath, "TZ": "UTC"}, cwd=Path(__file__).resolve().parent.parent)
        if process.returncode:
            raise RuntimeError("YouTube metadata worker failed")
        result = json.loads(process.stdout)
        if not isinstance(result, dict):
            raise ValueError("YouTube metadata is not an object")
        return result
