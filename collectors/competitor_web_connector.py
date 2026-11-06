"""Configured competitor page snapshots and changes, without narrative inference."""
from __future__ import annotations
import hashlib
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

from core.collection_result import collection_limit
from core.public_web import fetch_public
from .research_capture import ResearchCapture


class CompetitorWebCollector(ResearchCapture):
    name = "competitor_web"

    def __init__(self, *args, renderer=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.renderer = renderer

    def collect(self):
        with self.queue.lifecycle() as lifecycle:
            for index, page in enumerate(self.settings["pages"]):
                if index >= collection_limit(self.config, "max_pages"):
                    self.source(f"web:{page['url']}", time_basis="observed_at")
                    self.coverage.limited(f"web:{page['url']}")
                    continue
                self._page(lifecycle, page)
        return self.receipt()

    def _fetch(self, url, hosts):
        return fetch_public(url, allowed_hosts=hosts,
            timeout=collection_limit(self.config, "request_timeout_seconds"),
            max_bytes=int(collection_limit(self.config, "max_response_bytes")),
            max_redirects=int(self.settings["max_redirects"]), user_agent=self.settings["user_agent"])

    def _page(self, lifecycle, page):
        url = page["url"]
        source = f"web:{url}"
        summary = self.source(source, time_basis="observed_at")
        hosts = set(page.get("redirect_hosts", [])) | {urlsplit(url).hostname}
        try:
            robots_url = f"https://{urlsplit(url).hostname}/robots.txt"
            robots = self._fetch(robots_url, hosts)
            if robots["status"] == 200:
                rules = RobotFileParser()
                rules.parse(robots["body"].decode("utf-8", errors="replace").splitlines())
                if not rules.can_fetch(self.settings["user_agent"], url):
                    self.coverage.fail(source, "robots_refused")
                    return
            elif robots["status"] != 404:
                self.coverage.fail(source, f"robots_http_{robots['status']}")
                return
            response = self._fetch(url, hosts)
            if response["status"] != 200:
                self.coverage.fail(source, f"http_{response['status']}")
                return
            if "html" not in (response["headers"].get("Content-Type") or "").lower():
                self.coverage.fail(source, "unsupported_content_type")
                return
            html = response["body"].decode("utf-8", errors="replace")
            method = "http"
            if page.get("render"):
                if self.renderer is None:
                    self.coverage.fail(source, "browser_unavailable")
                    return
                # The Cissa renderer owns network/subresource isolation and
                # browser limits; this is not an unrestricted Playwright handle.
                html = self.renderer(url, allowed_hosts=hosts, budgets=dict(self.config.get("collection", {})))
                if not isinstance(html, str) or len(html.encode()) > collection_limit(self.config, "max_response_bytes"):
                    raise ValueError("Rendered page exceeds response budget")
                method = "browser"
            parser = PageText()
            parser.feed(html)
            body = "\n".join(parser.text).strip()
            if not body:
                self.coverage.fail(source, "empty_or_dynamic_content")
                return
            digest = hashlib.sha256(body.encode()).hexdigest()
            key = "competitor_snapshot:" + hashlib.sha256(url.encode()).hexdigest()
            previous = self.db.get_automation_state(key) or {}
            observed = datetime.now(timezone.utc).isoformat()
            raw = {"url": url, "final_url": response["url"], "status": response["status"],
                   "headers": response["headers"], "redirects": response["redirects"],
                   "html": html, "method": method, "text_hash": digest,
                   "declared_publication_time": parser.published_at}
            self.raw_page(source, observed, raw)
            summary.update(observed_count=1, changed=previous.get("text_hash") != digest,
                           previous_hash=previous.get("text_hash"), current_hash=digest,
                           capture_method=method, observed_at=observed,
                           declared_publication_time=parser.published_at)
            if previous.get("text_hash") != digest:
                self.capture(lifecycle, source=source, native_id=url, uri=url, timestamp=observed,
                    title=parser.title or page.get("name", url), text=body, raw=raw, filter_window=False)
            self.db.upsert_automation_state(key, {"text_hash": digest, "observed_at": observed, "method": method})
        except Exception as exc:
            self.coverage.fail(source, type(exc).__name__)


class PageText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text, self.title, self.published_at = [], "", None
        self.ignored, self.in_title = 0, False

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "noscript", "template"}:
            self.ignored += 1
        if tag == "title":
            self.in_title = True
        values = dict(attrs)
        if tag == "meta" and values.get("property") == "article:published_time":
            self.published_at = values.get("content")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript", "template"}:
            self.ignored = max(0, self.ignored - 1)
        if tag == "title":
            self.in_title = False

    def handle_data(self, data):
        if not self.ignored and data.strip():
            value = " ".join(data.split())
            if self.in_title:
                self.title += value
            else:
                self.text.append(value)
