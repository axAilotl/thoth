"""
ArXiv Collector - Discovers research papers via ArXiv RSS/API.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.parse
from datetime import datetime
from typing import Any, List, Optional

import feedparser

from core.artifacts.paper import PaperArtifact
from core.metadata_db import MetadataDB, IngestionQueueEntry, get_metadata_db

logger = logging.getLogger(__name__)


class ArXivCollector:
    """Collector for research papers from ArXiv."""

    BASE_API_URL = "https://export.arxiv.org/api/query?"
    BASE_RSS_URL = "https://rss.arxiv.org/rss/"
    BASE_ATOM_URL = "https://rss.arxiv.org/atom/"

    def __init__(self, db: Optional[MetadataDB] = None):
        self.db = db or get_metadata_db()

    def discover_papers(
        self, 
        topics: List[str], 
        max_results: int = 50,
        relevance_threshold: float = 0.0 # Placeholder for future ML ranking
    ) -> List[PaperArtifact]:
        """
        Search ArXiv for papers matching topics and add to ingestion queue.
        
        Args:
            topics: List of keyword strings to search for
            max_results: Maximum papers to fetch per topic
            relevance_threshold: Minimum score to keep (placeholder)
            
        Returns:
            List of discovered PaperArtifact objects
        """
        discovered = []
        
        for topic in topics:
            logger.info(f"Searching ArXiv for topic: {topic}")
            
            # Construct query
            # Search in all fields: ti (title), au (author), abs (abstract)
            query = f'all:"{topic}"'
            params = {
                "search_query": query,
                "start": 0,
                "max_results": max_results,
                "sortBy": "submittedDate",
                "sortOrder": "descending"
            }
            
            url = self.BASE_API_URL + urllib.parse.urlencode(params)
            feed = feedparser.parse(url)

            for entry in feed.entries:
                paper = self._queue_paper_from_entry(entry, source="arxiv")
                if paper:
                    discovered.append(paper)
                    
        return discovered

    def scan_rss_feeds(
        self,
        categories: List[str],
        max_results: int = 2000,
        feed_format: str = "rss",
    ) -> List[PaperArtifact]:
        """Scan arXiv category feeds and queue newly discovered papers."""
        discovered = []
        feed_limit = min(max_results, 2000)
        base_url = self.BASE_RSS_URL if feed_format == "rss" else self.BASE_ATOM_URL

        for raw_category in categories:
            category = raw_category.strip()
            if not category:
                continue

            logger.info(f"Scanning arXiv {feed_format.upper()} feed for category: {category}")
            encoded_category = urllib.parse.quote(category, safe=".+")
            feed = feedparser.parse(f"{base_url}{encoded_category}")

            for entry in feed.entries[:feed_limit]:
                paper = self._queue_paper_from_entry(entry, source="arxiv_rss")
                if paper:
                    discovered.append(paper)

        return discovered

    def _queue_paper_from_entry(self, entry: Any, source: str) -> Optional[PaperArtifact]:
        """Convert a feed entry to a queued paper artifact."""
        artifact_id = self._extract_artifact_id(entry)
        if not artifact_id:
            logger.warning("Skipping arXiv entry without a recognizable paper id")
            return None

        if self.db.get_ingestion_entry(artifact_id):
            return None

        ingested_at = datetime.now().isoformat()
        paper = PaperArtifact(
            id=artifact_id,
            source_type="arxiv",
            raw_content=json.dumps(self._to_serializable(entry)),
            created_at=getattr(entry, "published", None) or getattr(entry, "updated", None) or ingested_at,
            ingested_at=ingested_at,
            title=self._clean_text(getattr(entry, "title", "")),
            authors=self._extract_authors(entry),
            abstract=self._clean_text(
                getattr(entry, "summary", None) or getattr(entry, "description", "")
            ),
            arxiv_id=artifact_id,
            pdf_url=self._extract_pdf_url(entry, artifact_id),
            relevance_score=1.0,
        )

        queue_entry = IngestionQueueEntry(
            artifact_id=paper.id,
            artifact_type="paper",
            source=source,
            payload_json=json.dumps(paper.to_dict()),
            created_at=paper.ingested_at,
            capabilities_json=json.dumps(list(paper.capabilities)),
        )

        if not self.db.upsert_ingestion_entry(queue_entry):
            return None

        logger.info(f"Queued ArXiv paper: {paper.title} ({paper.id})")
        return paper

    def _extract_artifact_id(self, entry: Any) -> Optional[str]:
        """Extract the arXiv identifier from common feed entry fields."""
        candidates = [
            getattr(entry, "id", None),
            getattr(entry, "link", None),
        ]
        for link in getattr(entry, "links", []) or []:
            candidates.append(getattr(link, "href", None))

        patterns = [
            r"/(?:abs|pdf)/([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)",
            r"^([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)$",
        ]

        for candidate in candidates:
            if not candidate:
                continue
            for pattern in patterns:
                match = re.search(pattern, str(candidate))
                if match:
                    return match.group(1)

        return None

    def _extract_authors(self, entry: Any) -> List[str]:
        """Normalize author names from Atom or RSS entries."""
        authors = []
        for author in getattr(entry, "authors", []) or []:
            name = getattr(author, "name", None)
            if name:
                authors.append(name)

        if authors:
            return authors

        author = getattr(entry, "author", None)
        if author:
            return [self._clean_text(author)]

        return []

    def _extract_pdf_url(self, entry: Any, artifact_id: str) -> str:
        """Return the PDF URL from entry links or derive it from the arXiv id."""
        for link in getattr(entry, "links", []) or []:
            href = getattr(link, "href", None)
            link_type = getattr(link, "type", None)
            if href and (link_type == "application/pdf" or href.endswith(".pdf")):
                return href

        return f"https://arxiv.org/pdf/{artifact_id}.pdf"

    def _clean_text(self, value: str) -> str:
        return " ".join((value or "").split())

    def _to_serializable(self, value: Any) -> Any:
        """Convert feedparser objects into JSON-serializable data."""
        if isinstance(value, dict):
            return {key: self._to_serializable(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._to_serializable(item) for item in value]
        if hasattr(value, "__dict__"):
            return {
                key: self._to_serializable(item)
                for key, item in vars(value).items()
                if not key.startswith("_")
            }
        return value
