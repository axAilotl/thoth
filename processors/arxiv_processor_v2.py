"""
ArXiv Processor V2 - Enhanced ArXiv processor using unified DocumentProcessor base
"""

import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import List, Optional, Dict, Any
import xml.etree.ElementTree as ET
import requests

from core.data_models import Tweet
from core.config import config
from core.pdf_text import PDFTextExtractionError, extract_pdf_title
from core.pipeline_registry import PipelineStage, register_pipeline_stages
from .document_processor import DocumentProcessor, DocumentLink, URL_PATTERNS

logger = logging.getLogger(__name__)

ARXIV_METADATA_TIMEOUT_SECONDS = 20.0
ARXIV_METADATA_MIN_INTERVAL_SECONDS = 3.0
ARXIV_METADATA_MAX_ATTEMPTS = 4
ARXIV_METADATA_BACKOFF_BASE_SECONDS = 10.0
ARXIV_METADATA_MAX_BACKOFF_SECONDS = 60.0
ARXIV_METADATA_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

_ARXIV_METADATA_PACING_LOCK = threading.Lock()
_ARXIV_METADATA_LAST_REQUEST_AT = 0.0


def _pace_arxiv_metadata_request() -> None:
    """Space arXiv API calls out across threads to stay under the rate limit."""
    global _ARXIV_METADATA_LAST_REQUEST_AT
    with _ARXIV_METADATA_PACING_LOCK:
        now = time.monotonic()
        wait = ARXIV_METADATA_MIN_INTERVAL_SECONDS - (now - _ARXIV_METADATA_LAST_REQUEST_AT)
        if wait > 0:
            time.sleep(wait)
        _ARXIV_METADATA_LAST_REQUEST_AT = time.monotonic()


def _arxiv_metadata_retry_delay(response: Optional[requests.Response], delay: float) -> float:
    """Honor Retry-After when arXiv sends one, otherwise grow the backoff."""
    if response is not None:
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return min(max(float(retry_after), 1.0), ARXIV_METADATA_MAX_BACKOFF_SECONDS)
            except ValueError:
                pass
    return min(max(delay, 1.0), ARXIV_METADATA_MAX_BACKOFF_SECONDS)


PIPELINE_STAGES = (
    PipelineStage(
        name='documents.arxiv_papers',
        config_path='documents.arxiv_papers',
        description='Download arXiv PDFs and metadata.',
        processor='ArXivProcessorV2',
        capabilities=('documents', 'arxiv'),
        config_keys=('paths.vault_dir', 'processing.documents.concurrent_workers', 'database.enabled')
    ),
)


register_pipeline_stages(*PIPELINE_STAGES)


class ArXivPaper(DocumentLink):
    """ArXiv paper with metadata"""

    def __init__(self, url: str, title: str, arxiv_id: str,
                 filename: Optional[str] = None, downloaded: bool = False):
        super().__init__(url, title, 'arxiv', filename, downloaded)
        self.arxiv_id = arxiv_id
        self.abs_url = f"https://arxiv.org/abs/{arxiv_id}"
        self.pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
        self.abstract = ""
        self.authors = []
        self.categories = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'url': self.url,
            'title': self.title,
            'arxiv_id': self.arxiv_id,
            'abs_url': self.abs_url,
            'pdf_url': self.pdf_url,
            'abstract': self.abstract,
            'authors': self.authors,
            'categories': self.categories,
            'filename': self.filename,
            'downloaded': self.downloaded
        }


class ArXivProcessorV2(DocumentProcessor):
    """Enhanced ArXiv processor using DocumentProcessor base"""

    def __init__(self, output_dir: str = None):
        self.papers_dir = Path(output_dir or config.get('vault_dir', 'knowledge_vault')) / 'papers'
        super().__init__(self.papers_dir)

    def extract_urls_from_tweet(self, tweet: Tweet) -> List[str]:
        """Extract ArXiv URLs from tweet"""
        urls = self._extract_urls_from_text_and_mappings(tweet, URL_PATTERNS['arxiv'])
        # Also detect bare arXiv IDs and convert to abs URLs
        text = tweet.full_text or ""
        bare_ids = re.findall(r'\b(\d{4}\.\d{4,5}v?\d*)\b', text)
        for arxiv_id in bare_ids:
            abs_url = f"https://arxiv.org/abs/{arxiv_id}"
            if abs_url not in urls:
                urls.append(abs_url)
        return urls

    def download_document(self, url: str, tweet_id: str, resume: bool = True) -> Optional[ArXivPaper]:
        """Download ArXiv paper and extract metadata"""
        try:
            # Extract ArXiv ID from URL
            arxiv_id = self._extract_arxiv_id(url)
            if not arxiv_id:
                logger.warning(f"Could not extract ArXiv ID from URL: {url}")
                return None

            # Create paper object
            paper = ArXivPaper(url, "", arxiv_id)
            legacy_route_path = self._find_legacy_route_path(arxiv_id=arxiv_id)

            # Get metadata from ArXiv API
            metadata = self._fetch_arxiv_metadata(arxiv_id)
            metadata_title = None
            if metadata:
                metadata_title = metadata.get('title')
                paper.title = metadata_title or f"ArXiv Paper {arxiv_id}"
                paper.abstract = metadata.get('abstract', '')
                paper.authors = metadata.get('authors', [])
                paper.categories = metadata.get('categories', [])
            else:
                paper.title = f"ArXiv Paper {arxiv_id}"

            paper.title = self._resolve_paper_title(
                arxiv_id=arxiv_id,
                metadata_title=metadata_title,
                candidate_paths=(legacy_route_path,),
            )

            filename = self._build_filename(arxiv_id=arxiv_id, title=paper.title)
            paper.filename = filename

            # Download PDF if not resuming or if file doesn't exist
            pdf_path = self.papers_dir / filename
            if resume and pdf_path.exists():
                paper.downloaded = True
                logger.debug(f"Skipping existing ArXiv paper: {filename}")
                # Upsert DB
                try:
                    if config.get('database.enabled', False):
                        from core.metadata_db import get_metadata_db, FileMetadata, DownloadMetadata
                        from datetime import datetime
                        db = get_metadata_db()
                        try:
                            rel_path = pdf_path.relative_to(self.papers_dir.parent)
                        except Exception:
                            rel_path = pdf_path
                        size_bytes = pdf_path.stat().st_size
                        db.upsert_file(FileMetadata(
                            path=str(rel_path),
                            file_type="pdf",
                            size_bytes=size_bytes,
                            updated_at=datetime.now().isoformat(),
                            source_id=tweet_id
                        ))
                        db.upsert_download(DownloadMetadata(
                            url=paper.pdf_url,
                            status="success",
                            target_path=str(rel_path),
                            size_bytes=size_bytes
                        ))
                except Exception:
                    pass
            else:
                if (
                    legacy_route_path is not None
                    and legacy_route_path.exists()
                    and not pdf_path.exists()
                ):
                    try:
                        legacy_route_path.rename(pdf_path)
                        paper.downloaded = True
                        logger.info(f"Renamed legacy ArXiv file {legacy_route_path.name} -> {filename}")
                        # Rename the old index row before upserting the canonical path.
                        # Reversing this order collides with the path primary key and
                        # leaves the stale legacy row behind.
                        self._rename_db_file_entry(
                            old_path=legacy_route_path,
                            new_path=pdf_path,
                            tweet_id=tweet_id,
                        )
                        # Upsert DB for renamed file
                        try:
                            if config.get('database.enabled', False):
                                from core.metadata_db import get_metadata_db, FileMetadata, DownloadMetadata
                                from datetime import datetime
                                db = get_metadata_db()
                                try:
                                    rel_path = pdf_path.relative_to(self.papers_dir.parent)
                                except Exception:
                                    rel_path = pdf_path
                                size_bytes = pdf_path.stat().st_size
                                db.upsert_file(FileMetadata(
                                    path=str(rel_path),
                                    file_type="pdf",
                                    size_bytes=size_bytes,
                                    updated_at=datetime.now().isoformat(),
                                    source_id=tweet_id
                                ))
                                db.upsert_download(DownloadMetadata(
                                    url=paper.pdf_url,
                                    status="success",
                                    target_path=str(rel_path),
                                    size_bytes=size_bytes
                                ))
                        except Exception:
                            pass
                    except Exception as e:
                        logger.warning(f"Failed to rename legacy ArXiv file {legacy_route_path.name}: {e}")
                        paper.downloaded = False
                if not paper.downloaded:
                    success = self._download_file(paper.pdf_url, pdf_path)
                    paper.downloaded = success
                    if success:
                        # Upsert DB for new download
                        try:
                            if config.get('database.enabled', False):
                                from core.metadata_db import get_metadata_db, FileMetadata, DownloadMetadata
                                from datetime import datetime
                                db = get_metadata_db()
                                try:
                                    rel_path = pdf_path.relative_to(self.papers_dir.parent)
                                except Exception:
                                    rel_path = pdf_path
                                size_bytes = pdf_path.stat().st_size
                                db.upsert_file(FileMetadata(
                                    path=str(rel_path),
                                    file_type="pdf",
                                    size_bytes=size_bytes,
                                    updated_at=datetime.now().isoformat(),
                                    source_id=tweet_id
                                ))
                                db.upsert_download(DownloadMetadata(
                                    url=paper.pdf_url,
                                    status="success",
                                    target_path=str(rel_path),
                                    size_bytes=size_bytes
                                ))
                        except Exception:
                            pass

            return paper

        except Exception as e:
            logger.error(f"Error processing ArXiv paper {url}: {e}")
            return None

    def extract_metadata(self, document_link: ArXivPaper) -> Dict[str, Any]:
        """Extract metadata from ArXiv paper"""
        return document_link.to_dict()

    def _attach_documents_to_tweet(self, tweet: Tweet, document_links: List[ArXivPaper]):
        """Attach ArXiv papers to tweet"""
        if not hasattr(tweet, 'arxiv_papers'):
            tweet.arxiv_papers = []
        tweet.arxiv_papers.extend(document_links)

    def _extract_arxiv_id(self, url: str) -> Optional[str]:
        """Extract ArXiv ID from URL"""
        patterns = [
            r'arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}v?\d*)',
            r'arxiv\.org/pdf/([0-9]{4}\.[0-9]{4,5}v?\d*)',
            r'^(?P<id>[0-9]{4}\.[0-9]{4,5}v?\d*)$'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1) if match.lastindex else match.group('id')

        return None

    def _parse_arxiv_metadata(self, body: str) -> Optional[Dict[str, Any]]:
        """Parse an arXiv Atom response into metadata fields."""
        root = ET.fromstring(body)
        namespaces = {
            "atom": "http://www.w3.org/2005/Atom",
            "arxiv": "http://arxiv.org/schemas/atom",
        }
        entry = root.find("atom:entry", namespaces)
        if entry is None:
            return None

        title = self._xml_text(entry.find("atom:title", namespaces))
        abstract = self._xml_text(entry.find("atom:summary", namespaces))
        authors = [
            self._xml_text(author)
            for author in entry.findall("atom:author/atom:name", namespaces)
            if self._xml_text(author)
        ]
        categories = [
            category.get("term")
            for category in entry.findall("atom:category", namespaces)
            if category.get("term")
        ]

        return {
            'title': title,
            'abstract': abstract,
            'authors': authors,
            'categories': categories
        }

    def _fetch_arxiv_metadata(self, arxiv_id: str) -> Optional[Dict[str, Any]]:
        """Fetch metadata from ArXiv API, paced and retried on throttling.

        A discovery burst can queue hundreds of papers at once, and arXiv
        answers with 429s long before the burst drains.  Requests are spaced
        globally and retried with exponential backoff instead of dropping the
        metadata for every throttled paper.
        """
        api_url = f"https://export.arxiv.org/api/query?id_list={arxiv_id}"
        delay = ARXIV_METADATA_BACKOFF_BASE_SECONDS
        last_error: Optional[Exception] = None
        for attempt in range(1, ARXIV_METADATA_MAX_ATTEMPTS + 1):
            _pace_arxiv_metadata_request()
            try:
                response = self.session.get(
                    api_url, timeout=ARXIV_METADATA_TIMEOUT_SECONDS
                )
                response.raise_for_status()
                return self._parse_arxiv_metadata(response.text)
            except requests.HTTPError as exc:
                last_error = exc
                status = exc.response.status_code if exc.response is not None else None
                if status not in ARXIV_METADATA_RETRY_STATUSES:
                    break
                delay = _arxiv_metadata_retry_delay(exc.response, delay)
            except requests.RequestException as exc:
                last_error = exc
                delay = _arxiv_metadata_retry_delay(None, delay)
            except ET.ParseError as exc:
                last_error = exc
                break
            if attempt < ARXIV_METADATA_MAX_ATTEMPTS:
                logger.warning(
                    "ArXiv metadata request for %s failed (attempt %s/%s): %s; retrying in %.1fs",
                    arxiv_id, attempt, ARXIV_METADATA_MAX_ATTEMPTS, last_error, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, ARXIV_METADATA_MAX_BACKOFF_SECONDS)
        logger.error(f"Failed to fetch ArXiv metadata for {arxiv_id}: {last_error}")
        return None

    def _xml_text(self, element: Optional[ET.Element]) -> str:
        """Return normalized text from an XML element."""
        if element is None or element.text is None:
            return ""
        return " ".join(element.text.split())

    def _find_legacy_query_title_path(self, *, arxiv_id: str) -> Optional[Path]:
        """Return the old query-title filename for an arXiv id, if present."""
        matches = sorted(self.papers_dir.glob(f"{arxiv_id}-arxiv-query-*.pdf"))
        if not matches:
            return None
        return matches[0]

    def _find_legacy_generic_title_path(self, *, arxiv_id: str) -> Optional[Path]:
        """Return the old generic placeholder filename for an arXiv id, if present."""
        matches = sorted(self.papers_dir.glob(f"{arxiv_id}-arxiv-paper-*.pdf"))
        if not matches:
            return None
        return matches[0]

    def _find_legacy_route_path(self, *, arxiv_id: str) -> Optional[Path]:
        """Return a noncanonical existing file that should be normalized by PDF title."""
        candidates = (
            self._find_legacy_query_title_path(arxiv_id=arxiv_id),
            self._find_legacy_generic_title_path(arxiv_id=arxiv_id),
            self.papers_dir / f"{arxiv_id}.pdf",
        )
        for path in candidates:
            if path is not None and path.exists():
                return path
        return None

    def _build_filename(self, *, arxiv_id: str, title: str) -> str:
        """Build the canonical PDF filename for an ArXiv paper."""
        title_clean = title.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        title_clean = ''.join(ch for ch in title_clean if ord(ch) >= 32)
        title_clean = re.sub(r'\s+', ' ', title_clean).strip().lower()
        # Replace non-alnum with hyphens, collapse multiples
        slug = re.sub(r'[^a-z0-9]+', '-', title_clean)
        slug = slug.strip('-')[:80]
        if not slug:
            slug = 'paper'
        return f"{arxiv_id}-{slug}.pdf"

    def _resolve_paper_title(
        self,
        *,
        arxiv_id: str,
        metadata_title: Optional[str],
        candidate_paths: tuple[Optional[Path], ...],
    ) -> str:
        """Choose the best available paper title, preferring metadata then local PDF."""
        normalized_metadata_title = self._normalize_title(metadata_title)
        if normalized_metadata_title and not self._looks_like_query_title(normalized_metadata_title):
            return normalized_metadata_title

        for path in candidate_paths:
            if path is None or not path.exists():
                continue
            pdf_title = self._extract_title_from_pdf(path)
            if pdf_title:
                return pdf_title

        if normalized_metadata_title and not self._looks_like_query_title(normalized_metadata_title):
            return normalized_metadata_title
        return f"ArXiv Paper {arxiv_id}"

    def _normalize_title(self, title: Optional[str]) -> str:
        """Normalize title whitespace and control characters."""
        if not title:
            return ""
        cleaned = title.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        cleaned = ''.join(ch for ch in cleaned if ord(ch) >= 32)
        return re.sub(r'\s+', ' ', cleaned).strip()

    def _looks_like_query_title(self, title: str) -> bool:
        """Detect arXiv API/query titles that should never become filenames."""
        lowered = title.lower()
        return (
            lowered.startswith("arxiv query:")
            or "search_query=" in lowered
            or "id_list=" in lowered
            or "max_results=" in lowered
        )

    def _extract_title_from_pdf(self, path: Path) -> str:
        """Extract a plausible title from PDF metadata or first-page text."""
        try:
            return extract_pdf_title(path)
        except PDFTextExtractionError as exc:
            logger.warning("Failed to extract title from PDF %s: %s", path, exc)
            return ""

    def _rename_db_file_entry(self, *, old_path: Path, new_path: Path, tweet_id: str) -> None:
        """Keep the file index aligned with on-disk legacy filename migrations."""
        if not config.get('database.enabled', False):
            return
        from core.metadata_db import get_metadata_db

        try:
            db = get_metadata_db()
        except Exception as exc:
            logger.warning("Failed to update file index during legacy rename: %s", exc)
            return

        try:
            old_rel_path = old_path.relative_to(self.papers_dir.parent)
        except Exception:
            old_rel_path = old_path
        try:
            new_rel_path = new_path.relative_to(self.papers_dir.parent)
        except Exception:
            new_rel_path = new_path

        db.rename_file_entry(
            old_path=str(old_rel_path),
            new_path=str(new_rel_path),
            file_type="pdf",
            size_bytes=new_path.stat().st_size,
            source_id=tweet_id,
        )
