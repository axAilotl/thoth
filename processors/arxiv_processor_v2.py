"""
ArXiv Processor V2 - Enhanced ArXiv processor using unified DocumentProcessor base
"""

import json
import logging
import re
from pathlib import Path
from typing import List, Optional, Dict, Any
import xml.etree.ElementTree as ET
import requests

from core.data_models import Tweet
from core.config import config
from core.pipeline_registry import PipelineStage, register_pipeline_stages
from .document_processor import DocumentProcessor, DocumentLink, URL_PATTERNS

logger = logging.getLogger(__name__)


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
            
            # Get metadata from ArXiv API
            metadata = self._fetch_arxiv_metadata(arxiv_id)
            if metadata:
                paper.title = metadata.get('title', f"ArXiv Paper {arxiv_id}")
                paper.abstract = metadata.get('abstract', '')
                paper.authors = metadata.get('authors', [])
                paper.categories = metadata.get('categories', [])
            else:
                paper.title = f"ArXiv Paper {arxiv_id}"
            
            # Create hyphenated slug for title (no spaces), max 80 chars
            title_clean = paper.title.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            title_clean = ''.join(ch for ch in title_clean if ord(ch) >= 32)
            title_clean = re.sub(r'\s+', ' ', title_clean).strip().lower()
            # Replace non-alnum with hyphens, collapse multiples
            slug = re.sub(r'[^a-z0-9]+', '-', title_clean)
            slug = slug.strip('-')[:80]
            if not slug:
                slug = 'paper'
            filename = f"{arxiv_id}-{slug}.pdf"
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
                        vault_dir = Path(config.get('vault_dir', 'knowledge_vault'))
                        try:
                            rel_path = pdf_path.relative_to(vault_dir)
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
                # Migrate legacy id.pdf if present
                legacy_path = self.papers_dir / f"{arxiv_id}.pdf"
                if legacy_path.exists() and not pdf_path.exists():
                    try:
                        legacy_path.rename(pdf_path)
                        paper.downloaded = True
                        logger.info(f"Renamed legacy ArXiv file {legacy_path.name} -> {filename}")
                        # Upsert DB for renamed file
                        try:
                            if config.get('database.enabled', False):
                                from core.metadata_db import get_metadata_db, FileMetadata, DownloadMetadata
                                from datetime import datetime
                                db = get_metadata_db()
                                vault_dir = Path(config.get('vault_dir', 'knowledge_vault'))
                                try:
                                    rel_path = pdf_path.relative_to(vault_dir)
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
                        logger.warning(f"Failed to rename legacy ArXiv file {legacy_path.name}: {e}")
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
                                vault_dir = Path(config.get('vault_dir', 'knowledge_vault'))
                                try:
                                    rel_path = pdf_path.relative_to(vault_dir)
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
    
    def _fetch_arxiv_metadata(self, arxiv_id: str) -> Optional[Dict[str, Any]]:
        """Fetch metadata from ArXiv API"""
        try:
            api_url = f"https://export.arxiv.org/api/query?id_list={arxiv_id}"
            response = self.session.get(api_url, timeout=10)
            response.raise_for_status()

            root = ET.fromstring(response.text)
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
            
        except Exception as e:
            logger.error(f"Failed to fetch ArXiv metadata for {arxiv_id}: {e}")
            return None

    def _xml_text(self, element: Optional[ET.Element]) -> str:
        """Return normalized text from an XML element."""
        if element is None or element.text is None:
            return ""
        return " ".join(element.text.split())
