"""
Document Processor Base - Unified base class for document processing
Consolidates patterns between ArXiv, PDF, and README processors
"""

import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.data_models import Tweet, ProcessingStats
from core.config import config
from core.download_tracker import get_download_tracker
from core.staged_assets import (
    StagedAssetPublisher,
    StagedAssetValidationError,
    validate_existing_asset,
)

logger = logging.getLogger(__name__)


class DocumentLink:
    """Base class for document links (PDFs, ArXiv papers, READMEs, etc.)"""
    
    def __init__(self, url: str, title: str, document_type: str, 
                 filename: Optional[str] = None, downloaded: bool = False):
        self.url = url
        self.title = title
        self.document_type = document_type  # 'arxiv', 'pdf', 'readme'
        self.filename = filename
        self.downloaded = downloaded
        self.metadata: Dict[str, Any] = {}
    
    def __repr__(self):
        return f"{self.__class__.__name__}(url='{self.url}', title='{self.title}', downloaded={self.downloaded})"


class DocumentProcessor(ABC):
    """Abstract base class for document processors"""
    
    def __init__(self, output_dir: str = None):
        self.output_dir = Path(output_dir) if output_dir else None
        self.download_tracker = get_download_tracker()
        self.asset_publisher = StagedAssetPublisher(config)
        
        # Setup requests session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Updated parameter name
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    @abstractmethod
    def extract_urls_from_tweet(self, tweet: Tweet) -> List[str]:
        """Extract relevant URLs from a tweet"""
        pass
    
    @abstractmethod
    def download_document(self, url: str, tweet_id: str, resume: bool = True) -> Optional[DocumentLink]:
        """Download a document and return a DocumentLink"""
        pass
    
    @abstractmethod
    def extract_metadata(self, document_link: DocumentLink) -> Dict[str, Any]:
        """Extract metadata from a downloaded document"""
        pass
    
    def process_tweets(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Process tweets to find and download documents"""
        stats = ProcessingStats()
        
        for tweet in tweets:
            try:
                # Extract URLs from tweet
                urls = self.extract_urls_from_tweet(tweet)
                if not urls:
                    stats.skipped += 1
                    continue
                
                # Process each URL
                document_links = []
                for url in urls:
                    try:
                        doc_link = self.download_document(url, tweet.id, resume)
                        if doc_link:
                            document_links.append(doc_link)
                    except Exception as e:
                        logger.error(f"Failed to download {url} for tweet {tweet.id}: {e}")
                        stats.errors += 1
                
                # Add document links to tweet
                if document_links:
                    self._attach_documents_to_tweet(tweet, document_links)
                    stats.updated += 1
                else:
                    stats.skipped += 1
                
                stats.total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing tweet {tweet.id}: {e}")
                stats.errors += 1
        
        return stats
    
    def _attach_documents_to_tweet(self, tweet: Tweet, document_links: List[DocumentLink]):
        """Attach document links to a tweet (to be overridden by subclasses)"""
        # Default implementation - subclasses should override
        if not hasattr(tweet, 'document_links'):
            tweet.document_links = []
        tweet.document_links.extend(document_links)
    
    def _create_safe_filename(self, url: str, title: str = None, extension: str = None) -> str:
        """Create a safe filename from URL and title"""
        if title:
            # Use title if provided
            filename = re.sub(r'[<>:"/\\|?*]', '_', title)
        else:
            # Extract from URL
            filename = url.split('/')[-1].split('?')[0]
            if not filename:
                filename = "document"
        
        # Ensure extension
        if extension and not filename.lower().endswith(extension.lower()):
            filename = f"{filename}.{extension.lstrip('.')}"
        
        # Limit filename length
        if len(filename) > 200:
            name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
            filename = f"{name[:190]}{'.' + ext if ext else ''}"
        
        return filename
    
    def _download_file(self, url: str, filepath: Path, timeout: int = 30) -> bool:
        """Download a file from URL to filepath"""
        asset_type = "pdf" if filepath.suffix.lower() == ".pdf" else "binary"
        try:
            # Check if already downloaded and resume is enabled
            if filepath.exists():
                if validate_existing_asset(filepath, asset_type=asset_type):
                    logger.debug(f"File already exists: {filepath}")
                    return True
                logger.warning(f"Existing asset failed validation and will be replaced: {filepath}")
            
            # Create directory if it doesn't exist
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Download file
            logger.debug(f"Downloading {url} to {filepath}")
            response = self.session.get(url, timeout=timeout, stream=True)
            try:
                response.raise_for_status()
                published = self.asset_publisher.publish_chunks(
                    filepath,
                    response.iter_content(chunk_size=8192),
                    asset_type=asset_type,
                )
            finally:
                response.close()

            logger.info(f"Successfully downloaded: {filepath}")
            try:
                self.download_tracker.record_success(
                    url,
                    filepath.name,
                    str(filepath),
                    published.size_bytes,
                )
            except Exception:
                pass
            return True
                
        except requests.exceptions.RequestException as e:
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            if status_code == 404:
                logger.warning(f"Document not found (404): {url}")
                self.download_tracker.record_404(url, str(e))
            else:
                logger.error(f"Download failed: {url} - {e}")
                self.download_tracker.record_error(url, str(e))
            return False
        except StagedAssetValidationError as e:
            logger.error(f"Downloaded asset failed validation: {url} - {e}")
            self.download_tracker.record_error(url, str(e))
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            self.download_tracker.record_error(url, str(e))
            return False
    
    def _extract_urls_from_text_and_mappings(self, tweet: Tweet, patterns: List[str]) -> List[str]:
        """Extract URLs from tweet text and URL mappings using regex patterns"""
        urls = set()
        
        # Check tweet text
        text = tweet.full_text or ""
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            urls.update(matches)
        
        # Check URL mappings for expanded URLs
        if hasattr(tweet, 'url_mappings') and tweet.url_mappings:
            for mapping in tweet.url_mappings:
                if mapping.expanded_url:
                    for pattern in patterns:
                        if re.search(pattern, mapping.expanded_url, re.IGNORECASE):
                            urls.add(mapping.expanded_url)
        
        return list(urls)
    
    def get_stats_summary(self, stats: ProcessingStats, document_type: str) -> str:
        """Generate a formatted stats summary"""
        return (f"{document_type} processing: {stats.updated} processed, "
                f"{stats.skipped} skipped, {stats.errors} errors")


# Common URL patterns for different document types
URL_PATTERNS = {
    'arxiv': [
        r'https?://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5})',
        r'https?://arxiv\.org/pdf/([0-9]{4}\.[0-9]{4,5})(?:\.pdf)?'
    ],
    'pdf': [
        r'https?://[^\s]+\.pdf(?:\?[^\s]*)?'
    ],
    'github': [
        r'https?://github\.com/[^/\s]+/[^/\s]+(?:/[^\s]*)?'
    ],
    'huggingface': [
        r'https?://huggingface\.co/[^/\s]+/[^/\s]+(?:/[^\s]*)?'
    ]
}
