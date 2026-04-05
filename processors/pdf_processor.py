"""
PDF Processor - Unified PDF document processor using DocumentProcessor base
"""

import logging
import re
from pathlib import Path
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

from core.data_models import Tweet
from core.config import config
from core.pipeline_registry import PipelineStage, register_pipeline_stages
from .document_processor import DocumentProcessor, DocumentLink, URL_PATTERNS

logger = logging.getLogger(__name__)


PIPELINE_STAGES = (
    PipelineStage(
        name='documents.general_pdfs',
        config_path='documents.general_pdfs',
        description='Download non-arXiv PDF documents referenced in tweets.',
        processor='PDFProcessor',
        capabilities=('documents', 'pdf'),
        config_keys=('paths.vault_dir', 'processing.documents.concurrent_workers')
    ),
)


register_pipeline_stages(*PIPELINE_STAGES)


class PDFDocument(DocumentLink):
    """PDF document with metadata"""
    
    def __init__(self, url: str, title: str, filename: Optional[str] = None, downloaded: bool = False):
        super().__init__(url, title, 'pdf', filename, downloaded)
        self.size_bytes = 0
        self.source_domain = ""
        
        # Extract domain from URL
        try:
            parsed = urlparse(url)
            self.source_domain = parsed.netloc
        except Exception:
            self.source_domain = "unknown"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'url': self.url,
            'title': self.title,
            'filename': self.filename,
            'downloaded': self.downloaded,
            'size_bytes': self.size_bytes,
            'source_domain': self.source_domain
        }


class PDFProcessor(DocumentProcessor):
    """PDF processor using DocumentProcessor base"""
    
    def __init__(self, output_dir: str = None):
        self.pdfs_dir = Path(output_dir or config.get('vault_dir', 'knowledge_vault')) / 'pdfs'
        super().__init__(self.pdfs_dir)
    
    def extract_urls_from_tweet(self, tweet: Tweet) -> List[str]:
        """Extract PDF URLs from tweet"""
        # Exclude ArXiv PDFs as they're handled by ArXivProcessor
        pdf_urls = self._extract_urls_from_text_and_mappings(tweet, URL_PATTERNS['pdf'])
        
        # Filter out ArXiv URLs
        non_arxiv_pdfs = []
        for url in pdf_urls:
            if 'arxiv.org' not in url.lower():
                non_arxiv_pdfs.append(url)
        
        return non_arxiv_pdfs
    
    def download_document(self, url: str, tweet_id: str, resume: bool = True) -> Optional[PDFDocument]:
        """Download PDF document"""
        try:
            # Extract title from URL
            title = self._extract_title_from_url(url)
            
            # Create PDF document object
            pdf_doc = PDFDocument(url, title)
            
            # Create safe filename
            filename = self._create_safe_filename(url, title, 'pdf')
            pdf_doc.filename = filename
            
            # Download PDF if not resuming or if file doesn't exist
            pdf_path = self.pdfs_dir / filename
            if not resume or not pdf_path.exists():
                success = self._download_file(url, pdf_path)
                pdf_doc.downloaded = success
                
                # Get file size if downloaded
                if success and pdf_path.exists():
                    pdf_doc.size_bytes = pdf_path.stat().st_size
            else:
                pdf_doc.downloaded = pdf_path.exists()
                if pdf_path.exists():
                    pdf_doc.size_bytes = pdf_path.stat().st_size
                logger.debug(f"Skipping existing PDF: {filename}")
            
            return pdf_doc
            
        except Exception as e:
            logger.error(f"Error processing PDF {url}: {e}")
            return None
    
    def extract_metadata(self, document_link: PDFDocument) -> Dict[str, Any]:
        """Extract metadata from PDF document"""
        return document_link.to_dict()
    
    def _attach_documents_to_tweet(self, tweet: Tweet, document_links: List[PDFDocument]):
        """Attach PDF documents to tweet"""
        if not hasattr(tweet, 'pdf_links'):
            tweet.pdf_links = []
        tweet.pdf_links.extend(document_links)
    
    def _extract_title_from_url(self, url: str) -> str:
        """Extract title from PDF URL"""
        try:
            # Try to extract meaningful title from URL path
            path = urlparse(url).path
            filename = path.split('/')[-1]
            
            # Remove .pdf extension and decode URL encoding
            title = filename.replace('.pdf', '').replace('_', ' ').replace('-', ' ')
            
            # Clean up and capitalize
            title = re.sub(r'[^\w\s]', ' ', title)
            title = ' '.join(word.capitalize() for word in title.split() if word)
            
            if len(title) < 5:  # If title too short, use domain
                domain = urlparse(url).netloc
                title = f"PDF from {domain}"
            
            return title or "PDF Document"
            
        except Exception as e:
            logger.debug(f"Could not extract title from URL {url}: {e}")
            return "PDF Document"
