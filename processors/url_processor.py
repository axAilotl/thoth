"""
URL Processor - Handles URL expansion and validation
Applies GraphQL URL mappings to tweet content and downloads PDFs
"""

import logging
import requests
import hashlib
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import List, Dict, NamedTuple, Optional
from core.data_models import Tweet, ProcessingStats
from core.config import config
from core.download_tracker import get_download_tracker
from core.github_utils import convert_github_url_for_download
from core.path_layout import resolve_vault_root
from core.pipeline_registry import PipelineStage, register_pipeline_stages
from core.staged_assets import (
    StagedAssetPublisher,
    StagedAssetValidationError,
    validate_existing_asset,
)

logger = logging.getLogger(__name__)


PIPELINE_STAGES = (
    PipelineStage(
        name='url_expansion',
        config_path='url_expansion',
        description='Expand shortened URLs using cached GraphQL mappings.',
        processor='URLProcessor',
        capabilities=('url_expansion',),
        config_keys=('paths.vault_dir', 'database.enabled')
    ),
)


register_pipeline_stages(*PIPELINE_STAGES)


class PDFLink(NamedTuple):
    """PDF link metadata"""
    url: str
    title: str
    filename: Optional[str] = None
    downloaded: bool = False




class URLProcessor:
    """Processes and expands URLs in tweet content and downloads PDFs"""
    
    def __init__(self, pdfs_dir: str = None):
        self.url_cache: Dict[str, str] = {}
        
        vault_dir = resolve_vault_root(config, override=pdfs_dir)
        self.pdfs_dir = vault_dir / 'pdfs'
        self.pdfs_dir.mkdir(parents=True, exist_ok=True)
        
        self.repos_dir = vault_dir / 'repos'
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        
        # Session for HTTP requests
        # Use shared download session for better connection pooling
        from core.download_session import get_download_session
        self.download_session = get_download_session()
        
        # Download tracker
        self.download_tracker = get_download_tracker()
        self.asset_publisher = StagedAssetPublisher(config)
        
        # Metadata database
        if config.get('database.enabled', False):
            from core.metadata_db import get_metadata_db
            self.metadata_db = get_metadata_db()
        else:
            self.metadata_db = None
    
    def apply_url_expansions(self, tweets: List[Tweet], url_mappings: Dict[str, str]) -> ProcessingStats:
        """Apply URL expansions from GraphQL data to tweet content"""
        stats = ProcessingStats()
        
        for tweet in tweets:
            try:
                original_text = tweet.full_text
                if not original_text:
                    stats.skipped += 1
                    continue
                
                updated_text = original_text
                expansions_applied = 0
                
                # Apply URL mappings
                for short_url, expanded_url in url_mappings.items():
                    if short_url in updated_text:
                        updated_text = updated_text.replace(short_url, expanded_url)
                        expansions_applied += 1
                
                # Update tweet if changes were made
                if updated_text != original_text:
                    tweet.full_text = updated_text
                    
                    # Update URL mappings list
                    if not tweet.extracted_urls:
                        tweet.extracted_urls = []
                    
                    # Add expanded URLs to extracted URLs list
                    for short_url, expanded_url in url_mappings.items():
                        if short_url in original_text and expanded_url not in tweet.extracted_urls:
                            tweet.extracted_urls.append(expanded_url)
                            # Persist URL mapping in DB
                            if config.get('database.enabled', False):
                                try:
                                    from core.metadata_db import get_metadata_db
                                    db = get_metadata_db()
                                    db.upsert_url_mapping(short_url, expanded_url, tweet.id)
                                except Exception:
                                    pass
                    
                    stats.updated += 1
                else:
                    stats.skipped += 1
                
                stats.total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing URLs for tweet {tweet.id}: {e}")
                stats.errors += 1
        
        logger.info(f"🔗 URL processing complete: {stats.updated} tweets updated, {stats.skipped} skipped")
        return stats
    
    def extract_urls_from_graphql(self, graphql_enhanced_tweets: Dict[str, Dict]) -> Dict[str, str]:
        """Extract URL mappings from GraphQL enhanced tweet data"""
        url_mappings = {}
        
        for tweet_id, tweet_data in graphql_enhanced_tweets.items():
            urls = tweet_data.get('urls', [])
            for url_data in urls:
                short_url = url_data.get('url')
                expanded_url = url_data.get('expanded_url')
                if short_url and expanded_url and short_url != expanded_url:
                    url_mappings[short_url] = expanded_url
        
        logger.info(f"🔗 Extracted {len(url_mappings)} URL mappings from GraphQL data")
        return url_mappings
    
    def validate_urls(self, tweets: List[Tweet]) -> ProcessingStats:
        """Validate expanded URLs (placeholder for future implementation)"""
        stats = ProcessingStats()
        logger.info("🔍 URL validation - placeholder implementation")
        return stats
    
    def categorize_urls(self, tweets: List[Tweet]) -> Dict[str, List[str]]:
        """Categorize URLs by domain type"""
        categories = {
            'github': [],
            'arxiv': [],
            'huggingface': [],
            'youtube': [],
            'substack': [],
            'medium': [],
            'other': []
        }
        
        for tweet in tweets:
            if tweet.extracted_urls:
                for url in tweet.extracted_urls:
                    if 'github.com' in url:
                        categories['github'].append(url)
                    elif 'arxiv.org' in url:
                        categories['arxiv'].append(url)
                    elif 'huggingface.co' in url:
                        categories['huggingface'].append(url)
                    elif 'youtube.com' in url or 'youtu.be' in url:
                        categories['youtube'].append(url)
                    elif 'substack.com' in url:
                        categories['substack'].append(url)
                    elif 'medium.com' in url:
                        categories['medium'].append(url)
                    else:
                        categories['other'].append(url)
        
        return categories
    
    def process_pdf_links(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Find and download PDF links in tweets (excluding ArXiv PDFs)"""
        stats = ProcessingStats()
        
        for tweet in tweets:
            try:
                if not tweet.extracted_urls:
                    stats.skipped += 1
                    continue
                
                pdf_links = self._find_pdf_links(tweet.extracted_urls)
                if not pdf_links:
                    stats.skipped += 1
                    continue
                
                downloaded_pdfs = []
                for pdf_url in pdf_links:
                    # Check if we should skip this URL (404 or already downloaded)
                    if not self.download_tracker.should_download(pdf_url):
                        if self.download_tracker.is_404(pdf_url):
                            logger.debug(f"Skipping 404 PDF URL: {pdf_url}")
                        elif self.download_tracker.is_downloaded(pdf_url):
                            # Create PDFLink from existing download
                            existing_path = self.download_tracker.get_download_path(pdf_url)
                            if existing_path and Path(existing_path).exists():
                                title = self._extract_pdf_title(pdf_url) or "Downloaded PDF"
                                pdf_link = PDFLink(
                                    url=pdf_url,
                                    title=title,
                                    filename=Path(existing_path).name,
                                    downloaded=True
                                )
                                downloaded_pdfs.append(pdf_link)
                                logger.debug(f"Found existing PDF: {pdf_link.filename}")
                        stats.skipped += 1
                        continue
                    
                    pdf_link = self._process_pdf_link(pdf_url, tweet.id, resume)
                    if pdf_link:
                        downloaded_pdfs.append(pdf_link)
                        stats.updated += 1
                    else:
                        stats.errors += 1
                
                # Store PDFs in tweet for content processor
                if downloaded_pdfs:
                    if not hasattr(tweet, 'pdf_links'):
                        tweet.pdf_links = []
                    tweet.pdf_links.extend(downloaded_pdfs)
                
                stats.total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing PDF links for tweet {tweet.id}: {e}")
                stats.errors += 1
        
        logger.info(f"📄 PDF processing complete: {stats.updated} PDFs processed, {stats.skipped} tweets skipped")
        return stats

    def _process_pdf_link(self, pdf_url: str, tweet_id: str, resume: bool) -> Optional[PDFLink]:
        """Process a single PDF link with GitHub URL conversion and tracking"""
        try:
            # Convert GitHub URLs to raw format for direct download
            download_url = convert_github_url_for_download(pdf_url)
            
            # Record pending download
            self.download_tracker.record_pending(download_url)
            
            # Generate a unique filename
            url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:8]
            parsed_url = urlparse(pdf_url)  # Use original URL for filename extraction
            
            # Try to extract a meaningful filename from URL
            path_parts = parsed_url.path.split('/')
            original_filename = path_parts[-1] if path_parts[-1] else f"document_{url_hash}"
            
            # Ensure .pdf extension
            if not original_filename.lower().endswith('.pdf'):
                original_filename += '.pdf'
            
            # Clean filename for filesystem
            safe_filename = self._make_safe_filename(original_filename, url_hash)
            pdf_path = self.pdfs_dir / safe_filename
            
            # Skip if already processed and resume is enabled
            if resume and pdf_path.exists():
                if validate_existing_asset(pdf_path, asset_type="pdf"):
                    file_size = pdf_path.stat().st_size
                    title = self._extract_pdf_title(pdf_url) or original_filename
                    
                    # Record success in tracker
                    self.download_tracker.record_success(download_url, safe_filename, str(pdf_path), file_size)
                    logger.debug(f"PDF {safe_filename} already downloaded, skipping")
                    
                    # Upsert into metadata DB if enabled
                    try:
                        if self.metadata_db:
                            from datetime import datetime
                            from core.metadata_db import FileMetadata, DownloadMetadata
                            self.metadata_db.upsert_file(FileMetadata(
                                path=str(pdf_path.relative_to(Path.cwd())),
                                file_type="pdf",
                                size_bytes=file_size,
                                updated_at=datetime.now().isoformat(),
                                source_id=tweet_id
                            ))
                            self.metadata_db.upsert_download(DownloadMetadata(
                                url=pdf_url,
                                status="success",
                                target_path=str(pdf_path),
                                size_bytes=file_size
                            ))
                    except Exception as e:
                        logger.debug(f"Metadata DB upsert failed for PDF {pdf_url}: {e}")
                    
                    return PDFLink(
                        url=pdf_url,
                        title=title,
                        filename=safe_filename,
                        downloaded=True
                    )
                logger.warning(f"Existing PDF failed validation and will be replaced: {pdf_path}")
            
            # Try to get a better title from the URL or content
            title = self._extract_pdf_title(pdf_url) or original_filename
            
            # Download PDF using converted URL
            if self._download_pdf(download_url, pdf_path, pdf_url):
                file_size = pdf_path.stat().st_size
                
                # Record successful download
                self.download_tracker.record_success(download_url, safe_filename, str(pdf_path), file_size)
                logger.debug(f"Downloaded PDF: {safe_filename}")

                # Upsert into metadata DB if enabled
                try:
                    if self.metadata_db:
                        from datetime import datetime
                        from core.metadata_db import FileMetadata, DownloadMetadata
                        self.metadata_db.upsert_file(FileMetadata(
                            path=str(pdf_path.relative_to(Path.cwd())),
                            file_type="pdf",
                            size_bytes=file_size,
                            updated_at=datetime.now().isoformat(),
                            source_id=tweet_id
                        ))
                        self.metadata_db.upsert_download(DownloadMetadata(
                            url=pdf_url,
                            status="success",
                            target_path=str(pdf_path),
                            size_bytes=file_size
                        ))
                except Exception as e:
                    logger.debug(f"Metadata DB upsert failed for PDF {pdf_url}: {e}")
                
                return PDFLink(
                    url=pdf_url,
                    title=title,
                    filename=safe_filename,
                    downloaded=True
                )
            else:
                logger.error(f"Failed to download PDF from {pdf_url}")
                try:
                    if self.metadata_db:
                        from core.metadata_db import DownloadMetadata
                        self.metadata_db.upsert_download(DownloadMetadata(
                            url=pdf_url,
                            status="error",
                            target_path=str(pdf_path) if 'pdf_path' in locals() else None,
                            error_msg="download failed"
                        ))
                except Exception as e:
                    logger.debug(f"Metadata DB upsert failed (error) for PDF {pdf_url}: {e}")
                return None
        except Exception as e:
            logger.error(f"Error processing PDF link {pdf_url}: {e}")
            try:
                if self.metadata_db:
                    from core.metadata_db import DownloadMetadata
                    self.metadata_db.upsert_download(DownloadMetadata(
                        url=pdf_url,
                        status="error",
                        target_path=str(pdf_path) if 'pdf_path' in locals() else None,
                        error_msg=str(e)
                    ))
            except Exception:
                pass
            return None
    
    def _make_safe_filename(self, filename: str, url_hash: str) -> str:
        """Create a filesystem-safe filename"""
        # Remove or replace unsafe characters
        import re
        # strip control characters and newlines
        filename = filename.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        filename = ''.join(ch for ch in filename if ord(ch) >= 32)
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        safe_filename = safe_filename.strip()
        
        # Ensure it's not too long
        if len(safe_filename) > 100:
            name, ext = safe_filename.rsplit('.', 1)
            safe_filename = f"{name[:90]}...{url_hash}.{ext}"
        
        return safe_filename
    
    def _extract_pdf_title(self, url: str) -> Optional[str]:
        """Try to extract a meaningful title from PDF URL"""
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        
        # Look for meaningful path components
        for part in reversed(path_parts):
            if part and not part.lower().endswith('.pdf'):
                # Clean up the part to make a nice title
                title = unquote(part).replace('_', ' ').replace('-', ' ')
                if len(title) > 5:  # Only use if it seems meaningful
                    return title.title()
        
        return None
    
    def _download_pdf(self, pdf_url: str, pdf_path: Path, original_url: str = None) -> bool:
        """Download PDF from URL with 404 tracking"""
        try:
            logger.debug(f"Downloading PDF: {pdf_url}")
            
            # First, do a HEAD request to check if it's actually a PDF
            head_response = self.download_session.head(pdf_url)
            
            # Check for 404 in HEAD request
            if head_response.status_code == 404:
                head_response.close()
                self.download_tracker.record_404(pdf_url, f"404 Not Found: {head_response.reason}")
                logger.warning(f"PDF URL returned 404: {pdf_url}")
                return False
            
            content_type = head_response.headers.get('content-type', '').lower()
            head_response.close()
            
            if 'application/pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                logger.warning(f"URL doesn't appear to be a PDF: {pdf_url} (Content-Type: {content_type})")
                # Continue anyway as some servers don't set proper content-type
            
            # Download the file
            response = self.download_session.get(pdf_url, timeout=60, stream=True)
            
            # Check for 404 in GET request
            if response.status_code == 404:
                response.close()
                self.download_tracker.record_404(pdf_url, f"404 Not Found: {response.reason}")
                logger.warning(f"PDF URL returned 404: {pdf_url}")
                return False
            
            try:
                response.raise_for_status()
                published = self.asset_publisher.publish_chunks(
                    pdf_path,
                    response.iter_content(chunk_size=8192),
                    asset_type="pdf",
                )
            finally:
                response.close()
            
            # Update metadata database
            if self.metadata_db:
                from core.metadata_db import DownloadMetadata, FileMetadata
                from datetime import datetime
                
                # Record successful download
                download_meta = DownloadMetadata(
                    url=pdf_url,
                    status="success",
                    target_path=str(pdf_path.relative_to(Path.cwd())),
                    size_bytes=published.size_bytes,
                    updated_at=datetime.now().isoformat()
                )
                self.metadata_db.upsert_download(download_meta)
                
                # Record file in index
                file_meta = FileMetadata(
                    path=str(pdf_path.relative_to(Path.cwd())),
                    file_type="pdf",
                    size_bytes=published.size_bytes,
                    updated_at=datetime.now().isoformat()
                )
                self.metadata_db.upsert_file(file_meta)
            
            logger.debug(f"Downloaded PDF: {pdf_path} ({published.size_bytes} bytes)")
            return True
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response and e.response.status_code == 404:
                self.download_tracker.record_404(pdf_url, str(e))
                logger.warning(f"PDF URL returned 404: {pdf_url}")
                status = "404"
            else:
                self.download_tracker.record_error(pdf_url, str(e))
                logger.error(f"Failed to download PDF {pdf_url}: {e}")
                status = "error"
            
            # Record failed download in metadata database
            if self.metadata_db:
                from core.metadata_db import DownloadMetadata
                from datetime import datetime
                
                download_meta = DownloadMetadata(
                    url=pdf_url,
                    status=status,
                    updated_at=datetime.now().isoformat(),
                    error_msg=str(e)
                )
                self.metadata_db.upsert_download(download_meta)
            
            return False
        except StagedAssetValidationError as e:
            self.download_tracker.record_error(pdf_url, str(e))
            logger.error(f"Failed to validate PDF {pdf_url}: {e}")

            if self.metadata_db:
                from core.metadata_db import DownloadMetadata
                from datetime import datetime

                download_meta = DownloadMetadata(
                    url=pdf_url,
                    status="error",
                    updated_at=datetime.now().isoformat(),
                    error_msg=str(e)
                )
                self.metadata_db.upsert_download(download_meta)

            return False
        except Exception as e:
            self.download_tracker.record_error(pdf_url, str(e))
            logger.error(f"Failed to download PDF {pdf_url}: {e}")
            
            # Record failed download in metadata database
            if self.metadata_db:
                from core.metadata_db import DownloadMetadata
                from datetime import datetime
                
                download_meta = DownloadMetadata(
                    url=pdf_url,
                    status="error",
                    updated_at=datetime.now().isoformat(),
                    error_msg=str(e)
                )
                self.metadata_db.upsert_download(download_meta)
            
            return False
    
    def get_statistics(self) -> Dict:
        """Get PDF processing statistics"""
        pdf_files = list(self.pdfs_dir.glob("*.pdf")) if self.pdfs_dir.exists() else []
        
        return {
            'total_pdfs': len(pdf_files),
            'pdfs_directory': str(self.pdfs_dir),
            'pdfs_size_mb': sum(f.stat().st_size for f in pdf_files if f.is_file()) / (1024*1024)
        }
