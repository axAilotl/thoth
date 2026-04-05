"""
Download Tracker - Comprehensive tracking system for all downloads
Tracks successful downloads, 404s, and failures to prevent re-attempts
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Iterable, List
from datetime import datetime
from dataclasses import dataclass, asdict
from core.config import config
from core.path_layout import build_path_layout

logger = logging.getLogger(__name__)


@dataclass
class DownloadRecord:
    """Record of a download attempt"""
    url: str
    status: str  # 'success', '404', 'error', 'pending'
    filename: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    error_message: Optional[str] = None
    timestamp: str = None
    attempts: int = 1
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


class DownloadTracker:
    """Manages download tracking across all file types (media, PDFs, etc.)"""
    
    def __init__(self, tracking_file: str = None):
        if tracking_file:
            self.tracking_file = Path(tracking_file)
        else:
            self.tracking_file = build_path_layout(config).download_tracking_file
        self._downloads: Dict[str, DownloadRecord] = {}
        self._load_tracking_data()
    
    def _load_tracking_data(self):
        """Load existing tracking data from JSON file"""
        if self.tracking_file.exists():
            try:
                with open(self.tracking_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert dict records back to DownloadRecord objects
                for url, record_data in data.items():
                    self._downloads[url] = DownloadRecord(**record_data)
                
                logger.debug(f"Loaded {len(self._downloads)} download records")
                
            except Exception as e:
                logger.error(f"Error loading download tracking data: {e}")
                self._downloads = {}
    
    def _save_tracking_data(self):
        """Save tracking data to JSON file"""
        try:
            # Convert DownloadRecord objects to dicts
            data = {url: asdict(record) for url, record in self._downloads.items()}
            
            with open(self.tracking_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            logger.debug(f"Saved {len(self._downloads)} download records")
            
        except Exception as e:
            logger.error(f"Error saving download tracking data: {e}")
    
    def should_download(self, url: str) -> bool:
        """Check if a URL should be downloaded (not already successful or 404)"""
        if url not in self._downloads:
            return True
        
        record = self._downloads[url]
        
        # Don't download if successful or 404
        if record.status in ['success', '404']:
            return False
        
        # Allow retry for errors (but track attempts)
        return True
    
    def is_404(self, url: str) -> bool:
        """Check if a URL is known to return 404"""
        record = self._downloads.get(url)
        return record is not None and record.status == '404'
    
    def is_downloaded(self, url: str) -> bool:
        """Check if a URL was successfully downloaded"""
        record = self._downloads.get(url)
        return record is not None and record.status == 'success'
    
    def get_download_path(self, url: str) -> Optional[str]:
        """Get the file path for a successfully downloaded URL"""
        record = self._downloads.get(url)
        if record and record.status == 'success':
            return record.file_path
        return None

    def find_by_file_path(self, file_path: str) -> Optional[DownloadRecord]:
        """Find a download record associated with a specific file path."""
        target = Path(file_path).resolve(strict=False)
        for record in self._downloads.values():
            if not record.file_path:
                continue
            record_path = Path(record.file_path).resolve(strict=False)
            if record_path == target:
                return record
        return None

    def iter_records(self) -> Iterable[DownloadRecord]:
        """Iterate over all download records."""
        return list(self._downloads.values())
    
    def record_success(self, url: str, filename: str, file_path: str, file_size: int):
        """Record a successful download"""
        record = self._downloads.get(url, DownloadRecord(url=url, status='success'))
        record.status = 'success'
        record.filename = filename
        record.file_path = str(file_path)
        record.file_size = file_size
        record.timestamp = datetime.now().isoformat()
        record.error_message = None
        
        self._downloads[url] = record
        self._save_tracking_data()
        
        logger.debug(f"Recorded successful download: {filename}")
    
    def record_404(self, url: str, error_message: str = None):
        """Record a 404 error"""
        record = self._downloads.get(url, DownloadRecord(url=url, status='404'))
        record.status = '404'
        record.error_message = error_message or "404 Not Found"
        record.timestamp = datetime.now().isoformat()
        
        self._downloads[url] = record
        self._save_tracking_data()
        
        logger.debug(f"Recorded 404 for URL: {url}")
    
    def record_error(self, url: str, error_message: str):
        """Record a download error (will allow retry)"""
        record = self._downloads.get(url, DownloadRecord(url=url, status='error'))
        record.status = 'error'
        record.error_message = error_message
        record.timestamp = datetime.now().isoformat()
        record.attempts = record.attempts + 1 if hasattr(record, 'attempts') else 1
        
        self._downloads[url] = record
        self._save_tracking_data()
        
        logger.debug(f"Recorded error for URL: {url} (attempt {record.attempts})")
    
    def record_pending(self, url: str):
        """Record a pending download (started but not finished)"""
        if url not in self._downloads:
            record = DownloadRecord(url=url, status='pending')
            self._downloads[url] = record
            self._save_tracking_data()
    
    def get_stats(self) -> Dict[str, int]:
        """Get download statistics"""
        stats = {
            'total_tracked': len(self._downloads),
            'successful': 0,
            '404_errors': 0,
            'other_errors': 0,
            'pending': 0
        }
        
        for record in self._downloads.values():
            if record.status == 'success':
                stats['successful'] += 1
            elif record.status == '404':
                stats['404_errors'] += 1
            elif record.status == 'error':
                stats['other_errors'] += 1
            elif record.status == 'pending':
                stats['pending'] += 1
        
        return stats
    
    def get_404_urls(self) -> List[str]:
        """Get list of URLs that returned 404"""
        return [url for url, record in self._downloads.items() if record.status == '404']
    
    def get_failed_urls(self) -> List[str]:
        """Get list of URLs that failed (excluding 404s)"""
        return [url for url, record in self._downloads.items() if record.status == 'error']
    
    def cleanup_old_errors(self, days: int = 7):
        """Clean up old error records to allow retry after specified days"""
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        urls_to_remove = []
        for url, record in self._downloads.items():
            if record.status == 'error':
                record_date = datetime.fromisoformat(record.timestamp).timestamp()
                if record_date < cutoff_date:
                    urls_to_remove.append(url)
        
        for url in urls_to_remove:
            del self._downloads[url]
            logger.debug(f"Cleaned up old error record for: {url}")
        
        if urls_to_remove:
            self._save_tracking_data()
            logger.info(f"Cleaned up {len(urls_to_remove)} old error records")


# Global tracker instance
_global_tracker = None

def get_download_tracker() -> DownloadTracker:
    """Get global download tracker instance"""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = DownloadTracker()
    return _global_tracker
