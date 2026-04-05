"""
Shared Download Session - Reusable HTTP session for efficient downloads
Provides connection pooling and consistent configuration across processors
"""

import logging
import requests
from typing import Optional, Dict, Any
from pathlib import Path

from .config import config

logger = logging.getLogger(__name__)


class SharedDownloadSession:
    """Shared HTTP session with connection pooling and consistent configuration"""
    
    def __init__(self):
        self._session: Optional[requests.Session] = None
        self._setup_session()
    
    def _setup_session(self):
        """Setup the shared session with optimized configuration"""
        self._session = requests.Session()
        
        # Configure headers
        user_agent = config.get_download_setting('user_agent', 'Mozilla/5.0 (compatible; Thoth/2.0)')
        self._session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Configure connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)
        
        logger.debug("Shared download session initialized with connection pooling")
    
    @property
    def session(self) -> requests.Session:
        """Get the shared session, creating it if needed"""
        if self._session is None:
            self._setup_session()
        return self._session
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make a GET request with default timeout and error handling"""
        timeout = config.get_download_setting('timeout_seconds', 30)
        kwargs.setdefault('timeout', timeout)
        kwargs.setdefault('allow_redirects', True)
        
        return self.session.get(url, **kwargs)
    
    def head(self, url: str, **kwargs) -> requests.Response:
        """Make a HEAD request with default timeout"""
        timeout = config.get_download_setting('timeout_seconds', 30)
        kwargs.setdefault('timeout', timeout)
        kwargs.setdefault('allow_redirects', True)
        
        return self.session.head(url, **kwargs)
    
    def download_file(self, url: str, target_path: Path, **kwargs) -> bool:
        """Download a file to the specified path with retry logic"""
        retry_attempts = config.get_download_setting('retry_attempts', 3)
        
        for attempt in range(retry_attempts):
            try:
                logger.debug(f"Downloading {url} to {target_path} (attempt {attempt + 1})")
                
                response = self.get(url, stream=True, **kwargs)
                response.raise_for_status()
                
                # Ensure target directory exists
                target_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write file in chunks
                with open(target_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.debug(f"Successfully downloaded {target_path.name} ({target_path.stat().st_size} bytes)")
                return True
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retry_attempts - 1:
                    logger.error(f"Failed to download {url} after {retry_attempts} attempts")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error downloading {url}: {e}")
                return False
        
        return False
    
    def check_url_availability(self, url: str) -> Dict[str, Any]:
        """Check if a URL is available and get basic metadata"""
        try:
            response = self.head(url)
            
            result = {
                'available': response.status_code == 200,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': response.headers.get('content-length'),
                'last_modified': response.headers.get('last-modified'),
                'error': None
            }
            
            # Convert content length to int if available
            if result['content_length']:
                try:
                    result['content_length'] = int(result['content_length'])
                except ValueError:
                    result['content_length'] = None
            
            return result
            
        except requests.exceptions.RequestException as e:
            return {
                'available': False,
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                'content_type': '',
                'content_length': None,
                'last_modified': None,
                'error': str(e)
            }
    
    def close(self):
        """Close the session and clean up resources"""
        if self._session:
            self._session.close()
            self._session = None
            logger.debug("Shared download session closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Global shared session instance
download_session = SharedDownloadSession()


def get_download_session() -> SharedDownloadSession:
    """Get the global download session instance"""
    return download_session
