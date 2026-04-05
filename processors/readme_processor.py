"""
README Processor - Unified repository README processor using DocumentProcessor base
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
        name='documents.github_readmes',
        config_path='documents.github_readmes',
        description='Download README files from GitHub repositories.',
        processor='READMEProcessor',
        capabilities=('documents', 'readme', 'github'),
        config_keys=('paths.vault_dir', 'processing.documents.concurrent_workers')
    ),
    PipelineStage(
        name='documents.huggingface_readmes',
        config_path='documents.huggingface_readmes',
        description='Download README files from HuggingFace repositories.',
        processor='READMEProcessor',
        capabilities=('documents', 'readme', 'huggingface'),
        config_keys=('paths.vault_dir', 'processing.documents.concurrent_workers')
    )
)


register_pipeline_stages(*PIPELINE_STAGES)


class RepositoryREADME(DocumentLink):
    """Repository README with metadata"""
    
    def __init__(self, url: str, repo_name: str, platform: str,
                 filename: Optional[str] = None, downloaded: bool = False):
        super().__init__(url, repo_name, 'readme', filename, downloaded)
        self.repo_name = repo_name
        self.platform = platform  # 'github' or 'huggingface'
        self.readme_url = self._build_readme_url(url)
        self.llm_summary = ""
        self.language = ""
        self.topics = []
    
    def _build_readme_url(self, repo_url: str) -> str:
        """Build README URL from repository URL"""
        try:
            if self.platform == 'github':
                # Convert github.com/user/repo to raw.githubusercontent.com/user/repo/main/README.md
                match = re.search(r'github\.com/([^/]+/[^/]+)', repo_url)
                if match:
                    repo_path = match.group(1)
                    return f"https://raw.githubusercontent.com/{repo_path}/main/README.md"
            elif self.platform == 'huggingface':
                # Convert huggingface.co/user/repo to huggingface.co/user/repo/raw/main/README.md
                match = re.search(r'huggingface\.co/([^/]+/[^/]+)', repo_url)
                if match:
                    repo_path = match.group(1)
                    return f"https://huggingface.co/{repo_path}/raw/main/README.md"
            
            return repo_url
            
        except Exception as e:
            logger.debug(f"Could not build README URL from {repo_url}: {e}")
            return repo_url
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'url': self.url,
            'repo_name': self.repo_name,
            'platform': self.platform,
            'readme_url': self.readme_url,
            'filename': self.filename,
            'downloaded': self.downloaded,
            'llm_summary': self.llm_summary,
            'language': self.language,
            'topics': self.topics
        }


class READMEProcessor(DocumentProcessor):
    """Repository README processor using DocumentProcessor base"""
    
    def __init__(self, output_dir: str = None):
        # Unify to knowledge_vault/repos to match URL and pipeline processors
        self.readmes_dir = Path(output_dir or config.get('vault_dir', 'knowledge_vault')) / 'repos'
        super().__init__(self.readmes_dir)
    
    def extract_urls_from_tweet(self, tweet: Tweet) -> List[str]:
        """Extract repository URLs from tweet"""
        github_urls = self._extract_urls_from_text_and_mappings(tweet, URL_PATTERNS['github'])
        hf_urls = self._extract_urls_from_text_and_mappings(tweet, URL_PATTERNS['huggingface'])
        
        return github_urls + hf_urls
    
    def download_document(self, url: str, tweet_id: str, resume: bool = True) -> Optional[RepositoryREADME]:
        """Download repository README"""
        try:
            # Determine platform and extract repo name
            platform = self._determine_platform(url)
            repo_name = self._extract_repo_name(url, platform)
            
            if not repo_name:
                logger.warning(f"Could not extract repository name from URL: {url}")
                return None
            
            # Create README object
            readme = RepositoryREADME(url, repo_name, platform)
            
            # Create safe filename with platform prefix to match unified convention
            safe_repo_name = repo_name.replace('/', '_')
            if platform == 'github':
                filename = f"github_{safe_repo_name}_README.md"
            elif platform == 'huggingface':
                filename = f"hf_{safe_repo_name}_README.md"
            else:
                filename = f"{safe_repo_name}_README.md"
            readme.filename = filename
            
            # Download README if not resuming or if file doesn't exist
            readme_path = self.readmes_dir / filename
            if not resume or not readme_path.exists():
                success = self._download_file(readme.readme_url, readme_path)
                readme.downloaded = success
                
                # Extract basic metadata from README content
                if success and readme_path.exists():
                    self._extract_readme_metadata(readme, readme_path)
            else:
                readme.downloaded = readme_path.exists()
                if readme_path.exists():
                    self._extract_readme_metadata(readme, readme_path)
                logger.debug(f"Skipping existing README: {filename}")
            
            return readme
            
        except Exception as e:
            logger.error(f"Error processing repository README {url}: {e}")
            return None
    
    def extract_metadata(self, document_link: RepositoryREADME) -> Dict[str, Any]:
        """Extract metadata from repository README"""
        return document_link.to_dict()
    
    def _attach_documents_to_tweet(self, tweet: Tweet, document_links: List[RepositoryREADME]):
        """Attach repository READMEs to tweet"""
        if not hasattr(tweet, 'repo_links'):
            tweet.repo_links = []
        tweet.repo_links.extend(document_links)
    
    def _determine_platform(self, url: str) -> str:
        """Determine repository platform from URL"""
        url_lower = url.lower()
        if 'github.com' in url_lower:
            return 'github'
        elif 'huggingface.co' in url_lower:
            return 'huggingface'
        else:
            return 'unknown'
    
    def _extract_repo_name(self, url: str, platform: str) -> Optional[str]:
        """Extract repository name (owner/repo) from URL"""
        try:
            if platform == 'github':
                match = re.search(r'github\.com/([^/]+/[^/\?#]+)', url)
            elif platform == 'huggingface':
                match = re.search(r'huggingface\.co/([^/]+/[^/\?#]+)', url)
            else:
                return None
            
            if match:
                repo_path = match.group(1)
                # Handle URLs with additional path components
                parts = repo_path.split('/')
                if len(parts) >= 2:
                    return f"{parts[0]}/{parts[1]}"
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not extract repo name from {url}: {e}")
            return None
    
    def _extract_readme_metadata(self, readme: RepositoryREADME, readme_path: Path):
        """Extract metadata from README content"""
        try:
            with open(readme_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract language from badges or content
            language_patterns = [
                r'!\[.*?\]\(.*?shields\.io.*?language-([^-\)]+)',
                r'```(\w+)',  # Code blocks
                r'language:\s*(\w+)'  # YAML frontmatter
            ]
            
            for pattern in language_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    readme.language = matches[0].lower()
                    break
            
            # Extract topics from badges or headers
            topic_patterns = [
                r'!\[([^\]]+)\]\(.*?shields\.io',
                r'##\s*([^#\n]+)',
                r'#\s*([^#\n]+)'
            ]
            
            topics = set()
            for pattern in topic_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches[:5]:  # Limit to first 5
                    clean_topic = re.sub(r'[^\w\s]', '', match.strip().lower())
                    if clean_topic and len(clean_topic) > 2:
                        topics.add(clean_topic)
            
            readme.topics = list(topics)[:10]  # Limit to 10 topics
            
        except Exception as e:
            logger.debug(f"Could not extract metadata from README {readme_path}: {e}")
