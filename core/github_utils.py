"""
GitHub Utilities - Handle GitHub URLs for raw file access
Converts GitHub blob URLs to raw URLs for direct file downloads
"""

import re
import logging
from urllib.parse import urlparse, urlunparse
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class GitHubUrlConverter:
    """Handles conversion of GitHub URLs to raw download URLs"""
    
    # Pattern to match GitHub blob URLs
    GITHUB_BLOB_PATTERN = re.compile(
        r'https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)'
    )
    
    # Pattern to match GitHub tree URLs (for directories)
    GITHUB_TREE_PATTERN = re.compile(
        r'https://github\.com/([^/]+)/([^/]+)/tree/([^/]+)/(.+)'
    )
    
    @classmethod
    def is_github_url(cls, url: str) -> bool:
        """Check if URL is a GitHub URL"""
        return 'github.com' in url.lower()
    
    @classmethod
    def is_github_blob_url(cls, url: str) -> bool:
        """Check if URL is a GitHub blob URL (viewable file)"""
        return bool(cls.GITHUB_BLOB_PATTERN.match(url))
    
    @classmethod
    def is_github_tree_url(cls, url: str) -> bool:
        """Check if URL is a GitHub tree URL (directory)"""
        return bool(cls.GITHUB_TREE_PATTERN.match(url))
    
    @classmethod
    def convert_to_raw_url(cls, url: str) -> Optional[str]:
        """
        Convert GitHub blob URL to raw URL for direct download
        
        Examples:
        https://github.com/user/repo/blob/main/file.pdf
        -> https://raw.githubusercontent.com/user/repo/refs/heads/main/file.pdf
        
        https://github.com/MoonshotAI/Kimi-k1.5/blob/main/Kimi_k1.5.pdf
        -> https://raw.githubusercontent.com/MoonshotAI/Kimi-k1.5/refs/heads/main/Kimi_k1.5.pdf
        """
        if not cls.is_github_blob_url(url):
            return None
        
        match = cls.GITHUB_BLOB_PATTERN.match(url)
        if not match:
            return None
        
        user, repo, ref, file_path = match.groups()
        
        # Convert to raw URL format
        raw_url = f"https://raw.githubusercontent.com/{user}/{repo}/refs/heads/{ref}/{file_path}"
        
        logger.debug(f"Converted GitHub URL: {url} -> {raw_url}")
        return raw_url
    
    @classmethod
    def get_repo_info(cls, url: str) -> Optional[Tuple[str, str, str]]:
        """
        Extract repository information from GitHub URL
        Returns: (user, repo, ref) or None
        """
        match = cls.GITHUB_BLOB_PATTERN.match(url) or cls.GITHUB_TREE_PATTERN.match(url)
        if match:
            user, repo, ref, _ = match.groups()
            return user, repo, ref
        return None
    
    @classmethod
    def get_file_path(cls, url: str) -> Optional[str]:
        """Extract file path from GitHub blob URL"""
        match = cls.GITHUB_BLOB_PATTERN.match(url)
        if match:
            return match.groups()[3]  # file_path
        return None
    
    @classmethod
    def build_raw_url(cls, user: str, repo: str, ref: str, file_path: str) -> str:
        """Build raw GitHub URL from components"""
        return f"https://raw.githubusercontent.com/{user}/{repo}/refs/heads/{ref}/{file_path}"
    
    @classmethod
    def build_readme_url(cls, user: str, repo: str, ref: str = "main") -> str:
        """Build URL for README.md file"""
        return cls.build_raw_url(user, repo, ref, "README.md")
    
    @classmethod
    def get_readme_from_repo_url(cls, repo_url: str) -> Optional[str]:
        """
        Get README.md URL from a repository URL
        
        Examples:
        https://github.com/user/repo -> README.md raw URL
        https://github.com/user/repo/blob/main/file.pdf -> README.md raw URL for same repo
        """
        repo_info = cls.get_repo_info(repo_url)
        if not repo_info:
            # Try to parse as basic repo URL
            match = re.match(r'https://github\.com/([^/]+)/([^/]+)/?', repo_url)
            if match:
                user, repo = match.groups()
                return cls.build_readme_url(user, repo)
            return None
        
        user, repo, ref = repo_info
        return cls.build_readme_url(user, repo, ref)


def convert_github_url_for_download(url: str) -> str:
    """
    Convert GitHub URL for direct download if needed
    Returns the original URL if not a GitHub blob URL
    """
    converter = GitHubUrlConverter()
    
    if converter.is_github_blob_url(url):
        raw_url = converter.convert_to_raw_url(url)
        if raw_url:
            logger.info(f"Converted GitHub URL for download: {url} -> {raw_url}")
            return raw_url
    
    return url


def get_github_readme_url(github_url: str) -> Optional[str]:
    """
    Get README.md URL from any GitHub URL
    Useful for future README download functionality
    """
    converter = GitHubUrlConverter()
    return converter.get_readme_from_repo_url(github_url)