"""
GitHub Stars Processor - Fetches starred repositories and creates summaries
Downloads READMEs and generates LLM summaries for starred GitHub repositories
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

import requests

from core.data_models import ProcessingStats
from core.config import config
from core.llm_interface import LLMInterface
from core.metadata_db import MetadataDB
from core.non_live_state import (
    get_known_readme_filename,
    record_readme_probe_outcome,
    should_skip_readme_probe,
)
from .document_processor import DocumentProcessor, DocumentLink

logger = logging.getLogger(__name__)


@dataclass
class GitHubRepo:
    """GitHub repository data structure"""
    id: int
    name: str
    full_name: str
    description: str
    html_url: str
    stargazers_count: int
    forks_count: int
    language: Optional[str]
    topics: List[str]
    created_at: str
    updated_at: str
    pushed_at: str
    license_name: Optional[str]
    readme_content: Optional[str] = None
    llm_summary: Optional[str] = None
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'GitHubRepo':
        """Create GitHubRepo from GitHub API response"""
        return cls(
            id=data.get('id', 0),
            name=data.get('name', ''),
            full_name=data.get('full_name', ''),
            description=data.get('description') or '',
            html_url=data.get('html_url', ''),
            stargazers_count=data.get('stargazers_count', 0),
            forks_count=data.get('forks_count', 0),
            language=data.get('language'),
            topics=data.get('topics', []),
            created_at=data.get('created_at', ''),
            updated_at=data.get('updated_at', ''),
            pushed_at=data.get('pushed_at', ''),
            license_name=data.get('license', {}).get('name') if data.get('license') else None
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'id': self.id,
            'name': self.name,
            'full_name': self.full_name,
            'description': self.description,
            'html_url': self.html_url,
            'stargazers_count': self.stargazers_count,
            'forks_count': self.forks_count,
            'language': self.language,
            'topics': self.topics,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'pushed_at': self.pushed_at,
            'license_name': self.license_name,
            'readme_content': self.readme_content,
            'llm_summary': self.llm_summary
        }


class GitHubStarsProcessor:
    """Processes GitHub starred repositories"""
    
    def __init__(self, vault_path: str = None, *, metadata_db: MetadataDB | None = None):
        self.vault_path = Path(vault_path or config.get('vault_dir', 'knowledge_vault'))
        self.stars_dir = self.vault_path / 'stars'
        self.repos_dir = self.vault_path / 'repos'
        self.metadata_db = metadata_db or MetadataDB()
        
        # Create directories
        self.stars_dir.mkdir(parents=True, exist_ok=True)
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        
        # Get GitHub API key
        self.github_token = (
            config.get('sources.github.token')
            or os.getenv('GITHUB_API')
            or os.getenv('GITHUB_TOKEN')
        )
        if not self.github_token:
            raise ValueError("GITHUB_API, GITHUB_TOKEN, or sources.github.token is required")
        
        # Setup session with GitHub API headers
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {self.github_token}',
            'X-GitHub-Api-Version': '2022-11-28'
        })
        
        # Initialize LLM interface for summaries
        self.llm_interface = None
        llm_config = config.get('llm', {})
        if llm_config.get('tasks', {}).get('summary', {}).get('enabled', False):
            try:
                self.llm_interface = LLMInterface(llm_config)
                logger.info("LLM interface initialized for GitHub stars summaries")
            except Exception as e:
                logger.warning(f"Could not initialize LLM interface: {e}")
    
    async def fetch_and_process_starred_repos(self, limit: int = None, resume: bool = True) -> ProcessingStats:
        """Fetch starred repositories and process them"""
        stats = ProcessingStats()
        
        try:
            # Fetch starred repositories
            logger.info("📡 Fetching starred repositories from GitHub API...")
            repos = await self._fetch_starred_repos(limit)
            
            if not repos:
                logger.warning("No starred repositories found")
                return stats
            
            logger.info(f"📦 Found {len(repos)} starred repositories")
            
            # Process repositories
            for repo in repos:
                try:
                    processed = await self._process_single_repo(repo, resume)
                    if processed:
                        stats.updated += 1
                    else:
                        stats.skipped += 1
                    
                    stats.total_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing repo {repo.full_name}: {e}")
                    stats.errors += 1
            
            # Save repository index
            await self._save_repos_index(repos)
            
            logger.info(f"⭐ GitHub stars processing complete: {stats.updated} processed, {stats.skipped} skipped, {stats.errors} errors")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to fetch starred repositories: {e}")
            stats.errors = 1
            return stats
    
    async def _fetch_starred_repos(self, limit: int = None) -> List[GitHubRepo]:
        """Fetch starred repositories from GitHub API"""
        repos = []
        page = 1
        per_page = 100
        
        while True:
            try:
                response = self.session.get(
                    'https://api.github.com/user/starred',
                    params={
                        'page': page,
                        'per_page': per_page,
                        'sort': 'updated'
                    },
                    timeout=30
                )
                response.raise_for_status()
                
                batch_repos = response.json()
                if not batch_repos:
                    break
                
                # Convert to GitHubRepo objects
                for repo_data in batch_repos:
                    repo = GitHubRepo.from_api_response(repo_data)
                    repos.append(repo)
                
                logger.debug(f"Fetched page {page}: {len(batch_repos)} repositories")
                
                # Check if we've reached the limit
                if limit and len(repos) >= limit:
                    repos = repos[:limit]
                    break
                
                # Check if we've fetched all repositories
                if len(batch_repos) < per_page:
                    break
                
                page += 1
                
                # Rate limiting - GitHub allows 5000 requests/hour for authenticated requests
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error fetching starred repos page {page}: {e}")
                break
        
        return repos
    
    async def _process_single_repo(self, repo: GitHubRepo, resume: bool) -> bool:
        """Process a single repository"""
        try:
            summary_file, readme_file = self._repo_paths(repo.full_name)
            
            cached_readme_exists = (
                readme_file.exists() and readme_file.is_file() and readme_file.stat().st_size > 0
            )
            cached_summary_exists = (
                summary_file.exists() and summary_file.is_file() and summary_file.stat().st_size > 0
            )

            if resume and cached_readme_exists:
                repo.readme_content = self._load_cached_readme(readme_file)
                if cached_summary_exists:
                    logger.debug(f"Skipping existing GitHub README and summary: {repo.full_name}")
                    return False

                if repo.readme_content:
                    if self.llm_interface:
                        summary = await self._generate_repo_summary(repo)
                        repo.llm_summary = summary
                    await self._create_summary_file(repo, summary_file)
                    logger.info(f"📝 Backfilled GitHub summary from cached README: {repo.full_name}")
                    return True
                logger.warning(f"Cached GitHub README was unreadable and will be re-fetched: {readme_file}")

            if resume and should_skip_readme_probe(
                self.metadata_db,
                provider="github",
                repo_name=repo.full_name,
                repo_revision=repo.updated_at or repo.pushed_at,
            ):
                logger.debug(f"Skipping cached missing GitHub README probe: {repo.full_name}")
                return False
            
            # Download README
            known_filename = get_known_readme_filename(
                self.metadata_db,
                provider="github",
                repo_name=repo.full_name,
                repo_revision=repo.updated_at or repo.pushed_at,
            )
            download_result = await self._download_readme(repo, preferred_filename=known_filename)
            if download_result:
                readme_content, downloaded_filename = download_result
                repo.readme_content = readme_content
                
                # Save README to repos folder
                with open(readme_file, 'w', encoding='utf-8') as f:
                    f.write(readme_content)
                record_readme_probe_outcome(
                    self.metadata_db,
                    provider="github",
                    repo_name=repo.full_name,
                    repo_revision=repo.updated_at or repo.pushed_at,
                    found=True,
                    filename=downloaded_filename,
                )
                
                # Generate LLM summary if available
                if self.llm_interface:
                    summary = await self._generate_repo_summary(repo)
                    repo.llm_summary = summary
                
                # Create summary markdown file
                await self._create_summary_file(repo, summary_file)
                
                logger.info(f"✅ Processed repo: {repo.full_name} ({repo.stargazers_count} ⭐)")
                return True
            else:
                record_readme_probe_outcome(
                    self.metadata_db,
                    provider="github",
                    repo_name=repo.full_name,
                    repo_revision=repo.updated_at or repo.pushed_at,
                    found=False,
                )
                logger.warning(f"Could not download README for {repo.full_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing repo {repo.full_name}: {e}")
            raise

    def _repo_paths(self, repo_name: str) -> tuple[Path, Path]:
        safe_name = repo_name.replace('/', '_')
        return (
            self.stars_dir / f"{safe_name}_summary.md",
            self.repos_dir / f"github_{safe_name}_README.md",
        )

    def _load_cached_readme(self, readme_file: Path) -> Optional[str]:
        """Load a cached README from disk for local summary backfill."""
        try:
            with open(readme_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            return content.strip() or None
        except Exception as e:
            logger.warning(f"Failed to read cached GitHub README {readme_file}: {e}")
            return None
    
    async def _download_readme(
        self,
        repo: GitHubRepo,
        *,
        preferred_filename: Optional[str] = None,
    ) -> Optional[tuple[str, str]]:
        """Download README content for a repository"""
        try:
            readme_files: List[str] = []
            if preferred_filename:
                readme_files.append(preferred_filename)
            readme_files.extend(['README.md', 'README.rst', 'README.txt', 'README'])
            unique_readme_files = list(dict.fromkeys(readme_files))
            
            for readme_file in unique_readme_files:
                try:
                    url = f"https://api.github.com/repos/{repo.full_name}/contents/{readme_file}"
                    response = self.session.get(url, timeout=30)
                    
                    if response.status_code == 200:
                        content_data = response.json()
                        
                        # GitHub API returns base64 encoded content
                        if content_data.get('encoding') == 'base64':
                            import base64
                            content = base64.b64decode(content_data['content']).decode('utf-8')
                            logger.debug(f"Downloaded README for {repo.full_name}")
                            return content, readme_file
                
                except Exception as e:
                    logger.debug(f"Could not download {readme_file} for {repo.full_name}: {e}")
                    continue
            
            # If API download fails, try raw download
            try:
                raw_url = f"https://raw.githubusercontent.com/{repo.full_name}/main/README.md"
                response = self.session.get(raw_url, timeout=30)
                if response.status_code == 200:
                    return response.text, "README.md"
                
                # Try master branch
                raw_url = f"https://raw.githubusercontent.com/{repo.full_name}/master/README.md"
                response = self.session.get(raw_url, timeout=30)
                if response.status_code == 200:
                    return response.text, "README.md"
                    
            except Exception as e:
                logger.debug(f"Raw README download failed for {repo.full_name}: {e}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error downloading README for {repo.full_name}: {e}")
            return None
    
    async def _generate_repo_summary(self, repo: GitHubRepo) -> Optional[str]:
        """Generate LLM summary for repository"""
        try:
            if not self.llm_interface or not repo.readme_content:
                return None
            
            # Create prompt for repository summarization
            prompt_template = config.get('llm.prompts.readme', 
                "Provide a concise 2-3 sentence summary of this GitHub repository, focusing on what the project does and its key features.")
            
            context = f"""
Repository: {repo.full_name}
Description: {repo.description}
Language: {repo.language or 'Unknown'}
Stars: {repo.stargazers_count}
Topics: {', '.join(repo.topics) if repo.topics else 'None'}

README Content:
{repo.readme_content[:3000]}...  # Limit content to prevent token overflow
"""
            
            summary = await self.llm_interface.summarize_content(context, "readme")
            if summary and not summary.startswith("Summary unavailable"):
                return summary.strip()
            logger.warning(f"LLM summary generation failed for {repo.full_name}")
            return None
                
        except Exception as e:
            logger.error(f"Error generating summary for {repo.full_name}: {e}")
            return None
    
    async def _create_summary_file(self, repo: GitHubRepo, summary_file: Path):
        """Create summary markdown file"""
        try:
            # Create comprehensive summary markdown
            content = f"""---
type: github_star
repo_id: {repo.id}
full_name: {repo.full_name}
stars: {repo.stargazers_count}
forks: {repo.forks_count}
language: {repo.language or 'Unknown'}
created_at: {repo.created_at}
updated_at: {repo.updated_at}
processed_at: {datetime.now().isoformat()}
---

# ⭐ {repo.full_name}

## 📊 Repository Stats
- **Stars**: {repo.stargazers_count} ⭐
- **Forks**: {repo.forks_count} 🍴
- **Language**: {repo.language or 'Unknown'}
- **License**: {repo.license_name or 'Unknown'}

## 📝 Description
{repo.description or 'No description provided'}

## 🏷️ Topics
{', '.join([f'`{topic}`' for topic in repo.topics]) if repo.topics else 'No topics'}

## 🔗 Links
- **Repository**: [{repo.full_name}]({repo.html_url})
- **README**: [[github_{repo.full_name.replace('/', '_')}_README.md]]

"""

            # Add LLM summary if available
            if repo.llm_summary:
                content += f"""## 🤖 AI Summary
{repo.llm_summary}

"""

            # Add tags
            tags = [f"#{tag.replace('-', '_').replace(' ', '_').lower()}" for tag in repo.topics[:5]]
            tags.extend([f"#github", f"#starred", f"#{repo.language.lower()}" if repo.language else "#unknown"])
            
            if repo.stargazers_count >= 10000:
                tags.append("#popular")
            elif repo.stargazers_count >= 1000:
                tags.append("#trending")
            
            content += ' '.join(tags)
            
            # Write summary file
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.debug(f"Created summary file: {summary_file}")
            
        except Exception as e:
            logger.error(f"Error creating summary file for {repo.full_name}: {e}")
            raise
    
    async def _save_repos_index(self, repos: List[GitHubRepo]):
        """Save repository index for reference"""
        try:
            index_file = self.stars_dir / "starred_repos_index.json"
            
            index_data = {
                "generated_at": datetime.now().isoformat(),
                "total_repos": len(repos),
                "repositories": [repo.to_dict() for repo in repos]
            }
            
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, indent=2, default=str)
            
            logger.info(f"💾 Saved repository index: {index_file}")
            
        except Exception as e:
            logger.error(f"Error saving repository index: {e}")
    
    def get_stats_summary(self, stats: ProcessingStats) -> str:
        """Generate formatted stats summary"""
        return (f"GitHub Stars processing: {stats.updated} processed, "
                f"{stats.skipped} skipped, {stats.errors} errors")
