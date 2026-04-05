"""
Filename Normalization Utilities - Standardized filename generation and migration
Ensures consistent filenames across all processors and provides migration tools
"""

import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse

from .config import config
from .download_tracker import get_download_tracker

logger = logging.getLogger(__name__)


class FilenameNormalizer:
    """Utilities for normalized filename generation and migration"""
    
    def __init__(self):
        self.patterns = self._load_naming_patterns()
    
    def _load_naming_patterns(self) -> Dict[str, str]:
        """Load naming patterns from configuration"""
        return {
            'tweet': config.get_naming_pattern('tweet') or 'tweets_{tweet_id}_{screen_name}.md',
            'thread': config.get_naming_pattern('thread') or 'thread_{thread_id}_{screen_name}.md',
            'media': config.get_naming_pattern('media') or '{tweet_id}_media_{post_num}_{file_num}.{ext}',
            'transcript_twitter': config.get_naming_pattern('transcript_twitter') or 'twitter_{tweet_id}_{screen_name}.md',
            'transcript_youtube': config.get_naming_pattern('transcript_youtube') or 'youtube_{video_id}_{sanitized_title}.md',
            'readme_github': config.get_naming_pattern('readme_github') or 'github_{owner}_{repo}_README.md',
            'readme_huggingface': config.get_naming_pattern('readme_huggingface') or 'hf_{owner}_{repo}_README.md'
        }
    
    def sanitize_filename(self, filename: str, max_length: int = 200) -> str:
        """Sanitize filename for filesystem compatibility"""
        # Remove or replace invalid characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Replace multiple underscores with single
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing dots and spaces
        sanitized = sanitized.strip('. ')
        
        # Truncate if too long, preserving extension
        if len(sanitized) > max_length:
            name, ext = self._split_filename(sanitized)
            if ext:
                max_name_length = max_length - len(ext) - 1
                sanitized = name[:max_name_length] + '.' + ext
            else:
                sanitized = sanitized[:max_length]
        
        return sanitized or 'unnamed'
    
    def _split_filename(self, filename: str) -> Tuple[str, str]:
        """Split filename into name and extension"""
        parts = filename.rsplit('.', 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return filename, ''
    
    def generate_tweet_filename(self, tweet_id: str, screen_name: str) -> str:
        """Generate normalized tweet filename"""
        pattern = self.patterns['tweet']
        filename = pattern.format(
            tweet_id=tweet_id,
            screen_name=self.sanitize_filename(screen_name, 50)
        )
        return self.sanitize_filename(filename)
    
    def generate_thread_filename(self, thread_id: str, screen_name: str) -> str:
        """Generate normalized thread filename"""
        pattern = self.patterns['thread']
        filename = pattern.format(
            thread_id=thread_id,
            screen_name=self.sanitize_filename(screen_name, 50)
        )
        return self.sanitize_filename(filename)
    
    def generate_media_filename(self, tweet_id: str, post_num: int, file_num: int, extension: str) -> str:
        """Generate normalized media filename"""
        pattern = self.patterns['media']
        filename = pattern.format(
            tweet_id=tweet_id,
            post_num=post_num,
            file_num=file_num,
            ext=extension.lstrip('.')
        )
        return self.sanitize_filename(filename)
    
    def generate_twitter_transcript_filename(self, tweet_id: str, screen_name: str) -> str:
        """Generate normalized Twitter transcript filename"""
        pattern = self.patterns['transcript_twitter']
        filename = pattern.format(
            tweet_id=tweet_id,
            screen_name=self.sanitize_filename(screen_name, 50)
        )
        return self.sanitize_filename(filename)
    
    def generate_youtube_transcript_filename(self, video_id: str, title: str) -> str:
        """Generate normalized YouTube transcript filename"""
        pattern = self.patterns['transcript_youtube']
        sanitized_title = self.sanitize_filename(title, 100)
        filename = pattern.format(
            video_id=video_id,
            sanitized_title=sanitized_title
        )
        return self.sanitize_filename(filename)
    
    def generate_github_readme_filename(self, owner: str, repo: str) -> str:
        """Generate normalized GitHub README filename"""
        pattern = self.patterns['readme_github']
        filename = pattern.format(
            owner=self.sanitize_filename(owner, 50),
            repo=self.sanitize_filename(repo, 50)
        )
        return self.sanitize_filename(filename)
    
    def generate_huggingface_readme_filename(self, owner: str, repo: str) -> str:
        """Generate normalized HuggingFace README filename"""
        pattern = self.patterns['readme_huggingface']
        filename = pattern.format(
            owner=self.sanitize_filename(owner, 50),
            repo=self.sanitize_filename(repo, 50)
        )
        return self.sanitize_filename(filename)
    
    def parse_legacy_tweet_filename(self, filename: str) -> Optional[Dict[str, str]]:
        """Parse legacy tweet filename to extract metadata"""
        # Handle various legacy formats
        patterns = [
            r'^(\d+)_([^.]+)\.md$',  # tweet_id_screen_name.md
            r'^([^_]+)_(\d+)\.md$',  # screen_name_tweet_id.md
            r'^tweet_(\d+)_([^.]+)\.md$',  # tweet_tweet_id_screen_name.md
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                groups = match.groups()
                # Try to determine which is tweet_id (numeric) vs screen_name
                if groups[0].isdigit():
                    return {'tweet_id': groups[0], 'screen_name': groups[1]}
                elif groups[1].isdigit():
                    return {'tweet_id': groups[1], 'screen_name': groups[0]}
        
        return None
    
    def parse_legacy_thread_filename(self, filename: str) -> Optional[Dict[str, str]]:
        """Parse legacy thread filename to extract metadata"""
        patterns = [
            r'^thread_(\d+)_([^.]+)\.md$',  # thread_thread_id_screen_name.md
            r'^(\d+)_thread_([^.]+)\.md$',  # thread_id_thread_screen_name.md
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                return {'thread_id': match.group(1), 'screen_name': match.group(2)}
        
        return None
    
    def needs_migration(self, current_filename: str, expected_filename: str) -> bool:
        """Check if a filename needs migration to the new format"""
        return current_filename != expected_filename
    
    def get_migration_mapping(self, directory: Path, file_type: str) -> Dict[str, str]:
        """Get mapping of old filenames to new normalized filenames"""
        migration_map = {}
        
        if not directory.exists():
            return migration_map
        
        pattern_map = {
            'tweet': ('*.md', self.parse_legacy_tweet_filename),
            'thread': ('*.md', self.parse_legacy_thread_filename)
        }
        
        if file_type not in pattern_map:
            return migration_map
        
        glob_pattern, parser = pattern_map[file_type]
        
        for file_path in directory.glob(glob_pattern):
            if file_path.is_file():
                current_name = file_path.name
                parsed = parser(current_name)
                
                if parsed:
                    if file_type == 'tweet':
                        new_name = self.generate_tweet_filename(
                            parsed['tweet_id'], parsed['screen_name']
                        )
                    elif file_type == 'thread':
                        new_name = self.generate_thread_filename(
                            parsed['thread_id'], parsed['screen_name']
                        )
                    
                    if self.needs_migration(current_name, new_name):
                        migration_map[current_name] = new_name
        
        return migration_map


class FilenameMigrator:
    """Handles migration from legacy filenames to normalized format"""
    
    def __init__(self, vault_path: str = None):
        vault_dir = Path(vault_path or config.get('vault_dir', 'knowledge_vault'))
        self.vault_path = vault_dir
        self.normalizer = FilenameNormalizer()
        self.download_tracker = get_download_tracker()
        
        # Directory mappings
        self.directories = {
            'tweets': self.vault_path / 'tweets',
            'threads': self.vault_path / 'threads',
            'transcripts': self.vault_path / 'transcripts',
            'repos': self.vault_path / 'repos'
        }
    
    def analyze_migrations_needed(self) -> Dict[str, Dict[str, str]]:
        """Analyze what migrations are needed across all directories"""
        all_migrations = {}
        
        for dir_type, directory in self.directories.items():
            if not directory.exists():
                continue

            if dir_type == 'repos':
                migrations = self._get_readme_migrations(directory)
            else:
                file_type = dir_type.rstrip('s')  # tweets -> tweet, threads -> thread
                migrations = self.normalizer.get_migration_mapping(directory, file_type)

            if migrations:
                all_migrations[dir_type] = migrations
        
        return all_migrations
    
    def create_migration_plan(self) -> Dict[str, Any]:
        """Create a comprehensive migration plan"""
        migrations = self.analyze_migrations_needed()
        
        plan = {
            'total_files': sum(len(files) for files in migrations.values()),
            'directories': {},
            'backlinks_to_update': []
        }
        
        for dir_type, file_migrations in migrations.items():
            plan['directories'][dir_type] = {
                'count': len(file_migrations),
                'migrations': file_migrations
            }
            
            # Find files that might contain backlinks to these files
            if dir_type in ['tweets', 'threads', 'repos']:
                for old_name, new_name in file_migrations.items():
                    plan['backlinks_to_update'].extend(
                        self._find_backlink_references(old_name)
                    )
        
        return plan

    def _get_readme_migrations(self, directory: Path) -> Dict[str, str]:
        """Determine README files that need to be renamed to the normalized scheme."""
        migrations: Dict[str, str] = {}

        for file_path in directory.glob('*.md'):
            filename = file_path.name
            lower_name = filename.lower()

            if not lower_name.endswith('_readme.md'):
                continue

            # Already using normalized naming convention
            if lower_name.startswith('github_') or lower_name.startswith('hf_'):
                continue

            target_name = self._determine_readme_target(file_path)
            if not target_name:
                continue

            if self.normalizer.needs_migration(filename, target_name):
                migrations[filename] = target_name

        return migrations

    def _determine_readme_target(self, file_path: Path) -> Optional[str]:
        """Resolve the normalized filename for a README, using tracker data when available."""
        record = self.download_tracker.find_by_file_path(file_path)

        platform: Optional[str] = None
        owner: Optional[str] = None
        repo: Optional[str] = None

        if record:
            platform, owner, repo = self._extract_repo_info_from_url(record.url)
            if owner and repo:
                logger.debug(
                    "Resolved README %s from download tracker URL -> %s/%s (%s)",
                    file_path.name,
                    owner,
                    repo,
                    platform or 'github'
                )

        if not (owner and repo):
            guess_platform, guess_owner, guess_repo = self._infer_readme_from_filename(file_path.name)
            if guess_owner and guess_repo:
                owner = owner or guess_owner
                repo = repo or guess_repo
            if guess_platform:
                platform = platform or guess_platform

        if not (owner and repo):
            logger.debug("Attempting content-based inference for README %s", file_path.name)

        if not platform:
            platform = self._guess_readme_platform(file_path)

        if not (owner and repo):
            logger.debug("Skipping README migration for %s - unable to determine repository name", file_path.name)
            return None

        if platform == 'huggingface':
            return self.normalizer.generate_huggingface_readme_filename(owner, repo)

        # Default to GitHub naming when platform is unknown
        return self.normalizer.generate_github_readme_filename(owner, repo)

    @staticmethod
    def _extract_repo_info_from_url(url: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract platform, owner, and repo from a download URL."""
        if not url:
            return None, None, None

        try:
            parsed = urlparse(url)
        except Exception:
            return None, None, None

        netloc = parsed.netloc.lower()
        parts = [part for part in parsed.path.split('/') if part]

        if 'huggingface.co' in netloc and len(parts) >= 2:
            return 'huggingface', parts[0], parts[1]

        if ('githubusercontent.com' in netloc or 'github.com' in netloc) and len(parts) >= 2:
            return 'github', parts[0], parts[1]

        return None, None, None

    def _infer_readme_from_filename(self, filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Infer repository information from legacy README filenames."""
        if not filename.lower().endswith('_readme.md'):
            return None, None, None

        base = filename[:-len('_README.md')]
        if not base:
            return None, None, None

        # Detect legacy huggingface prefix (hf_) if present
        if base.startswith('hf_'):
            trimmed = base[len('hf_'):]
            return 'huggingface', *self._split_owner_repo(trimmed)

        # Already normalized GitHub prefix
        if base.startswith('github_'):
            trimmed = base[len('github_'):]
            return 'github', *self._split_owner_repo(trimmed)

        owner, repo = self._split_owner_repo(base)
        return None, owner, repo

    def _split_owner_repo(self, identifier: str) -> Tuple[Optional[str], Optional[str]]:
        """Split a sanitized owner_repo identifier into its components."""
        if '_' not in identifier:
            return None, None
        owner, repo = identifier.split('_', 1)
        return owner, repo

    def _guess_readme_platform(self, file_path: Path) -> str:
        """Guess platform from file contents, defaulting to GitHub."""
        try:
            content = file_path.read_text(encoding='utf-8', errors='ignore')[:2048]
        except Exception:
            content = ''

        lowered = content.lower()
        if 'huggingface.co' in lowered or '🤗' in content or 'model card' in lowered:
            return 'huggingface'
        return 'github'

    def _find_backlink_references(self, filename: str) -> List[str]:
        """Find files that reference the given filename in backlinks"""
        references = []
        base_name = filename.replace('.md', '')
        
        # Search in common directories that may contain wiki links to the file
        search_dirs = [
            self.vault_path / 'threads',
            self.vault_path / 'transcripts',
            self.vault_path / 'tweets',
            self.vault_path / 'stars'
        ]
        
        for search_dir in search_dirs:
            if search_dir.exists():
                for file_path in search_dir.glob('*.md'):
                    try:
                        content = file_path.read_text(encoding='utf-8')
                        if f'[[{base_name}]]' in content or f'[[{filename}]]' in content:
                            references.append(str(file_path.relative_to(self.vault_path)))
                    except Exception as e:
                        logger.warning(f"Could not read {file_path}: {e}")
        
        return references
    
    def execute_migration(self, dry_run: bool = True) -> Dict[str, Any]:
        """Execute the migration plan"""
        plan = self.create_migration_plan()
        results = {
            'dry_run': dry_run,
            'successful_renames': 0,
            'failed_renames': 0,
            'backlinks_updated': 0,
            'errors': []
        }
        
        if plan['total_files'] == 0:
            logger.info("No files need migration")
            return results
        
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Migrating {plan['total_files']} files")
        
        # Rename files
        for dir_type, dir_plan in plan['directories'].items():
            directory = self.directories[dir_type]
            
            for old_name, new_name in dir_plan['migrations'].items():
                old_path = directory / old_name
                new_path = directory / new_name
                tracker_record = self.download_tracker.find_by_file_path(old_path)

                try:
                    if not dry_run:
                        if new_path.exists():
                            logger.warning(f"Target file already exists: {new_path}")
                            continue

                        old_path.rename(new_path)

                        if tracker_record:
                            try:
                                size_bytes = new_path.stat().st_size if new_path.exists() else 0
                                self.download_tracker.record_success(
                                    tracker_record.url,
                                    new_name,
                                    str(new_path),
                                    size_bytes
                                )
                            except Exception as tracker_error:
                                logger.debug(
                                    f"Could not update download tracker for {new_name}: {tracker_error}"
                                )

                    logger.info(f"{'Would rename' if dry_run else 'Renamed'}: {old_name} -> {new_name}")
                    results['successful_renames'] += 1

                except Exception as e:
                    error_msg = f"Failed to rename {old_name}: {e}"
                    results['errors'].append(error_msg)
                    results['failed_renames'] += 1
                    logger.error(error_msg)
        
        # Update backlinks
        if not dry_run and plan['backlinks_to_update']:
            results['backlinks_updated'] = self._update_backlinks(plan)
        
        return results
    
    def _update_backlinks(self, plan: Dict[str, Any]) -> int:
        """Update backlinks in files that reference migrated files"""
        updated_count = 0
        
        # Create mapping of old -> new names without extensions
        name_mapping = {}
        for dir_plan in plan['directories'].values():
            for old_name, new_name in dir_plan['migrations'].items():
                old_base = old_name.replace('.md', '')
                new_base = new_name.replace('.md', '')
                name_mapping[old_base] = new_base
        
        # Update references in identified files
        for ref_file in set(plan['backlinks_to_update']):
            ref_path = self.vault_path / ref_file
            
            try:
                content = ref_path.read_text(encoding='utf-8')
                original_content = content
                
                # Replace backlinks
                for old_base, new_base in name_mapping.items():
                    content = content.replace(f'[[{old_base}]]', f'[[{new_base}]]')
                
                if content != original_content:
                    ref_path.write_text(content, encoding='utf-8')
                    updated_count += 1
                    logger.info(f"Updated backlinks in: {ref_file}")
                    
            except Exception as e:
                logger.error(f"Failed to update backlinks in {ref_file}: {e}")
        
        return updated_count


# Global instances
filename_normalizer = FilenameNormalizer()
filename_migrator = FilenameMigrator()


def get_filename_normalizer() -> FilenameNormalizer:
    """Get the global filename normalizer instance"""
    return filename_normalizer


def get_filename_migrator() -> FilenameMigrator:
    """Get the global filename migrator instance"""
    return filename_migrator
