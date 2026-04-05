"""
Digest Generator - Creates weekly/periodic digest notes for Obsidian
Aggregates content by category and importance for easy discovery
"""

import logging
import re
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field

import yaml
import requests

from core.config import config

logger = logging.getLogger(__name__)


def send_ntfy_notification(
    title: str,
    message: str,
    server: str = None,
    topic: str = None,
    priority: str = "default"
) -> bool:
    """Send a notification via ntfy

    Args:
        title: Notification title
        message: Notification body
        server: ntfy server URL (default: from NTFY_SERVER env or config)
        topic: ntfy topic (default: from config or "xmarks")
        priority: Notification priority (min, low, default, high, urgent)

    Returns:
        True if notification was sent successfully

    Authentication:
        Set NTFY_USER and NTFY_PASS environment variables, or configure
        ntfy.user and ntfy.password in config.json
    """
    # Get server from env, config, or default
    server = server or os.environ.get("NTFY_SERVER") or config.get("ntfy.server")
    topic = topic or config.get("ntfy.topic", "xmarks")

    if not server:
        logger.debug("No ntfy server configured, skipping notification")
        return False

    # Get auth credentials
    user = os.environ.get("NTFY_USER") or config.get("ntfy.user")
    password = os.environ.get("NTFY_PASS") or config.get("ntfy.password")

    # Clean up server URL
    server = server.rstrip("/")
    url = f"{server}/{topic}"

    try:
        # Build request kwargs
        kwargs = {
            "data": message.encode("utf-8"),
            "headers": {
                "Title": title,
                "Priority": priority,
            },
            "timeout": 10
        }

        # Add basic auth if credentials provided
        if user and password:
            kwargs["auth"] = (user, password)

        response = requests.post(url, **kwargs)
        response.raise_for_status()
        logger.info(f"Sent ntfy notification to {topic}")
        return True
    except Exception as e:
        logger.warning(f"Failed to send ntfy notification: {e}")
        return False


@dataclass
class DigestItem:
    """Represents an item in the digest"""
    filepath: Path
    frontmatter: Dict[str, Any]
    title: str = ""

    @property
    def importance(self) -> int:
        return self.frontmatter.get('importance', 0)

    @property
    def likes(self) -> int:
        return self.frontmatter.get('likes', 0)

    @property
    def author(self) -> str:
        return self.frontmatter.get('author', 'unknown')

    @property
    def created(self) -> str:
        return self.frontmatter.get('created', '')

    @property
    def tags(self) -> List[str]:
        return self.frontmatter.get('tags', [])

    @property
    def content_type(self) -> str:
        return self.frontmatter.get('type', 'tweet')

    @property
    def summary(self) -> str:
        return self.frontmatter.get('summary', '')

    @property
    def obsidian_link(self) -> str:
        """Return Obsidian wikilink to this file"""
        return f"[[{self.filepath.stem}]]"


@dataclass
class DigestStats:
    """Statistics for the digest period"""
    total_items: int = 0
    papers: int = 0
    repos: int = 0
    threads: int = 0
    videos: int = 0
    youtube: int = 0
    tweets: int = 0
    top_authors: List[Tuple[str, int]] = field(default_factory=list)
    top_tags: List[Tuple[str, int]] = field(default_factory=list)


class DigestGenerator:
    """Generates periodic digest notes for content discovery"""

    def __init__(self, vault_path: str = None):
        self.vault_path = Path(vault_path or config.get("paths.vault_dir", "knowledge_vault"))
        self.digests_dir = self.vault_path / "_digests"
        self.tweets_dir = self.vault_path / "tweets"
        self.threads_dir = self.vault_path / "threads"

        # Dataview paths - relative to Obsidian vault root
        # If knowledge_vault is a subfolder of the Obsidian vault, we need the prefix
        self.dv_prefix = config.get("dataview.path_prefix", "knowledge_vault")
        self.dv_tweets = f'"{self.dv_prefix}/tweets"' if self.dv_prefix else '"tweets"'
        self.dv_threads = f'"{self.dv_prefix}/threads"' if self.dv_prefix else '"threads"'
        self.dv_digests = f'"{self.dv_prefix}/_digests"' if self.dv_prefix else '"_digests"'
        self.dv_from = f'{self.dv_tweets} OR {self.dv_threads}'

    def ensure_digest_dir(self):
        """Create _digests directory if it doesn't exist"""
        self.digests_dir.mkdir(parents=True, exist_ok=True)

    def parse_frontmatter(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Extract YAML frontmatter from a markdown file"""
        try:
            content = filepath.read_text(encoding='utf-8')
            if not content.startswith('---'):
                return None

            # Find the closing ---
            end_idx = content.find('---', 3)
            if end_idx == -1:
                return None

            yaml_content = content[3:end_idx].strip()
            return yaml.safe_load(yaml_content)
        except Exception as e:
            logger.debug(f"Failed to parse frontmatter from {filepath}: {e}")
            return None

    def extract_title_from_content(self, filepath: Path) -> str:
        """Extract first heading or snippet from file content"""
        try:
            content = filepath.read_text(encoding='utf-8')
            # Skip frontmatter
            if content.startswith('---'):
                end_idx = content.find('---', 3)
                if end_idx != -1:
                    content = content[end_idx + 3:].strip()

            # Look for first heading
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('# '):
                    return line[2:].strip()
                if line.startswith('## '):
                    return line[3:].strip()

            # Fallback: first non-empty line (truncated)
            for line in content.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    return line[:100] + ('...' if len(line) > 100 else '')

            return filepath.stem
        except Exception:
            return filepath.stem

    def extract_summary_from_content(self, filepath: Path) -> Optional[str]:
        """Extract the LLM-generated summary from the markdown content"""
        try:
            content = filepath.read_text(encoding='utf-8')
            # Skip frontmatter
            if content.startswith('---'):
                end_idx = content.find('---', 3)
                if end_idx != -1:
                    content = content[end_idx + 3:].strip()

            # Look for ## Summary section
            lines = content.split('\n')
            in_summary = False
            summary_lines = []

            for line in lines:
                if line.strip().startswith('## Summary'):
                    in_summary = True
                    continue
                elif in_summary:
                    # Stop at next heading or empty section
                    if line.strip().startswith('## ') or line.strip().startswith('# '):
                        break
                    if line.strip():
                        summary_lines.append(line.strip())

            if summary_lines:
                return ' '.join(summary_lines)[:300]

            return None
        except Exception:
            return None

    def extract_first_paragraph(self, filepath: Path) -> Optional[str]:
        """Extract the first paragraph of actual content (tweet text)"""
        try:
            content = filepath.read_text(encoding='utf-8')
            # Skip frontmatter
            if content.startswith('---'):
                end_idx = content.find('---', 3)
                if end_idx != -1:
                    content = content[end_idx + 3:].strip()

            lines = content.split('\n')
            paragraph_lines = []

            for line in lines:
                stripped = line.strip()
                # Skip headings, empty lines at start, and special sections
                if stripped.startswith('#'):
                    if paragraph_lines:  # Already got content, stop at heading
                        break
                    continue
                if stripped.startswith('![['):  # Skip media embeds
                    continue
                if stripped.startswith('*') and stripped.endswith('*'):  # Skip alt text
                    continue
                if stripped.startswith('>'):  # Skip callouts
                    continue
                if stripped.startswith('```'):  # Skip code blocks
                    continue
                if stripped.startswith('---'):  # Skip dividers
                    if paragraph_lines:
                        break
                    continue

                if stripped:
                    paragraph_lines.append(stripped)
                    if len(' '.join(paragraph_lines)) > 200:
                        break
                elif paragraph_lines:
                    break  # Empty line after content = end of paragraph

            if paragraph_lines:
                text = ' '.join(paragraph_lines)
                if len(text) > 200:
                    text = text[:200] + '...'
                return text

            return None
        except Exception:
            return None

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from frontmatter, returning naive datetime"""
        if not date_str:
            return None

        # Common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%a %b %d %H:%M:%S %z %Y",  # Twitter format
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Convert to naive datetime for consistent comparison
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                return dt
            except ValueError:
                continue

        # Try parsing with dateutil if available
        try:
            from dateutil import parser
            dt = parser.parse(date_str)
            # Convert to naive datetime
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except Exception:
            pass

        return None

    def scan_vault_items(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[DigestItem]:
        """Scan vault for items within date range"""
        items = []

        # Scan tweets directory
        if self.tweets_dir.exists():
            for md_file in self.tweets_dir.glob("*.md"):
                fm = self.parse_frontmatter(md_file)
                if fm:
                    # Extract summary: prefer LLM summary, fallback to first paragraph
                    summary = self.extract_summary_from_content(md_file)
                    if not summary:
                        summary = self.extract_first_paragraph(md_file)

                    item = DigestItem(
                        filepath=md_file,
                        frontmatter=fm,
                        title=self.extract_title_from_content(md_file)
                    )
                    # Store summary in frontmatter for easy access
                    if summary:
                        item.frontmatter['_summary'] = summary

                    # Filter by date if specified
                    if start_date or end_date:
                        item_date = self.parse_date(fm.get('created', '') or fm.get('processed', ''))
                        if item_date:
                            if start_date and item_date < start_date:
                                continue
                            if end_date and item_date > end_date:
                                continue

                    items.append(item)

        # Scan threads directory
        if self.threads_dir.exists():
            for md_file in self.threads_dir.glob("*.md"):
                fm = self.parse_frontmatter(md_file)
                if fm:
                    # Extract summary for threads
                    summary = self.extract_summary_from_content(md_file)
                    if not summary:
                        summary = self.extract_first_paragraph(md_file)

                    item = DigestItem(
                        filepath=md_file,
                        frontmatter=fm,
                        title=self.extract_title_from_content(md_file)
                    )
                    if summary:
                        item.frontmatter['_summary'] = summary

                    # Filter by date
                    if start_date or end_date:
                        item_date = self.parse_date(fm.get('created', '') or fm.get('processed', ''))
                        if item_date:
                            if start_date and item_date < start_date:
                                continue
                            if end_date and item_date > end_date:
                                continue

                    items.append(item)

        return items

    def categorize_items(self, items: List[DigestItem]) -> Dict[str, List[DigestItem]]:
        """Categorize items by content type"""
        categories = {
            'papers': [],
            'repos': [],
            'threads': [],
            'videos': [],
            'youtube': [],
            'tweets': [],
        }

        for item in items:
            fm = item.frontmatter

            # Check content type
            if fm.get('type') == 'thread':
                categories['threads'].append(item)
            elif fm.get('has_paper'):
                categories['papers'].append(item)
            elif fm.get('has_repo'):
                categories['repos'].append(item)
            elif fm.get('has_youtube'):
                categories['youtube'].append(item)
            elif fm.get('has_video'):
                categories['videos'].append(item)
            else:
                categories['tweets'].append(item)

        # Sort each category by importance
        for cat in categories:
            categories[cat].sort(key=lambda x: x.importance, reverse=True)

        return categories

    def compute_stats(self, items: List[DigestItem]) -> DigestStats:
        """Compute statistics for the digest period"""
        stats = DigestStats(total_items=len(items))

        author_counts = defaultdict(int)
        tag_counts = defaultdict(int)

        for item in items:
            fm = item.frontmatter

            # Count by type
            if fm.get('type') == 'thread':
                stats.threads += 1
            elif fm.get('has_paper'):
                stats.papers += 1
            elif fm.get('has_repo'):
                stats.repos += 1
            elif fm.get('has_youtube'):
                stats.youtube += 1
            elif fm.get('has_video'):
                stats.videos += 1
            else:
                stats.tweets += 1

            # Count authors
            author = fm.get('author', 'unknown')
            author_counts[author] += 1

            # Count tags
            for tag in fm.get('tags', []):
                tag_counts[tag] += 1

        # Top authors and tags
        stats.top_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        stats.top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:15]

        return stats

    def generate_weekly_digest(
        self,
        week_start: Optional[datetime] = None,
        limit_per_category: int = 10
    ) -> str:
        """Generate a weekly digest markdown file

        Args:
            week_start: Start of the week (defaults to current week's Monday)
            limit_per_category: Max items to show per category

        Returns:
            Path to the generated digest file
        """
        self.ensure_digest_dir()

        # Calculate week range
        if week_start is None:
            today = datetime.now()
            # Find Monday of current week
            week_start = today - timedelta(days=today.weekday())

        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_start + timedelta(days=7)

        # Format for filename: 2024-W52
        year = week_start.year
        week_num = week_start.isocalendar()[1]
        filename = f"{year}-W{week_num:02d}.md"
        filepath = self.digests_dir / filename

        # Scan and categorize items
        items = self.scan_vault_items(start_date=week_start, end_date=week_end)
        categories = self.categorize_items(items)
        stats = self.compute_stats(items)

        # Generate markdown content
        lines = []

        # Frontmatter
        lines.append("---")
        lines.append(f'type: "digest"')
        lines.append(f'period: "weekly"')
        lines.append(f'week: "{year}-W{week_num:02d}"')
        lines.append(f'start_date: "{week_start.strftime("%Y-%m-%d")}"')
        lines.append(f'end_date: "{week_end.strftime("%Y-%m-%d")}"')
        lines.append(f'generated: "{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}"')
        lines.append(f'total_items: {stats.total_items}')
        lines.append("---")
        lines.append("")

        # Title
        lines.append(f"# Weekly Digest: {week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}")
        lines.append("")

        # Summary box
        lines.append("> [!summary] This Week at a Glance")
        lines.append(f"> - **{stats.total_items}** new items captured")
        lines.append(f"> - **{stats.papers}** papers | **{stats.repos}** repos | **{stats.threads}** threads")
        lines.append(f"> - **{stats.youtube}** YouTube videos | **{stats.videos}** Twitter videos")
        lines.append("")

        # Papers section
        if categories['papers']:
            lines.append("## Papers")
            lines.append("")
            for item in categories['papers'][:limit_per_category]:
                fm = item.frontmatter
                likes = fm.get('likes', 0)
                tags = fm.get('tags', [])
                summary = fm.get('_summary', '')

                lines.append(f"### {item.obsidian_link}")
                lines.append(f"**@{item.author}** · {likes:,} likes")
                if summary:
                    lines.append(f"> {summary}")
                if tags:
                    lines.append(f"Tags: {' '.join(['#' + t for t in tags[:5]])}")
                lines.append("")

        # Repos section
        if categories['repos']:
            lines.append("## Repositories")
            lines.append("")
            for item in categories['repos'][:limit_per_category]:
                fm = item.frontmatter
                likes = fm.get('likes', 0)
                tags = fm.get('tags', [])
                summary = fm.get('_summary', '')

                lines.append(f"### {item.obsidian_link}")
                lines.append(f"**@{item.author}** · {likes:,} likes")
                if summary:
                    lines.append(f"> {summary}")
                if tags:
                    lines.append(f"Tags: {' '.join(['#' + t for t in tags[:5]])}")
                lines.append("")

        # Threads section
        if categories['threads']:
            lines.append("## Threads")
            lines.append("")
            for item in categories['threads'][:limit_per_category]:
                fm = item.frontmatter
                tweet_count = fm.get('tweet_count', 0)
                likes = fm.get('likes', 0)
                tags = fm.get('tags', [])
                summary = fm.get('_summary', '')

                lines.append(f"### {item.obsidian_link}")
                lines.append(f"**@{item.author}** · {tweet_count} tweets · {likes:,} likes")
                if summary:
                    lines.append(f"> {summary}")
                if tags:
                    lines.append(f"Tags: {' '.join(['#' + t for t in tags[:5]])}")
                lines.append("")

        # YouTube section
        if categories['youtube']:
            lines.append("## YouTube Videos")
            lines.append("")
            for item in categories['youtube'][:limit_per_category]:
                fm = item.frontmatter
                likes = fm.get('likes', 0)
                tags = fm.get('tags', [])
                summary = fm.get('_summary', '')

                lines.append(f"### {item.obsidian_link}")
                lines.append(f"**@{item.author}** · {likes:,} likes")
                if summary:
                    lines.append(f"> {summary}")
                if tags:
                    lines.append(f"Tags: {' '.join(['#' + t for t in tags[:5]])}")
                lines.append("")

        # Videos section (Twitter videos)
        if categories['videos']:
            lines.append("## Twitter Videos")
            lines.append("")
            for item in categories['videos'][:limit_per_category]:
                fm = item.frontmatter
                likes = fm.get('likes', 0)
                tags = fm.get('tags', [])
                summary = fm.get('_summary', '')

                lines.append(f"### {item.obsidian_link}")
                lines.append(f"**@{item.author}** · {likes:,} likes")
                if summary:
                    lines.append(f"> {summary}")
                if tags:
                    lines.append(f"Tags: {' '.join(['#' + t for t in tags[:5]])}")
                lines.append("")

        # High engagement tweets (only if we have some good ones)
        high_engagement = [i for i in categories['tweets'] if i.likes >= 100]
        if high_engagement:
            lines.append("## Notable Tweets")
            lines.append("")
            for item in high_engagement[:limit_per_category]:
                fm = item.frontmatter
                tags = fm.get('tags', [])
                summary = fm.get('_summary', '')

                lines.append(f"### {item.obsidian_link}")
                lines.append(f"**@{item.author}** · {item.likes:,} likes")
                if summary:
                    lines.append(f"> {summary}")
                if tags:
                    lines.append(f"Tags: {' '.join(['#' + t for t in tags[:5]])}")
                lines.append("")

        # Tag cloud
        if stats.top_tags:
            lines.append("## Trending Tags")
            lines.append("")
            tag_parts = [f"#{tag} ({count})" for tag, count in stats.top_tags]
            lines.append(" | ".join(tag_parts))
            lines.append("")

        # Top authors
        if stats.top_authors:
            lines.append("## Top Authors This Week")
            lines.append("")
            for author, count in stats.top_authors[:5]:
                lines.append(f"- @{author} ({count} items)")
            lines.append("")

        # Dataview queries section
        lines.append("## Quick Queries")
        lines.append("")
        lines.append("### Unread Papers")
        lines.append("```dataview")
        lines.append("TABLE author, likes, importance")
        lines.append(f'FROM {self.dv_from}')
        lines.append('WHERE has_paper = true AND status = "unread"')
        lines.append("SORT importance DESC")
        lines.append("LIMIT 10")
        lines.append("```")
        lines.append("")
        lines.append("### Unread High-Importance")
        lines.append("```dataview")
        lines.append("TABLE author, likes, tags")
        lines.append(f'FROM {self.dv_from}')
        lines.append('WHERE status = "unread" AND importance >= 50')
        lines.append("SORT importance DESC")
        lines.append("LIMIT 15")
        lines.append("```")
        lines.append("")

        # Footer
        lines.append("---")
        lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by XMarks*")

        # Write file
        content = "\n".join(lines)
        filepath.write_text(content, encoding='utf-8')

        logger.info(f"Generated weekly digest: {filepath}")
        return str(filepath)

    def generate_inbox_view(self) -> str:
        """Generate an inbox view of all unread content"""
        self.ensure_digest_dir()

        filepath = self.digests_dir / "inbox.md"

        # Scan all items
        items = self.scan_vault_items()

        # Filter to unread only
        unread = [i for i in items if i.frontmatter.get('status') == 'unread']

        # Categorize
        categories = self.categorize_items(unread)
        stats = self.compute_stats(unread)

        lines = []

        # Frontmatter
        lines.append("---")
        lines.append('type: "inbox"')
        lines.append(f'generated: "{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}"')
        lines.append(f'unread_count: {len(unread)}')
        lines.append("---")
        lines.append("")

        lines.append("# Inbox")
        lines.append("")
        lines.append(f"> {len(unread)} unread items")
        lines.append("")

        # Priority items (high importance)
        priority = sorted(unread, key=lambda x: x.importance, reverse=True)[:20]
        if priority:
            lines.append("## Priority Reading")
            lines.append("")
            lines.append("```dataview")
            lines.append("TABLE author, importance, tags")
            lines.append(f'FROM {self.dv_from}')
            lines.append('WHERE status = "unread"')
            lines.append("SORT importance DESC")
            lines.append("LIMIT 20")
            lines.append("```")
            lines.append("")

        # Papers queue
        lines.append("## Papers Queue")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, likes")
        lines.append(f'FROM {self.dv_from}')
        lines.append('WHERE has_paper = true AND status = "unread"')
        lines.append("SORT importance DESC")
        lines.append("```")
        lines.append("")

        # Repos queue
        lines.append("## Repos Queue")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, likes")
        lines.append(f'FROM {self.dv_from}')
        lines.append('WHERE has_repo = true AND status = "unread"')
        lines.append("SORT importance DESC")
        lines.append("```")
        lines.append("")

        # Recent by date
        lines.append("## Recent Additions")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, type, importance")
        lines.append(f'FROM {self.dv_from}')
        lines.append('WHERE status = "unread"')
        lines.append("SORT processed DESC")
        lines.append("LIMIT 30")
        lines.append("```")
        lines.append("")

        lines.append("---")
        lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by XMarks*")

        content = "\n".join(lines)
        filepath.write_text(content, encoding='utf-8')

        logger.info(f"Generated inbox view: {filepath}")
        return str(filepath)

    def generate_discovery_dashboard(self) -> str:
        """Generate a main discovery dashboard with Dataview queries"""
        self.ensure_digest_dir()

        filepath = self.digests_dir / "dashboard.md"

        lines = []

        # Frontmatter
        lines.append("---")
        lines.append('type: "dashboard"')
        lines.append(f'generated: "{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}"')
        lines.append("---")
        lines.append("")

        lines.append("# XMarks Discovery Dashboard")
        lines.append("")

        # Quick links
        lines.append("## Quick Links")
        lines.append("")
        lines.append("- [[inbox|Inbox - Unread Items]]")
        lines.append("- Browse by: [[#Papers]] | [[#Repos]] | [[#Threads]] | [[#Videos]]")
        lines.append("")

        # Stats
        lines.append("## Collection Stats")
        lines.append("")
        lines.append("```dataview")
        lines.append('TABLE length(rows) as "Count"')
        lines.append(f'FROM {self.dv_from}')
        lines.append("GROUP BY type")
        lines.append("```")
        lines.append("")

        # Unread counts
        lines.append("## Unread by Category")
        lines.append("")
        lines.append("```dataview")
        lines.append('TABLE WITHOUT ID')
        lines.append('  "Papers" as Category,')
        lines.append('  length(filter(rows, (r) => r.has_paper)) as Count')
        lines.append(f'FROM {self.dv_from}')
        lines.append('WHERE status = "unread"')
        lines.append("```")
        lines.append("")

        # Papers section
        lines.append("## Papers")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, likes, status, importance")
        lines.append(f'FROM {self.dv_from}')
        lines.append("WHERE has_paper = true")
        lines.append("SORT importance DESC")
        lines.append("LIMIT 15")
        lines.append("```")
        lines.append("")

        # Repos section
        lines.append("## Repos")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, likes, status, importance")
        lines.append(f'FROM {self.dv_from}')
        lines.append("WHERE has_repo = true")
        lines.append("SORT importance DESC")
        lines.append("LIMIT 15")
        lines.append("```")
        lines.append("")

        # Threads section
        lines.append("## Threads")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, tweet_count, likes, status")
        lines.append(f'FROM {self.dv_threads}')
        lines.append("SORT importance DESC")
        lines.append("LIMIT 15")
        lines.append("```")
        lines.append("")

        # Videos section
        lines.append("## Videos")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE author, likes, status")
        lines.append(f'FROM {self.dv_from}')
        lines.append("WHERE has_video = true OR has_youtube = true")
        lines.append("SORT importance DESC")
        lines.append("LIMIT 15")
        lines.append("```")
        lines.append("")

        # Top authors
        lines.append("## Top Authors")
        lines.append("")
        lines.append("```dataview")
        lines.append('TABLE length(rows) as "Items", sum(rows.likes) as "Total Likes"')
        lines.append(f'FROM {self.dv_from}')
        lines.append("GROUP BY author")
        lines.append('SORT length(rows) DESC')
        lines.append("LIMIT 10")
        lines.append("```")
        lines.append("")

        # Recent digests
        lines.append("## Recent Digests")
        lines.append("")
        lines.append("```dataview")
        lines.append("LIST")
        lines.append(f'FROM {self.dv_digests}')
        lines.append('WHERE type = "digest"')
        lines.append("SORT week DESC")
        lines.append("LIMIT 8")
        lines.append("```")
        lines.append("")

        lines.append("---")
        lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by XMarks*")

        content = "\n".join(lines)
        filepath.write_text(content, encoding='utf-8')

        logger.info(f"Generated discovery dashboard: {filepath}")
        return str(filepath)
