"""
Content Processor - Generates markdown files for tweets
Extracted and cleaned from real_thread_and_url_fix.py
"""

import time
from pathlib import Path
from typing import List, Dict
import logging

from core.data_models import Tweet, ProcessingStats
from core.config import config
from .markdown_generator import MarkdownGenerator

logger = logging.getLogger(__name__)


class ContentProcessor:
    """Generates markdown content for tweets"""

    def __init__(self, vault_path: str = None):
        self.vault_path = Path(vault_path or config.get("vault_dir", "knowledge_vault"))
        self.tweets_dir = self.vault_path / "tweets"
        self.tweets_dir.mkdir(parents=True, exist_ok=True)

    def process_tweets(
        self, tweets: List[Tweet], resume: bool = True
    ) -> ProcessingStats:
        """Process tweets into markdown files"""
        stats = ProcessingStats()

        for tweet in tweets:
            try:
                # Skip tweets that are part of threads (they'll be in thread files instead)
                if tweet.is_self_thread:
                    stats.skipped += 1
                    continue

                from core.filename_utils import get_filename_normalizer

                normalizer = get_filename_normalizer()
                filename = normalizer.generate_tweet_filename(
                    tweet.id, tweet.screen_name
                )
                filepath = self.tweets_dir / filename

                # Skip if exists and resume enabled
                if resume and filepath.exists():
                    stats.skipped += 1
                    continue

                # Generate markdown content
                content = self.create_tweet_markdown(tweet)

                # Write file
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)

                # Update metadata database
                if config.get("database.enabled", False):
                    from core.metadata_db import get_metadata_db, FileMetadata
                    from datetime import datetime

                    db = get_metadata_db()
                    try:
                        rel_path = filepath.relative_to(self.vault_path)
                    except Exception:
                        rel_path = filepath
                    file_meta = FileMetadata(
                        path=str(rel_path),
                        file_type="tweet",
                        size_bytes=len(content.encode("utf-8")),
                        updated_at=datetime.now().isoformat(),
                        source_id=tweet.id,
                    )
                    db.upsert_file(file_meta)

                if filepath.exists():
                    stats.updated += 1
                else:
                    stats.created += 1

                stats.total_processed += 1

            except Exception as e:
                logger.error(f"Error processing tweet {tweet.id}: {e}")
                stats.errors += 1

        logger.info(
            f"📝 Content processing complete: {stats.created} created, {stats.updated} updated, {stats.skipped} skipped"
        )
        return stats

    def generate_tweet_content(self, tweet: Tweet, include_header: bool = True) -> str:
        """Generate tweet content that can be used in both individual tweets and threads"""
        import re

        content_lines = []

        # Add tweet text
        if tweet.full_text:
            # Remove any remaining t.co links that point to media (since media is displayed separately)
            text = tweet.full_text

            # Remove t.co URLs that correspond to downloaded media
            if tweet.media_items:
                for media in tweet.media_items:
                    if media.filename:  # If we have the media file
                        # Remove the t.co URL for this media
                        if hasattr(media, "original_url") and media.original_url:
                            text = text.replace(media.original_url, "")

                        # Also check URL mappings for t.co URLs that expand to this media
                        if tweet.url_mappings:
                            for url_mapping in tweet.url_mappings:
                                if url_mapping.expanded_url == media.media_url:
                                    text = text.replace(url_mapping.short_url, "")

            # Clean up any extra spaces
            text = " ".join(text.split())
            if text:
                content_lines.append(text)

        # Add media files using the new media generation method
        if tweet.media_items:
            from processors.markdown_generator import MarkdownGenerator

            media_lines = MarkdownGenerator.generate_media_embeds(tweet.media_items)
            content_lines.extend(media_lines)

        # Add transcript link if available
        from core.filename_utils import get_filename_normalizer

        normalizer = get_filename_normalizer()
        transcript_filename = normalizer.generate_twitter_transcript_filename(
            tweet.id, tweet.screen_name
        )
        transcript_path = self.vault_path / "transcripts" / transcript_filename
        if transcript_path.exists():
            content_lines.append(f"- **Transcript**: [[{transcript_filename}]]")

        # Note: ArXiv papers and PDF documents are handled in separate sections for individual tweets
        # For threads, we might want different behavior, but keeping it simple for now

        return "\n".join(content_lines) if content_lines else ""

    def create_tweet_markdown(self, tweet: Tweet) -> str:
        """Create enhanced markdown content for tweet"""
        lines = []

        # YAML frontmatter using MarkdownGenerator with rich metadata
        metadata = MarkdownGenerator.build_tweet_frontmatter(tweet)
        lines.extend(MarkdownGenerator.generate_frontmatter(metadata))

        # Main content
        lines.append(f"# Tweet by @{tweet.screen_name}")
        lines.append("")
        lines.append("## Content")

        # Use shared content generation
        tweet_content = self.generate_tweet_content(tweet)
        lines.append(tweet_content)
        lines.append("")

        # ArXiv papers section using MarkdownGenerator
        if hasattr(tweet, "arxiv_papers") and tweet.arxiv_papers:
            lines.extend(
                MarkdownGenerator.generate_arxiv_section(
                    tweet.arxiv_papers, detailed=True
                )
            )

        # PDF links section using MarkdownGenerator
        if hasattr(tweet, "pdf_links") and tweet.pdf_links:
            lines.extend(
                MarkdownGenerator.generate_pdf_section(tweet.pdf_links, detailed=True)
            )

        # Repository links section using MarkdownGenerator
        if hasattr(tweet, "repo_links") and tweet.repo_links:
            lines.extend(
                MarkdownGenerator.generate_repo_section(tweet.repo_links, detailed=True)
            )

        # YouTube videos section using MarkdownGenerator
        if hasattr(tweet, "youtube_videos") and tweet.youtube_videos:
            lines.extend(
                MarkdownGenerator.generate_youtube_section(
                    tweet.youtube_videos, detailed=True
                )
            )

        # Thread info section using MarkdownGenerator
        lines.extend(
            MarkdownGenerator.generate_thread_info_section(tweet, detailed=True)
        )

        # Add LLM summary for long tweets using MarkdownGenerator
        if (
            hasattr(tweet, "llm_summary")
            and tweet.llm_summary
            and MarkdownGenerator.should_summarize_tweet(tweet)
        ):
            lines.extend(
                MarkdownGenerator.generate_summary_section(
                    tweet.llm_summary, detailed=True
                )
            )

        # Add LLM-generated tags using MarkdownGenerator
        if hasattr(tweet, "llm_tags") and tweet.llm_tags:
            tag_lines = MarkdownGenerator.generate_tags(
                tweet.llm_tags, tweet.screen_name
            )
            if tag_lines:
                lines.extend(tag_lines)

        return "\n".join(lines)
