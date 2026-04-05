"""
Thread Processor - Handles thread detection and thread file generation
Extracted and cleaned from real_thread_and_url_fix.py
"""

import time
import logging
from pathlib import Path
from typing import List, Dict
from core.data_models import Tweet, ThreadInfo, ProcessingStats
from core.config import config
from .content_processor import ContentProcessor
from .markdown_generator import MarkdownGenerator

logger = logging.getLogger(__name__)


class ThreadProcessor:
    """Processes thread data and generates thread markdown files"""

    def __init__(self, vault_path: str = None):
        self.vault_path = Path(vault_path or config.get("vault_dir", "knowledge_vault"))
        self.threads_dir = self.vault_path / "threads"
        self.threads_dir.mkdir(parents=True, exist_ok=True)
        self.content_processor = ContentProcessor(vault_path)

    def detect_threads_from_tweets(self, tweets: List[Tweet]) -> Dict[str, List[Tweet]]:
        """Detect threads from enhanced tweet data"""
        threads = {}

        for tweet in tweets:
            if tweet.is_self_thread and tweet.thread_id:
                if tweet.thread_id not in threads:
                    threads[tweet.thread_id] = []
                threads[tweet.thread_id].append(tweet)

        # Filter threads with at least 2 tweets
        valid_threads = {
            tid: tweets for tid, tweets in threads.items() if len(tweets) >= 2
        }

        logger.info(
            f"🧵 Detected {len(valid_threads)} threads from {len(tweets)} tweets"
        )
        return valid_threads

    def process_threads(
        self, threads: Dict[str, List[Tweet]], resume: bool = True
    ) -> ProcessingStats:
        """Process threads into markdown files"""
        stats = ProcessingStats()

        for thread_id, thread_tweets in threads.items():
            try:
                if len(thread_tweets) < 2:
                    stats.skipped += 1
                    continue

                # Sort tweets by creation date
                thread_tweets.sort(key=lambda t: t.created_at)
                first_tweet = thread_tweets[0]

                from core.filename_utils import get_filename_normalizer

                normalizer = get_filename_normalizer()
                filename = normalizer.generate_thread_filename(
                    thread_id, first_tweet.screen_name
                )
                filepath = self.threads_dir / filename

                # Skip if exists and resume enabled
                if resume and filepath.exists():
                    stats.skipped += 1
                    continue

                # Generate thread content
                content = self.create_thread_markdown(thread_id, thread_tweets)

                # Write file
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)

                if filepath.exists():
                    stats.updated += 1
                else:
                    stats.created += 1

                # Upsert into metadata DB
                if config.get("database.enabled", False):
                    try:
                        from core.metadata_db import get_metadata_db, FileMetadata
                        from datetime import datetime

                        db = get_metadata_db()
                        try:
                            rel_path = filepath.relative_to(self.vault_path)
                        except Exception:
                            rel_path = filepath
                        db.upsert_file(
                            FileMetadata(
                                path=str(rel_path),
                                file_type="thread",
                                size_bytes=len(content.encode("utf-8")),
                                updated_at=datetime.now().isoformat(),
                                source_id=thread_id,
                            )
                        )
                    except Exception:
                        pass

                stats.total_processed += 1

            except Exception as e:
                logger.error(f"Error processing thread {thread_id}: {e}")
                stats.errors += 1

        logger.info(
            f"🧵 Thread processing complete: {stats.created} created, {stats.updated} updated, {stats.skipped} skipped"
        )
        return stats

    def create_thread_markdown(self, thread_id: str, thread_tweets: List[Tweet]) -> str:
        """Create thread markdown content"""
        first_tweet = thread_tweets[0]

        lines = []

        # YAML frontmatter using MarkdownGenerator with rich metadata
        metadata = MarkdownGenerator.build_thread_frontmatter(thread_id, thread_tweets)
        lines.extend(MarkdownGenerator.generate_frontmatter(metadata))

        # Main content
        lines.append(f"# Thread by @{first_tweet.screen_name}")
        lines.append("")
        lines.append(f"**Thread contains {len(thread_tweets)} tweets**")
        lines.append("")

        # Add each tweet in the thread
        for i, tweet in enumerate(thread_tweets, 1):
            lines.append(f"## Tweet {i}")
            lines.append("")

            # Use the shared content generation method from ContentProcessor
            tweet_content = self.content_processor.generate_tweet_content(tweet)
            lines.append(tweet_content)
            lines.append("")

            # Add ArXiv papers section using MarkdownGenerator (compact format for threads)
            if hasattr(tweet, "arxiv_papers") and tweet.arxiv_papers:
                lines.extend(
                    MarkdownGenerator.generate_arxiv_section(
                        tweet.arxiv_papers, detailed=False
                    )
                )

            # Add PDF documents section using MarkdownGenerator (compact format for threads)
            if hasattr(tweet, "pdf_links") and tweet.pdf_links:
                lines.extend(
                    MarkdownGenerator.generate_pdf_section(
                        tweet.pdf_links, detailed=False
                    )
                )

            # Add repository links section using MarkdownGenerator (compact format for threads)
            if hasattr(tweet, "repo_links") and tweet.repo_links:
                lines.extend(
                    MarkdownGenerator.generate_repo_section(
                        tweet.repo_links, detailed=False
                    )
                )

            lines.append("---")
            lines.append("")

        # Add thread summary using MarkdownGenerator
        if hasattr(first_tweet, "thread_summary") and first_tweet.thread_summary:
            lines.extend(
                MarkdownGenerator.generate_summary_section(
                    first_tweet.thread_summary, detailed=True
                )
            )

        # Add thread tags using MarkdownGenerator
        if hasattr(first_tweet, "thread_tags") and first_tweet.thread_tags:
            tag_lines = MarkdownGenerator.generate_tags(
                first_tweet.thread_tags, first_tweet.screen_name
            )
            if tag_lines:
                lines.extend(tag_lines)

        return "\n".join(lines)
