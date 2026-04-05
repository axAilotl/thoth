"""
Markdown Generator - Shared utility for generating markdown sections
Consolidates duplicate markdown generation logic from ContentProcessor and ThreadProcessor
"""

import time
import math
from typing import List, Optional, Dict, Any, Union
from datetime import datetime


class MarkdownGenerator:
    """Shared utility for generating markdown sections used by both tweet and thread processors"""

    @staticmethod
    def generate_frontmatter(metadata: Dict[str, Any]) -> List[str]:
        """Generate YAML frontmatter for markdown files

        Handles various types:
        - strings: quoted
        - lists: YAML list format
        - booleans/numbers: unquoted
        - None: skipped
        """
        lines = ["---"]

        for key, value in metadata.items():
            if value is None:
                continue
            elif isinstance(value, list):
                # YAML list format for Dataview compatibility
                if value:  # Only add non-empty lists
                    formatted_items = [f'"{item}"' if isinstance(item, str) else str(item) for item in value]
                    lines.append(f"{key}: [{', '.join(formatted_items)}]")
            elif isinstance(value, bool):
                lines.append(f"{key}: {str(value).lower()}")
            elif isinstance(value, str):
                # Escape quotes in strings
                escaped = value.replace('"', '\\"')
                lines.append(f'{key}: "{escaped}"')
            else:
                lines.append(f"{key}: {value}")

        lines.append("---")
        lines.append("")
        return lines

    @staticmethod
    def calculate_importance_score(tweet: Any) -> int:
        """Calculate an importance score (0-100) for prioritizing content

        Factors:
        - Engagement (likes, retweets, replies)
        - Content richness (papers, repos, threads, media)
        - Content length
        """
        score = 0.0

        # Engagement scoring (up to 50 points)
        likes = getattr(tweet, 'favorite_count', 0) or 0
        retweets = getattr(tweet, 'retweet_count', 0) or 0
        replies = getattr(tweet, 'reply_count', 0) or 0

        # Log scale for engagement (viral tweets don't dominate too much)
        if likes > 0:
            score += min(math.log10(likes + 1) * 10, 25)
        if retweets > 0:
            score += min(math.log10(retweets + 1) * 8, 15)
        if replies > 0:
            score += min(math.log10(replies + 1) * 5, 10)

        # Content richness (up to 40 points)
        if hasattr(tweet, 'arxiv_papers') and tweet.arxiv_papers:
            score += 15  # Papers are high value
        if hasattr(tweet, 'repo_links') and tweet.repo_links:
            score += 10
        if hasattr(tweet, 'youtube_videos') and tweet.youtube_videos:
            score += 8
        if hasattr(tweet, 'pdf_links') and tweet.pdf_links:
            score += 5
        if getattr(tweet, 'is_self_thread', False):
            score += 5  # Threads often have more context

        # Content length bonus (up to 10 points)
        text_len = len(getattr(tweet, 'full_text', '') or '')
        if text_len > 500:
            score += 10
        elif text_len > 280:
            score += 5

        return min(int(score), 100)

    @staticmethod
    def build_tweet_frontmatter(tweet: Any, include_status: bool = True) -> Dict[str, Any]:
        """Build comprehensive frontmatter metadata from a Tweet object

        This creates Dataview-queryable frontmatter with:
        - Basic info (type, id, author, dates)
        - Engagement metrics (likes, retweets, replies)
        - Content flags (has_paper, has_repo, etc.)
        - Tags (as YAML list)
        - Importance score
        - Reading status
        """
        metadata = {
            # Basic identification
            "type": "tweet",
            "id": tweet.id,
            "author": tweet.screen_name,
            "author_name": getattr(tweet, 'name', None),

            # Dates
            "created": tweet.created_at,
            "processed": time.strftime("%Y-%m-%dT%H:%M:%S"),

            # URL
            "url": f"https://twitter.com/{tweet.screen_name}/status/{tweet.id}",

            # Engagement metrics (Dataview can query these!)
            "likes": getattr(tweet, 'favorite_count', 0) or 0,
            "retweets": getattr(tweet, 'retweet_count', 0) or 0,
            "replies": getattr(tweet, 'reply_count', 0) or 0,

            # Content classification flags
            "has_paper": bool(hasattr(tweet, 'arxiv_papers') and tweet.arxiv_papers),
            "has_repo": bool(hasattr(tweet, 'repo_links') and tweet.repo_links),
            "has_video": bool(any(m.media_type in ['video', 'animated_gif'] for m in (tweet.media_items or []))),
            "has_images": bool(any(m.media_type == 'photo' for m in (tweet.media_items or []))),
            "has_youtube": bool(hasattr(tweet, 'youtube_videos') and tweet.youtube_videos),
            "has_pdf": bool(hasattr(tweet, 'pdf_links') and tweet.pdf_links),
            "is_thread": getattr(tweet, 'is_self_thread', False),

            # Content metrics
            "word_count": len((getattr(tweet, 'full_text', '') or '').split()),

            # Thread info
            "thread_id": getattr(tweet, 'thread_id', None),

            # Importance score for prioritization
            "importance": MarkdownGenerator.calculate_importance_score(tweet),

            # Processing metadata
            "enhanced": getattr(tweet, 'enhanced', False),
        }

        # Add tags as YAML list (for Dataview)
        if hasattr(tweet, 'llm_tags') and tweet.llm_tags:
            # Clean tags: remove #, lowercase, replace spaces
            clean_tags = [tag.lstrip('#').lower().replace(' ', '-') for tag in tweet.llm_tags]
            metadata["tags"] = clean_tags

        # Reading status (can be updated manually or via script)
        if include_status:
            metadata["status"] = "unread"

        return metadata

    @staticmethod
    def build_thread_frontmatter(thread_id: str, thread_tweets: List[Any]) -> Dict[str, Any]:
        """Build comprehensive frontmatter for a thread"""
        first_tweet = thread_tweets[0]

        # Aggregate engagement across all tweets in thread
        total_likes = sum(getattr(t, 'favorite_count', 0) or 0 for t in thread_tweets)
        total_retweets = sum(getattr(t, 'retweet_count', 0) or 0 for t in thread_tweets)
        total_replies = sum(getattr(t, 'reply_count', 0) or 0 for t in thread_tweets)

        # Check for papers/repos across all tweets
        has_paper = any(hasattr(t, 'arxiv_papers') and t.arxiv_papers for t in thread_tweets)
        has_repo = any(hasattr(t, 'repo_links') and t.repo_links for t in thread_tweets)
        has_youtube = any(hasattr(t, 'youtube_videos') and t.youtube_videos for t in thread_tweets)
        has_pdf = any(hasattr(t, 'pdf_links') and t.pdf_links for t in thread_tweets)

        # Total word count
        total_words = sum(len((getattr(t, 'full_text', '') or '').split()) for t in thread_tweets)

        metadata = {
            "type": "thread",
            "thread_id": thread_id,
            "author": first_tweet.screen_name,
            "author_name": getattr(first_tweet, 'name', None),
            "tweet_count": len(thread_tweets),

            # Dates
            "created": first_tweet.created_at,
            "processed": time.strftime("%Y-%m-%dT%H:%M:%S"),

            # URL
            "url": f"https://twitter.com/{first_tweet.screen_name}/status/{first_tweet.id}",

            # Aggregated engagement
            "likes": total_likes,
            "retweets": total_retweets,
            "replies": total_replies,

            # Content flags
            "has_paper": has_paper,
            "has_repo": has_repo,
            "has_youtube": has_youtube,
            "has_pdf": has_pdf,

            # Content metrics
            "word_count": total_words,

            # Calculate importance (use first tweet as proxy, boost for being a thread)
            "importance": min(MarkdownGenerator.calculate_importance_score(first_tweet) + 10, 100),

            "enhanced": True,
            "status": "unread",
        }

        # Add thread tags
        if hasattr(first_tweet, 'thread_tags') and first_tweet.thread_tags:
            clean_tags = [tag.lstrip('#').lower().replace(' ', '-') for tag in first_tweet.thread_tags]
            metadata["tags"] = clean_tags

        return metadata
    
    @staticmethod
    def generate_arxiv_section(papers: List[Any], detailed: bool = True) -> List[str]:
        """Generate ArXiv papers section for markdown files
        
        Args:
            papers: List of ArXiv papers
            detailed: If True, include full details; if False, use compact format
        """
        if not papers:
            return []
        
        lines = []
        section_title = "## ArXiv Papers" if detailed else "### ArXiv Papers"
        lines.append(section_title)
        
        for i, paper in enumerate(papers, 1):
            if detailed:
                lines.append(f"### Paper {i}: {paper.title}")
                lines.append(f"- **ArXiv ID**: [{paper.arxiv_id}]({paper.abs_url})")
                lines.append(f"- **Abstract**: {paper.abstract}")
            else:
                lines.append(f"**Paper {i}: {paper.title}**")
                lines.append(f"- **ArXiv ID**: [{paper.arxiv_id}]({paper.abs_url})")
                lines.append(f"- **Abstract**: {paper.abstract}")
            
            if paper.downloaded and paper.filename:
                lines.append(f"- **PDF**: [[{paper.filename}]]")
            lines.append("")
        
        return lines
    
    @staticmethod
    def generate_pdf_section(pdf_links: List[Any], detailed: bool = True) -> List[str]:
        """Generate PDF documents section for markdown files
        
        Args:
            pdf_links: List of PDF links
            detailed: If True, include full details; if False, use compact format
        """
        if not pdf_links:
            return []
        
        lines = []
        section_title = "## PDF Documents" if detailed else "### PDF Documents"
        lines.append(section_title)
        
        for i, pdf_link in enumerate(pdf_links, 1):
            if detailed:
                lines.append(f"### Document {i}: {pdf_link.title}")
                lines.append(f"- **URL**: [{pdf_link.url}]({pdf_link.url})")
            else:
                lines.append(f"**Document {i}: {pdf_link.title}**")
                lines.append(f"- **URL**: [{pdf_link.url}]({pdf_link.url})")
            
            if pdf_link.downloaded and pdf_link.filename:
                lines.append(f"- **PDF**: [[{pdf_link.filename}]]")
            lines.append("")
        
        return lines
    
    @staticmethod
    def generate_repo_section(repo_links: List[Any], detailed: bool = True) -> List[str]:
        """Generate repository READMEs section for markdown files
        
        Args:
            repo_links: List of repository links
            detailed: If True, include full details; if False, use compact format
        """
        if not repo_links:
            return []
        
        lines = []
        section_title = "## Repository READMEs" if detailed else "### Repository READMEs"
        lines.append(section_title)
        
        for repo_link in repo_links:
            platform_name = repo_link.platform.upper()
            if detailed:
                lines.append(f"### {platform_name} REPO: {repo_link.repo_name}")
            else:
                lines.append(f"**{platform_name} REPO: {repo_link.repo_name}**")
            
            # Use LLM summary if available, otherwise placeholder
            if hasattr(repo_link, 'llm_summary') and repo_link.llm_summary:
                lines.append(f"- **Summary**: {repo_link.llm_summary}")
            else:
                lines.append(f"- **Summary**: PLACEHOLDER")
            
            if repo_link.downloaded and repo_link.filename:
                lines.append(f"- **README**: [[{repo_link.filename}]]")
            lines.append("")
        
        return lines
    
    @staticmethod
    def generate_youtube_section(youtube_videos: List[Any], detailed: bool = True) -> List[str]:
        """Generate YouTube videos section for markdown files
        
        Args:
            youtube_videos: List of YouTube video objects
            detailed: If True, include full details; if False, use compact format
        """
        if not youtube_videos:
            return []
        
        lines = []
        section_title = "## YouTube Videos" if detailed else "### YouTube Videos"
        lines.append(section_title)
        
        for i, video in enumerate(youtube_videos, 1):
            if detailed:
                lines.append(f"### Video {i}: {video.title}")
                lines.append(f"- **Channel**: {video.channel_title}")
                lines.append(f"- **Duration**: {video.duration or 'Unknown'}")
                lines.append(f"- **Views**: {f'{video.view_count:,}' if video.view_count else 'Unknown'}")
                lines.append(f"- **YouTube Link**: [https://youtu.be/{video.video_id}](https://youtu.be/{video.video_id})")
                
                # Embed video if embeddings enabled
                from core.config import config
                if config.get('youtube.enable_embeddings', True):
                    lines.append(f"- **Video**: ![YouTube Video](https://youtu.be/{video.video_id})")
                
                # Include transcript link if available
                if video.transcript:
                    safe_title = f"youtube_{video.video_id}_{MarkdownGenerator._sanitize_filename(video.title)}"
                    lines.append(f"- **Transcript**: [[{safe_title}.md]]")
            else:
                lines.append(f"**Video {i}: {video.title}**")
                lines.append(f"- **Channel**: {video.channel_title}")
                lines.append(f"- **Link**: [https://youtu.be/{video.video_id}](https://youtu.be/{video.video_id})")
            
            lines.append("")
        
        return lines
    
    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """Sanitize filename for safe filesystem usage"""
        import re
        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        # Replace spaces and special chars with underscores
        filename = re.sub(r'[\s\-\[\]()]+', '_', filename)
        # Remove multiple underscores
        filename = re.sub(r'_+', '_', filename)
        # Limit length
        filename = filename[:50]
        # Remove trailing underscores
        filename = filename.strip('_')
        return filename or 'untitled'
    
    @staticmethod
    def generate_media_embeds(media_items: List[Any]) -> List[str]:
        """Generate media embeds with Obsidian-style links"""
        if not media_items:
            return []
        
        lines = []
        for media in media_items:
            if media.media_type in ['video', 'animated_gif']:
                # For videos: show thumbnail but link to video file
                if media.filename and media.video_filename:
                    # Show thumbnail image but link to video
                    lines.append(f"[[{media.video_filename}|![[{media.filename}]]]]")
                elif media.filename:
                    # Fallback: just show thumbnail if video not downloaded
                    lines.append(f"![[{media.filename}]]")
                    lines.append("*Video file not available*")
            else:
                # For photos: embed directly
                if media.filename:
                    lines.append(f"![[{media.filename}]]")
            
            # Add alt text if available
            if hasattr(media, 'alt_text') and media.alt_text:
                lines.append(f"*{media.alt_text}*")
        
        return lines
    
    @staticmethod
    def generate_tags(tags: List[str], author: str) -> List[str]:
        """Generate a tag section with header and formatted tags (author first)"""
        if not tags:
            return []

        lines: List[str] = []
        lines.append("## Tags")

        # Ensure author tag is included and comes first
        tags_with_author = [f"@{author}"] + [tag for tag in tags if tag != f"@{author}"]
        tags_str = ' '.join([f"#{tag.replace(' ', '-').replace('@', '').lower()}" for tag in tags_with_author])
        lines.append(tags_str)
        lines.append("")
        return lines
    
    @staticmethod
    def generate_thread_info_section(tweet: Any, detailed: bool = True) -> List[str]:
        """Generate thread info section for tweets that are part of threads"""
        if tweet.display_type != 'SelfThread':
            return []
        
        lines = []
        section_title = "## Thread Info" if detailed else "### Thread Info"
        lines.append(section_title)
        lines.append("- **Type**: Self Thread")
        
        if tweet.thread_id:
            lines.append(f"- **Thread ID**: {tweet.thread_id}")
            lines.append(f"- **Thread File**: [[thread_{tweet.thread_id}_{tweet.screen_name}]]")
        
        lines.append("")
        return lines
    
    @staticmethod
    def generate_summary_section(summary: str, detailed: bool = True) -> List[str]:
        """Generate summary section if summary exists"""
        if not summary:
            return []
        
        lines = []
        section_title = "## Summary" if detailed else "### Summary"
        lines.append(section_title)
        lines.append(summary)
        lines.append("")
        return lines
    
    @staticmethod
    def should_summarize_tweet(tweet: Any) -> bool:
        """Check if tweet should be summarized (512+ characters)"""
        if not tweet.full_text:
            return False
        return len(tweet.full_text) > 512