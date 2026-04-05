"""
YouTube Video Processor - Embeds YouTube videos and retrieves transcripts
Processes YouTube URLs from tweets and creates embedded videos with transcripts
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass

from core.data_models import ProcessingStats
from core.config import config
from core.path_layout import resolve_vault_root
from core.pipeline_registry import PipelineStage, register_pipeline_stages

logger = logging.getLogger(__name__)


def _youtube_stage_active(cfg) -> bool:
    """Stage predicate ensuring at least one YouTube feature is active."""
    return bool(cfg.get('youtube.enable_embeddings', False) or cfg.get('youtube.enable_transcripts', False))


PIPELINE_STAGES = (
    PipelineStage(
        name='transcripts.youtube_videos',
        config_path='transcripts.youtube_videos',
        description='Fetch YouTube video metadata, transcripts, and embeddings.',
        processor='YouTubeProcessor',
        capabilities=('transcripts', 'youtube'),
        config_keys=(
            'paths.vault_dir',
            'youtube.enable_transcripts',
            'youtube.enable_embeddings',
            'youtube.api_timeout_seconds',
            'youtube.transcript_chunk_size',
            'youtube.enable_llm_transcript_processing'
        ),
        predicate=_youtube_stage_active
    ),
)


register_pipeline_stages(*PIPELINE_STAGES)

@dataclass
class YouTubeVideo:
    """YouTube video data structure"""
    video_id: str
    title: str
    description: str
    published_at: str
    channel_id: str
    channel_title: str
    duration: Optional[str] = None
    view_count: Optional[int] = None
    thumbnail_url: Optional[str] = None
    transcript: Optional[str] = None
    formatted_transcript: Optional[str] = None
    transcript_summary: Optional[str] = None
    transcript_tags: Optional[str] = None
    chunk_metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'video_id': self.video_id,
            'title': self.title,
            'description': self.description,
            'published_at': self.published_at,
            'channel_id': self.channel_id,
            'channel_title': self.channel_title,
            'duration': self.duration,
            'view_count': self.view_count,
            'thumbnail_url': self.thumbnail_url,
            'transcript': self.transcript,
            'formatted_transcript': self.formatted_transcript,
            'transcript_summary': self.transcript_summary,
            'transcript_tags': self.transcript_tags,
            'chunk_metadata': self.chunk_metadata
        }


class YouTubeProcessor:
    """Processes YouTube videos from tweets"""

    def __init__(self, vault_path: str = None):
        self.vault_path = resolve_vault_root(config, override=vault_path)
        self.transcripts_dir = self.vault_path / 'transcripts'
        
        # Create directories
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)
        
        # Get YouTube API key
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        if not self.api_key:
            logger.warning("YOUTUBE_API_KEY environment variable not set - YouTube processing disabled")
        
        # Configuration flags
        self.enable_embeddings = config.get('youtube.enable_embeddings', True)
        self.enable_transcripts = config.get('youtube.enable_transcripts', True)
        
        # Import YouTube transcript API
        self.transcript_api = None
        if self.enable_transcripts:
            try:
                from youtube_transcript_api import YouTubeTranscriptApi
                self.transcript_api = YouTubeTranscriptApi
                logger.info("YouTube transcript API initialized")
            except ImportError:
                logger.warning("youtube-transcript-api package not found. Install with: pip install youtube-transcript-api")
        
        # Initialize transcript LLM processor
        self.transcript_llm_processor = None
        if self.enable_transcripts:
            try:
                from .transcript_llm_processor import TranscriptLLMProcessor
                self.transcript_llm_processor = TranscriptLLMProcessor()
                if self.transcript_llm_processor.is_enabled():
                    logger.info("Transcript LLM processor initialized")
                else:
                    logger.warning("Transcript LLM processor disabled due to configuration")
            except ImportError as e:
                logger.warning(f"Could not initialize transcript LLM processor: {e}")
        
        # HTTP session for API calls
        try:
            import requests
            self.session = requests.Session()
        except ImportError:
            raise ImportError("requests package required. Install with: pip install requests")

        timeout_setting = config.get('youtube.api_timeout_seconds', 30)
        try:
            self.api_timeout = float(timeout_setting)
        except (TypeError, ValueError):
            self.api_timeout = 30.0
    
    def extract_youtube_urls(self, text: str) -> List[str]:
        """Extract YouTube URLs from text"""
        youtube_patterns = [
            r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]+)',
            r'https?://youtu\.be/([a-zA-Z0-9_-]+)',
            r'https?://(?:www\.)?youtube\.com/embed/([a-zA-Z0-9_-]+)',
            r'https?://(?:www\.)?youtube\.com/v/([a-zA-Z0-9_-]+)'
        ]
        
        video_ids = []
        for pattern in youtube_patterns:
            matches = re.findall(pattern, text)
            video_ids.extend(matches)
        
        # Convert video IDs back to standard URLs
        urls = [f"https://youtu.be/{video_id}" for video_id in video_ids]
        return list(set(urls))  # Remove duplicates
    
    def extract_video_id(self, url: str) -> Optional[str]:
        """Extract video ID from YouTube URL"""
        patterns = [
            r'youtube\.com/watch\?v=([a-zA-Z0-9_-]+)',
            r'youtu\.be/([a-zA-Z0-9_-]+)',
            r'youtube\.com/embed/([a-zA-Z0-9_-]+)',
            r'youtube\.com/v/([a-zA-Z0-9_-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None

    def find_existing_transcript_files(self, video_id: str) -> List[Path]:
        """Return durable transcript files already published for a video."""
        return sorted(self.transcripts_dir.glob(f"youtube_{video_id}_*.md"))

    def has_existing_transcript(self, video_id: str) -> bool:
        """Check whether a YouTube transcript markdown already exists."""
        return bool(self.find_existing_transcript_files(video_id))
    
    async def get_video_info(self, video_id: str) -> Optional[YouTubeVideo]:
        """Get video information from YouTube API"""
        if not self.api_key:
            logger.warning("YouTube API key not available")
            return None
        
        try:
            url = f"https://www.googleapis.com/youtube/v3/videos"
            params = {
                'part': 'snippet,statistics,contentDetails',
                'id': video_id,
                'key': self.api_key
            }
            
            response = self.session.get(url, params=params, timeout=self.api_timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('items'):
                logger.warning(f"No video found for ID: {video_id}")
                return None
            
            item = data['items'][0]
            snippet = item['snippet']
            statistics = item.get('statistics', {})
            content_details = item.get('contentDetails', {})
            
            # Get thumbnail URL
            thumbnails = snippet.get('thumbnails', {})
            thumbnail_url = None
            for quality in ['maxres', 'standard', 'high', 'medium', 'default']:
                if quality in thumbnails:
                    thumbnail_url = thumbnails[quality]['url']
                    break
            
            video = YouTubeVideo(
                video_id=video_id,
                title=snippet['title'],
                description=snippet.get('description', ''),
                published_at=snippet['publishedAt'],
                channel_id=snippet['channelId'],
                channel_title=snippet.get('channelTitle', ''),
                duration=content_details.get('duration'),
                view_count=int(statistics.get('viewCount', 0)) if statistics.get('viewCount') else None,
                thumbnail_url=thumbnail_url
            )
            
            logger.info(f"Retrieved video info: {video.title}")
            return video
            
        except Exception as e:
            logger.error(f"Error getting video info for {video_id}: {e}")
            return None
    
    async def get_video_transcript(self, video_id: str) -> Optional[str]:
        """Get video transcript using youtube-transcript-api"""
        if not self.transcript_api or not self.enable_transcripts:
            return None
        
        try:
            # Create API instance and get transcript list
            api = self.transcript_api()
            transcript_list = api.list(video_id)
            
            # Try to get English transcript first
            try:
                transcript = transcript_list.find_transcript(['en'])
            except:
                # If no English, try to get any available transcript
                try:
                    available_transcripts = list(transcript_list)
                    if available_transcripts:
                        transcript = available_transcripts[0]
                    else:
                        logger.debug(f"No transcripts available for {video_id}")
                        return None
                except Exception as list_error:
                    logger.debug(f"Could not list transcripts for {video_id}: {list_error}")
                    return None
            
            # Fetch and format transcript
            transcript_data = transcript.fetch()
            
            # Convert to readable text with timestamps and proper formatting
            transcript_lines = []
            for entry in transcript_data:
                if hasattr(entry, 'text') and hasattr(entry, 'start'):
                    text = entry.text.strip()
                    start_time = entry.start
                elif isinstance(entry, dict):
                    text = entry.get('text', '').strip()
                    start_time = entry.get('start', 0)
                else:
                    continue
                    
                if text:
                    # Convert seconds to MM:SS format
                    minutes = int(start_time // 60)
                    seconds = int(start_time % 60)
                    timestamp = f"[{minutes:02d}:{seconds:02d}]"
                    transcript_lines.append(f"{timestamp} {text}")
            
            # Join with newlines for clean formatting
            full_transcript = '\n'.join(transcript_lines)
            logger.info(f"Retrieved transcript for {video_id} ({len(full_transcript)} characters)")
            return full_transcript
            
        except Exception as e:
            logger.debug(f"Could not get transcript for {video_id}: {e}")
            return None
    async def process_video(
        self,
        video_id: str,
        resume_metadata: bool = True,
        resume_transcripts: bool = True,
        source_label: Optional[str] = None,
    ) -> Tuple[Optional[YouTubeVideo], Dict[str, Any]]:
        """Process a single YouTube video and capture timing/health metrics."""
        metrics = {
            'metadata_seconds': 0.0,
            'transcript_seconds': 0.0,
            'transcript_attempts': 0,
            'transcript_completed': 0,
            'transcript_failed': 0,
        }

        try:
            log_source = source_label or "unattributed source"
            logger.info("📺 [YT] Start video %s for %s", video_id, log_source)

            existing_transcripts = self.find_existing_transcript_files(video_id)
            if resume_transcripts and existing_transcripts:
                logger.info(
                    "Skipping YouTube transcript for video %s from %s - existing note %s",
                    video_id,
                    log_source,
                    existing_transcripts[0],
                )
                return None, metrics
            if not resume_transcripts and existing_transcripts:
                logger.info(
                    "Removing %s existing transcript note(s) for video %s from %s (rerun)",
                    len(existing_transcripts),
                    video_id,
                    log_source,
                )
                for existing in existing_transcripts:
                    try:
                        existing.unlink()
                    except Exception as cleanup_error:
                        logger.warning(f"Could not remove old transcript {existing.name}: {cleanup_error}")

            # Get video information
            metadata_start = time.time()
            video = await self.get_video_info(video_id)
            metrics['metadata_seconds'] += time.time() - metadata_start
            if not video:
                logger.warning(f"API failed for {video_id}, creating basic video object")
                video = YouTubeVideo(
                    video_id=video_id,
                    title=f"YouTube Video {video_id}",
                    description="Video information unavailable (API access failed)",
                    published_at="Unknown",
                    channel_id="Unknown",
                    channel_title="Unknown"
                )

            safe_title = f"youtube_{video_id}_{self._sanitize_filename(video.title)}"
            transcript_file = self.transcripts_dir / f"{safe_title}.md"
            logger.info(
                "YouTube transcript target for video %s from %s: %s",
                video_id,
                log_source,
                transcript_file,
            )

            chunk_metadata: Optional[Dict[str, Any]] = None

            # Get transcript if enabled
            if self.enable_transcripts:
                logger.debug(f"📝 [YT] Try transcript {video_id}")
                transcript_fetch_start = time.time()
                raw_transcript = await self.get_video_transcript(video_id)
                metrics['transcript_seconds'] += time.time() - transcript_fetch_start

                formatted_transcript = None
                if raw_transcript and self.transcript_llm_processor and self.transcript_llm_processor.is_enabled():
                    logger.debug(f"🤖 [YT] LLM format transcript {video_id}")
                    logger.info(
                        "Processing transcript with LLM for video %s from %s -> %s",
                        video_id,
                        log_source,
                        transcript_file,
                    )
                    metrics['transcript_attempts'] += 1
                    llm_start = time.time()
                    formatted_transcript = await self.transcript_llm_processor.process_transcript(
                        raw_transcript,
                        context_id=video_id,
                        source_label=f"{log_source} / youtube:{video_id}",
                        output_path=transcript_file,
                    )
                    metrics['transcript_seconds'] += time.time() - llm_start

                video.transcript = raw_transcript
                video.transcript_summary = None
                video.transcript_tags = None

                if formatted_transcript:
                    if isinstance(formatted_transcript, dict):
                        formatted_text = formatted_transcript.get('text') or ''
                        video.formatted_transcript = formatted_text or None
                        video.transcript_summary = formatted_transcript.get('summary') or None
                        video.transcript_tags = formatted_transcript.get('tags') or None
                        chunk_metadata = formatted_transcript.get('chunk_metadata') or {}
                        formatted_length = len(formatted_text)
                    else:
                        video.formatted_transcript = formatted_transcript
                        formatted_length = len(formatted_transcript)
                        chunk_metadata = None
                    if raw_transcript:
                        logger.info(f"✅ LLM formatted transcript: {len(raw_transcript)} → {formatted_length} characters")
                    else:
                        logger.info(f"✅ LLM generated transcript: {formatted_length} characters")

                    if chunk_metadata and chunk_metadata.get('chunks_failed'):
                        metrics['transcript_failed'] += chunk_metadata.get('chunks_failed', 0)
                        if chunk_metadata.get('fallback_used') and not chunk_metadata.get('chunks_failed'):
                            metrics['transcript_failed'] += 1
                    else:
                        metrics['transcript_completed'] += 1
                else:
                    video.formatted_transcript = None
                    if raw_transcript:
                        logger.info("Using raw transcript (LLM formatting failed or disabled)")
                        metrics['transcript_failed'] += 1
                        chunk_metadata = {
                            'chunks_total': 0,
                            'chunks_processed': 0,
                            'chunks_failed': 1,
                            'fallback_used': True,
                        }

                video.chunk_metadata = chunk_metadata
            else:
                video.chunk_metadata = None

            logger.debug(f"💾 [YT] Write transcript file {transcript_file.name}")
            await self._create_transcript_file(video, transcript_file)

            logger.info(
                "✅ Processed YouTube video %s for %s -> %s",
                video.title,
                log_source,
                transcript_file,
            )
            return video, metrics

        except Exception as e:
            logger.error(f"Error processing video {video_id} for {source_label or 'unattributed source'}: {e}")
            metrics['transcript_failed'] += 1
            return None, metrics
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe filesystem usage"""
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
    
    async def _create_transcript_file(self, video: YouTubeVideo, file_path: Path):
        """Create transcript markdown file"""
        try:
            # Format published date
            try:
                from datetime import datetime
                pub_date = datetime.fromisoformat(video.published_at.replace('Z', '+00:00'))
                formatted_date = pub_date.strftime('%Y-%m-%d %H:%M:%S UTC')
            except:
                formatted_date = video.published_at
            
            # Create content
            chunk_meta = video.chunk_metadata or {}
            chunk_yaml = ""
            if chunk_meta:
                yaml_lines = ["chunk_health:"]
                for key in ['chunks_total', 'chunks_processed', 'chunks_failed', 'fallback_used']:
                    if key in chunk_meta:
                        yaml_lines.append(f"  {key}: {chunk_meta[key]}")
                failed = chunk_meta.get('failed_chunks') or []
                if failed:
                    yaml_lines.append("  failed_chunks:")
                    for idx in failed:
                        yaml_lines.append(f"    - {idx}")
                chunk_yaml = "\n".join(yaml_lines) + "\n"

            content = f"""---
video_id: {video.video_id}
title: {video.title}
channel: {video.channel_title}
channel_id: {video.channel_id}
published_at: {formatted_date}
duration: {video.duration or 'Unknown'}
view_count: {video.view_count or 'Unknown'}
processed_at: {datetime.now().isoformat()}
{chunk_yaml if chunk_yaml else ''}---

# 📺 {video.title}

## 📊 Video Info
- **Channel**: {video.channel_title}
- **Published**: {formatted_date}
- **Duration**: {video.duration or 'Unknown'}
- **Views**: {f'{video.view_count:,}' if video.view_count else 'Unknown'}

## 🔗 Links
- **YouTube**: [https://youtu.be/{video.video_id}](https://youtu.be/{video.video_id})

## 📝 Description
{video.description[:1000] + ('...' if len(video.description) > 1000 else '') if video.description else 'No description available'}

"""

            if video.transcript_summary:
                content += f"""## 🧠 LLM Summary
{video.transcript_summary}

"""

            if chunk_meta:
                total = chunk_meta.get('chunks_total')
                processed_chunks = chunk_meta.get('chunks_processed')
                failed_chunks = chunk_meta.get('chunks_failed')
                fallback_used = chunk_meta.get('fallback_used')
                content += "## 🧩 Chunk Health\n"
                if total is not None and processed_chunks is not None:
                    content += f"- Processed: {processed_chunks}/{total}\n"
                if failed_chunks:
                    content += f"- Failed chunks: {failed_chunks}\n"
                content += f"- Fallback used: {'Yes' if fallback_used else 'No'}\n\n"

            # Add embedded video
            if self.enable_embeddings:
                content += f"""## 📺 Embedded Video
![YouTube Video](https://youtu.be/{video.video_id})

"""

            # Add transcript if available (prefer formatted, fallback to raw)
            if video.formatted_transcript:
                content += f"""## 📄 Transcript (LLM Formatted)
{video.formatted_transcript}

"""
            elif video.transcript:
                content += f"""## 📄 Transcript
{video.transcript}

"""

            # Add tags
            extra_tags = []
            if video.transcript_tags:
                for tag in video.transcript_tags.split(','):
                    tag = tag.strip()
                    if tag:
                        extra_tags.append(f"#{tag.replace(' ', '_')}")
            channel_slug = (video.channel_title or 'unknown_channel').lower().replace(' ', '_')
            tag_line = f"#youtube #video #{channel_slug} #transcript"
            if extra_tags:
                tag_line += " " + " ".join(extra_tags)
            content += tag_line
            
            # Write file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.debug(f"Created transcript file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error creating transcript file for {video.video_id}: {e}")
            raise
    
    async def process_youtube_urls(
        self,
        urls: List[str],
        resume_metadata: bool = True,
        resume_transcripts: bool = True,
        source_label: Optional[str] = None,
    ) -> ProcessingStats:
        """Process multiple YouTube URLs while tracking timing/health metrics."""
        stats = ProcessingStats()
        aggregate = {
            'metadata_seconds': 0.0,
            'transcript_seconds': 0.0,
            'transcript_attempts': 0,
            'transcript_completed': 0,
            'transcript_failed': 0,
        }
        processed_videos: List[YouTubeVideo] = []

        for url in urls:
            video_id = self.extract_video_id(url)
            if not video_id:
                logger.warning(f"Could not extract video ID from URL: {url}")
                stats.errors += 1
                continue

            try:
                video, metrics = await self.process_video(
                    video_id,
                    resume_metadata=resume_metadata,
                    resume_transcripts=resume_transcripts,
                    source_label=source_label,
                )
                for key in aggregate:
                    aggregate[key] += metrics.get(key, 0)

                if video:
                    processed_videos.append(video)
                    stats.updated += 1
                else:
                    stats.skipped += 1
                stats.total_processed += 1

            except Exception as e:
                logger.error(
                    "Error processing YouTube URL %s for %s: %s",
                    url,
                    source_label or "unattributed source",
                    e,
                )
                stats.errors += 1

        stats.extras.update(aggregate)
        stats.extras['videos'] = processed_videos
        return stats
    
    def get_stats_summary(self, stats: ProcessingStats) -> str:
        """Generate formatted stats summary"""
        return (f"YouTube processing: {stats.updated} processed, "
                f"{stats.skipped} skipped, {stats.errors} errors")
