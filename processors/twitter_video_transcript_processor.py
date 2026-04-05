"""
Twitter Video Transcript Processor - Processes Twitter videos with Whisper local transcription
Extracts audio from Twitter videos, transcribes with local Whisper, and creates transcript files
"""

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from openai import OpenAI

from core.data_models import Tweet, ProcessingStats
from core.config import config
from processors.transcript_llm_processor import TranscriptLLMProcessor
from processors.deepgram_transcript_processor import DeepgramTranscriptProcessor

logger = logging.getLogger(__name__)


class TwitterVideoTranscriptProcessor:
    """Processes Twitter videos to generate transcripts using local Whisper"""
    
    def __init__(self, vault_path: str = None):
        self.config = config
        self.vault_path = Path(vault_path or config.get('vault_dir', 'knowledge_vault'))
        self.transcripts_dir = self.vault_path / 'transcripts'
        self.temp_dir = Path(config.get('whisper.temp_dir', 'temp_audio'))
        
        # Create directories
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Configuration
        self.enabled = config.get('whisper.enabled', True)
        self.base_url = config.get('whisper.base_url', 'http://localhost:11434/v1')
        self.model = config.get('whisper.model', 'Systran/faster-distil-whisper-large-v3')
        self.response_format = config.get('whisper.response_format', 'text')
        self.max_chunk_mb = config.get('whisper.max_chunk_mb', 25)
        self.chunk_duration_minutes = config.get('whisper.chunk_duration_minutes', 10)
        self.min_duration_seconds = config.get('whisper.min_duration_seconds', 60)
        self.ffmpeg_path = config.get('whisper.ffmpeg_path', 'ffmpeg')
        self.ffprobe_path = config.get('whisper.ffprobe_path', 'ffprobe')
        self.target_bitrate_kbps = config.get('whisper.target_bitrate_kbps', 128)
        
        # Initialize OpenAI client for local Whisper
        self.whisper_client = None
        if self.enabled:
            try:
                self.whisper_client = OpenAI(
                    base_url=self.base_url,
                    api_key="sk-local"  # Dummy key for local server
                )
                logger.info(f"Whisper client initialized with base_url: {self.base_url}")
            except Exception as e:
                logger.warning(f"Could not initialize Whisper client: {e}")
                self.enabled = False
        
        # Initialize transcript LLM processor for cleaning
        self.transcript_llm_processor = None
        if self.enabled:
            try:
                self.transcript_llm_processor = TranscriptLLMProcessor()
                if not self.transcript_llm_processor.is_enabled():
                    logger.warning("Transcript LLM processor not enabled - will use raw transcripts")
            except Exception as e:
                logger.warning(f"Could not initialize transcript LLM processor: {e}")
    
    def is_enabled(self) -> bool:
        """Check if the processor is enabled and properly configured"""
        # Check if either Whisper or Deepgram is enabled
        whisper_enabled = self.enabled and self.whisper_client is not None
        deepgram_enabled = self.config.get('deepgram.enabled', False)
        
        return whisper_enabled or deepgram_enabled
    
    def _get_processor(self):
        """Get the appropriate processor (Deepgram preferred if enabled, otherwise Whisper)"""
        deepgram_enabled = self.config.get('deepgram.enabled', False)
        
        if deepgram_enabled:
            deepgram_processor = DeepgramTranscriptProcessor()
            if deepgram_processor.is_enabled():
                logger.info("Using Deepgram for transcription (faster)")
                return deepgram_processor
            else:
                logger.warning("Deepgram enabled but not configured, falling back to Whisper")
        
        if self.enabled and self.whisper_client is not None:
            logger.info("Using Whisper for transcription")
            return self
        else:
            logger.error("No transcription service available")
            return None
    
    async def process_tweets(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Process tweets for video transcripts"""
        stats = ProcessingStats()
        
        if not self.is_enabled():
            logger.warning("Twitter video transcript processor is not enabled or properly configured")
            return stats
        
        # Get the appropriate processor (Deepgram or Whisper)
        processor = self._get_processor()
        if not processor:
            logger.error("No transcription service available")
            return stats
        
        # If using Deepgram, delegate to it
        if isinstance(processor, DeepgramTranscriptProcessor):
            result = await processor.process_tweets(tweets, resume=resume)
            stats.processed = result.get("processed", 0)
            stats.created = result.get("created", 0)
            stats.skipped = result.get("skipped", 0)
            stats.errors = result.get("errors", 0)
            return stats
        
        # Continue with Whisper processing
        # Check if ffmpeg/ffprobe are available
        if not self._check_ffmpeg_available():
            logger.error("ffmpeg/ffprobe not available - cannot process video transcripts")
            return stats
        
        # Skip connectivity test - let actual transcription handle server issues
        logger.info("Whisper server configured, will test during actual transcription")
        
        for tweet in tweets:
            try:
                if not tweet.media_items:
                    stats.skipped += 1
                    continue
                
                # Find videos in this tweet
                videos = []
                for media in tweet.media_items:
                    if media.media_type in ['video', 'animated_gif']:
                        # If video_filename is not set, try to find the video file
                        if not media.video_filename:
                            # Try to construct the expected filename
                            for i in range(1, 5):  # Check up to 4 media items
                                potential_filename = f"{tweet.id}_media_1_{i}.mp4"
                                potential_path = self.vault_path / 'media' / potential_filename
                                if potential_path.exists():
                                    media.video_filename = potential_filename
                                    logger.debug(f"Found video file for tweet {tweet.id}: {potential_filename}")
                                    break
                        
                        if media.video_filename:
                            videos.append(media)
                
                if not videos:
                    stats.skipped += 1
                    continue
                
                # Check if transcript already exists
                transcript_filename = f"twitter_{tweet.id}_{tweet.screen_name}.md"
                transcript_file = self.transcripts_dir / transcript_filename
                
                if resume and transcript_file.exists():
                    logger.debug(f"Transcript already exists for tweet {tweet.id}")
                    stats.skipped += 1
                    continue
                
                # Process each video
                for video in videos:
                    if await self._process_video_transcript(tweet, video):
                        stats.updated += 1
                        break  # Only create one transcript per tweet
                
                stats.total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing tweet {tweet.id} for video transcripts: {e}")
                stats.errors += 1
        
        logger.info(f"🎤 Video transcript processing complete: {stats.updated} created, {stats.skipped} skipped, {stats.errors} errors")
        return stats
    
    async def _process_video_transcript(self, tweet: Tweet, video) -> bool:
        """Process a single video to generate transcript"""
        try:
            # Check video duration
            duration_seconds = await self._get_video_duration(video)
            if duration_seconds is None:
                logger.warning(f"Could not determine duration for video {video.media_id}")
                return False
            
            if duration_seconds < self.min_duration_seconds:
                logger.debug(f"Video {video.media_id} too short ({duration_seconds}s < {self.min_duration_seconds}s)")
                return False
            
            logger.info(f"Processing video transcript for tweet {tweet.id} (duration: {duration_seconds}s)")
            
            # Extract audio
            audio_file = await self._extract_audio(video)
            if not audio_file:
                return False
            
            try:
                # Transcribe audio
                raw_transcript = await self._transcribe_audio(audio_file)
                if not raw_transcript:
                    return False
                
                # Clean transcript with LLM
                cleaned_transcript = raw_transcript
                if self.transcript_llm_processor and self.transcript_llm_processor.is_enabled():
                    context_id = f"{tweet.id}:{getattr(video, 'media_id', '')}"
                    cleaned_transcript = await self.transcript_llm_processor.process_transcript(
                        raw_transcript,
                        context_id=context_id
                    )
                    if not cleaned_transcript:
                        logger.warning("LLM transcript cleaning failed, using raw transcript")
                        cleaned_transcript = raw_transcript
                
                # Create transcript file
                await self._create_transcript_file(tweet, cleaned_transcript)
                
                logger.info(f"✅ Created transcript for tweet {tweet.id}")
                return True
                
            finally:
                # Clean up audio file
                if audio_file and audio_file.exists():
                    audio_file.unlink()
                    logger.debug(f"Cleaned up audio file: {audio_file}")
        
        except Exception as e:
            logger.error(f"Error processing video transcript for tweet {tweet.id}: {e}")
            return False
    
    async def _get_video_duration(self, video) -> Optional[float]:
        """Get video duration in seconds"""
        # First try to use duration from GraphQL data
        if video.duration_millis:
            return video.duration_millis / 1000.0
        
        # Fallback: use ffprobe on downloaded video file
        if not video.video_filename:
            return None
        
        video_path = self.vault_path / 'media' / video.video_filename
        if not video_path.exists():
            return None
        
        try:
            cmd = [
                self.ffprobe_path,
                '-v', 'quiet',
                '-show_entries', 'format=duration',
                '-of', 'csv=p=0',
                str(video_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                duration = float(result.stdout.strip())
                logger.debug(f"Video duration from ffprobe: {duration}s")
                return duration
            else:
                logger.warning(f"ffprobe failed: {result.stderr}")
                return None
                
        except Exception as e:
            logger.warning(f"Error getting video duration: {e}")
            return None
    
    async def _extract_audio(self, video) -> Optional[Path]:
        """Extract audio from video file to MP3"""
        if not video.video_filename:
            return None
        
        video_path = self.vault_path / 'media' / video.video_filename
        if not video_path.exists():
            logger.warning(f"Video file not found: {video_path}")
            return None
        
        # Generate audio filename
        audio_filename = f"{video.media_id}_audio.mp3"
        audio_path = self.temp_dir / audio_filename
        
        try:
            cmd = [
                self.ffmpeg_path,
                '-i', str(video_path),
                '-vn',  # No video
                '-acodec', 'mp3',
                '-ab', f'{self.target_bitrate_kbps}k',
                '-ar', '16000',  # 16kHz sample rate for Whisper
                '-y',  # Overwrite output file
                str(audio_path)
            ]
            
            logger.debug(f"Extracting audio: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            
            if result.returncode == 0 and audio_path.exists():
                file_size_mb = audio_path.stat().st_size / (1024 * 1024)
                logger.debug(f"Audio extracted: {audio_path} ({file_size_mb:.1f} MB)")
                return audio_path
            else:
                logger.error(f"ffmpeg failed: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting audio: {e}")
            return None
    
    async def _transcribe_audio(self, audio_path: Path) -> Optional[str]:
        """Transcribe audio file using local Whisper"""
        try:
            # Get audio duration to determine if we need to chunk
            duration = await self._get_audio_duration(audio_path)
            if duration is None:
                return None
            
            chunk_duration_seconds = self.chunk_duration_minutes * 60
            
            if duration <= chunk_duration_seconds:
                # Single file transcription
                raw_transcript = await self._transcribe_single_file(audio_path)
                if raw_transcript:
                    # Process with LLM for better formatting
                    if self.transcript_llm_processor:
                        try:
                            formatted_result = await self.transcript_llm_processor.process_transcript(
                                raw_transcript,
                                context_id=f"{tweet.id}:{getattr(video, 'media_id', '')}"
                            )
                            if formatted_result and isinstance(formatted_result, dict):
                                logger.info(f"LLM formatted single file transcript: {len(formatted_result['text'])} characters")
                                return formatted_result
                            elif formatted_result:
                                # Fallback for old string format
                                logger.info(f"LLM formatted single file transcript: {len(formatted_result)} characters")
                                return {'text': formatted_result, 'summary': '', 'tags': ''}
                        except Exception as e:
                            logger.warning(f"LLM formatting failed for single file, using raw transcript: {e}")
                    
                    return {'text': raw_transcript, 'summary': '', 'tags': ''}
                return None
            else:
                # Split and transcribe chunks by time
                return await self._transcribe_chunked_file(audio_path, duration, chunk_duration_seconds)
                
        except Exception as e:
            logger.error(f"Error transcribing audio: {e}")
            return None
    
    async def _get_audio_duration(self, audio_path: Path) -> Optional[float]:
        """Get audio file duration in seconds"""
        try:
            cmd = [
                self.ffprobe_path,
                '-v', 'quiet',
                '-show_entries', 'format=duration',
                '-of', 'csv=p=0',
                str(audio_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return float(result.stdout.strip())
            else:
                logger.error(f"Could not get audio duration: {result.stderr}")
                return None
        except Exception as e:
            logger.error(f"Error getting audio duration: {e}")
            return None
    
    async def _transcribe_single_file(self, audio_path: Path) -> Optional[str]:
        """Transcribe a single audio file using direct requests"""
        try:
            import requests
            
            url = f"{self.base_url}/v1/audio/transcriptions"
            
            with open(audio_path, 'rb') as audio_file:
                files = {
                    'file': (audio_path.name, audio_file, 'audio/mpeg')
                }
                data = {
                    'model': self.model,
                    'response_format': self.response_format
                }
                
                response = requests.post(url, files=files, data=data, timeout=600)
                
                if response.status_code == 200:
                    transcript = response.text
                    logger.debug(f"Transcribed single file: {len(transcript)} characters")
                    return transcript
                else:
                    logger.error(f"Transcription failed: {response.status_code} - {response.text}")
                    return None
            
        except Exception as e:
            logger.error(f"Error transcribing single file: {e}")
            return None
    
    async def _transcribe_chunked_file(self, audio_path: Path, total_duration: float, chunk_duration_seconds: float) -> Optional[str]:
        """Split large audio file and transcribe chunks by time intervals"""
        try:
            num_chunks = int(total_duration / chunk_duration_seconds) + 1
            
            logger.info(f"Splitting audio into {num_chunks} chunks of {self.chunk_duration_minutes} minutes each")
            
            # Create chunk files
            chunk_files = []
            for i in range(num_chunks):
                start_time = i * chunk_duration_seconds
                # Calculate actual chunk duration (last chunk might be shorter)
                actual_chunk_duration = min(chunk_duration_seconds, total_duration - start_time)
                
                chunk_filename = f"{audio_path.stem}_chunk_{i:03d}.mp3"
                chunk_path = self.temp_dir / chunk_filename
                
                cmd = [
                    self.ffmpeg_path,
                    '-i', str(audio_path),
                    '-ss', str(start_time),
                    '-t', str(actual_chunk_duration),
                    '-c', 'copy',
                    '-y',
                    str(chunk_path)
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0 and chunk_path.exists():
                    chunk_files.append(chunk_path)
                    logger.debug(f"Created chunk {i+1}/{num_chunks}: {start_time:.1f}s-{start_time + actual_chunk_duration:.1f}s")
                else:
                    logger.warning(f"Failed to create chunk {i}: {result.stderr}")
            
            # Transcribe each chunk sequentially
            transcripts = []
            for i, chunk_path in enumerate(chunk_files):
                logger.info(f"Transcribing chunk {i+1}/{len(chunk_files)}")
                chunk_transcript = await self._transcribe_single_file(chunk_path)
                if chunk_transcript:
                    transcripts.append(chunk_transcript)
                    logger.debug(f"Chunk {i+1} transcribed: {len(chunk_transcript)} characters")
                else:
                    logger.warning(f"Failed to transcribe chunk {i+1}")
                
                # Clean up chunk file
                chunk_path.unlink()
            
            # Combine transcripts and send to LLM for formatting
            if transcripts:
                full_transcript = ' '.join(transcripts)
                logger.info(f"Combined {len(transcripts)} chunks into {len(full_transcript)} character transcript")
                
                # Process with LLM for better formatting
                if self.transcript_llm_processor:
                    try:
                        formatted_result = await self.transcript_llm_processor.process_transcript(
                            full_transcript,
                            context_id=f"{tweet.id}:{getattr(video, 'media_id', '')}"
                        )
                        if formatted_result and isinstance(formatted_result, dict):
                            logger.info(f"LLM formatted transcript: {len(formatted_result['text'])} characters")
                            return formatted_result
                        elif formatted_result:
                            # Fallback for old string format
                            logger.info(f"LLM formatted transcript: {len(formatted_result)} characters")
                            return {'text': formatted_result, 'summary': '', 'tags': ''}
                    except Exception as e:
                        logger.warning(f"LLM formatting failed, using raw transcript: {e}")
                
                return {'text': full_transcript, 'summary': '', 'tags': ''}
            else:
                logger.error("No chunks were successfully transcribed")
                return None
                
        except Exception as e:
            logger.error(f"Error transcribing chunked file: {e}")
            return None
    
    async def _create_transcript_file(self, tweet: Tweet, transcript_data):
        """Create transcript markdown file"""
        try:
            transcript_filename = f"twitter_{tweet.id}_{tweet.screen_name}.md"
            transcript_file = self.transcripts_dir / transcript_filename
            
            # Handle both old string format and new dict format
            if isinstance(transcript_data, dict):
                transcript_text = transcript_data.get('text', '')
                summary = transcript_data.get('summary', '')
                tags = transcript_data.get('tags', '')
            else:
                # Fallback for old string format
                transcript_text = str(transcript_data)
                summary = ''
                tags = ''
            
            # Format tags for Obsidian (convert comma-separated to #tag format)
            formatted_tags = self._format_tags_for_obsidian(tags)
            
            # Determine source link
            if tweet.is_self_thread and tweet.thread_id:
                source_link = f"[[thread_{tweet.thread_id}_{tweet.screen_name}]]"
            else:
                source_link = f"[[{tweet.id}_{tweet.screen_name}]]"
            
            # Create content with summary and tags sections
            content = f"""---
type: "twitter_video_transcript"
thread_id: "{tweet.thread_id or ''}"
author: "{tweet.screen_name}"
created_at: "{tweet.created_at}"
url: "https://twitter.com/{tweet.screen_name}/status/{tweet.id}"
processed_at: "{datetime.now().isoformat()}"
---

## Source
{source_link}

## Summary
{summary if summary else 'No summary available'}

## Transcript
{transcript_text}

## Tags
{formatted_tags}

#twitter #video #transcript #{tweet.screen_name.lower()}
"""
            
            # Write file
            with open(transcript_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.debug(f"Created transcript file: {transcript_file}")
            
        except Exception as e:
            logger.error(f"Error creating transcript file for tweet {tweet.id}: {e}")
            raise
    
    def _format_tags_for_obsidian(self, tags_string: str) -> str:
        """Format comma-separated tags string into Obsidian-style #tag format"""
        if not tags_string or tags_string.strip() == '':
            return 'No tags available'
        
        # Split by comma and clean up each tag
        tags = [tag.strip() for tag in tags_string.split(',') if tag.strip()]
        
        # Format each tag with # prefix and clean up for Obsidian
        formatted_tags = []
        for tag in tags:
            # Remove any existing # symbols and clean up
            clean_tag = tag.replace('#', '').strip()
            # Replace spaces with underscores for Obsidian compatibility
            clean_tag = clean_tag.replace(' ', '_').lower()
            # Remove special characters that might cause issues
            clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c == '_')
            if clean_tag:
                formatted_tags.append(f"#{clean_tag}")
        
        return ' '.join(formatted_tags) if formatted_tags else 'No tags available'
    
    def _check_ffmpeg_available(self) -> bool:
        """Check if ffmpeg and ffprobe are available"""
        try:
            # Check ffmpeg
            result = subprocess.run([self.ffmpeg_path, '-version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error(f"ffmpeg not available: {result.stderr}")
                return False
            
            # Check ffprobe
            result = subprocess.run([self.ffprobe_path, '-version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error(f"ffprobe not available: {result.stderr}")
                return False
            
            logger.debug("ffmpeg and ffprobe are available")
            return True
            
        except Exception as e:
            logger.error(f"Error checking ffmpeg availability: {e}")
            return False
    
