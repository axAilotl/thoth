"""
Deepgram-based Twitter video transcript processor
Faster alternative to Whisper for video transcription
"""

import asyncio
import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from core.config import config
from core.data_models import Tweet, MediaItem
from processors.transcript_llm_processor import TranscriptLLMProcessor

logger = logging.getLogger(__name__)


class DeepgramTranscriptProcessor:
    """Process Twitter video transcripts using Deepgram API"""
    
    def __init__(self, config_path: str = 'config.json'):
        self.config = config
        
        # Paths
        vault_path = self.config.get('vault_dir', 'knowledge_vault')
        self.vault_path = Path(vault_path)
        self.transcripts_dir = self.vault_path / 'transcripts'
        self.temp_dir = Path(self.config.get('deepgram.temp_dir', 'temp_audio'))
        
        # Create directories
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Configuration
        self.enabled = self.config.get('deepgram.enabled', False)
        self.api_key = os.getenv(self.config.get('deepgram.api_key_env', 'DEEPGRAM_API_KEY'))
        self.model = self.config.get('deepgram.model', 'nova-2')
        self.smart_format = self.config.get('deepgram.smart_format', True)
        self.chunk_duration_minutes = self.config.get('deepgram.chunk_duration_minutes', 10)
        self.min_duration_seconds = self.config.get('deepgram.min_duration_seconds', 60)
        self.ffmpeg_path = self.config.get('whisper.ffmpeg_path', 'ffmpeg')  # Reuse ffmpeg settings
        self.ffprobe_path = self.config.get('whisper.ffprobe_path', 'ffprobe')
        self.target_bitrate_kbps = self.config.get('whisper.target_bitrate_kbps', 128)
        
        # Initialize LLM processor for transcript formatting
        self.transcript_llm_processor = None
        try:
            transcript_task = self.config.get('llm', {}).get('tasks', {}).get('transcript', {})
            if transcript_task.get('enabled', True):
                self.transcript_llm_processor = TranscriptLLMProcessor()
                if not self.transcript_llm_processor.is_enabled():
                    logger.warning("LLM transcript processing disabled")
                    self.transcript_llm_processor = None
        except Exception as e:
            logger.warning(f"Could not initialize LLM processor: {e}")
            self.transcript_llm_processor = None
    
    def is_enabled(self) -> bool:
        """Check if Deepgram processing is enabled and configured"""
        if not self.enabled:
            return False
        
        if not self.api_key:
            logger.error("Deepgram API key not found in environment variables")
            return False
        
        # Check if ffmpeg is available
        try:
            subprocess.run([self.ffmpeg_path, '-version'], capture_output=True, timeout=10)
            subprocess.run([self.ffprobe_path, '-version'], capture_output=True, timeout=10)
            return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            logger.error("ffmpeg/ffprobe not available")
            return False
    
    async def process_tweets(self, tweets: list[Tweet], limit: int = None, resume: bool = True) -> dict:
        """Process video transcripts for a list of tweets"""
        if not self.is_enabled():
            logger.error("Deepgram transcript processing not enabled or configured")
            return {"processed": 0, "created": 0, "skipped": 0, "errors": 0}
        
        logger.info(f"🎤 Processing {len(tweets)} tweets for video transcripts using Deepgram")
        logger.info(f"🎤 Model: {self.model}")
        logger.info(f"🎤 Min duration: {self.min_duration_seconds}s")
        logger.info(f"📂 Transcripts will be saved to: {self.transcripts_dir}")
        
        stats = {"processed": 0, "created": 0, "skipped": 0, "errors": 0}
        
        for i, tweet in enumerate(tweets[:limit] if limit else tweets):
            try:
                # Check if transcript already exists (resume mode)
                if resume:
                    transcript_filename = f"twitter_{tweet.id}_{tweet.screen_name}.md"
                    transcript_file = self.transcripts_dir / transcript_filename
                    if transcript_file.exists():
                        stats["skipped"] += 1
                        continue
                
                # Find videos in this tweet
                videos = []
                for media in tweet.media_items:
                    if media.media_type in ['video', 'animated_gif']:
                        # If video_filename is not set, try to find the video file
                        if not media.video_filename:
                            # Try to construct the expected filename
                            for j in range(1, 5):  # Check up to 4 media items
                                potential_filename = f"{tweet.id}_media_1_{j}.mp4"
                                potential_path = self.vault_path / 'media' / potential_filename
                                if potential_path.exists():
                                    media.video_filename = potential_filename
                                    logger.debug(f"Found video file for tweet {tweet.id}: {potential_filename}")
                                    break
                        
                        if media.video_filename:
                            videos.append(media)
                
                if not videos:
                    stats["skipped"] += 1
                    continue
                
                # Process each video
                for video in videos:
                    result = await self._process_video_transcript(tweet, video)
                    if result:
                        stats["created"] += 1
                        logger.info(f"✅ Created transcript for tweet {tweet.id}")
                    else:
                        stats["errors"] += 1
                        logger.error(f"❌ Failed to create transcript for tweet {tweet.id}")
                
                stats["processed"] += 1
                
            except Exception as e:
                logger.error(f"Error processing tweet {tweet.id}: {e}")
                stats["errors"] += 1
        
        return stats
    
    async def _process_video_transcript(self, tweet: Tweet, video: MediaItem) -> bool:
        """Process a single video transcript"""
        try:
            # Check video duration
            duration = await self._get_video_duration(video)
            if duration is None:
                logger.warning(f"Could not get duration for video {video.media_id}")
                return False
            
            duration_seconds = duration / 1000
            if duration_seconds < self.min_duration_seconds:
                logger.debug(f"Video {video.media_id} too short ({duration_seconds:.1f}s < {self.min_duration_seconds}s)")
                return False
            
            logger.info(f"Processing video transcript for tweet {tweet.id} (duration: {duration_seconds:.3f}s)")
            
            # Extract audio
            audio_path = await self._extract_audio(video)
            if not audio_path:
                return False
            
            # Transcribe audio
            transcript = await self._transcribe_audio(
                audio_path,
                context_id=f"{tweet.id}:{video.media_id}"
            )
            if not transcript:
                return False
            
            # Create transcript file
            await self._create_transcript_file(tweet, transcript)
            
            # Clean up
            if audio_path.exists():
                audio_path.unlink()
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing video transcript: {e}")
            return False
    
    async def _get_video_duration(self, video: MediaItem) -> Optional[int]:
        """Get video duration in milliseconds"""
        # First try to use duration from media item
        if video.duration_millis:
            return video.duration_millis
        
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
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                duration = float(result.stdout.strip())
                return int(duration * 1000)  # Convert to milliseconds
            else:
                logger.error(f"ffprobe failed: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting video duration: {e}")
            return None
    
    async def _extract_audio(self, video: MediaItem) -> Optional[Path]:
        """Extract audio from video file"""
        if not video.video_filename:
            return None
        
        video_path = self.vault_path / 'media' / video.video_filename
        if not video_path.exists():
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
                '-ar', '16000',  # 16kHz sample rate
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
    
    async def _transcribe_audio(self, audio_path: Path, context_id: Optional[str] = None) -> Optional[str]:
        """Transcribe audio file using Deepgram API"""
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
                                context_id=context_id
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
                return await self._transcribe_chunked_file(
                    audio_path,
                    duration,
                    chunk_duration_seconds,
                    context_id=context_id
                )
                
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
        """Transcribe a single audio file using Deepgram API"""
        try:
            import requests
            
            # Upload file to Deepgram
            url = "https://api.deepgram.com/v1/listen"
            headers = {
                "Authorization": f"Token {self.api_key}",
                "Content-Type": "audio/mpeg"
            }
            
            params = {
                "model": self.model,
                "smart_format": str(self.smart_format).lower()
            }
            
            with open(audio_path, 'rb') as audio_file:
                response = requests.post(url, headers=headers, params=params, data=audio_file, timeout=600)
            
            if response.status_code == 200:
                result = response.json()
                transcript = result['results']['channels'][0]['alternatives'][0]['transcript']
                logger.debug(f"Transcribed single file: {len(transcript)} characters")
                return transcript
            else:
                logger.error(f"Deepgram transcription failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error transcribing single file: {e}")
            return None
    
    async def _transcribe_chunked_file(
        self,
        audio_path: Path,
        total_duration: float,
        chunk_duration_seconds: float,
        context_id: Optional[str] = None
    ) -> Optional[str]:
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
                            context_id=context_id
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
            if tweet.thread_id and tweet.is_self_thread:
                source_link = f"[[thread_{tweet.thread_id}_{tweet.screen_name}.md]]"
            else:
                source_link = f"[[tweet_{tweet.id}_{tweet.screen_name}.md]]"
            
            # Create transcript content with summary and tags sections
            content = f"""---
type: "twitter_video_transcript"
thread_id: "{tweet.thread_id or ''}"
author: "{tweet.screen_name}"
created_at: "{tweet.created_at}"
url: "https://twitter.com/{tweet.screen_name}/status/{tweet.id}"
processed_at: "{tweet.processed_at or ''}"
---

## Source
{source_link}

## Summary
{summary if summary else 'No summary available'}

## Transcript
{transcript_text}

## Tags
{formatted_tags}
"""
            
            # Write file
            transcript_file.write_text(content, encoding='utf-8')
            logger.debug(f"Created transcript file: {transcript_file}")
            
        except Exception as e:
            logger.error(f"Error creating transcript file: {e}")
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
