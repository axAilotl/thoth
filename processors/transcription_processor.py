"""
Unified Transcription Processor
Selects Deepgram or Whisper (local/OpenAI-compatible) via configuration
and provides a consistent interface for the rest of the codebase.
"""

import logging
from pathlib import Path
from typing import List, Optional

from core.config import config
from core.data_models import Tweet, ProcessingStats
from core.pipeline_registry import PipelineStage, register_pipeline_stages

logger = logging.getLogger(__name__)


def _twitter_transcript_stage_active(cfg) -> bool:
    """Stage predicate ensuring a transcript backend is enabled."""
    return bool(cfg.get('whisper.enabled', False) or cfg.get('deepgram.enabled', False))


PIPELINE_STAGES = (
    PipelineStage(
        name='transcripts.twitter_videos',
        config_path='transcripts.twitter_videos',
        description='Generate transcripts for Twitter-hosted videos.',
        processor='TranscriptionProcessor',
        capabilities=('transcripts', 'twitter'),
        config_keys=('paths.vault_dir', 'whisper.enabled', 'whisper.min_duration_seconds', 'deepgram.enabled', 'deepgram.min_duration_seconds'),
        predicate=_twitter_transcript_stage_active
    ),
)


register_pipeline_stages(*PIPELINE_STAGES)

class TranscriptionProcessor:
    """Facade over DeepgramTranscriptProcessor and TwitterVideoTranscriptProcessor"""

    def __init__(self, vault_path: str = None):
        self.config = config
        self.vault_path = Path(vault_path or config.get('vault_dir', 'knowledge_vault'))
        self.backend = None
        self.backend_name: Optional[str] = None
        self._select_backend()

    def _select_backend(self):
        """Choose the best available backend based on config and availability."""
        # Prefer Deepgram if enabled and usable
        try:
            if self.config.get('deepgram.enabled', False):
                from .deepgram_transcript_processor import DeepgramTranscriptProcessor
                dg = DeepgramTranscriptProcessor()
                if dg.is_enabled():
                    self.backend = dg
                    self.backend_name = 'deepgram'
                    logger.info("Using Deepgram transcription backend")
                    return
        except Exception as e:
            logger.debug(f"Deepgram backend not available: {e}")

        # Fallback to Whisper (local/OpenAI-compatible)
        try:
            if self.config.get('whisper.enabled', False):
                from .twitter_video_transcript_processor import TwitterVideoTranscriptProcessor
                whisper = TwitterVideoTranscriptProcessor(self.vault_path)
                if whisper.is_enabled():
                    self.backend = whisper
                    self.backend_name = 'whisper'
                    logger.info("Using Whisper transcription backend")
                    return
        except Exception as e:
            logger.debug(f"Whisper backend not available: {e}")

        self.backend = None
        self.backend_name = None
        logger.warning("No transcription backend available")

    def get_provider_name(self) -> str:
        return self.backend_name or 'none'

    def is_enabled(self) -> bool:
        return bool(self.backend and getattr(self.backend, 'is_enabled', lambda: False)())

    async def process_tweets(self, tweets: List[Tweet], resume: bool = True) -> ProcessingStats:
        """Process a list of tweets to generate transcripts; returns ProcessingStats."""
        stats = ProcessingStats()
        if not self.backend:
            return stats

        # Delegate and normalize return type
        result = await self.backend.process_tweets(tweets, resume=resume)

        # Deepgram returns a dict; Whisper returns ProcessingStats
        if isinstance(result, dict):
            stats.total_processed = result.get('processed', 0)
            stats.updated = result.get('created', 0)
            stats.skipped = result.get('skipped', 0)
            stats.errors = result.get('errors', 0)
            return stats

        # Assume ProcessingStats-like
        return result

    async def _process_video_transcript(self, tweet: Tweet, video) -> bool:
        """Delegate single-video processing (used by pipeline)."""
        if not self.backend:
            return False
        return await self.backend._process_video_transcript(tweet, video)
