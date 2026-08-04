"""
Transcript LLM Processor - Processes YouTube transcripts with LLM to format them properly
Combines fragmented sentences into coherent paragraphs and removes timestamps
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from core.chunking import TextChunk, chunk_text
from core.config import config
from core.connector_budgets import ConnectorBudgetError, start_connector_budget_run
from core.llm_interface import LLMInterface
from core.llm_cache import llm_cache
from core.llm_validation import (
    LLMJSONField,
    LLMOutputValidationError,
    parse_llm_json_response,
    validate_comma_separated_tags,
    validate_llm_json_object,
)
from core.metadata_db import get_metadata_db
from core.pipeline_registry import PipelineStage, pipeline_registry, register_pipeline_stages
from core.prompt_security import wrap_untrusted_content
from core.sensitive_redaction import redact_sensitive_text

logger = logging.getLogger(__name__)


def _transcript_stage_active(cfg) -> bool:
    """Stage predicate ensuring transcript formatting is permitted."""
    return bool(cfg.get('youtube.enable_llm_transcript_processing', True))


PIPELINE_STAGES = (
    PipelineStage(
        name='llm_processing.transcript_formatting',
        config_path='llm_processing.transcript_formatting',
        description='Format transcripts with LLM post-processing.',
        processor='TranscriptLLMProcessor',
        capabilities=('llm', 'transcript'),
        required_config=('processing.enable_llm_features', 'llm.tasks.transcript.enabled'),
        config_keys=(
            'processing.enable_llm_features',
            'llm.tasks.transcript.enabled',
            'youtube.enable_llm_transcript_processing',
            'youtube.transcript_chunk_size'
        ),
        predicate=_transcript_stage_active
    ),
)


register_pipeline_stages(*PIPELINE_STAGES)


TRANSCRIPT_RESPONSE_FIELDS = (
    LLMJSONField("text", str, allow_empty=False, max_length=200000),
    LLMJSONField("summary", str, allow_empty=False, max_length=5000),
    LLMJSONField(
        "tags",
        str,
        allow_empty=False,
        max_length=1000,
        validator=validate_comma_separated_tags,
    ),
)


class TranscriptLLMProcessor:
    """Processes YouTube transcripts using LLM to format them into readable paragraphs"""
    
    def __init__(self):
        self.config = config
        self.llm_interface = None
        # Stage controls consolidate llm task + youtube specific toggles
        self.enabled = pipeline_registry.is_enabled('llm_processing.transcript_formatting')
        self.chunk_size = config.get('youtube.transcript_chunk_size', 75000)
        self.metadata_db = None
        retry_interval = config.get('llm.tasks.transcript.retry_interval_hours', 12)
        try:
            self.retry_interval_hours = max(0.0, float(retry_interval))
        except (TypeError, ValueError):
            self.retry_interval_hours = 12.0

        if self.enabled:
            try:
                # Get the full LLM config for initialization
                llm_config = config.get('llm', {})
                self.llm_interface = LLMInterface(llm_config)
                logger.info(f"Transcript LLM processor initialized with chunk size: {self.chunk_size}")
            except Exception as e:
                logger.error(f"Failed to initialize transcript LLM processor: {e}")
                self.enabled = False

        if self.enabled and config.get('database.enabled', False):
            try:
                self.metadata_db = get_metadata_db()
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug(f"Transcript chunk cache unavailable: {exc}")
                self.metadata_db = None
    
    async def process_transcript(
        self,
        raw_transcript: str,
        context_id: Optional[str] = None,
        *,
        source_label: Optional[str] = None,
        output_path: Optional[str | Path] = None,
    ) -> Optional[dict]:
        """
        Process a raw transcript with timestamps into formatted paragraphs with summary and tags
        For long transcripts, splits into chunks and processes each separately
        
        Args:
            raw_transcript: Raw transcript text with timestamps
            
        Returns:
            Dictionary with 'text', 'summary', and 'tags' keys, or None if processing fails
        """
        if not self.enabled or not self.llm_interface or not raw_transcript:
            return None
        
        try:
            route = self.llm_interface.resolve_task_route('transcript') if self.llm_interface else None
            if not route:
                logger.warning("Transcript LLM task disabled or no provider available")
                return None

            provider, model, model_cfg = route
            target_label = self._format_target_label(
                context_id=context_id,
                source_label=source_label,
                output_path=output_path,
            )
            budget = start_connector_budget_run(config, "youtube")
            budget.add_transcript_text(raw_transcript, label=target_label)
            prompt = config.get('youtube.transcript_processing_prompt', 
                              "Process the following transcript. Combine fragmented sentences into coherent paragraphs, remove all timestamps, and insert newlines between paragraphs where the context shifts. Do not edit the content beyond paragraph formation.\n\nReturn the result strictly as a JSON object with the following fields:\n- \"text\": the processed transcript in plain text paragraphs\n- \"summary\": a concise 2–4 sentence summary of the transcript\n- \"tags\": 3–8 relevant tags, returned as a single comma-separated string with no # symbols and no explanations\n\nReturn ONLY the JSON object, with no preamble or extra text.")
            cache_provider = f"{provider}:{model}" if provider or model else ""
            chunks = self._chunk_transcript(raw_transcript)
            if not chunks:
                return None

            # Check if we need to chunk the transcript
            if len(chunks) == 1:
                chunk = chunks[0]
                # Single chunk processing
                logger.info(
                    "Processing transcript with %s (%s) for %s - single chunk",
                    provider,
                    model,
                    target_label,
                )
                return await self._process_single_chunk(
                    chunk.text,
                    prompt,
                    provider,
                    model,
                    cache_provider,
                    model_cfg=model_cfg,
                    target_label=target_label,
                    context_id=context_id,
                    chunk_index=chunk.index,
                    chunk_id=chunk.chunk_id,
                    chunks_total=chunk.total,
                    source_hash=chunk.source_hash,
                )
            else:
                # Multi-chunk processing
                logger.info(
                    "Processing transcript with %s (%s) for %s - splitting into %s deterministic chunks",
                    provider,
                    model,
                    target_label,
                    len(chunks),
                )
                return await self._process_chunked_transcript(
                    raw_transcript,
                    prompt,
                    provider,
                    model,
                    cache_provider,
                    model_cfg=model_cfg,
                    target_label=target_label,
                    context_id=context_id,
                    chunks=chunks,
                )
                
        except ConnectorBudgetError:
            raise
        except Exception as e:
            logger.error(f"Error processing transcript: {e}")
            return None
    
    async def _process_single_chunk(
        self,
        transcript_text: str,
        prompt: str,
        provider: str,
        model: str,
        cache_provider: str,
        model_cfg: Dict[str, object],
        target_label: str,
        context_id: Optional[str] = None,
        chunk_index: Optional[int] = None,
        chunk_id: Optional[str] = None,
        chunks_total: int = 1,
        source_hash: Optional[str] = None,
    ) -> Optional[dict]:
        """Process a single chunk of transcript"""
        chunk_hash = self._hash_content(transcript_text)
        chunk_info = {
            'chunks_total': chunks_total,
            'chunks_processed': 0,
            'chunks_failed': 0,
            'fallback_used': False,
            'failed_chunks': [],
        }
        if chunk_id:
            chunk_info['chunk_id'] = chunk_id
            chunk_info['chunk_ids'] = [chunk_id]
        if source_hash:
            chunk_info['source_hash'] = source_hash

        try:
            # Try cache first
            cached = llm_cache.get(transcript_text, 'transcript_fmt', cache_provider)
            cached = self._validate_cached_chunk_payload(
                cached,
                source="LLM cache",
                target_label=target_label,
                chunk_index=chunk_index,
            )
            if cached:
                logger.debug(
                    "Transcript LLM cache HIT for %s chunk %s",
                    target_label,
                    chunk_index if chunk_index is not None else 1,
                )
                normalized = self._normalize_cached_chunk_result(cached, chunk_info)
                self._record_chunk_success(
                    context_id,
                    chunk_index,
                    chunk_hash,
                    cache_provider,
                    normalized,
                    chunk_id=chunk_id,
                )
                return normalized

            cached_record = self._load_transcript_chunk_record(
                context_id,
                chunk_index,
                chunk_hash,
                cache_provider,
                expected_chunk_id=chunk_id,
            )
            cached_result = self._parse_cached_chunk_result(cached_record)
            if cached_result:
                logger.debug(
                    "Transcript chunk cache HIT for %s chunk %s",
                    target_label,
                    chunk_index if chunk_index is not None else 1,
                )
                return self._normalize_cached_chunk_result(cached_result, chunk_info)

            if self._should_backoff_failed_chunk(cached_record):
                logger.info(
                    "Skipping transcript LLM retry for %s chunk %s due to recent failure",
                    target_label,
                    chunk_index if chunk_index is not None else 1,
                )
                return self._build_fallback_chunk_result(
                    transcript_text,
                    chunk_index,
                    chunks_total=chunks_total,
                    chunk_id=chunk_id,
                    source_hash=source_hash,
                )

            wrapped_transcript = wrap_untrusted_content(
                transcript_text,
                label=f"{target_label}:chunk:{chunk_index or 1}",
                scope="context",
            )
            response = await self.llm_interface.generate(
                prompt=wrapped_transcript,
                system_prompt=prompt,
                provider=provider,
                model=model,
                task="transcript",
                usage_model_config=model_cfg,
                max_tokens=16000,  # Large enough for long transcripts
                temperature=0.1   # Low temperature for consistent formatting
            )
            
            if response and not response.error and response.content:
                try:
                    result = self._parse_transcript_response(
                        response.content,
                        target_label=target_label,
                        chunk_index=chunk_index,
                    )

                    # Cache formatted chunk
                    try:
                        llm_cache.set(transcript_text, 'transcript_fmt', result, cache_provider)
                    except Exception:
                        pass

                    logger.info(
                        "✅ Successfully formatted %s chunk %s: %s → %s characters",
                        target_label,
                        chunk_index if chunk_index is not None else 1,
                        len(transcript_text),
                        len(result['text']),
                    )
                    chunk_info['chunks_processed'] = 1 if not chunk_info['chunks_failed'] else 0
                    result['chunk_metadata'] = chunk_info
                    self._record_chunk_success(
                        context_id,
                        chunk_index,
                        chunk_hash,
                        cache_provider,
                        result,
                        chunk_id=chunk_id,
                    )
                    return result
                except LLMOutputValidationError as e:
                    logger.error(
                        "Invalid transcript LLM response for %s chunk %s: %s",
                        target_label,
                        chunk_index if chunk_index is not None else 1,
                        e,
                    )
                    chunk_info['fallback_used'] = True
                    chunk_info['chunks_failed'] = 1
                    if chunk_index is not None:
                        chunk_info['failed_chunks'] = [chunk_index]
                    self._record_chunk_failure(
                        context_id,
                        chunk_index,
                        chunk_hash,
                        cache_provider,
                        f'validation_error: {e}',
                        chunk_id=chunk_id,
                    )
                    return self._build_fallback_chunk_result(
                        transcript_text,
                        chunk_index,
                        chunks_total=chunks_total,
                        chunk_id=chunk_id,
                        source_hash=source_hash,
                    )
            else:
                error_msg = response.error if response else "No response received"
                logger.warning(
                    "❌ Single chunk formatting failed for %s chunk %s: %s",
                    target_label,
                    chunk_index if chunk_index is not None else 1,
                    error_msg,
                )
                self._record_chunk_failure(
                    context_id,
                    chunk_index,
                    chunk_hash,
                    cache_provider,
                    error_msg or 'no_response',
                    chunk_id=chunk_id,
                )
                return self._build_fallback_chunk_result(
                    transcript_text,
                    chunk_index,
                    chunks_total=chunks_total,
                    chunk_id=chunk_id,
                    source_hash=source_hash,
                )
        except ConnectorBudgetError:
            raise
        except Exception as e:
            logger.error(
                "Error processing single chunk for %s chunk %s: %s",
                target_label,
                chunk_index if chunk_index is not None else 1,
                e,
            )
            self._record_chunk_failure(
                context_id,
                chunk_index,
                chunk_hash,
                cache_provider,
                str(e),
                chunk_id=chunk_id,
            )
            return self._build_fallback_chunk_result(
                transcript_text,
                chunk_index,
                chunks_total=chunks_total,
                chunk_id=chunk_id,
                source_hash=source_hash,
            )
    
    async def _process_chunked_transcript(
        self,
        raw_transcript: str,
        prompt: str,
        provider: str,
        model: str,
        cache_provider: str,
        model_cfg: Dict[str, object],
        target_label: str,
        context_id: Optional[str] = None,
        chunks: Optional[Tuple[TextChunk, ...]] = None,
    ) -> Optional[dict]:
        """Process transcript in chunks and stitch results together"""
        try:
            chunks = chunks or self._chunk_transcript(raw_transcript)
            logger.info("Split transcript for %s into %s chunks", target_label, len(chunks))
            
            # Process each chunk
            processed_chunks: Dict[int, dict] = {}
            fallback_chunks: Dict[int, Tuple[TextChunk, str]] = {}
            for chunk in chunks:
                i = chunk.index
                logger.info(
                    "Processing %s chunk %s/%s (%s characters)",
                    target_label,
                    i,
                    len(chunks),
                    len(chunk.text),
                )

                chunk_hash = chunk.content_hash
                cached_record = self._load_transcript_chunk_record(
                    context_id,
                    i,
                    chunk_hash,
                    cache_provider,
                    expected_chunk_id=chunk.chunk_id,
                )
                cached_chunk = self._parse_cached_chunk_result(cached_record)

                if cached_chunk:
                    processed_chunks[i] = self._normalize_cached_chunk_result(
                        cached_chunk,
                        self._chunk_info(chunk, chunks_total=len(chunks)),
                    )
                    logger.info("♻️ Reused cached %s chunk %s/%s", target_label, i, len(chunks))
                    continue

                if self._should_backoff_failed_chunk(cached_record):
                    logger.info(
                        "Skipping transcript LLM retry for %s chunk %s due to recent failure",
                        target_label,
                        i,
                    )
                    fallback_chunks[i] = (chunk, chunk_hash)
                    continue

                processed_chunk = await self._process_single_chunk(
                    chunk.text,
                    prompt,
                    provider,
                    model,
                    cache_provider,
                    model_cfg=model_cfg,
                    target_label=target_label,
                    context_id=context_id,
                    chunk_index=i,
                    chunk_id=chunk.chunk_id,
                    chunks_total=len(chunks),
                    source_hash=chunk.source_hash,
                )
                if processed_chunk:
                    processed_chunks[i] = processed_chunk
                    logger.info("✅ %s chunk %s/%s processed successfully", target_label, i, len(chunks))
                else:
                    logger.warning("❌ Failed to process %s chunk %s/%s", target_label, i, len(chunks))
                    fallback_chunks[i] = (chunk, chunk_hash)
                    # Continue with other chunks even if one fails

            if processed_chunks:
                # Combine text from all chunks
                if fallback_chunks:
                    logger.warning(
                        "Using redacted transcript fallback for %s due to %s failed chunk(s)",
                        target_label,
                        len(fallback_chunks),
                    )

                combined_text_segments = []
                for chunk in chunks:
                    processed = processed_chunks.get(chunk.index)
                    if processed and processed.get('text'):
                        combined_text_segments.append(processed['text'])
                    elif chunk.index in fallback_chunks:
                        combined_text_segments.append(self._redact_fallback_text(chunk.text))

                combined_text = '\n\n'.join(segment for segment in combined_text_segments if segment)
                ordered_processed = [
                    processed_chunks[index]
                    for index in sorted(processed_chunks)
                ]
                failed_indices = sorted(fallback_chunks)

                # Create final result with combined text and aggregate summary/tags
                result = {
                    'text': combined_text,
                    'summary': self._combine_summaries([chunk['summary'] for chunk in ordered_processed]),
                    'tags': self._combine_tags([chunk['tags'] for chunk in ordered_processed])
                }

                chunk_metadata = {
                    'chunks_total': len(chunks),
                    'chunks_processed': len(processed_chunks),
                    'chunks_failed': len(fallback_chunks),
                    'fallback_used': bool(fallback_chunks),
                    'failed_chunks': failed_indices,
                    'failed_chunk_ids': [
                        chunks[index - 1].chunk_id
                        for index in failed_indices
                        if 0 <= index - 1 < len(chunks)
                    ],
                    'chunk_ids': [chunk.chunk_id for chunk in chunks],
                    'source_hash': chunks[0].source_hash if chunks else self._hash_content(raw_transcript),
                }

                logger.info(
                    "✅ Successfully processed %s chunks for %s: %s/%s chunks, %s → %s characters",
                    target_label,
                    target_label,
                    len(processed_chunks),
                    len(chunks),
                    len(raw_transcript),
                    len(result['text']),
                )
                if context_id and self.metadata_db:
                    try:
                        self.metadata_db.prune_transcript_chunks(
                            context_id,
                            max_chunk_index=len(chunks),
                        )
                    except Exception:
                        pass
                result['chunk_metadata'] = chunk_metadata
                return result
            elif fallback_chunks:
                logger.warning(
                    "Returning redacted transcript fallback for %s because all LLM chunk attempts failed",
                    target_label,
                )
                fallback_text = '\n\n'.join(
                    self._redact_fallback_text(fallback_chunks[index][0].text)
                    for index in sorted(fallback_chunks)
                )
                failed_indices = sorted(fallback_chunks)
                return {
                    'text': fallback_text,
                    'summary': '',
                    'tags': '',
                    'chunk_metadata': {
                        'chunks_total': len(chunks),
                        'chunks_processed': 0,
                        'chunks_failed': len(fallback_chunks),
                        'fallback_used': True,
                        'failed_chunks': failed_indices,
                        'failed_chunk_ids': [
                            chunks[index - 1].chunk_id
                            for index in failed_indices
                            if 0 <= index - 1 < len(chunks)
                        ],
                        'chunk_ids': [chunk.chunk_id for chunk in chunks],
                        'source_hash': chunks[0].source_hash if chunks else self._hash_content(raw_transcript),
                    }
                }
            else:
                logger.error("❌ No chunks were processed successfully for %s", target_label)
                return None

        except Exception as e:
            logger.error(f"Error processing chunked transcript for {target_label}: {e}")
            return None

    def _load_cached_chunk(
        self,
        context_id: str,
        chunk_index: int,
        expected_hash: str,
        cache_provider: str,
        expected_chunk_id: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Load a cached transcript chunk from the metadata database when available."""
        record = self._load_transcript_chunk_record(
            context_id,
            chunk_index,
            expected_hash,
            cache_provider,
            expected_chunk_id=expected_chunk_id,
        )
        return self._parse_cached_chunk_result(record)

    def _load_transcript_chunk_record(
        self,
        context_id: Optional[str],
        chunk_index: Optional[int],
        expected_hash: str,
        cache_provider: str,
        expected_chunk_id: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Load a transcript chunk cache row when it matches the current content/provider."""
        if not self.metadata_db or not context_id or chunk_index is None:
            return None
        try:
            cached = self.metadata_db.get_transcript_chunk(context_id, chunk_index)
        except Exception:
            return None

        if not cached:
            return None

        if cached.get('content_hash') != expected_hash:
            return None

        stored_chunk_id = cached.get('chunk_id') or ''
        if stored_chunk_id and expected_chunk_id and stored_chunk_id != expected_chunk_id:
            return None

        stored_provider = cached.get('model_provider') or ''
        if stored_provider and cache_provider and stored_provider != cache_provider:
            return None

        return cached

    def _parse_cached_chunk_result(self, cached: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        """Parse a successful chunk payload from the metadata cache."""
        if not cached:
            return None

        try:
            payload = cached.get('result_json') or ''
            raw_payload = json.loads(payload)
        except Exception:
            return None

        data = self._validate_cached_chunk_payload(
            raw_payload,
            source="transcript chunk cache",
            target_label="transcript",
            chunk_index=None,
        )
        if not data:
            return None

        if isinstance(data, dict) and isinstance(raw_payload, dict):
            meta = raw_payload.get('chunk_metadata')
            if isinstance(meta, dict):
                data['chunk_metadata'] = dict(meta)

        return data

    def _parse_transcript_response(
        self,
        content: str,
        *,
        target_label: str,
        chunk_index: Optional[int],
    ) -> Dict[str, str]:
        """Parse a fresh transcript-formatting response from the LLM."""
        return parse_llm_json_response(
            content,
            fields=TRANSCRIPT_RESPONSE_FIELDS,
            object_name=f"transcript LLM response for {target_label} chunk {chunk_index or 1}",
            reject_extra_fields=True,
            allow_code_fence=True,
            allow_trailing_commas=True,
        )

    def _validate_cached_chunk_payload(
        self,
        payload: object,
        *,
        source: str,
        target_label: str,
        chunk_index: Optional[int],
    ) -> Optional[Dict[str, str]]:
        """Validate cached LLM transcript payloads before trusting them."""
        if not payload:
            return None
        try:
            return validate_llm_json_object(
                payload,
                fields=TRANSCRIPT_RESPONSE_FIELDS,
                object_name=f"{source} payload for {target_label} chunk {chunk_index or 1}",
                reject_extra_fields=False,
            )
        except LLMOutputValidationError as exc:
            logger.warning(
                "Ignoring invalid %s transcript payload for %s chunk %s: %s",
                source,
                target_label,
                chunk_index if chunk_index is not None else 1,
                exc,
            )
            return None

    def _normalize_cached_chunk_result(
        self,
        result: Dict[str, str],
        chunk_info: Dict[str, object],
    ) -> Dict[str, str]:
        """Ensure cached chunk results include chunk metadata."""
        normalized = dict(result)
        cached_meta = dict(normalized.get('chunk_metadata') or {})
        if not cached_meta:
            cached_meta = dict(chunk_info)
            cached_meta['chunks_processed'] = 1
        else:
            for key, value in chunk_info.items():
                if key not in cached_meta and value not in (None, [], {}):
                    cached_meta[key] = value
        normalized['chunk_metadata'] = cached_meta
        return normalized

    def _should_backoff_failed_chunk(self, cached: Optional[Dict[str, str]]) -> bool:
        """Honor recent chunk failures so the same transcript isn't re-sent every pass."""
        if not cached or self.retry_interval_hours <= 0:
            return False

        try:
            payload = json.loads(cached.get('result_json') or '')
        except Exception:
            return False

        if not isinstance(payload, dict) or payload.get('status') != 'failed':
            return False

        updated_at = cached.get('updated_at')
        if not updated_at:
            return False

        try:
            updated = datetime.fromisoformat(updated_at)
        except ValueError:
            return False

        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)

        retry_at = updated + timedelta(hours=self.retry_interval_hours)
        return retry_at > datetime.now(timezone.utc)

    def _build_fallback_chunk_result(
        self,
        transcript_text: str,
        chunk_index: Optional[int],
        *,
        chunks_total: int,
        chunk_id: Optional[str] = None,
        source_hash: Optional[str] = None,
    ) -> Dict[str, object]:
        """Return a redacted-text fallback result without spending another LLM call."""
        failed_chunks = [chunk_index] if chunk_index is not None else []
        redaction = redact_sensitive_text(transcript_text)
        metadata = {
            'chunks_total': chunks_total,
            'chunks_processed': 0,
            'chunks_failed': len(failed_chunks) or 1,
            'fallback_used': True,
            'failed_chunks': failed_chunks,
        }
        if chunk_id:
            metadata['chunk_id'] = chunk_id
            metadata['chunk_ids'] = [chunk_id]
            metadata['failed_chunk_ids'] = [chunk_id]
        if source_hash:
            metadata['source_hash'] = source_hash
        if redaction.has_findings:
            metadata['redaction'] = redaction.to_metadata()
        return {
            'text': redaction.redacted_text,
            'summary': '',
            'tags': '',
            'chunk_metadata': metadata,
        }

    def _redact_fallback_text(self, transcript_text: str) -> str:
        """Redact sensitive values in transcript text used after LLM chunk failure."""
        return redact_sensitive_text(transcript_text).redacted_text

    def _hash_content(self, text: str) -> str:
        """Hash transcript content for cache validation."""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def _chunk_info(self, chunk: TextChunk, *, chunks_total: int) -> Dict[str, object]:
        return {
            'chunks_total': chunks_total,
            'chunks_processed': 1,
            'chunks_failed': 0,
            'fallback_used': False,
            'failed_chunks': [],
            'chunk_id': chunk.chunk_id,
            'chunk_ids': [chunk.chunk_id],
            'source_hash': chunk.source_hash,
        }

    def _record_chunk_success(
        self,
        context_id: Optional[str],
        chunk_index: Optional[int],
        content_hash: str,
        model_provider: str,
        result: Dict[str, object],
        *,
        chunk_id: Optional[str] = None,
    ) -> None:
        """Persist a successful chunk result when metadata storage is available."""
        if not self.metadata_db or not context_id or chunk_index is None:
            return
        try:
            self.metadata_db.upsert_transcript_chunk(
                context_id,
                chunk_index,
                content_hash,
                json.dumps(result),
                model_provider,
                chunk_id=chunk_id,
            )
        except Exception:
            pass

    def _record_chunk_failure(
        self,
        context_id: Optional[str],
        chunk_index: Optional[int],
        content_hash: str,
        model_provider: str,
        reason: str,
        *,
        chunk_id: Optional[str] = None,
    ) -> None:
        """Persist chunk failure information when available."""
        if not self.metadata_db or not context_id or chunk_index is None:
            return
        try:
            payload = json.dumps({
                'status': 'failed',
                'reason': reason,
                'chunk_index': chunk_index,
                'chunk_id': chunk_id,
            })
            self.metadata_db.upsert_transcript_chunk(
                context_id,
                chunk_index,
                content_hash,
                payload,
                model_provider,
                chunk_id=chunk_id,
            )
        except Exception:
            pass

    def _combine_summaries(self, summaries: List[str]) -> str:
        """Combine multiple summaries into a single coherent summary"""
        if not summaries:
            return ""
        
        # For now, just join with periods and clean up
        combined = ". ".join(summaries)
        # Remove any double periods
        combined = combined.replace("..", ".")
        # Ensure it ends with a period
        if not combined.endswith("."):
            combined += "."
        
        return combined
    
    def _combine_tags(self, tags_list: List[str]) -> str:
        """Combine multiple tag strings into a single comma-separated string"""
        if not tags_list:
            return ""
        
        # Split all tag strings and combine
        all_tags = []
        for tags_str in tags_list:
            if tags_str:
                # Split by comma and clean up
                tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
                all_tags.extend(tags)
        
        # Remove duplicates while preserving order
        unique_tags = []
        seen = set()
        for tag in all_tags:
            if tag.lower() not in seen:
                unique_tags.append(tag)
                seen.add(tag.lower())
        
        # Limit to 8 tags max
        return ", ".join(unique_tags[:8])
    
    def _split_transcript_into_chunks(self, transcript: str) -> List[str]:
        """Split transcript into deterministic chunks, returning text only."""
        return [chunk.text for chunk in self._chunk_transcript(transcript)]

    def _chunk_transcript(self, transcript: str) -> Tuple[TextChunk, ...]:
        """Split transcript into deterministic chunks with stable ids."""
        return chunk_text(
            transcript,
            chunk_size=self.chunk_size,
            namespace="transcript",
        )

    def _format_target_label(
        self,
        *,
        context_id: Optional[str],
        source_label: Optional[str],
        output_path: Optional[str | Path],
    ) -> str:
        """Build a stable operator-facing label for transcript work."""
        parts: List[str] = []

        if source_label:
            parts.append(str(source_label))
        if context_id:
            parts.append(f"context={context_id}")
        if output_path:
            parts.append(f"note={Path(output_path)}")

        if not parts:
            return "transcript"
        return " | ".join(parts)

    def is_enabled(self) -> bool:
        """Check if transcript processing is enabled"""
        return self.enabled and self.llm_interface is not None
