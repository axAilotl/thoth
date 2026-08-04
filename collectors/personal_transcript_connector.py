"""Personal transcript connector for Omi-style exports."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from collectors.omi_api_client import (
    OMI_DEFAULT_BASE_URL,
    OMI_DEFAULT_PAGE_SIZE,
    OmiConversationQuery,
    fetch_omi_conversations,
    normalize_categories,
)
from core.artifacts import TranscriptArtifact
from core.capture_event_store import CaptureEventStore
from core.capture_lifecycle import CaptureLifecycleService
from core.config import Config, config
from core.connector_capture import ConnectorCaptureQueue
from core.metadata_db import MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout


DEFAULT_FILE_PATTERNS = ("*.json", "*.jsonl", "*.csv", "*.txt", "*.md", "*.markdown")


@dataclass(frozen=True)
class PersonalTranscriptRecord:
    """Artifact produced for one personal transcript session."""

    artifact_id: str
    session_id: str
    source_name: str
    raw_export_path: Path
    transcript_path: Path
    queued: bool = True


@dataclass(frozen=True)
class PersonalTranscriptResult:
    """Summary of one personal transcript connector run."""

    records: tuple[PersonalTranscriptRecord, ...] = field(default_factory=tuple)
    export_paths: tuple[str, ...] = field(default_factory=tuple)
    export_dirs: tuple[str, ...] = field(default_factory=tuple)
    api_conversation_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [
                {
                    "artifact_id": record.artifact_id,
                    "session_id": record.session_id,
                    "source_name": record.source_name,
                    "raw_export_path": str(record.raw_export_path),
                    "transcript_path": str(record.transcript_path),
                    "queued": record.queued,
                }
                for record in self.records
            ],
            "queued_count": sum(1 for record in self.records if record.queued),
            "export_paths": list(self.export_paths),
            "export_dirs": list(self.export_dirs),
            "api_conversation_count": self.api_conversation_count,
        }


@dataclass(frozen=True)
class _TranscriptDefaults:
    source_name: str
    device_id: str | None = None
    speaker: str | None = None
    session_id: str | None = None
    language: str | None = None


@dataclass(frozen=True)
class _NormalizedTranscriptSession:
    session_id: str
    title: str
    raw_transcript: str
    summary: str | None = None
    speaker: str | None = None
    device_id: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    language: str | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)


class PersonalTranscriptConnector:
    """Collect Omi-style transcript exports through the artifact queue."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()
        self.capture_queue = ConnectorCaptureQueue(
            self.config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )

    async def collect(
        self,
        *,
        export_paths: Iterable[str | Path] | None = None,
        export_dirs: Iterable[str | Path] | None = None,
        file_patterns: Iterable[str] | None = None,
        api_key: str | None = None,
        api_key_env: str | None = None,
        api_base_url: str | None = None,
        api_limit: int | None = None,
        api_page_size: int | None = None,
        include_transcript: bool | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        categories: Iterable[str] | str | None = None,
        folder_id: str | None = None,
        starred: bool | None = None,
        timeout_seconds: float | None = None,
        source_name: str | None = None,
        device_id: str | None = None,
        speaker: str | None = None,
        session_id: str | None = None,
        language: str | None = None,
        limit: int | None = None,
    ) -> PersonalTranscriptResult:
        """Collect transcript exports and queue normalized transcript artifacts."""
        configured = self.config.get("sources.omi", {}) or {}
        if not isinstance(configured, Mapping):
            configured = {}

        resolved_source = _clean_string(
            source_name or configured.get("source_name")
        ) or "omi"
        defaults = _TranscriptDefaults(
            source_name=resolved_source,
            device_id=_clean_string(device_id or configured.get("device_id")),
            speaker=_clean_string(speaker or configured.get("speaker")),
            session_id=_clean_string(session_id or configured.get("session_id")),
            language=_clean_string(language or configured.get("language")),
        )
        resolved_paths = _path_list(export_paths) or _path_list(
            configured.get("export_paths") or configured.get("export_path")
        )
        resolved_dirs = _path_list(export_dirs) or _path_list(
            configured.get("export_dirs") or configured.get("export_dir")
        )
        resolved_patterns = tuple(
            _string_list(file_patterns)
            or _string_list(configured.get("file_patterns"))
            or DEFAULT_FILE_PATTERNS
        )
        api_requested = _omi_api_requested(
            has_local_sources=bool(resolved_paths or resolved_dirs),
            configured=configured,
            api_key=api_key,
            api_key_env=api_key_env,
            api_base_url=api_base_url,
            api_limit=api_limit,
            api_page_size=api_page_size,
            include_transcript=include_transcript,
            start_date=start_date,
            end_date=end_date,
            categories=categories,
            folder_id=folder_id,
            starred=starred,
            timeout_seconds=timeout_seconds,
        )
        api_query = (
            _resolve_omi_api_query(
                configured,
                api_key=api_key,
                api_key_env=api_key_env,
                api_base_url=api_base_url,
                api_limit=api_limit if api_limit is not None else limit,
                api_page_size=api_page_size,
                include_transcript=include_transcript,
                start_date=start_date,
                end_date=end_date,
                categories=categories,
                folder_id=folder_id,
                starred=starred,
                timeout_seconds=timeout_seconds,
            )
            if api_requested
            else None
        )
        if not resolved_paths and not resolved_dirs and api_query is None:
            raise ValueError(
                "Personal transcript ingestion requires export_paths, export_dirs, or an Omi API key"
            )

        records: list[PersonalTranscriptRecord] = []
        run_id = datetime.now().isoformat()
        if resolved_paths or resolved_dirs:
            with self.capture_queue.lifecycle() as lifecycle:
                input_files = self._resolve_input_files(
                    export_paths=resolved_paths,
                    export_dirs=resolved_dirs,
                    file_patterns=resolved_patterns,
                )
                if limit is not None:
                    input_files = input_files[: max(0, int(limit))]

                for export_path in input_files:
                    raw_export_path = await asyncio.to_thread(
                        self._preserve_raw_export,
                        export_path,
                        defaults.source_name,
                    )
                    sessions = await asyncio.to_thread(
                        self._parse_export_file,
                        export_path,
                        defaults,
                    )
                    for session in sessions:
                        records.append(
                            self._queue_session(
                                session,
                                source_name=defaults.source_name,
                                raw_export_path=raw_export_path,
                                lifecycle=lifecycle,
                                run_id=run_id,
                            )
                        )

        api_conversation_count = 0
        if api_query is not None:
            conversations = await fetch_omi_conversations(api_query)
            api_conversation_count = len(conversations)
            with self.capture_queue.lifecycle() as lifecycle:
                for index, conversation in enumerate(conversations):
                    raw_export_path = await asyncio.to_thread(
                        self._preserve_raw_payload,
                        conversation,
                        defaults.source_name,
                        source_label="api",
                    )
                    session = _session_from_mapping(
                        conversation,
                        fallback_id="omi-api",
                        index=index,
                        defaults=defaults,
                    )
                    records.append(
                        self._queue_session(
                            session,
                            source_name=defaults.source_name,
                            raw_export_path=raw_export_path,
                            lifecycle=lifecycle,
                            run_id=run_id,
                        )
                    )

        return PersonalTranscriptResult(
            records=tuple(records),
            export_paths=tuple(str(path) for path in resolved_paths),
            export_dirs=tuple(str(path) for path in resolved_dirs),
            api_conversation_count=api_conversation_count,
        )

    def _resolve_input_files(
        self,
        *,
        export_paths: Iterable[Path],
        export_dirs: Iterable[Path],
        file_patterns: Iterable[str],
    ) -> list[Path]:
        candidates: list[Path] = []
        for path in export_paths:
            resolved = path.expanduser()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"Personal transcript export path does not exist: {resolved}"
                )
            if resolved.is_dir():
                candidates.extend(_files_in_dir(resolved, file_patterns))
            elif resolved.is_file():
                candidates.append(resolved)
            else:
                raise ValueError(f"Personal transcript export path is not a file: {resolved}")

        for directory in export_dirs:
            resolved = directory.expanduser()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"Personal transcript export directory does not exist: {resolved}"
                )
            if not resolved.is_dir():
                raise ValueError(
                    f"Personal transcript export directory is not a directory: {resolved}"
                )
            candidates.extend(_files_in_dir(resolved, file_patterns))

        return _dedupe_paths(candidates)

    def _preserve_raw_export(self, export_path: Path, source_name: str) -> Path:
        raw_root = self.layout.raw_root / "personal_transcripts" / _safe_slug(source_name)
        raw_root.mkdir(parents=True, exist_ok=True)
        payload = export_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()[:12]
        raw_name = f"{_safe_slug(export_path.stem)}-{digest}{export_path.suffix.lower()}"
        raw_path = raw_root / raw_name
        try:
            if export_path.resolve() == raw_path.resolve():
                return raw_path
        except FileNotFoundError:
            pass
        if not raw_path.exists():
            shutil.copy2(export_path, raw_path)
        return raw_path

    def _preserve_raw_payload(
        self,
        payload: Mapping[str, Any],
        source_name: str,
        *,
        source_label: str,
    ) -> Path:
        raw_root = self.layout.raw_root / "personal_transcripts" / _safe_slug(source_name)
        raw_root.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode(
            "utf-8"
        )
        digest = hashlib.sha256(encoded).hexdigest()[:12]
        session_id = _safe_slug(
            _first_string(
                payload,
                "id",
                "session_id",
                "sessionId",
                "conversation_id",
                "memory_id",
            )
            or "conversation"
        )
        raw_path = raw_root / f"{_safe_slug(source_label)}_{session_id}-{digest}.json"
        if not raw_path.exists():
            raw_path.write_bytes(encoded + b"\n")
        return raw_path

    def _parse_export_file(
        self,
        export_path: Path,
        defaults: _TranscriptDefaults,
    ) -> list[_NormalizedTranscriptSession]:
        suffix = export_path.suffix.lower()
        text = export_path.read_text(encoding="utf-8")
        fallback_id = defaults.session_id or export_path.stem
        if suffix == ".json":
            payload = json.loads(text)
            return _sessions_from_json(payload, fallback_id=fallback_id, defaults=defaults)
        if suffix == ".jsonl":
            records = [
                json.loads(line)
                for line in text.splitlines()
                if line.strip()
            ]
            return _sessions_from_records(records, fallback_id=fallback_id, defaults=defaults)
        if suffix == ".csv":
            records = list(csv.DictReader(io.StringIO(text)))
            return _sessions_from_records(records, fallback_id=fallback_id, defaults=defaults)
        if suffix in {".txt", ".md", ".markdown"}:
            title = _title_from_text(text) or export_path.stem
            return [
                _NormalizedTranscriptSession(
                    session_id=_stable_session_id(fallback_id),
                    title=title,
                    raw_transcript=text.strip(),
                    speaker=defaults.speaker,
                    device_id=defaults.device_id,
                    language=defaults.language,
                    tags=(_safe_slug(defaults.source_name), "personal-transcript"),
                )
            ]
        raise ValueError(f"Unsupported personal transcript export type: {export_path}")

    def _write_transcript_file(
        self,
        session: _NormalizedTranscriptSession,
        *,
        source_name: str,
        raw_export_path: Path,
    ) -> Path:
        transcript_root = self.layout.vault_root / "transcripts" / "personal"
        transcript_root.mkdir(parents=True, exist_ok=True)
        transcript_path = transcript_root / (
            f"{_safe_slug(source_name)}_{_safe_slug(session.session_id)}.md"
        )
        raw_ref = self._relative_to_vault(raw_export_path)
        content = _render_transcript_markdown(
            session,
            source_name=source_name,
            raw_export_ref=raw_ref,
        )
        transcript_path.write_text(content, encoding="utf-8")
        return transcript_path

    def _build_transcript_artifact(
        self,
        session: _NormalizedTranscriptSession,
        *,
        source_name: str,
        raw_export_path: Path,
        transcript_path: Path,
    ) -> TranscriptArtifact:
        artifact_id = f"{_safe_slug(source_name)}_transcript_{_safe_slug(session.session_id)}"
        raw_ref = self._relative_to_vault(raw_export_path)
        transcript_ref = self._relative_to_vault(transcript_path)
        raw_payload = {
            "source_name": source_name,
            "session_id": session.session_id,
            "title": session.title,
            "speaker": session.speaker,
            "device_id": session.device_id,
            "started_at": session.started_at,
            "ended_at": session.ended_at,
            "summary": session.summary,
            "raw_export_path": raw_ref,
            "transcript_path": transcript_ref,
            "raw_transcript": session.raw_transcript,
        }
        normalized_metadata = {
            "artifact_id": artifact_id,
            "source_type": source_name,
            "session_id": session.session_id,
            "device_id": session.device_id,
            "speaker": session.speaker,
            "started_at": session.started_at,
            "ended_at": session.ended_at,
            "language": session.language,
        }
        return TranscriptArtifact(
            id=artifact_id,
            source_type=source_name,
            raw_content=json.dumps(raw_payload, ensure_ascii=False),
            created_at=session.started_at,
            ingested_at=datetime.now().isoformat(),
            capabilities=(
                "transcript",
                "personal_data",
                "speaker_metadata",
                "session_metadata",
            ),
            transcript_id=artifact_id,
            title=session.title,
            source_url="",
            transcript_path=transcript_ref,
            raw_transcript=session.raw_transcript,
            processed_transcript=session.raw_transcript,
            summary=session.summary,
            tags=list(session.tags),
            language=session.language,
            speaker=session.speaker,
            session_id=session.session_id,
            device_id=session.device_id,
            custom_metadata={
                "raw_payload_path": raw_ref,
                "source_path": raw_ref,
                "started_at": session.started_at,
                "ended_at": session.ended_at,
            },
            output_paths={"markdown": transcript_ref} if transcript_ref else {},
            normalized_metadata={
                key: value
                for key, value in normalized_metadata.items()
                if value not in (None, "")
            },
        )

    def _queue_artifact(
        self,
        lifecycle: CaptureLifecycleService,
        artifact: TranscriptArtifact,
        *,
        session: _NormalizedTranscriptSession,
        source_name: str,
        raw_export_path: Path,
        run_id: str,
    ) -> None:
        self.capture_queue.queue_artifact(
            lifecycle,
            artifact,
            artifact_type="transcript",
            source={
                "source_name": source_name,
                "source_type": "personal_transcript",
                "collector": "personal_transcript_connector",
                "native_source_id": session.device_id,
                "metadata": {
                    "language": session.language,
                    "speaker": session.speaker,
                },
            },
            session={
                "session_type": "personal_transcript",
                "native_session_id": session.session_id,
                "started_at": session.started_at or run_id,
                "ended_at": session.ended_at,
                "metadata": {
                    "source_name": source_name,
                    "device_id": session.device_id,
                    "speaker": session.speaker,
                    "language": session.language,
                    "capture_run_id": run_id,
                },
            },
            event={
                "event_type": "personal_transcript",
                "native_event_id": session.session_id,
                "occurred_at": session.started_at,
                "captured_at": artifact.ingested_at,
                "privacy": {
                    "classification": "personal",
                    "privacy_class": "personal",
                    "subject_ref": session.speaker or session.device_id or "",
                },
                "retention": {
                    "retention_class": "personal_export",
                    "policy": "personal_export",
                    "action": "retain",
                    "scope": [
                        "raw_capture",
                        "artifact_queue",
                        "transcript_file",
                    ],
                },
                "provenance": {
                    "collector": "personal_transcript_connector",
                    "capture_run_id": run_id,
                    "raw_preserved": True,
                    "security_policy": "prompt_security_scan_on_queue",
                    "source_trust_reason": "personal_export_import",
                },
            },
            raw_path=raw_export_path,
        )
        if self.db.get_ingestion_entry(artifact.id) is None:
            raise RuntimeError(f"Failed to queue personal transcript artifact: {artifact.id}")

    def _relative_to_vault(self, path: Path | None) -> str | None:
        if path is None:
            return None
        try:
            return path.relative_to(self.layout.vault_root).as_posix()
        except ValueError:
            return str(path)

    def _queue_session(
        self,
        session: _NormalizedTranscriptSession,
        *,
        source_name: str,
        raw_export_path: Path,
        lifecycle: CaptureLifecycleService,
        run_id: str,
    ) -> PersonalTranscriptRecord:
        transcript_path = self._write_transcript_file(
            session,
            source_name=source_name,
            raw_export_path=raw_export_path,
        )
        transcript_artifact = self._build_transcript_artifact(
            session,
            source_name=source_name,
            raw_export_path=raw_export_path,
            transcript_path=transcript_path,
        )
        self._queue_artifact(
            lifecycle,
            transcript_artifact,
            session=session,
            source_name=source_name,
            raw_export_path=raw_export_path,
            run_id=run_id,
        )
        return PersonalTranscriptRecord(
            artifact_id=transcript_artifact.id,
            session_id=session.session_id,
            source_name=source_name,
            raw_export_path=raw_export_path,
            transcript_path=transcript_path,
        )


def _sessions_from_json(
    payload: Any,
    *,
    fallback_id: str,
    defaults: _TranscriptDefaults,
) -> list[_NormalizedTranscriptSession]:
    if isinstance(payload, Mapping):
        for key in ("sessions", "conversations", "transcripts", "memories", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return _sessions_from_records(
                    value,
                    fallback_id=fallback_id,
                    defaults=defaults,
                )
        return [
            _session_from_mapping(
                payload,
                fallback_id=fallback_id,
                index=0,
                defaults=defaults,
            )
        ]
    if isinstance(payload, list):
        return _sessions_from_records(
            payload,
            fallback_id=fallback_id,
            defaults=defaults,
        )
    raise ValueError("Personal transcript JSON export must contain an object or array")


def _resolve_omi_api_query(
    configured: Mapping[str, Any],
    *,
    api_key: str | None,
    api_key_env: str | None,
    api_base_url: str | None,
    api_limit: int | None,
    api_page_size: int | None,
    include_transcript: bool | None,
    start_date: str | None,
    end_date: str | None,
    categories: Iterable[str] | str | None,
    folder_id: str | None,
    starred: bool | None,
    timeout_seconds: float | None,
) -> OmiConversationQuery | None:
    env_name = _clean_string(api_key_env or configured.get("api_key_env")) or "OMI_API_KEY"
    resolved_key = _clean_string(api_key or configured.get("api_key") or os.getenv(env_name))
    if not resolved_key:
        return None

    limit = _positive_int(
        api_limit
        if api_limit is not None
        else configured.get("api_limit", configured.get("limit", OMI_DEFAULT_PAGE_SIZE)),
        field_name="sources.omi.api_limit",
    )
    page_size = _positive_int(
        api_page_size
        if api_page_size is not None
        else configured.get("api_page_size", min(limit, OMI_DEFAULT_PAGE_SIZE)),
        field_name="sources.omi.api_page_size",
    )
    include = (
        bool(include_transcript)
        if include_transcript is not None
        else _bool_setting(configured.get("include_transcript"), default=True)
    )
    resolved_starred = (
        starred
        if starred is not None
        else _optional_bool_setting(configured.get("starred"))
    )
    return OmiConversationQuery(
        api_key=resolved_key,
        base_url=_clean_string(api_base_url or configured.get("base_url"))
        or OMI_DEFAULT_BASE_URL,
        limit=limit,
        page_size=page_size,
        include_transcript=include,
        start_date=_clean_string(start_date or configured.get("start_date")),
        end_date=_clean_string(end_date or configured.get("end_date")),
        categories=normalize_categories(categories or configured.get("categories")),
        folder_id=_clean_string(folder_id or configured.get("folder_id")),
        starred=resolved_starred,
        timeout_seconds=_positive_float(
            timeout_seconds
            if timeout_seconds is not None
            else configured.get("timeout_seconds", 30.0),
            field_name="sources.omi.timeout_seconds",
        ),
    )


def _omi_api_requested(
    *,
    has_local_sources: bool,
    configured: Mapping[str, Any],
    api_key: str | None,
    api_key_env: str | None,
    api_base_url: str | None,
    api_limit: int | None,
    api_page_size: int | None,
    include_transcript: bool | None,
    start_date: str | None,
    end_date: str | None,
    categories: Iterable[str] | str | None,
    folder_id: str | None,
    starred: bool | None,
    timeout_seconds: float | None,
) -> bool:
    if not has_local_sources:
        return True
    if _bool_setting(configured.get("api_enabled"), default=False):
        return True
    return any(
        value not in (None, "", [], ())
        for value in (
            api_key,
            api_key_env,
            api_base_url,
            api_limit,
            api_page_size,
            include_transcript,
            start_date,
            end_date,
            categories,
            folder_id,
            starred,
            timeout_seconds,
        )
    )


def _sessions_from_records(
    records: list[Any],
    *,
    fallback_id: str,
    defaults: _TranscriptDefaults,
) -> list[_NormalizedTranscriptSession]:
    mappings = [record for record in records if isinstance(record, Mapping)]
    if not mappings:
        return []
    if any(_looks_like_session(record) for record in mappings):
        return [
            _session_from_mapping(
                record,
                fallback_id=fallback_id,
                index=index,
                defaults=defaults,
            )
            for index, record in enumerate(mappings)
        ]

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for index, record in enumerate(mappings):
        session_id = _first_string(
            record,
            "session_id",
            "sessionId",
            "conversation_id",
            "meeting_id",
            "memory_id",
        )
        if not session_id:
            session_id = defaults.session_id or fallback_id or f"session-{index + 1}"
        grouped.setdefault(_stable_session_id(session_id), []).append(record)

    return [
        _session_from_rows(rows, session_id=session_id, defaults=defaults)
        for session_id, rows in grouped.items()
    ]


def _session_from_mapping(
    record: Mapping[str, Any],
    *,
    fallback_id: str,
    index: int,
    defaults: _TranscriptDefaults,
) -> _NormalizedTranscriptSession:
    session_id = _stable_session_id(
        _first_string(
            record,
            "session_id",
            "sessionId",
            "id",
            "conversation_id",
            "meeting_id",
            "memory_id",
            "uuid",
        )
        or defaults.session_id
        or f"{fallback_id}-{index + 1}"
    )
    structured = record.get("structured") if isinstance(record.get("structured"), Mapping) else {}
    segments = _first_sequence(
        record,
        "transcript_segments",
        "segments",
        "messages",
        "utterances",
        "entries",
    )
    speaker_values: list[str] = []
    if segments:
        transcript = _render_segment_lines(segments, speaker_values=speaker_values)
    else:
        transcript = _first_string(
            record,
            "transcript",
            "text",
            "content",
            "markdown",
            "body",
        )
    if not transcript:
        raise ValueError(f"Personal transcript session {session_id} has no transcript text")

    title = (
        _first_string(record, "title", "name", "topic")
        or _first_string(structured, "title")
        or f"Personal transcript {session_id}"
    )
    speaker = (
        _first_string(record, "speaker", "speaker_name", "person")
        or defaults.speaker
        or _single_value(speaker_values)
    )
    device_id = _first_string(
        record,
        "device_id",
        "deviceId",
        "device",
        "source",
        "source_device",
    ) or defaults.device_id
    started_at = _first_string(
        record,
        "started_at",
        "start_time",
        "timestamp",
        "created_at",
        "createdAt",
        "date",
    )
    ended_at = _first_string(record, "ended_at", "end_time", "finished_at")
    tags = _string_list(record.get("tags"))
    category = _first_string(record, "category") or _first_string(structured, "category")
    if category:
        tags.append(category)
    tags.extend([_safe_slug(defaults.source_name), "personal-transcript"])
    return _NormalizedTranscriptSession(
        session_id=session_id,
        title=title,
        raw_transcript=transcript,
        summary=(
            _first_string(record, "summary", "description", "overview")
            or _first_string(structured, "overview")
        ),
        speaker=speaker,
        device_id=device_id,
        started_at=started_at,
        ended_at=ended_at,
        language=_first_string(record, "language", "lang") or defaults.language,
        tags=tuple(dict.fromkeys(tag for tag in tags if tag)),
    )


def _session_from_rows(
    rows: list[Mapping[str, Any]],
    *,
    session_id: str,
    defaults: _TranscriptDefaults,
) -> _NormalizedTranscriptSession:
    speaker_values: list[str] = []
    transcript = _render_segment_lines(rows, speaker_values=speaker_values)
    title = _first_present(rows, "title", "name", "topic") or f"Personal transcript {session_id}"
    device_id = _first_present(rows, "device_id", "deviceId", "device", "source_device") or defaults.device_id
    speaker = defaults.speaker or _single_value(speaker_values)
    started_at = _first_present(
        rows,
        "started_at",
        "start_time",
        "timestamp",
        "created_at",
        "createdAt",
        "date",
    )
    ended_at = _last_present(rows, "ended_at", "end_time", "finished_at", "timestamp")
    tags = [_safe_slug(defaults.source_name), "personal-transcript"]
    return _NormalizedTranscriptSession(
        session_id=session_id,
        title=title,
        raw_transcript=transcript,
        summary=_first_present(rows, "summary", "description", "overview"),
        speaker=speaker,
        device_id=device_id,
        started_at=started_at,
        ended_at=ended_at,
        language=_first_present(rows, "language", "lang") or defaults.language,
        tags=tuple(dict.fromkeys(tag for tag in tags if tag)),
    )


def _render_segment_lines(
    segments: Iterable[Any],
    *,
    speaker_values: list[str],
) -> str:
    lines: list[str] = []
    for segment in segments:
        if not isinstance(segment, Mapping):
            text = str(segment).strip()
            if text:
                lines.append(text)
            continue
        text = _first_string(segment, "text", "content", "transcript", "body")
        if not text:
            continue
        speaker = _first_string(segment, "speaker", "speaker_name", "person", "role")
        if speaker:
            speaker_values.append(speaker)
        timestamp = _first_string(
            segment,
            "timestamp",
            "start_time",
            "started_at",
            "start",
            "created_at",
            "createdAt",
            "time",
        )
        prefix_parts = []
        if timestamp:
            prefix_parts.append(f"[{timestamp}]")
        if speaker:
            prefix_parts.append(f"{speaker}:")
        prefix = " ".join(prefix_parts)
        lines.append(f"{prefix} {text}".strip())
    return "\n".join(lines).strip()


def _render_transcript_markdown(
    session: _NormalizedTranscriptSession,
    *,
    source_name: str,
    raw_export_ref: str | None,
) -> str:
    frontmatter = {
        "source_type": source_name,
        "transcript_id": f"{_safe_slug(source_name)}_transcript_{_safe_slug(session.session_id)}",
        "session_id": session.session_id,
        "device_id": session.device_id,
        "speaker": session.speaker,
        "started_at": session.started_at,
        "ended_at": session.ended_at,
        "language": session.language,
        "raw_export_path": raw_export_ref,
    }
    lines = ["---"]
    for key, value in frontmatter.items():
        if value not in (None, ""):
            lines.append(f"{key}: {json.dumps(value, ensure_ascii=False)}")
    lines.extend(["---", "", f"# {session.title}", ""])
    if session.summary:
        lines.extend(["## Summary", session.summary, ""])
    lines.extend(["## Transcript", session.raw_transcript, ""])
    if session.tags:
        lines.append(" ".join(f"#{tag.replace(' ', '_')}" for tag in session.tags))
    return "\n".join(lines)


def _files_in_dir(directory: Path, file_patterns: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in file_patterns:
        files.extend(path for path in directory.rglob(pattern) if path.is_file())
    return sorted(files)


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    deduped: dict[Path, Path] = {}
    for path in paths:
        deduped[path.resolve()] = path
    return list(deduped.values())


def _path_list(value: Any) -> list[Path]:
    return [Path(item).expanduser() for item in _string_list(value)]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _positive_int(value: Any, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{field_name} must be at least 1")
    return parsed


def _positive_float(value: Any, *, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _bool_setting(value: Any, *, default: bool) -> bool:
    parsed = _optional_bool_setting(value)
    return default if parsed is None else parsed


def _optional_bool_setting(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    raise ValueError("boolean settings must be true or false")


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars = [char if char.isalnum() else "-" for char in text]
    slug = "-".join(part for part in "".join(chars).split("-") if part)
    return slug or "transcript"


def _stable_session_id(value: Any) -> str:
    return str(value or "").strip() or "session"


def _title_from_text(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip() or None
        if stripped:
            return stripped[:80]
    return None


def _first_string(record: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = record.get(key)
        text = _clean_string(value)
        if text:
            return text
    return None


def _first_sequence(record: Mapping[str, Any], *keys: str) -> list[Any] | None:
    for key in keys:
        value = record.get(key)
        if isinstance(value, list):
            return value
    return None


def _first_present(rows: list[Mapping[str, Any]], *keys: str) -> str | None:
    for row in rows:
        value = _first_string(row, *keys)
        if value:
            return value
    return None


def _last_present(rows: list[Mapping[str, Any]], *keys: str) -> str | None:
    for row in reversed(rows):
        value = _first_string(row, *keys)
        if value:
            return value
    return None


def _single_value(values: Iterable[str]) -> str | None:
    deduped = tuple(dict.fromkeys(value for value in values if value))
    if len(deduped) == 1:
        return deduped[0]
    return None


def _looks_like_session(record: Mapping[str, Any]) -> bool:
    return any(
        key in record
        for key in (
            "segments",
            "transcript_segments",
            "messages",
            "utterances",
            "entries",
            "transcript",
            "summary",
            "title",
            "name",
            "topic",
        )
    )
