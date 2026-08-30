"""YouTube connector producing video and transcript artifacts."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import parse_qs, urlparse

from core.artifacts import TranscriptArtifact, VideoArtifact
from core.capture_event_store import CaptureEventStore
from core.capture_lifecycle import CaptureLifecycleService
from core.config import Config, config
from core.connector_budgets import (
    ConnectorBudgetError,
    ConnectorBudgetTracker,
    start_connector_budget_run,
)
from core.connector_capture import ConnectorCaptureQueue
from core.metadata_db import MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout
from processors.youtube_processor import YouTubeProcessor, YouTubeVideo

logger = logging.getLogger(__name__)


YOUTUBE_URL_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch\?[^ \n\r\t\"'<>]+|youtu\.be/[A-Za-z0-9_-]+[^ \n\r\t\"'<>]*)"
)


class YouTubeArchiveError(RuntimeError):
    """Raised when a bounded yt-dlp archival attempt fails."""

    def __init__(
        self,
        message: str,
        *,
        status: str,
        video_id: str,
    ) -> None:
        self.status = status
        self.video_id = video_id
        super().__init__(message)


@dataclass(frozen=True)
class ArchiveBounds:
    """Runtime bounds for a single yt-dlp archival attempt."""

    max_duration_seconds: float | None = None
    max_file_size_bytes: int | None = None
    archive_format: str | None = None
    timeout_seconds: float = 300.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_duration_seconds": self.max_duration_seconds,
            "max_file_size_bytes": self.max_file_size_bytes,
            "archive_format": self.archive_format,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass(frozen=True)
class YouTubeConnectorRecord:
    """Artifacts produced for one YouTube video."""

    video_id: str
    source_url: str
    raw_payload_path: Path
    video_artifact_id: str
    transcript_artifact_id: str | None = None
    transcript_path: Path | None = None
    archive_path: Path | None = None
    archive_status: str | None = None
    archive_error: str | None = None
    queued: bool = True


@dataclass(frozen=True)
class YouTubeConnectorResult:
    """Summary of one connector collection run."""

    records: tuple[YouTubeConnectorRecord, ...] = field(default_factory=tuple)
    skipped_urls: tuple[str, ...] = field(default_factory=tuple)
    errors: tuple[Mapping[str, str], ...] = field(default_factory=tuple)
    playlist_urls: tuple[str, ...] = field(default_factory=tuple)
    export_paths: tuple[str, ...] = field(default_factory=tuple)
    budget: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [
                {
                    "video_id": record.video_id,
                    "source_url": record.source_url,
                    "raw_payload_path": str(record.raw_payload_path),
                    "video_artifact_id": record.video_artifact_id,
                    "transcript_artifact_id": record.transcript_artifact_id,
                    "transcript_path": str(record.transcript_path)
                    if record.transcript_path
                    else None,
                    "archive_path": str(record.archive_path)
                    if record.archive_path
                    else None,
                    "archive_status": record.archive_status,
                    "archive_error": record.archive_error,
                    "queued": record.queued,
                }
                for record in self.records
            ],
            "queued_count": sum(1 for record in self.records if record.queued),
            "skipped_urls": list(self.skipped_urls),
            "errors": [dict(error) for error in self.errors],
            "playlist_urls": list(self.playlist_urls),
            "export_paths": list(self.export_paths),
            "budget": self.budget,
        }


class YouTubeConnector:
    """Collect YouTube videos/transcripts through the artifact queue."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        processor: YouTubeProcessor | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()
        self.processor = processor or YouTubeProcessor(vault_path=str(self.layout.vault_root))
        self.capture_queue = ConnectorCaptureQueue(
            self.config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )

    async def collect(
        self,
        *,
        urls: Iterable[str] | None = None,
        playlist_urls: Iterable[str] | None = None,
        export_paths: Iterable[str | Path] | None = None,
        limit: int | None = None,
        archive_video: bool | None = None,
        archive_max_duration_seconds: float | None = None,
        archive_max_file_size_bytes: int | None = None,
        archive_format: str | None = None,
        archive_timeout_seconds: float | None = None,
        resume: bool = True,
    ) -> YouTubeConnectorResult:
        """Collect configured YouTube sources and queue resulting artifacts."""
        bounds = self._resolve_archive_bounds(
            max_duration_seconds=archive_max_duration_seconds,
            max_file_size_bytes=archive_max_file_size_bytes,
            archive_format=archive_format,
            timeout_seconds=archive_timeout_seconds,
        )
        explicit_urls = _string_list(urls)
        playlist_inputs = _string_list(playlist_urls)
        export_inputs = [Path(path).expanduser() for path in _string_list(export_paths)]
        budget = start_connector_budget_run(self.config, "youtube")
        budget.add_files(export_inputs)
        if explicit_urls:
            budget.add_input_text("\n".join(explicit_urls), label="youtube urls")
        if playlist_inputs:
            budget.add_input_text(
                "\n".join(playlist_inputs),
                label="youtube playlist urls",
            )

        discovered_urls: list[str] = []
        discovered_urls.extend(explicit_urls)
        for export_path in export_inputs:
            discovered_urls.extend(self._urls_from_export(export_path))
        for playlist_url in playlist_inputs:
            discovered_urls.extend(await self._urls_from_playlist(playlist_url))

        unique_urls = _dedupe(discovered_urls)
        if limit is not None:
            unique_urls = unique_urls[: max(1, int(limit))]
        budget.add_estimated_output_artifacts(
            len(unique_urls) * 2,
            label="youtube video and transcript artifacts",
        )

        records: list[YouTubeConnectorRecord] = []
        skipped: list[str] = []
        errors: list[dict[str, str]] = []
        run_id = datetime.now().isoformat()
        for source_url in unique_urls:
            video_id = self.processor.extract_video_id(source_url)
            if not video_id:
                skipped.append(source_url)
                continue
            try:
                record = await self._collect_video(
                    video_id,
                    source_url=source_url,
                    archive_video=archive_video,
                    archive_bounds=bounds,
                    resume=resume,
                    run_id=run_id,
                    budget=budget,
                )
            except ConnectorBudgetError as exc:
                logger.warning(
                    "Skipping YouTube video %s after budget error: %s",
                    video_id,
                    exc,
                )
                skipped.append(source_url)
                errors.append(
                    {
                        "url": source_url,
                        "video_id": video_id,
                        "error_type": exc.__class__.__name__,
                        "error": str(exc),
                    }
                )
                continue
            records.append(record)

        return YouTubeConnectorResult(
            records=tuple(records),
            skipped_urls=tuple(skipped),
            errors=tuple(errors),
            playlist_urls=tuple(playlist_inputs),
            export_paths=tuple(str(path) for path in export_inputs),
            budget=budget.summary(),
        )

    async def _collect_video(
        self,
        video_id: str,
        *,
        source_url: str,
        archive_video: bool | None,
        archive_bounds: ArchiveBounds,
        resume: bool,
        run_id: str,
        budget: ConnectorBudgetTracker,
    ) -> YouTubeConnectorRecord:
        video, _metrics = await self.processor.process_video(
            video_id,
            resume_metadata=resume,
            resume_transcripts=resume,
            source_label="youtube connector",
        )
        if video is None:
            video = self._video_from_existing_or_stub(video_id, source_url)

        transcript_text = video.transcript or video.formatted_transcript or ""
        if transcript_text:
            budget.add_transcript_text(
                transcript_text,
                label=f"youtube transcript {video_id}",
            )

        archive_path = None
        archive_status = "skipped"
        archive_error = None
        if self._archive_enabled(archive_video):
            try:
                archive_path = await asyncio.to_thread(
                    self._archive_video,
                    source_url,
                    video_id,
                    video=video,
                    bounds=archive_bounds,
                )
                archive_status = "archived"
            except YouTubeArchiveError as exc:
                archive_status = exc.status
                archive_error = str(exc)
                logger.warning(
                    "YouTube archive failed for %s with status %s: %s",
                    video_id,
                    exc.status,
                    exc,
                )

        transcript_path = self._latest_transcript_path(video_id)
        raw_payload_path = self._write_raw_payload(
            video,
            source_url=source_url,
            transcript_path=transcript_path,
            archive_path=archive_path,
        )
        budget.add_bytes(
            raw_payload_path.stat().st_size,
            label=f"youtube raw payload {video_id}",
        )
        raw_payload_ref = self._relative_to_vault(raw_payload_path)
        transcript_ref = self._relative_to_vault(transcript_path) if transcript_path else None
        archive_ref = self._relative_to_vault(archive_path) if archive_path else None

        transcript_artifact_id = None
        with self.capture_queue.lifecycle() as lifecycle:
            if video.transcript or video.formatted_transcript or transcript_path:
                transcript_artifact = self._build_transcript_artifact(
                    video,
                    source_url=source_url,
                    raw_payload_ref=raw_payload_ref,
                    transcript_ref=transcript_ref,
                )
                transcript_artifact_id = transcript_artifact.id
                self._queue_artifact(
                    lifecycle,
                    transcript_artifact,
                    artifact_type="transcript",
                    raw_payload_path=raw_payload_path,
                    source_url=source_url,
                    run_id=run_id,
                )

            video_artifact = self._build_video_artifact(
                video,
                source_url=source_url,
                raw_payload_ref=raw_payload_ref,
                archive_ref=archive_ref,
                transcript_artifact_id=transcript_artifact_id,
            )
            self._queue_artifact(
                lifecycle,
                video_artifact,
                artifact_type="video",
                raw_payload_path=raw_payload_path,
                source_url=source_url,
                run_id=run_id,
            )

        return YouTubeConnectorRecord(
            video_id=video_id,
            source_url=source_url,
            raw_payload_path=raw_payload_path,
            video_artifact_id=video_artifact.id,
            transcript_artifact_id=transcript_artifact_id,
            transcript_path=transcript_path,
            archive_path=archive_path,
            archive_status=archive_status,
            archive_error=archive_error,
        )

    def _urls_from_export(self, path: Path) -> list[str]:
        if not path.exists():
            raise FileNotFoundError(f"YouTube export path does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"YouTube export path is not a file: {path}")
        text = path.read_text(encoding="utf-8")
        return _dedupe(match.group(0).rstrip(").,]") for match in YOUTUBE_URL_PATTERN.finditer(text))

    async def _urls_from_playlist(self, playlist_url: str) -> list[str]:
        playlist_id = _playlist_id_from_url(playlist_url)
        if not playlist_id:
            raise ValueError(f"YouTube playlist URL is missing list= id: {playlist_url}")
        api_key = str(self.config.get("sources.youtube.api_key") or "").strip()
        if not api_key:
            import os

            api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
        if not api_key:
            raise ValueError("YouTube playlist ingestion requires sources.youtube.api_key or YOUTUBE_API_KEY")

        return await asyncio.to_thread(self._fetch_playlist_urls, playlist_id, api_key)

    def _fetch_playlist_urls(self, playlist_id: str, api_key: str) -> list[str]:
        urls: list[str] = []
        page_token: str | None = None
        while True:
            response = self.processor.session.get(
                "https://www.googleapis.com/youtube/v3/playlistItems",
                params={
                    "part": "contentDetails",
                    "playlistId": playlist_id,
                    "maxResults": 50,
                    "pageToken": page_token,
                    "key": api_key,
                },
                timeout=self.processor.api_timeout,
            )
            response.raise_for_status()
            payload = response.json()
            for item in payload.get("items") or []:
                content_details = item.get("contentDetails") or {}
                video_id = str(content_details.get("videoId") or "").strip()
                if video_id:
                    urls.append(f"https://youtu.be/{video_id}")
            page_token = payload.get("nextPageToken")
            if not page_token:
                break
        return _dedupe(urls)

    def _video_from_existing_or_stub(self, video_id: str, source_url: str) -> YouTubeVideo:
        return YouTubeVideo(
            video_id=video_id,
            title=f"YouTube Video {video_id}",
            description="Video already processed or metadata unavailable",
            published_at="",
            channel_id="",
            channel_title="",
        )

    def _latest_transcript_path(self, video_id: str) -> Path | None:
        transcripts = self.processor.find_existing_transcript_files(video_id)
        if not transcripts:
            return None
        return max(transcripts, key=lambda path: path.stat().st_mtime)

    def _write_raw_payload(
        self,
        video: YouTubeVideo,
        *,
        source_url: str,
        transcript_path: Path | None,
        archive_path: Path | None,
    ) -> Path:
        raw_root = self.layout.raw_root / "youtube"
        raw_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "source_url": source_url,
            "captured_at": datetime.now().isoformat(),
            "video": video.to_dict(),
            "transcript_path": self._relative_to_vault(transcript_path)
            if transcript_path
            else None,
            "archive_path": self._relative_to_vault(archive_path)
            if archive_path
            else None,
        }
        raw_path = raw_root / f"{video.video_id}.json"
        raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return raw_path

    def _build_video_artifact(
        self,
        video: YouTubeVideo,
        *,
        source_url: str,
        raw_payload_ref: str,
        archive_ref: str | None,
        transcript_artifact_id: str | None,
    ) -> VideoArtifact:
        video_id = video.video_id
        artifact_id = f"yt_video_{video_id}"
        output_paths = {}
        if archive_ref:
            output_paths["archive"] = archive_ref
        return VideoArtifact(
            id=artifact_id,
            source_type="youtube",
            raw_content=json.dumps(video.to_dict(), ensure_ascii=False),
            created_at=video.published_at or None,
            ingested_at=datetime.now().isoformat(),
            video_id=video_id,
            title=video.title,
            description=video.description,
            source_url=source_url,
            channel_id=video.channel_id,
            channel_title=video.channel_title,
            published_at=video.published_at,
            duration=video.duration,
            view_count=video.view_count,
            thumbnail_url=video.thumbnail_url,
            archive_path=archive_ref,
            transcript_artifact_id=transcript_artifact_id,
            custom_metadata={"raw_payload_path": raw_payload_ref},
            output_paths=output_paths,
        )

    def _build_transcript_artifact(
        self,
        video: YouTubeVideo,
        *,
        source_url: str,
        raw_payload_ref: str,
        transcript_ref: str | None,
    ) -> TranscriptArtifact:
        video_id = video.video_id
        artifact_id = f"yt_transcript_{video_id}"
        tags = []
        if video.transcript_tags:
            tags = [tag.strip() for tag in video.transcript_tags.split(",") if tag.strip()]
        output_paths = {}
        if transcript_ref:
            output_paths["markdown"] = transcript_ref
        return TranscriptArtifact(
            id=artifact_id,
            source_type="youtube",
            raw_content=json.dumps(video.to_dict(), ensure_ascii=False),
            created_at=video.published_at or None,
            ingested_at=datetime.now().isoformat(),
            transcript_id=artifact_id,
            video_id=video_id,
            title=video.title,
            source_url=source_url,
            transcript_path=transcript_ref,
            raw_transcript=video.transcript or "",
            processed_transcript=video.formatted_transcript or "",
            summary=video.transcript_summary,
            tags=tags,
            language="en",
            custom_metadata={"raw_payload_path": raw_payload_ref},
            output_paths=output_paths,
        )

    def _queue_artifact(
        self,
        lifecycle: CaptureLifecycleService,
        artifact: VideoArtifact | TranscriptArtifact,
        *,
        artifact_type: str,
        raw_payload_path: Path,
        source_url: str,
        run_id: str,
    ) -> None:
        native_id = artifact.video_id if isinstance(artifact, VideoArtifact) else (
            artifact.video_id or artifact.transcript_id
        )
        self.capture_queue.queue_artifact(
            lifecycle,
            artifact,
            artifact_type=artifact_type,
            source={
                "source_name": "youtube",
                "source_type": "video_platform",
                "collector": "youtube_connector",
                "native_source_id": getattr(artifact, "channel_id", None),
                "base_uri": "https://www.youtube.com",
                "metadata": {
                    "source_url": source_url,
                    "channel_title": getattr(artifact, "channel_title", None),
                },
            },
            session={
                "session_type": "youtube_collect",
                "native_session_id": f"youtube:{run_id}",
                "started_at": run_id,
                "metadata": {"source_url": source_url},
            },
            event={
                "event_type": f"youtube_{artifact_type}",
                "native_event_id": f"{artifact_type}:{native_id}",
                "occurred_at": artifact.created_at,
                "captured_at": artifact.ingested_at,
                "provenance": {"collector": "youtube_connector"},
            },
            raw_path=raw_payload_path,
        )
        if self.db.get_ingestion_entry(artifact.id) is None:
            raise RuntimeError(f"Failed to queue YouTube artifact: {artifact.id}")

    def _resolve_archive_bounds(
        self,
        *,
        max_duration_seconds: float | None = None,
        max_file_size_bytes: int | None = None,
        archive_format: str | None = None,
        timeout_seconds: float | None = None,
    ) -> ArchiveBounds:
        """Resolve effective archive bounds from config and CLI overrides.

        Explicit override values take precedence over configuration. Invalid
        values fail closed rather than silently falling back.
        """
        cfg = self.config.get("sources.youtube", {}) or {}

        def _resolve(
            override: Any,
            config_key: str,
            default: Any,
            *,
            coerce: Callable[[Any], Any] | None = None,
        ) -> Any:
            if override is not None:
                value = override
            else:
                value = cfg.get(config_key, default)
            if coerce is not None and value is not None:
                value = coerce(value)
            return value

        def _positive_float(value: Any) -> float:
            try:
                parsed = float(value)
            except (TypeError, ValueError) as exc:
                raise YouTubeArchiveError(
                    f"archive timeout must be a positive number, got {value!r}",
                    status="config_error",
                    video_id="",
                ) from exc
            if parsed <= 0:
                raise YouTubeArchiveError(
                    f"archive timeout must be positive, got {parsed}",
                    status="config_error",
                    video_id="",
                )
            return parsed

        def _positive_int_or_none(value: Any) -> int | None:
            if value is None:
                return None
            try:
                parsed = int(value)
            except (TypeError, ValueError) as exc:
                raise YouTubeArchiveError(
                    f"archive size/duration limit must be a non-negative integer, got {value!r}",
                    status="config_error",
                    video_id="",
                ) from exc
            if parsed < 0:
                raise YouTubeArchiveError(
                    f"archive size/duration limit must be non-negative, got {parsed}",
                    status="config_error",
                    video_id="",
                )
            return parsed if parsed > 0 else None

        effective_timeout = _resolve(
            timeout_seconds,
            "archive_timeout_seconds",
            300.0,
            coerce=_positive_float,
        )
        effective_max_duration = _resolve(
            max_duration_seconds,
            "archive_max_duration_seconds",
            None,
            coerce=_positive_int_or_none,
        )
        effective_max_size = _resolve(
            max_file_size_bytes,
            "archive_max_file_size_bytes",
            None,
            coerce=_positive_int_or_none,
        )
        effective_format = _resolve(archive_format, "archive_format", None)
        if effective_format is not None:
            effective_format = str(effective_format).strip() or None

        return ArchiveBounds(
            max_duration_seconds=effective_max_duration,
            max_file_size_bytes=effective_max_size,
            archive_format=effective_format,
            timeout_seconds=effective_timeout,
        )

    def _archive_enabled(self, archive_video: bool | None) -> bool:
        if archive_video is not None:
            return bool(archive_video)
        return bool(self.config.get("sources.youtube.archive_video", False))

    def _archive_video(
        self,
        source_url: str,
        video_id: str,
        *,
        video: YouTubeVideo,
        bounds: ArchiveBounds,
    ) -> Path:
        archive_root = self.layout.library_root / "youtube" / "videos"
        archive_root.mkdir(parents=True, exist_ok=True)
        output_template = str(archive_root / f"{video_id}.%(ext)s")

        if bounds.max_duration_seconds is not None:
            duration_seconds = _parse_iso8601_duration(video.duration)
            if duration_seconds is not None and duration_seconds > bounds.max_duration_seconds:
                raise YouTubeArchiveError(
                    f"Video {video_id} duration {duration_seconds}s exceeds "
                    f"archive max duration {bounds.max_duration_seconds}s",
                    status="over_duration",
                    video_id=video_id,
                )

        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            "-o",
            output_template,
        ]
        if bounds.archive_format:
            cmd.extend(["-f", bounds.archive_format])
        cmd.append(source_url)

        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=bounds.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise YouTubeArchiveError(
                f"yt-dlp archive for {video_id} timed out after "
                f"{bounds.timeout_seconds}s",
                status="timeout",
                video_id=video_id,
            ) from exc
        except FileNotFoundError as exc:
            raise YouTubeArchiveError(
                "yt-dlp command not found; install yt-dlp to archive YouTube videos",
                status="error",
                video_id=video_id,
            ) from exc
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise YouTubeArchiveError(
                f"yt-dlp archive for {video_id} failed: {stderr or exc.returncode}",
                status="error",
                video_id=video_id,
            ) from exc

        candidates = sorted(archive_root.glob(f"{video_id}.*"))
        if not candidates:
            raise YouTubeArchiveError(
                f"yt-dlp completed without writing archive for {video_id}",
                status="error",
                video_id=video_id,
            )
        archive_path = candidates[0]

        if bounds.max_file_size_bytes is not None:
            file_size = archive_path.stat().st_size
            if file_size > bounds.max_file_size_bytes:
                try:
                    archive_path.unlink()
                except OSError:
                    pass
                raise YouTubeArchiveError(
                    f"Archive for {video_id} size {file_size} bytes exceeds "
                    f"max file size {bounds.max_file_size_bytes} bytes",
                    status="over_size",
                    video_id=video_id,
                )

        return archive_path

    def _relative_to_vault(self, path: Path | None) -> str | None:
        if path is None:
            return None
        try:
            return path.relative_to(self.layout.vault_root).as_posix()
        except ValueError:
            return str(path)


def _string_list(value: Iterable[str] | str | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


def _playlist_id_from_url(value: str) -> str | None:
    parsed = urlparse(value)
    params = parse_qs(parsed.query)
    list_values = params.get("list") or []
    if list_values:
        return list_values[0].strip() or None
    return None


_ISO8601_DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?"
    r"$"
)


def _parse_iso8601_duration(value: str | None) -> int | None:
    """Return the total seconds for an ISO 8601 duration, or None if unknown."""
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    match = _ISO8601_DURATION_RE.match(value)
    if not match:
        return None
    parts = match.groupdict()
    total = 0
    total += int(parts.get("days") or 0) * 86400
    total += int(parts.get("hours") or 0) * 3600
    total += int(parts.get("minutes") or 0) * 60
    total += int(parts.get("seconds") or 0)
    return total
