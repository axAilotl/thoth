"""YouTube connector producing video and transcript artifacts."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, urlparse

from core.artifacts import TranscriptArtifact, VideoArtifact
from core.config import Config, config
from core.metadata_db import IngestionQueueEntry, MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout
from processors.youtube_processor import YouTubeProcessor, YouTubeVideo

logger = logging.getLogger(__name__)


YOUTUBE_URL_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch\?[^ \n\r\t\"'<>]+|youtu\.be/[A-Za-z0-9_-]+[^ \n\r\t\"'<>]*)"
)


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
    queued: bool = True


@dataclass(frozen=True)
class YouTubeConnectorResult:
    """Summary of one connector collection run."""

    records: tuple[YouTubeConnectorRecord, ...] = field(default_factory=tuple)
    skipped_urls: tuple[str, ...] = field(default_factory=tuple)
    playlist_urls: tuple[str, ...] = field(default_factory=tuple)
    export_paths: tuple[str, ...] = field(default_factory=tuple)

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
                    "queued": record.queued,
                }
                for record in self.records
            ],
            "queued_count": sum(1 for record in self.records if record.queued),
            "skipped_urls": list(self.skipped_urls),
            "playlist_urls": list(self.playlist_urls),
            "export_paths": list(self.export_paths),
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
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()
        self.processor = processor or YouTubeProcessor(vault_path=str(self.layout.vault_root))

    async def collect(
        self,
        *,
        urls: Iterable[str] | None = None,
        playlist_urls: Iterable[str] | None = None,
        export_paths: Iterable[str | Path] | None = None,
        limit: int | None = None,
        archive_video: bool | None = None,
        resume: bool = True,
    ) -> YouTubeConnectorResult:
        """Collect configured YouTube sources and queue resulting artifacts."""
        explicit_urls = _string_list(urls)
        playlist_inputs = _string_list(playlist_urls)
        export_inputs = [Path(path).expanduser() for path in _string_list(export_paths)]

        discovered_urls: list[str] = []
        discovered_urls.extend(explicit_urls)
        for export_path in export_inputs:
            discovered_urls.extend(self._urls_from_export(export_path))
        for playlist_url in playlist_inputs:
            discovered_urls.extend(await self._urls_from_playlist(playlist_url))

        unique_urls = _dedupe(discovered_urls)
        if limit is not None:
            unique_urls = unique_urls[: max(1, int(limit))]

        records: list[YouTubeConnectorRecord] = []
        skipped: list[str] = []
        for source_url in unique_urls:
            video_id = self.processor.extract_video_id(source_url)
            if not video_id:
                skipped.append(source_url)
                continue
            record = await self._collect_video(
                video_id,
                source_url=source_url,
                archive_video=archive_video,
                resume=resume,
            )
            records.append(record)

        return YouTubeConnectorResult(
            records=tuple(records),
            skipped_urls=tuple(skipped),
            playlist_urls=tuple(playlist_inputs),
            export_paths=tuple(str(path) for path in export_inputs),
        )

    async def _collect_video(
        self,
        video_id: str,
        *,
        source_url: str,
        archive_video: bool | None,
        resume: bool,
    ) -> YouTubeConnectorRecord:
        video, _metrics = await self.processor.process_video(
            video_id,
            resume_metadata=resume,
            resume_transcripts=resume,
            source_label="youtube connector",
        )
        if video is None:
            video = self._video_from_existing_or_stub(video_id, source_url)

        archive_path = None
        if self._archive_enabled(archive_video):
            archive_path = await asyncio.to_thread(self._archive_video, source_url, video_id)

        transcript_path = self._latest_transcript_path(video_id)
        raw_payload_path = self._write_raw_payload(
            video,
            source_url=source_url,
            transcript_path=transcript_path,
            archive_path=archive_path,
        )
        raw_payload_ref = self._relative_to_vault(raw_payload_path)
        transcript_ref = self._relative_to_vault(transcript_path) if transcript_path else None
        archive_ref = self._relative_to_vault(archive_path) if archive_path else None

        transcript_artifact_id = None
        if video.transcript or video.formatted_transcript or transcript_path:
            transcript_artifact = self._build_transcript_artifact(
                video,
                source_url=source_url,
                raw_payload_ref=raw_payload_ref,
                transcript_ref=transcript_ref,
            )
            transcript_artifact_id = transcript_artifact.id
            self._queue_artifact(
                transcript_artifact,
                artifact_type="transcript",
                source="youtube",
            )

        video_artifact = self._build_video_artifact(
            video,
            source_url=source_url,
            raw_payload_ref=raw_payload_ref,
            archive_ref=archive_ref,
            transcript_artifact_id=transcript_artifact_id,
        )
        self._queue_artifact(video_artifact, artifact_type="video", source="youtube")

        return YouTubeConnectorRecord(
            video_id=video_id,
            source_url=source_url,
            raw_payload_path=raw_payload_path,
            video_artifact_id=video_artifact.id,
            transcript_artifact_id=transcript_artifact_id,
            transcript_path=transcript_path,
            archive_path=archive_path,
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
        artifact: VideoArtifact | TranscriptArtifact,
        *,
        artifact_type: str,
        source: str,
    ) -> None:
        entry = IngestionQueueEntry(
            artifact_id=artifact.id,
            artifact_type=artifact_type,
            source=source,
            payload_json=json.dumps(artifact.to_dict(), ensure_ascii=False),
            created_at=artifact.ingested_at,
            capabilities_json=json.dumps(list(artifact.capabilities)),
        )
        if not self.db.upsert_ingestion_entry(entry):
            raise RuntimeError(f"Failed to queue YouTube artifact: {artifact.id}")

    def _archive_enabled(self, archive_video: bool | None) -> bool:
        if archive_video is not None:
            return bool(archive_video)
        return bool(self.config.get("sources.youtube.archive_video", False))

    def _archive_video(self, source_url: str, video_id: str) -> Path:
        try:
            from yt_dlp import YoutubeDL
        except ImportError as exc:
            raise RuntimeError(
                "YouTube video archival requires yt-dlp when sources.youtube.archive_video is enabled"
            ) from exc

        archive_root = self.layout.library_root / "youtube" / "videos"
        archive_root.mkdir(parents=True, exist_ok=True)
        output_template = str(archive_root / f"{video_id}.%(ext)s")
        with YoutubeDL({"outtmpl": output_template, "quiet": True, "noplaylist": True}) as ydl:
            ydl.download([source_url])
        candidates = sorted(archive_root.glob(f"{video_id}.*"))
        if not candidates:
            raise RuntimeError(f"yt-dlp completed without writing archive for {video_id}")
        return candidates[0]

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
