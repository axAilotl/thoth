"""Media artifacts for video and transcript ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

from .base import KnowledgeArtifact


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


@dataclass
class VideoArtifact(KnowledgeArtifact):
    """Canonical artifact for an externally hosted or archived video."""

    source_type: str = "youtube"
    capabilities: Tuple[str, ...] = ("video", "metadata", "transcript")

    video_id: str = ""
    title: str = ""
    description: str = ""
    source_url: str = ""
    channel_id: Optional[str] = None
    channel_title: Optional[str] = None
    published_at: Optional[str] = None
    duration: Optional[str] = None
    view_count: Optional[int] = None
    thumbnail_url: Optional[str] = None
    archive_path: Optional[str] = None
    transcript_artifact_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "video_id": self.video_id,
                "title": self.title,
                "description": self.description,
                "source_url": self.source_url,
                "channel_id": self.channel_id,
                "channel_title": self.channel_title,
                "published_at": self.published_at,
                "duration": self.duration,
                "view_count": self.view_count,
                "thumbnail_url": self.thumbnail_url,
                "archive_path": self.archive_path,
                "transcript_artifact_id": self.transcript_artifact_id,
            }
        )
        return data

    @classmethod
    def from_queue_payload(cls, payload: Mapping[str, Any]) -> "VideoArtifact":
        artifact_payload = dict(payload)
        raw_content = artifact_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(artifact_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        video_id = str(
            artifact_payload.get("video_id")
            or artifact_payload.get("native_id")
            or artifact_payload.get("id")
            or artifact_payload.get("artifact_id")
            or ""
        ).strip()
        if not video_id:
            raise ValueError("video queue payload missing video_id")

        artifact_id = str(
            artifact_payload.get("id")
            or artifact_payload.get("artifact_id")
            or f"yt_video_{video_id}"
        )
        view_count = artifact_payload.get("view_count")

        return cls(
            id=artifact_id,
            source_type=str(artifact_payload.get("source_type") or "youtube"),
            raw_content=raw_content,
            created_at=artifact_payload.get("created_at")
            or artifact_payload.get("published_at"),
            ingested_at=artifact_payload.get("ingested_at"),
            processing_status=str(artifact_payload.get("processing_status") or "pending"),
            video_id=video_id,
            title=str(artifact_payload.get("title") or video_id),
            description=str(artifact_payload.get("description") or ""),
            source_url=str(
                artifact_payload.get("source_url")
                or artifact_payload.get("url")
                or f"https://youtu.be/{video_id}"
            ),
            channel_id=artifact_payload.get("channel_id"),
            channel_title=artifact_payload.get("channel_title"),
            published_at=artifact_payload.get("published_at"),
            duration=artifact_payload.get("duration"),
            view_count=int(view_count) if view_count not in (None, "") else None,
            thumbnail_url=artifact_payload.get("thumbnail_url"),
            archive_path=artifact_payload.get("archive_path"),
            transcript_artifact_id=artifact_payload.get("transcript_artifact_id"),
            **cls.base_fields_from_payload(artifact_payload),
        )


@dataclass
class TranscriptArtifact(KnowledgeArtifact):
    """Canonical artifact for a sourced transcript and its processed form."""

    source_type: str = "youtube"
    capabilities: Tuple[str, ...] = ("transcript", "text", "llm_summary")

    transcript_id: str = ""
    video_id: Optional[str] = None
    title: str = ""
    source_url: str = ""
    transcript_path: Optional[str] = None
    raw_transcript: str = ""
    processed_transcript: str = ""
    summary: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    language: Optional[str] = None
    speaker: Optional[str] = None
    session_id: Optional[str] = None
    device_id: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.tags is None:
            self.tags = []

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "transcript_id": self.transcript_id,
                "video_id": self.video_id,
                "title": self.title,
                "source_url": self.source_url,
                "transcript_path": self.transcript_path,
                "raw_transcript": self.raw_transcript,
                "processed_transcript": self.processed_transcript,
                "summary": self.summary,
                "tags": self.tags,
                "language": self.language,
                "speaker": self.speaker,
                "session_id": self.session_id,
                "device_id": self.device_id,
            }
        )
        return data

    @classmethod
    def from_queue_payload(cls, payload: Mapping[str, Any]) -> "TranscriptArtifact":
        artifact_payload = dict(payload)
        raw_content = artifact_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(artifact_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        transcript_id = str(
            artifact_payload.get("transcript_id")
            or artifact_payload.get("id")
            or artifact_payload.get("artifact_id")
            or ""
        ).strip()
        video_id = artifact_payload.get("video_id")
        if not transcript_id:
            if video_id:
                transcript_id = f"yt_transcript_{video_id}"
            else:
                raise ValueError("transcript queue payload missing transcript_id")

        base_fields = cls.base_fields_from_payload(artifact_payload)
        base_fields.pop("tags", None)

        return cls(
            id=str(artifact_payload.get("id") or artifact_payload.get("artifact_id") or transcript_id),
            source_type=str(artifact_payload.get("source_type") or "youtube"),
            raw_content=raw_content,
            created_at=artifact_payload.get("created_at"),
            ingested_at=artifact_payload.get("ingested_at"),
            processing_status=str(artifact_payload.get("processing_status") or "pending"),
            transcript_id=transcript_id,
            video_id=str(video_id) if video_id else None,
            title=str(artifact_payload.get("title") or transcript_id),
            source_url=str(artifact_payload.get("source_url") or artifact_payload.get("url") or ""),
            transcript_path=artifact_payload.get("transcript_path"),
            raw_transcript=str(artifact_payload.get("raw_transcript") or ""),
            processed_transcript=str(
                artifact_payload.get("processed_transcript")
                or artifact_payload.get("formatted_transcript")
                or ""
            ),
            summary=artifact_payload.get("summary")
            or artifact_payload.get("transcript_summary"),
            tags=_string_list(artifact_payload.get("tags")),
            language=artifact_payload.get("language"),
            speaker=artifact_payload.get("speaker"),
            session_id=artifact_payload.get("session_id"),
            device_id=artifact_payload.get("device_id"),
            **base_fields,
        )
