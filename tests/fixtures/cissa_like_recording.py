"""Synthetic Cissa-like completed-recording fixture.

This fixture models a completed audio capture that has already produced a
transcript: one Blob (audio), one transcript, stable origin, and provenance.
All content is synthetic and contains no personal data.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


CISSA_AUDIO_BLOB = b"RIFF\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00" + b"fake-cissa-audio-pcm"

CISSA_TRANSCRIPT_TEXT = """[00:00:00] Speaker 0: I want the schema open because adoption is still a win.
[00:00:05] Speaker 0: If we gate everything behind review, no one experiments.
[00:00:10] Speaker 0: But we also need to keep the pipeline auditable.
[00:00:15] Speaker 0: Containerizing the environment should help with both goals.
[00:00:20] Speaker 0: Let's schedule a follow-up on monitoring and alerts."""

CISSA_SESSION_ID = "cissa-session-open-schema-2026-08-29"
CISSA_DEVICE_ID = "cissa-device-synthetic-01"
CISSA_SOURCE_NAME = "cissa"
CISSA_SOURCE_TYPE = "voice_recorder"
CISSA_TITLE = "Open schema adoption and pipeline auditability"
CISSA_STARTED_AT = "2026-08-29T21:41:48Z"
CISSA_ENDED_AT = "2026-08-29T21:42:18Z"
CISSA_LANGUAGE = "en"


@dataclass(frozen=True)
class CissaLikeRecording:
    """Synthetic completed recording with audio, transcript, and provenance."""

    audio_blob: bytes
    transcript_text: str
    audio_sha256: str
    transcript_sha256: str
    session_id: str
    device_id: str
    source_name: str
    source_type: str
    title: str
    started_at: str
    ended_at: str
    language: str

    @property
    def audio_path(self) -> str:
        """Vault-relative path for the immutable audio Blob."""
        return f"raw/{self.source_name}/{self.session_id}.wav"

    @property
    def transcript_path(self) -> str:
        """Vault-relative path for the source transcript Markdown."""
        return f"transcripts/{self.source_name}/{self.session_id}.md"

    @property
    def artifact_id(self) -> str:
        return f"{self.source_name}_transcript_{self.session_id}"


def make_cissa_like_recording() -> CissaLikeRecording:
    return CissaLikeRecording(
        audio_blob=CISSA_AUDIO_BLOB,
        transcript_text=CISSA_TRANSCRIPT_TEXT,
        audio_sha256=hashlib.sha256(CISSA_AUDIO_BLOB).hexdigest(),
        transcript_sha256=hashlib.sha256(CISSA_TRANSCRIPT_TEXT.encode("utf-8")).hexdigest(),
        session_id=CISSA_SESSION_ID,
        device_id=CISSA_DEVICE_ID,
        source_name=CISSA_SOURCE_NAME,
        source_type=CISSA_SOURCE_TYPE,
        title=CISSA_TITLE,
        started_at=CISSA_STARTED_AT,
        ended_at=CISSA_ENDED_AT,
        language=CISSA_LANGUAGE,
    )
