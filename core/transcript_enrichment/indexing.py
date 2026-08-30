"""Index transcript derivatives for full-text search."""

from __future__ import annotations

from pathlib import Path

from core.archivist_retrieval.models import ArchivistCorpusDocument
from core.artifacts import TranscriptArtifact
from core.metadata_db import MetadataDB
from core.time_utils import utc_now_iso

from .models import TranscriptDerivative
from .rendering import classification_tags_from_content, extract_normalized_transcript_text
from .storage import resolve_derivative_path


def index_derivatives(
    artifact: TranscriptArtifact,
    *,
    vault_root: Path,
    db: MetadataDB,
    derivatives: tuple[TranscriptDerivative, ...],
) -> None:
    """Upsert a stable search row for the transcript derivative.

    The indexed ``path`` and ``scope_relative_path`` both describe the
    normalized transcript projection file under the vault. The full-text
    content is the normalized transcript body parsed from that Markdown file.
    Original audio and transcript source provenance remain in the stable
    source IDs and keys.
    """
    transcript_derivative = next(
        (d for d in derivatives if d.output_type == "transcript"), None
    )
    if transcript_derivative is None:
        return

    classification_derivative = next(
        (d for d in derivatives if d.output_type == "classification"), None
    )

    content_text = extract_normalized_transcript_text(transcript_derivative.content)

    title = artifact.title or artifact.transcript_id or artifact.id or "transcript"
    tags: tuple[str, ...] = ()
    if classification_derivative is not None:
        tags = tuple(classification_tags_from_content(classification_derivative.content))

    source_identity = artifact.source_identity
    source_name = source_identity.source_name if source_identity else artifact.source_type
    native_id = source_identity.native_id if source_identity else artifact.id

    absolute_path = resolve_derivative_path(transcript_derivative.path, vault_root)

    document = ArchivistCorpusDocument(
        candidate_key=f"transcript:{artifact.id}",
        path=absolute_path,
        scope="vault",
        scope_relative_path=transcript_derivative.path,
        source_type=artifact.source_type or "transcript",
        file_type="transcript",
        title=title,
        tags=tags,
        content_text=content_text,
        source_hash=transcript_derivative.source_hash,
        size_bytes=len(transcript_derivative.content.encode("utf-8")),
        updated_at=utc_now_iso(),
        source_id=source_name,
        source_key=native_id,
        artifact_id=artifact.id,
        event_id=None,
        privacy_class="personal"
        if "personal" in (artifact.source_type or "").lower()
        else "unspecified",
        retention_class="unspecified",
    )
    db.upsert_archivist_corpus_document(document)
