"""Canonical identity extraction and duplicate linking for ingested artifacts."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, quote, unquote, urlparse, urlunparse

from .artifacts import (
    KnowledgeArtifact,
    PaperArtifact,
    RepositoryArtifact,
    TranscriptArtifact,
    TweetArtifact,
    VideoArtifact,
    WebClipperArtifact,
)
from .metadata_db import CanonicalEntityRecord, MetadataDB


class CanonicalIdentityConflictError(ValueError):
    """Raised when deterministic identity keys point at different canonicals."""


@dataclass(frozen=True)
class CanonicalIdentityKey:
    """One deterministic source-native key for an entity."""

    entity_type: str
    key_type: str
    key_value: str
    priority: int

    @property
    def identity_key(self) -> str:
        payload = {
            "entity_type": self.entity_type,
            "key_type": self.key_type,
            "key_value": self.key_value,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return f"{self.entity_type}:{self.key_type}:{digest}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity_key": self.identity_key,
            "entity_type": self.entity_type,
            "key_type": self.key_type,
            "key_value": self.key_value,
            "priority": self.priority,
        }


@dataclass(frozen=True)
class CanonicalArtifactIdentity:
    """Resolved canonical identity for one artifact."""

    canonical_id: str
    entity_type: str
    artifact_id: str
    artifact_type: str
    source_type: str
    display_name: str
    keys: tuple[CanonicalIdentityKey, ...]
    match_key: str
    match_reason: str
    primary_artifact_id: str
    wiki_slug: str | None = None
    metadata_entity_ids: Mapping[str, tuple[str, ...]] | None = None

    @property
    def key_records(self) -> tuple[dict[str, Any], ...]:
        return tuple(key.to_dict() for key in self.keys)


class CanonicalIdentityService:
    """Resolve artifacts and metadata entities to stable canonical IDs.

    The service intentionally uses exact deterministic keys only. Ambiguous
    matches are raised for operator review rather than merged silently.
    """

    def __init__(self, db: MetadataDB):
        self.db = db

    def canonicalize_artifact(
        self,
        artifact: KnowledgeArtifact,
        *,
        artifact_type: str | None = None,
    ) -> CanonicalArtifactIdentity | None:
        entity_type = _artifact_entity_type(artifact)
        if not entity_type:
            return None
        resolved_artifact_type = artifact_type or entity_type
        keys = _artifact_identity_keys(artifact, entity_type)
        if not keys:
            keys = (
                _identity_key(
                    entity_type,
                    "artifact",
                    f"{artifact.source_type}:{artifact.id}",
                    priority=1000,
                ),
            )

        identity = self._resolve_identity(
            entity_type=entity_type,
            artifact_id=artifact.id,
            artifact_type=resolved_artifact_type,
            source_type=artifact.source_type,
            display_name=_display_name_for_artifact(artifact),
            keys=keys,
        )
        metadata_entity_ids = self._canonicalize_metadata_entities(artifact)
        self._apply_identity_metadata(artifact, identity, metadata_entity_ids)
        return CanonicalArtifactIdentity(
            canonical_id=identity.canonical_id,
            entity_type=identity.entity_type,
            artifact_id=artifact.id,
            artifact_type=resolved_artifact_type,
            source_type=artifact.source_type,
            display_name=identity.display_name or _display_name_for_artifact(artifact),
            keys=keys,
            match_key=keys[0].identity_key,
            match_reason="deterministic_identity_key",
            primary_artifact_id=identity.primary_artifact_id,
            wiki_slug=identity.wiki_slug,
            metadata_entity_ids=metadata_entity_ids,
        )

    def _resolve_identity(
        self,
        *,
        entity_type: str,
        artifact_id: str,
        artifact_type: str,
        source_type: str,
        display_name: str,
        keys: tuple[CanonicalIdentityKey, ...],
        link_artifact: bool = True,
    ) -> CanonicalEntityRecord:
        existing = self.db.find_canonical_entities_by_identity_keys(
            entity_type,
            tuple(key.identity_key for key in keys),
        )
        canonical_ids = {record.canonical_id for record in existing}
        if len(canonical_ids) > 1:
            raise CanonicalIdentityConflictError(
                "artifact identity keys match multiple canonical entities: "
                + ", ".join(sorted(canonical_ids))
            )

        if existing:
            canonical_id = existing[0].canonical_id
        else:
            canonical_id = _canonical_id(entity_type, keys[0])

        return self.db.upsert_canonical_entity(
            canonical_id=canonical_id,
            entity_type=entity_type,
            primary_artifact_id=artifact_id,
            primary_artifact_type=artifact_type,
            primary_source_type=source_type,
            display_name=display_name,
            identity_keys=tuple(key.to_dict() for key in keys),
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            source_type=source_type,
            match_key=keys[0].identity_key,
            match_reason="deterministic_identity_key",
            metadata={
                "key_types": [key.key_type for key in keys],
                "source_type": source_type,
            },
            link_artifact=link_artifact,
        )

    def _canonicalize_metadata_entities(
        self,
        artifact: KnowledgeArtifact,
    ) -> dict[str, tuple[str, ...]]:
        people = _metadata_people(artifact)
        projects = _metadata_projects(artifact)
        result: dict[str, tuple[str, ...]] = {}
        for entity_type, entities in (("person", people), ("project", projects)):
            canonical_ids: list[str] = []
            for entity in entities:
                label = _clean_text(entity.get("name") or entity.get("label"))
                native_id = _clean_text(entity.get("id") or entity.get("native_id"))
                if not label and not native_id:
                    continue
                keys: list[CanonicalIdentityKey] = []
                if native_id:
                    keys.append(
                        _identity_key(
                            entity_type,
                            "native_id",
                            native_id,
                            priority=10,
                        )
                    )
                if label:
                    keys.append(
                        _identity_key(
                            entity_type,
                            "exact_name",
                            _normalize_name(label),
                            priority=20,
                        )
                    )
                if not keys:
                    continue
                record = self._resolve_identity(
                    entity_type=entity_type,
                    artifact_id=artifact.id,
                    artifact_type=_artifact_entity_type(artifact) or "artifact",
                    source_type=artifact.source_type,
                    display_name=label or native_id,
                    keys=tuple(keys),
                    link_artifact=False,
                )
                canonical_ids.append(record.canonical_id)
            if canonical_ids:
                result[f"{entity_type}s"] = tuple(dict.fromkeys(canonical_ids))
        return result

    def _apply_identity_metadata(
        self,
        artifact: KnowledgeArtifact,
        identity: CanonicalEntityRecord,
        metadata_entity_ids: Mapping[str, tuple[str, ...]],
    ) -> None:
        artifact.normalized_metadata = {
            **dict(artifact.normalized_metadata or {}),
            "canonical_id": identity.canonical_id,
            "canonical_entity_type": identity.entity_type,
            "canonical_primary_artifact_id": identity.primary_artifact_id,
        }
        for key, value in metadata_entity_ids.items():
            artifact.normalized_metadata[f"canonical_{key}"] = list(value)

    def set_wiki_slug(self, canonical_id: str, wiki_slug: str) -> CanonicalEntityRecord:
        return self.db.set_canonical_wiki_slug(canonical_id, wiki_slug)


def _artifact_entity_type(artifact: KnowledgeArtifact) -> str | None:
    if isinstance(artifact, PaperArtifact):
        return "paper"
    if isinstance(artifact, RepositoryArtifact):
        return "repository"
    if isinstance(artifact, VideoArtifact):
        return "video"
    if isinstance(artifact, TranscriptArtifact):
        return "transcript"
    if isinstance(artifact, WebClipperArtifact):
        return "imported_doc"
    if isinstance(artifact, TweetArtifact):
        return "tweet"
    return None


def _artifact_identity_keys(
    artifact: KnowledgeArtifact,
    entity_type: str,
) -> tuple[CanonicalIdentityKey, ...]:
    keys: list[CanonicalIdentityKey] = []
    if isinstance(artifact, PaperArtifact):
        arxiv_id = _normalize_arxiv_id(artifact.arxiv_id or artifact.id)
        doi = _normalize_doi(artifact.doi)
        if arxiv_id:
            keys.append(_identity_key(entity_type, "arxiv_id", arxiv_id, priority=10))
        if doi:
            keys.append(_identity_key(entity_type, "doi", doi, priority=20))
        pdf_url = _normalize_url(artifact.pdf_url)
        if pdf_url:
            keys.append(_identity_key(entity_type, "pdf_url", pdf_url, priority=40))
    elif isinstance(artifact, RepositoryArtifact):
        repo_name = _normalize_repo_name(artifact.repo_name or artifact.id)
        provider = _repo_provider(artifact.source_type)
        if repo_name:
            keys.append(
                _identity_key(
                    entity_type,
                    "native_repo",
                    f"{provider}:{repo_name}",
                    priority=10,
                )
            )
        url = _repo_url_from_metadata(artifact)
        if url:
            keys.append(_identity_key(entity_type, "url", url, priority=20))
    elif isinstance(artifact, VideoArtifact):
        video_id = _clean_text(artifact.video_id) or _youtube_id_from_url(
            artifact.source_url
        )
        if video_id:
            keys.append(
                _identity_key(
                    entity_type,
                    "youtube_video_id",
                    video_id,
                    priority=10,
                )
            )
        url = _normalize_url(artifact.source_url)
        if url:
            keys.append(_identity_key(entity_type, "url", url, priority=30))
    elif isinstance(artifact, TranscriptArtifact):
        video_id = _clean_text(artifact.video_id) or _youtube_id_from_url(
            artifact.source_url
        )
        if video_id:
            keys.append(
                _identity_key(
                    entity_type,
                    "youtube_video_transcript",
                    video_id,
                    priority=10,
                )
            )
        if artifact.session_id:
            keys.append(
                _identity_key(
                    entity_type,
                    "session_id",
                    f"{artifact.source_type}:{artifact.session_id}",
                    priority=20,
                )
            )
        if artifact.transcript_id:
            keys.append(
                _identity_key(
                    entity_type,
                    "transcript_id",
                    f"{artifact.source_type}:{artifact.transcript_id}",
                    priority=30,
                )
            )
    elif isinstance(artifact, WebClipperArtifact):
        source_url = _normalize_url(
            artifact.source_url
            or artifact.frontmatter.get("canonical_url")
            or artifact.frontmatter.get("url")
            or artifact.frontmatter.get("source_url")
        )
        if source_url:
            keys.append(_identity_key(entity_type, "url", source_url, priority=10))
        checksum = _clean_text(artifact.source_checksum)
        if not checksum and artifact.raw_payload:
            checksum = _clean_text(artifact.raw_payload.sha256)
        if checksum:
            keys.append(
                _identity_key(
                    entity_type,
                    "sha256",
                    checksum.lower(),
                    priority=20,
                )
            )
        source_path = _clean_text(artifact.source_relative_path or artifact.source_path)
        if source_path:
            keys.append(
                _identity_key(
                    entity_type,
                    "source_path",
                    source_path,
                    priority=50,
                )
            )
    elif isinstance(artifact, TweetArtifact):
        tweet_id = _clean_text(artifact.id)
        if tweet_id:
            keys.append(_identity_key(entity_type, "tweet_id", tweet_id, priority=10))

    return tuple(_dedupe_keys(keys))


def _identity_key(
    entity_type: str,
    key_type: str,
    key_value: str,
    *,
    priority: int,
) -> CanonicalIdentityKey:
    value = _clean_text(key_value)
    if not value:
        raise ValueError("canonical identity key_value cannot be empty")
    return CanonicalIdentityKey(
        entity_type=entity_type,
        key_type=key_type,
        key_value=value,
        priority=priority,
    )


def _dedupe_keys(
    keys: Iterable[CanonicalIdentityKey],
) -> tuple[CanonicalIdentityKey, ...]:
    deduped: dict[tuple[str, str, str], CanonicalIdentityKey] = {}
    for key in sorted(keys, key=lambda item: item.priority):
        deduped.setdefault((key.entity_type, key.key_type, key.key_value), key)
    return tuple(deduped.values())


def _canonical_id(entity_type: str, key: CanonicalIdentityKey) -> str:
    safe_key_type = re.sub(r"[^a-z0-9_]+", "_", key.key_type.lower()).strip("_")
    readable = _canonical_readable_component(key.key_value)
    if readable:
        return f"{entity_type}:{safe_key_type}:{readable}"
    return f"{entity_type}:{safe_key_type}:{key.identity_key.rsplit(':', 1)[-1][:16]}"


def _canonical_readable_component(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unquote(text)
    text = re.sub(r"^https?://", "", text)
    text = text.replace("/", ":")
    text = re.sub(r"[^a-z0-9:._-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-:._")
    if not text or len(text) > 96:
        return ""
    return text


def _display_name_for_artifact(artifact: KnowledgeArtifact) -> str:
    if isinstance(artifact, PaperArtifact):
        return artifact.title or artifact.arxiv_id or artifact.id
    if isinstance(artifact, RepositoryArtifact):
        return artifact.repo_name or artifact.id
    if isinstance(artifact, VideoArtifact):
        return artifact.title or artifact.video_id or artifact.id
    if isinstance(artifact, TranscriptArtifact):
        return artifact.title or artifact.transcript_id or artifact.id
    if isinstance(artifact, WebClipperArtifact):
        return artifact.title or artifact.source_url or artifact.id
    return artifact.id


def _metadata_people(artifact: KnowledgeArtifact) -> tuple[dict[str, str], ...]:
    people: list[dict[str, str]] = []
    for mapping in _artifact_metadata_mappings(artifact):
        for key in ("people", "persons", "person"):
            people.extend(_named_entities(mapping.get(key)))
    if isinstance(artifact, PaperArtifact):
        people.extend(
            {"name": str(author)}
            for author in artifact.authors
            if str(author).strip()
        )
    return tuple(people)


def _metadata_projects(artifact: KnowledgeArtifact) -> tuple[dict[str, str], ...]:
    projects: list[dict[str, str]] = []
    for mapping in _artifact_metadata_mappings(artifact):
        for key in ("projects", "project"):
            projects.extend(_named_entities(mapping.get(key)))
    return tuple(projects)


def _artifact_metadata_mappings(
    artifact: KnowledgeArtifact,
) -> tuple[Mapping[str, Any], ...]:
    mappings: list[Mapping[str, Any]] = []
    for value in (artifact.normalized_metadata, artifact.custom_metadata):
        if isinstance(value, Mapping):
            mappings.append(value)
    if isinstance(artifact, WebClipperArtifact) and isinstance(
        artifact.frontmatter,
        Mapping,
    ):
        mappings.append(artifact.frontmatter)
    return tuple(mappings)


def _named_entities(value: Any) -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]

    entities: list[dict[str, str]] = []
    for item in values:
        if isinstance(item, Mapping):
            entity_id = _clean_text(
                item.get("id") or item.get("native_id") or item.get("slug")
            )
            name = _clean_text(
                item.get("name")
                or item.get("display_name")
                or item.get("title")
                or item.get("label")
            )
            if entity_id or name:
                entities.append({"id": entity_id or "", "name": name or ""})
        else:
            name = _clean_text(item)
            if name:
                entities.append({"name": name})
    return entities


def _normalize_arxiv_id(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    text = text.replace("https://arxiv.org/abs/", "")
    text = text.replace("http://arxiv.org/abs/", "")
    text = text.replace("https://arxiv.org/pdf/", "")
    text = text.replace("http://arxiv.org/pdf/", "")
    text = text.removeprefix("arxiv:")
    text = text.removesuffix(".pdf")
    text = re.sub(r"v\d+$", "", text, flags=re.IGNORECASE)
    return text.strip().lower() or None


def _normalize_doi(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    text = text.lower()
    text = text.removeprefix("doi:")
    text = text.replace("https://doi.org/", "")
    text = text.replace("http://doi.org/", "")
    return text.strip() or None


def _normalize_repo_name(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    text = text.removesuffix(".git")
    text = text.strip("/")
    parts = [part for part in text.split("/") if part]
    if len(parts) >= 2:
        return "/".join(parts[-2:]).lower()
    return text.lower()


def _repo_provider(source_type: str) -> str:
    source = str(source_type or "").strip().lower()
    if "huggingface" in source or source in {"hf", "hugging_face"}:
        return "huggingface"
    if "github" in source or source in {"gh"}:
        return "github"
    return source or "repository"


def _repo_url_from_metadata(artifact: RepositoryArtifact) -> str | None:
    for mapping in _artifact_metadata_mappings(artifact):
        for key in ("html_url", "source_url", "url", "uri"):
            url = _normalize_url(mapping.get(key))
            if url:
                return url
    return None


def _youtube_id_from_url(value: Any) -> str | None:
    url = _clean_text(value)
    if not url:
        return None
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    if host == "youtu.be":
        return parsed.path.strip("/") or None
    if host.endswith("youtube.com"):
        query_id = parse_qs(parsed.query).get("v", [None])[0]
        if query_id:
            return query_id
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2 and parts[0] in {"embed", "shorts", "live"}:
            return parts[1]
    return None


def _normalize_url(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text
    scheme = parsed.scheme.lower()
    host = parsed.netloc.lower()
    path = quote(unquote(parsed.path), safe="/:@")
    query = parsed.query
    if host.endswith("youtube.com"):
        video_id = _youtube_id_from_url(text)
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
    return urlunparse((scheme, host, path.rstrip("/") or "/", "", query, ""))


def _normalize_name(value: str) -> str:
    return " ".join(str(value or "").casefold().split())


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
