"""
Base KnowledgeArtifact class for Thoth.
All ingestible entities inherit from this.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


def _clean_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class ArtifactSourceIdentity:
    """Stable identity for the upstream source that produced an artifact."""

    source_name: str
    source_type: str
    native_id: str
    uri: str | None = None
    account: str | None = None
    collector: str | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "source_name": self.source_name,
                "source_type": self.source_type,
                "native_id": self.native_id,
                "uri": self.uri,
                "account": self.account,
                "collector": self.collector,
            }.items()
            if value is not None
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ArtifactSourceIdentity":
        source_name = _clean_optional_string(
            value.get("source_name") or value.get("source") or value.get("name")
        )
        source_type = _clean_optional_string(value.get("source_type") or source_name)
        native_id = _clean_optional_string(
            value.get("native_id") or value.get("artifact_id") or value.get("id")
        )
        if not source_name:
            raise ValueError("Artifact source identity requires source_name")
        if not source_type:
            raise ValueError("Artifact source identity requires source_type")
        if not native_id:
            raise ValueError("Artifact source identity requires native_id")
        return cls(
            source_name=source_name,
            source_type=source_type,
            native_id=native_id,
            uri=_clean_optional_string(value.get("uri") or value.get("url")),
            account=_clean_optional_string(value.get("account")),
            collector=_clean_optional_string(value.get("collector")),
        )


@dataclass(frozen=True)
class RawPayloadRef:
    """Reference to the immutable raw capture backing an artifact."""

    path: str | None = None
    content_key: str | None = "raw_content"
    media_type: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    immutable: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "path": self.path,
                "content_key": self.content_key,
                "media_type": self.media_type,
                "sha256": self.sha256,
                "size_bytes": self.size_bytes,
                "immutable": self.immutable,
            }.items()
            if value is not None
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RawPayloadRef":
        size_value = value.get("size_bytes")
        return cls(
            path=_clean_optional_string(
                value.get("path") or value.get("location") or value.get("source_path")
            ),
            content_key=_clean_optional_string(value.get("content_key")) or "raw_content",
            media_type=_clean_optional_string(value.get("media_type")),
            sha256=_clean_optional_string(value.get("sha256") or value.get("hash")),
            size_bytes=int(size_value) if size_value is not None else None,
            immutable=bool(value.get("immutable", True)),
        )


@dataclass(frozen=True)
class DerivedOutput:
    """Generated output derived from an artifact."""

    output_type: str
    path: str
    media_type: str | None = None
    created_at: str | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "output_type": self.output_type,
                "path": self.path,
                "media_type": self.media_type,
                "created_at": self.created_at,
            }.items()
            if value is not None
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DerivedOutput":
        output_type = _clean_optional_string(
            value.get("output_type") or value.get("kind") or value.get("type")
        )
        path = _clean_optional_string(value.get("path") or value.get("location"))
        if not output_type:
            raise ValueError("Derived output requires output_type")
        if not path:
            raise ValueError("Derived output requires path")
        return cls(
            output_type=output_type,
            path=path,
            media_type=_clean_optional_string(value.get("media_type")),
            created_at=_clean_optional_string(value.get("created_at")),
        )


@dataclass(frozen=True)
class ArtifactRelationship:
    """Typed relationship to another artifact or external entity."""

    relationship_type: str
    target_id: str
    target_type: str | None = None
    source_evidence: str | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "relationship_type": self.relationship_type,
                "target_id": self.target_id,
                "target_type": self.target_type,
                "source_evidence": self.source_evidence,
                "metadata": self.metadata,
            }.items()
            if value not in (None, {})
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ArtifactRelationship":
        relationship_type = _clean_optional_string(
            value.get("relationship_type") or value.get("type")
        )
        target_id = _clean_optional_string(
            value.get("target_id") or value.get("target")
        )
        if not relationship_type:
            raise ValueError("Artifact relationship requires relationship_type")
        if not target_id:
            raise ValueError("Artifact relationship requires target_id")
        metadata = value.get("metadata")
        return cls(
            relationship_type=relationship_type,
            target_id=target_id,
            target_type=_clean_optional_string(value.get("target_type")),
            source_evidence=_clean_optional_string(value.get("source_evidence")),
            metadata=dict(metadata) if isinstance(metadata, Mapping) else {},
        )


@dataclass(frozen=True)
class ArtifactProvenance:
    """How an artifact entered Thoth and which raw evidence backs it."""

    source_identity: ArtifactSourceIdentity
    captured_at: str | None = None
    ingested_at: str | None = None
    collector: str | None = None
    queue_id: str | None = None
    raw_payload: RawPayloadRef | None = None
    evidence_paths: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "source_identity": self.source_identity.to_dict(),
                "captured_at": self.captured_at,
                "ingested_at": self.ingested_at,
                "collector": self.collector,
                "queue_id": self.queue_id,
                "raw_payload": self.raw_payload.to_dict()
                if self.raw_payload
                else None,
                "evidence_paths": list(self.evidence_paths),
            }.items()
            if value not in (None, [], {})
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ArtifactProvenance":
        source_payload = value.get("source_identity") or {}
        if not isinstance(source_payload, Mapping):
            raise ValueError("Artifact provenance source_identity must be an object")
        raw_payload = value.get("raw_payload")
        evidence_paths = value.get("evidence_paths") or ()
        return cls(
            source_identity=ArtifactSourceIdentity.from_mapping(source_payload),
            captured_at=_clean_optional_string(value.get("captured_at")),
            ingested_at=_clean_optional_string(value.get("ingested_at")),
            collector=_clean_optional_string(value.get("collector")),
            queue_id=_clean_optional_string(value.get("queue_id")),
            raw_payload=RawPayloadRef.from_mapping(raw_payload)
            if isinstance(raw_payload, Mapping)
            else None,
            evidence_paths=tuple(str(path) for path in evidence_paths if str(path)),
        )


def _coerce_derived_outputs(
    values: Iterable[DerivedOutput | Mapping[str, Any]],
) -> Tuple[DerivedOutput, ...]:
    return tuple(
        value
        if isinstance(value, DerivedOutput)
        else DerivedOutput.from_mapping(value)
        for value in values
    )


def _coerce_relationships(
    values: Iterable[ArtifactRelationship | Mapping[str, Any]],
) -> Tuple[ArtifactRelationship, ...]:
    return tuple(
        value
        if isinstance(value, ArtifactRelationship)
        else ArtifactRelationship.from_mapping(value)
        for value in values
    )


def _mapping_or_empty(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence_or_empty(value: Any) -> tuple[Any, ...]:
    return tuple(value) if isinstance(value, (list, tuple)) else ()


@dataclass
class KnowledgeArtifact:
    """Base class for all ingestible knowledge entities."""

    id: str = ""  # Unique identifier
    source_type: str = "generic"  # 'twitter', 'arxiv', 'github', 'hermes', 'financial', etc.
    raw_content: str = ""  # Original content (text, JSON, HTML)
    created_at: Optional[str] = None  # When artifact was created (source time)
    ingested_at: Optional[str] = None  # When artifact entered Thoth
    processing_status: str = "pending"  # 'pending', 'processing', 'processed', 'failed'

    # Capability flags - what can this artifact provide?
    capabilities: Tuple[str, ...] = field(default_factory=tuple)
    # Examples: ('media', 'urls', 'transcription', 'llm_summary', 'embedding')

    # Metadata
    tags: List[str] = field(default_factory=list)
    importance_score: Optional[float] = None
    custom_metadata: Dict[str, Any] = field(default_factory=dict)

    # Output tracking
    output_paths: Dict[str, str] = field(default_factory=dict)  # {'markdown': 'path/to/file.md'}

    # Canonical artifact contract
    source_identity: ArtifactSourceIdentity | Mapping[str, Any] | None = None
    raw_payload: RawPayloadRef | Mapping[str, Any] | None = None
    normalized_metadata: Dict[str, Any] = field(default_factory=dict)
    provenance: ArtifactProvenance | Mapping[str, Any] | None = None
    derived_outputs: Tuple[DerivedOutput | Mapping[str, Any], ...] = field(
        default_factory=tuple
    )
    relationships: Tuple[ArtifactRelationship | Mapping[str, Any], ...] = field(
        default_factory=tuple
    )

    def __post_init__(self):
        """Ensure default values are set for lists and dicts."""
        if self.capabilities is None:
            self.capabilities = ()
        else:
            self.capabilities = tuple(str(item) for item in self.capabilities)
        if self.tags is None:
            self.tags = []
        if self.custom_metadata is None:
            self.custom_metadata = {}
        if self.output_paths is None:
            self.output_paths = {}
        if self.normalized_metadata is None:
            self.normalized_metadata = {}
        if self.derived_outputs is None:
            self.derived_outputs = ()
        if self.relationships is None:
            self.relationships = ()
        if self.source_identity is None:
            self.source_identity = ArtifactSourceIdentity(
                source_name=str(self.source_type or "generic"),
                source_type=str(self.source_type or "generic"),
                native_id=str(self.id),
            )
        elif isinstance(self.source_identity, Mapping):
            self.source_identity = ArtifactSourceIdentity.from_mapping(
                self.source_identity
            )

        if self.raw_payload is None:
            self.raw_payload = self._default_raw_payload_ref()
        elif isinstance(self.raw_payload, Mapping):
            self.raw_payload = RawPayloadRef.from_mapping(self.raw_payload)

        self.derived_outputs = self._coerce_derived_outputs_from_state()
        self.relationships = _coerce_relationships(self.relationships)
        if not self.normalized_metadata:
            self.normalized_metadata = self._default_normalized_metadata()

        if self.provenance is None:
            self.provenance = ArtifactProvenance(
                source_identity=self.source_identity,
                captured_at=self.created_at,
                ingested_at=self.ingested_at,
                collector=self.source_identity.collector,
                raw_payload=self.raw_payload,
                evidence_paths=tuple(
                    output.path for output in self.derived_outputs
                ),
            )
        elif isinstance(self.provenance, Mapping):
            self.provenance = ArtifactProvenance.from_mapping(self.provenance)

    def _default_raw_payload_ref(self) -> RawPayloadRef:
        source_path = (
            self.custom_metadata.get("raw_payload_path")
            or self.custom_metadata.get("source_path")
            or getattr(self, "source_path", None)
        )
        sha256 = (
            self.custom_metadata.get("raw_payload_sha256")
            or getattr(self, "source_checksum", None)
        )
        size_bytes = (
            self.custom_metadata.get("raw_payload_size_bytes")
            or getattr(self, "source_size_bytes", None)
        )
        return RawPayloadRef(
            path=_clean_optional_string(source_path),
            content_key="raw_content" if self.raw_content else None,
            sha256=_clean_optional_string(sha256),
            size_bytes=int(size_bytes) if size_bytes is not None else None,
            immutable=True,
        )

    def _coerce_derived_outputs_from_state(self) -> Tuple[DerivedOutput, ...]:
        explicit_outputs = _coerce_derived_outputs(self.derived_outputs)
        path_outputs = tuple(
            DerivedOutput(output_type=str(kind), path=str(path))
            for kind, path in self.output_paths.items()
            if str(kind).strip() and str(path).strip()
        )
        merged: dict[tuple[str, str], DerivedOutput] = {}
        for output in (*explicit_outputs, *path_outputs):
            merged[(output.output_type, output.path)] = output
        return tuple(merged.values())

    def _default_normalized_metadata(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "artifact_id": self.id,
            "source_type": self.source_type,
            "processing_status": self.processing_status,
        }
        if self.tags:
            data["tags"] = list(self.tags)
        if self.importance_score is not None:
            data["importance_score"] = self.importance_score
        return data

    def apply_queue_context(
        self,
        *,
        queue_id: str,
        queue_source: str,
        queue_created_at: str | None = None,
        capabilities: Iterable[str] | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> "KnowledgeArtifact":
        """Attach queue provenance without changing processor-facing behavior."""
        payload = payload or {}
        uri = (
            payload.get("uri")
            or payload.get("url")
            or payload.get("source_url")
            or payload.get("pdf_url")
            or payload.get("html_url")
        )
        source_identity = ArtifactSourceIdentity(
            source_name=str(queue_source or self.source_type or "generic"),
            source_type=str(self.source_type or queue_source or "generic"),
            native_id=str(self.id),
            uri=_clean_optional_string(uri),
            collector=str(queue_source) if queue_source else None,
        )
        self.source_identity = source_identity
        if capabilities is not None:
            self.capabilities = tuple(str(item) for item in capabilities)
        self.raw_payload = self._raw_payload_from_payload(payload)
        self.derived_outputs = self._coerce_derived_outputs_from_state()
        self.normalized_metadata = {
            **self._default_normalized_metadata(),
            **dict(self.normalized_metadata or {}),
            "queue_id": queue_id,
            "queue_source": queue_source,
        }
        self.provenance = ArtifactProvenance(
            source_identity=source_identity,
            captured_at=self.created_at or queue_created_at,
            ingested_at=self.ingested_at or queue_created_at,
            collector=str(queue_source) if queue_source else None,
            queue_id=queue_id,
            raw_payload=self.raw_payload,
            evidence_paths=tuple(
                output.path for output in self.derived_outputs
            ),
        )
        return self

    def _raw_payload_from_payload(self, payload: Mapping[str, Any]) -> RawPayloadRef:
        raw_payload = payload.get("raw_payload")
        if isinstance(raw_payload, Mapping):
            return RawPayloadRef.from_mapping(raw_payload)
        raw_path = (
            payload.get("raw_payload_path")
            or payload.get("source_path")
            or payload.get("source_file")
            or getattr(self, "source_path", None)
        )
        sha256 = (
            payload.get("raw_payload_sha256")
            or payload.get("source_checksum")
            or getattr(self, "source_checksum", None)
        )
        size_bytes = (
            payload.get("raw_payload_size_bytes")
            or payload.get("source_size_bytes")
            or getattr(self, "source_size_bytes", None)
        )
        return RawPayloadRef(
            path=_clean_optional_string(raw_path),
            content_key="raw_content" if self.raw_content else None,
            sha256=_clean_optional_string(sha256),
            size_bytes=int(size_bytes) if size_bytes is not None else None,
            immutable=True,
        )

    def canonical_record(self) -> Dict[str, Any]:
        """Return the stable agent-facing artifact contract."""
        return {
            "artifact_id": self.id,
            "artifact_class": self.__class__.__name__,
            "source_identity": self.source_identity.to_dict(),
            "raw_payload": self.raw_payload.to_dict() if self.raw_payload else {},
            "normalized_metadata": dict(self.normalized_metadata),
            "timestamps": {
                "created_at": self.created_at,
                "ingested_at": self.ingested_at,
            },
            "provenance": self.provenance.to_dict() if self.provenance else {},
            "capabilities": list(self.capabilities),
            "derived_outputs": [
                output.to_dict() for output in self.derived_outputs
            ],
            "relationships": [
                relationship.to_dict() for relationship in self.relationships
            ],
        }

    @classmethod
    def base_fields_from_payload(cls, payload: Mapping[str, Any]) -> Dict[str, Any]:
        """Extract shared KnowledgeArtifact fields from a queue payload."""
        tags = payload.get("tags")
        if isinstance(tags, str):
            tags = [tags]
        elif not isinstance(tags, list):
            tags = []

        data: Dict[str, Any] = {
            "tags": [str(tag) for tag in tags if str(tag).strip()],
            "custom_metadata": _mapping_or_empty(payload.get("custom_metadata")),
            "output_paths": {
                str(key): str(value)
                for key, value in _mapping_or_empty(payload.get("output_paths")).items()
                if str(key).strip() and str(value).strip()
            },
            "normalized_metadata": _mapping_or_empty(
                payload.get("normalized_metadata")
            ),
            "derived_outputs": _sequence_or_empty(payload.get("derived_outputs")),
            "relationships": _sequence_or_empty(payload.get("relationships")),
        }

        if "importance_score" in payload:
            data["importance_score"] = payload.get("importance_score")
        if isinstance(payload.get("source_identity"), Mapping):
            data["source_identity"] = payload["source_identity"]
        if isinstance(payload.get("raw_payload"), Mapping):
            data["raw_payload"] = payload["raw_payload"]
        if isinstance(payload.get("provenance"), Mapping):
            data["provenance"] = payload["provenance"]
        if isinstance(payload.get("capabilities"), (list, tuple)):
            data["capabilities"] = tuple(
                str(item) for item in payload["capabilities"] if str(item).strip()
            )

        return data

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "source_type": self.source_type,
            "raw_content": self.raw_content,
            "created_at": self.created_at,
            "ingested_at": self.ingested_at,
            "processing_status": self.processing_status,
            "capabilities": list(self.capabilities),
            "tags": self.tags,
            "importance_score": self.importance_score,
            "custom_metadata": self.custom_metadata,
            "output_paths": self.output_paths,
            "source_identity": self.source_identity.to_dict(),
            "raw_payload": self.raw_payload.to_dict() if self.raw_payload else None,
            "normalized_metadata": self.normalized_metadata,
            "provenance": self.provenance.to_dict() if self.provenance else None,
            "derived_outputs": [
                output.to_dict() for output in self.derived_outputs
            ],
            "relationships": [
                relationship.to_dict() for relationship in self.relationships
            ],
        }
