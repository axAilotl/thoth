"""Hybrid lexical search across wiki pages, queued artifacts, and capture events."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any

from .capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    CaptureSource,
    RawArtifactRef,
    SecurityFinding,
)
from .metadata_db import IngestionQueueEntry, MetadataDB
from .prompt_security import (
    THOTH_REDACTION_METADATA_KEY,
    THOTH_SECURITY_AUDIT_KEY,
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_FINDING_COUNT_KEY,
    THOTH_SECURITY_PATTERN_IDS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    prompt_security_requires_review,
)
from .wiki_contract import WikiContract, is_legacy_tweet_slug
from .wiki_io import read_document, truncate_summary

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_BLOCKING_SECURITY_STATUSES = {
    "blocked",
    "failed",
    "needs_review",
    "quarantined",
    "reviewed",
    "rejected",
}
_CLOSED_FINDING_STATUSES = {"closed", "resolved", "suppressed", "accepted"}
_OPEN_FINDING_STATUSES = {"new", "open", "active", "triage"}
_VALID_RESULT_TYPES = {"wiki_page", "artifact", "capture_event"}
_RESULT_TYPE_ALIASES = {
    "wiki": "wiki_page",
    "page": "wiki_page",
    "pages": "wiki_page",
    "wiki_pages": "wiki_page",
    "artifacts": "artifact",
    "event": "capture_event",
    "events": "capture_event",
    "capture_events": "capture_event",
}


@dataclass(frozen=True)
class HybridSearchFilters:
    """Typed filters accepted by the hybrid retrieval surface."""

    result_types: tuple[str, ...] = ()
    source_types: tuple[str, ...] = ()
    source_ids: tuple[str, ...] = ()
    source_paths: tuple[str, ...] = ()
    artifact_types: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    wiki_kinds: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    exclude_tags: tuple[str, ...] = ()
    security_statuses: tuple[str, ...] = ()
    min_trust_score: float | None = None
    time_after: str | None = None
    time_before: str | None = None
    created_after: str | None = None
    created_before: str | None = None
    updated_after: str | None = None
    updated_before: str | None = None
    include_quarantined: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "result_types",
            _normalize_result_types(self.result_types),
        )
        for attr in (
            "source_types",
            "source_ids",
            "source_paths",
            "artifact_types",
            "event_types",
            "wiki_kinds",
            "security_statuses",
        ):
            object.__setattr__(self, attr, _normalize_strings(getattr(self, attr)))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))
        object.__setattr__(self, "exclude_tags", _normalize_tags(self.exclude_tags))
        if self.min_trust_score is not None:
            object.__setattr__(self, "min_trust_score", float(self.min_trust_score))
        for attr in (
            "time_after",
            "time_before",
            "created_after",
            "created_before",
            "updated_after",
            "updated_before",
        ):
            value = getattr(self, attr)
            if value is not None:
                object.__setattr__(self, attr, str(value).strip() or None)

    def to_dict(self) -> dict[str, Any]:
        """Return a compact JSON-friendly filter payload."""

        payload: dict[str, Any] = {
            "result_types": list(self.result_types),
            "source_types": list(self.source_types),
            "source_ids": list(self.source_ids),
            "source_paths": list(self.source_paths),
            "artifact_types": list(self.artifact_types),
            "event_types": list(self.event_types),
            "wiki_kinds": list(self.wiki_kinds),
            "tags": list(self.tags),
            "exclude_tags": list(self.exclude_tags),
            "security_statuses": list(self.security_statuses),
            "min_trust_score": self.min_trust_score,
            "time_after": self.time_after,
            "time_before": self.time_before,
            "created_after": self.created_after,
            "created_before": self.created_before,
            "updated_after": self.updated_after,
            "updated_before": self.updated_before,
            "include_quarantined": self.include_quarantined,
        }
        return {
            key: value
            for key, value in payload.items()
            if value not in (None, [], (), "")
        }


@dataclass(frozen=True)
class HybridSearchHit:
    """One result from a hybrid search."""

    result_id: str
    result_type: str
    title: str
    summary: str
    score: float
    matched_fields: tuple[str, ...]
    source_type: str | None
    source_id: str | None
    timestamp: str | None
    created_at: str | None
    updated_at: str | None
    tags: tuple[str, ...]
    provenance: dict[str, Any]
    security: dict[str, Any]
    trust: dict[str, Any]
    search_modes: tuple[str, ...] = ("lexical",)
    slug: str | None = None
    page_path: str | None = None
    artifact_id: str | None = None
    event_id: str | None = None


@dataclass(frozen=True)
class HybridSearchResult:
    """Hybrid search response with explicit backend capabilities."""

    query: str
    hits: tuple[HybridSearchHit, ...]
    queried_at: str
    filters: dict[str, Any] = field(default_factory=dict)
    capabilities: dict[str, Any] = field(default_factory=dict)


class HybridSearchService:
    """Lexical-first search over local Thoth knowledge surfaces."""

    def __init__(
        self,
        *,
        contract: WikiContract | None = None,
        db: MetadataDB | None = None,
        event_store: CaptureEventStore | None = None,
    ) -> None:
        self.contract = contract
        self.db = db
        self.event_store = event_store

    def search(
        self,
        query: str,
        *,
        limit: int = 10,
        filters: HybridSearchFilters | None = None,
        use_embedding: bool = False,
    ) -> HybridSearchResult:
        """Search all configured surfaces, excluding quarantined content by default."""

        if limit <= 0:
            raise ValueError("Hybrid search limit must be positive")
        tokens = _query_tokens(query)
        search_filters = filters or HybridSearchFilters()
        _validate_filter_times(search_filters)

        result_types = search_filters.result_types or tuple(sorted(_VALID_RESULT_TYPES))
        hits: list[HybridSearchHit] = []
        if "wiki_page" in result_types and self.contract is not None:
            hits.extend(self._search_wiki_pages(query, tokens, search_filters))
        if "artifact" in result_types and self.db is not None:
            hits.extend(self._search_artifacts(query, tokens, search_filters, limit=limit))
        if "capture_event" in result_types and self.event_store is not None:
            hits.extend(self._search_capture_events(query, tokens, search_filters))

        hits.sort(
            key=lambda hit: (
                hit.score,
                _sort_datetime(hit.timestamp),
                hit.title.lower(),
                hit.result_id,
            ),
            reverse=True,
        )
        return HybridSearchResult(
            query=query,
            hits=tuple(hits[:limit]),
            queried_at=_now_iso(),
            filters=search_filters.to_dict(),
            capabilities={
                "lexical": {"available": True, "searched": True},
                "embedding": {
                    "available": False,
                    "searched": False,
                    "requested": bool(use_embedding),
                    "reason": "embedding search is unavailable for this local hybrid index",
                },
                "wiki_pages": {"available": self.contract is not None},
                "artifacts": {"available": self.db is not None},
                "capture_events": {"available": self.event_store is not None},
            },
        )

    def _search_wiki_pages(
        self,
        query: str,
        tokens: tuple[str, ...],
        filters: HybridSearchFilters,
    ) -> list[HybridSearchHit]:
        if self.contract is None:
            return []

        hits: list[HybridSearchHit] = []
        for page_path in sorted(self.contract.pages_dir.glob("*.md")):
            document = read_document(page_path)
            frontmatter = document.frontmatter if isinstance(document.frontmatter, dict) else {}
            slug = str(_frontmatter_value(frontmatter, "thoth_slug", "slug") or page_path.stem)
            if is_legacy_tweet_slug(slug):
                continue

            title = str(frontmatter.get("title") or page_path.stem)
            summary = str(
                _frontmatter_value(frontmatter, "description", "thoth_summary", "summary")
                or ""
            )
            kind = str(_frontmatter_value(frontmatter, "thoth_kind", "kind") or "topic")
            source_type = _optional_string(
                _frontmatter_value(frontmatter, "thoth_source_type", "source_type")
            )
            artifact_id = _optional_string(
                _frontmatter_value(frontmatter, "thoth_artifact_id", "artifact_id")
            )
            source_paths = _frontmatter_sequence(frontmatter, "thoth_source_paths", "source_paths")
            event_ids = _frontmatter_sequence(frontmatter, "thoth_event_ids", "event_ids")
            source_ids = _frontmatter_sequence(frontmatter, "thoth_source_ids", "source_ids")
            session_ids = _frontmatter_sequence(frontmatter, "thoth_session_ids", "session_ids")
            aliases = _frontmatter_sequence(frontmatter, "thoth_aliases", "aliases")
            related_slugs = _frontmatter_sequence(
                frontmatter,
                "thoth_related_slugs",
                "related_slugs",
            )
            influence_sources = _frontmatter_mapping_sequence(
                frontmatter,
                "thoth_influence_sources",
                "influence_sources",
            )
            tags = _normalize_tags(_frontmatter_value(frontmatter, "thoth_tags", "tags", "tag"))
            created_at = _optional_string(
                _frontmatter_value(frontmatter, "thoth_created_at", "created_at")
            )
            updated_at = _optional_string(
                _frontmatter_value(frontmatter, "thoth_updated_at", "updated_at", "timestamp")
            )
            timestamp = updated_at or created_at
            security = _wiki_security(frontmatter)
            trust = _wiki_trust(security, influence_sources)
            source_identifiers = tuple(
                value
                for value in (artifact_id, *event_ids, *source_ids, *session_ids, slug)
                if value
            )

            if not _passes_common_filters(
                filters,
                result_type="wiki_page",
                source_type=source_type,
                source_ids=source_identifiers,
                source_paths=source_paths,
                tags=tags,
                security=security,
                trust_score=_trust_score(trust),
                timestamp=timestamp,
                created_at=created_at,
                updated_at=updated_at,
            ):
                continue
            if filters.wiki_kinds and kind.lower() not in filters.wiki_kinds:
                continue

            haystacks = {
                "title": title,
                "summary": summary,
                "aliases": " ".join(aliases),
                "related_slugs": " ".join(related_slugs),
                "source_paths": " ".join(source_paths),
                "source_ids": " ".join(source_identifiers),
                "tags": " ".join(tags),
                "body": document.body,
            }
            score, matched_fields = _score_haystacks(query, tokens, haystacks)
            if score <= 0:
                continue

            provenance = {
                "source_type": source_type,
                "source_path": source_paths[0] if source_paths else str(page_path),
                "source_paths": list(source_paths),
                "artifact_id": artifact_id,
                "event_ids": list(event_ids),
                "source_ids": list(source_ids),
                "session_ids": list(session_ids),
                "slug": slug,
                "page_path": str(page_path),
                "related_slugs": list(related_slugs),
                "influence_sources": list(influence_sources),
            }
            hits.append(
                HybridSearchHit(
                    result_id=f"wiki_page:{slug}",
                    result_type="wiki_page",
                    title=title,
                    summary=truncate_summary(summary),
                    score=score,
                    matched_fields=matched_fields,
                    source_type=source_type,
                    source_id=source_ids[0] if source_ids else artifact_id,
                    timestamp=timestamp,
                    created_at=created_at,
                    updated_at=updated_at,
                    tags=tags,
                    provenance=_compact_mapping(provenance),
                    security=security,
                    trust=trust,
                    slug=slug,
                    page_path=str(page_path),
                    artifact_id=artifact_id,
                )
            )
        return hits

    def _search_artifacts(
        self,
        query: str,
        tokens: tuple[str, ...],
        filters: HybridSearchFilters,
        *,
        limit: int,
    ) -> list[HybridSearchHit]:
        if self.db is None:
            return []

        entries = self.db.list_ingestion_entries(limit=max(200, limit * 20))
        hits: list[HybridSearchHit] = []
        for entry in entries:
            payload = _json_payload(entry.payload_json)
            metadata = _security_metadata_from_payload(payload)
            security = _artifact_security(entry, metadata)
            source_type = _optional_string(payload.get("source_type")) or entry.source
            payload_id = _optional_string(payload.get("id") or payload.get("artifact_id"))
            source_id = _optional_string(payload.get("source_id"))
            source_path = _first_string(
                payload.get("source_path")
                or payload.get("raw_payload_path")
                or payload.get("path")
                or payload.get("url")
            )
            tags = _normalize_tags(
                payload.get("tags")
                or payload.get("tag")
                or payload.get("topics")
                or payload.get("labels")
            )
            created_at = _optional_string(entry.created_at)
            updated_at = _optional_string(entry.processed_at or entry.created_at)
            timestamp = updated_at or created_at
            trust = _artifact_trust(entry, security, payload)
            source_identifiers = tuple(
                value
                for value in (entry.artifact_id, payload_id, source_id, source_type)
                if value
            )
            source_paths = tuple(value for value in (source_path,) if value)

            if not _passes_common_filters(
                filters,
                result_type="artifact",
                source_type=source_type,
                source_ids=source_identifiers,
                source_paths=source_paths,
                tags=tags,
                security=security,
                trust_score=_trust_score(trust),
                timestamp=timestamp,
                created_at=created_at,
                updated_at=updated_at,
            ):
                continue
            if filters.artifact_types and entry.artifact_type.lower() not in filters.artifact_types:
                continue

            title = _artifact_title(entry, payload)
            summary = _artifact_summary(entry, payload, title=title)
            searchable_payload = {
                key: value
                for key, value in payload.items()
                if key not in {"normalized_metadata", "raw_content"}
            }
            haystacks = {
                "title": title,
                "summary": summary,
                "artifact_id": entry.artifact_id,
                "artifact_type": entry.artifact_type,
                "source": entry.source,
                "source_type": source_type,
                "source_path": source_path or "",
                "tags": " ".join(tags),
                "payload": json.dumps(
                    searchable_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            }
            score, matched_fields = _score_haystacks(query, tokens, haystacks)
            if score <= 0:
                continue

            provenance = {
                "artifact_id": entry.artifact_id,
                "payload_artifact_id": payload_id,
                "artifact_type": entry.artifact_type,
                "source_type": source_type,
                "source": entry.source,
                "source_id": source_id,
                "source_path": source_path,
                "queue_status": entry.status,
                "created_at": entry.created_at,
                "processed_at": entry.processed_at,
                "capabilities": _json_list(entry.capabilities_json),
            }
            hits.append(
                HybridSearchHit(
                    result_id=f"artifact:{entry.artifact_id}",
                    result_type="artifact",
                    title=title,
                    summary=truncate_summary(summary),
                    score=score,
                    matched_fields=matched_fields,
                    source_type=source_type,
                    source_id=source_id or payload_id or entry.artifact_id,
                    timestamp=timestamp,
                    created_at=created_at,
                    updated_at=updated_at,
                    tags=tags,
                    provenance=_compact_mapping(provenance),
                    security=security,
                    trust=trust,
                    artifact_id=entry.artifact_id,
                )
            )
        return hits

    def _search_capture_events(
        self,
        query: str,
        tokens: tuple[str, ...],
        filters: HybridSearchFilters,
    ) -> list[HybridSearchHit]:
        if self.event_store is None:
            return []

        hits: list[HybridSearchHit] = []
        for event in self.event_store.list_events():
            source = self.event_store.get_source(event.source_id)
            raw_refs = self.event_store.list_raw_refs(event_id=event.event_id)
            links = self.event_store.list_artifact_links(event_id=event.event_id)
            findings = _capture_security_findings(self.event_store, event, raw_refs)
            security = _capture_security(event, findings)
            source_type = source.source_type if source else None
            source_name = source.source_name if source else None
            source_paths = tuple(raw_ref.path for raw_ref in raw_refs if raw_ref.path)
            artifact_ids = tuple(link.artifact_id for link in links if link.artifact_id)
            tags = _normalize_tags(
                event.payload.get("tags")
                or event.payload.get("tag")
                or event.payload.get("topics")
                or event.provenance.get("tags")
                or (source.metadata.get("tags") if source else None)
            )
            created_at = _optional_string(event.created_at)
            updated_at = _optional_string(event.updated_at or event.captured_at)
            timestamp = _optional_string(event.occurred_at or event.captured_at or event.created_at)
            trust = _capture_trust(event, security)
            source_identifiers = tuple(
                value
                for value in (
                    event.event_id,
                    event.source_id,
                    event.session_id,
                    event.native_event_id,
                    source_name,
                    *artifact_ids,
                )
                if value
            )

            if not _passes_common_filters(
                filters,
                result_type="capture_event",
                source_type=source_type,
                source_ids=source_identifiers,
                source_paths=source_paths,
                tags=tags,
                security=security,
                trust_score=_trust_score(trust),
                timestamp=timestamp,
                created_at=created_at,
                updated_at=updated_at,
            ):
                continue
            if filters.event_types and event.event_type.lower() not in filters.event_types:
                continue

            title = _event_title(event, source)
            summary = _event_summary(event)
            haystacks = {
                "title": title,
                "summary": summary,
                "event_id": event.event_id,
                "event_type": event.event_type,
                "source": " ".join(value for value in (source_name, source_type) if value),
                "source_ids": " ".join(source_identifiers),
                "source_paths": " ".join(source_paths),
                "artifact_ids": " ".join(artifact_ids),
                "tags": " ".join(tags),
                "payload": json.dumps(
                    event.payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            }
            score, matched_fields = _score_haystacks(query, tokens, haystacks)
            if score <= 0:
                continue

            provenance = {
                "event_id": event.event_id,
                "source_id": event.source_id,
                "source_type": source_type,
                "source_name": source_name,
                "session_id": event.session_id,
                "native_event_id": event.native_event_id,
                "event_hash": event.event_hash,
                "artifact_ids": list(artifact_ids),
                "artifact_links": [_artifact_link_payload(link) for link in links],
                "raw_ref_ids": [raw_ref.raw_ref_id for raw_ref in raw_refs],
                "source_paths": list(source_paths),
                "occurred_at": event.occurred_at,
                "captured_at": event.captured_at,
            }
            hits.append(
                HybridSearchHit(
                    result_id=f"capture_event:{event.event_id}",
                    result_type="capture_event",
                    title=title,
                    summary=truncate_summary(summary),
                    score=score,
                    matched_fields=matched_fields,
                    source_type=source_type,
                    source_id=event.source_id,
                    timestamp=timestamp,
                    created_at=created_at,
                    updated_at=updated_at,
                    tags=tags,
                    provenance=_compact_mapping(provenance),
                    security=security,
                    trust=trust,
                    event_id=event.event_id,
                )
            )
        return hits


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _query_tokens(query: str) -> tuple[str, ...]:
    tokens = tuple(token for token in _TOKEN_RE.findall(str(query).lower()) if token)
    if not tokens:
        raise ValueError("Hybrid search query cannot be empty")
    return tokens


def _score_haystacks(
    query: str,
    tokens: tuple[str, ...],
    haystacks: Mapping[str, Any],
) -> tuple[float, tuple[str, ...]]:
    normalized_query = str(query).strip().lower()
    normalized_haystacks = {
        field_name: str(field_value or "").lower()
        for field_name, field_value in haystacks.items()
    }
    all_text = " ".join(normalized_haystacks.values())
    matched_fields: list[str] = []
    score = 0.0
    if normalized_query and normalized_query in all_text:
        score += 5.0
        matched_fields.append("phrase")

    for token in tokens:
        token_score = 0.0
        for field_name, field_value in normalized_haystacks.items():
            if token not in field_value:
                continue
            matched_fields.append(field_name)
            token_score += 1.0
            if field_name == "title":
                token_score += 3.0
            elif field_name == "summary":
                token_score += 2.0
            elif field_name in {"tags", "artifact_id", "event_id", "source_ids"}:
                token_score += 1.0
        score += token_score
    return score, tuple(dict.fromkeys(matched_fields))


def _passes_common_filters(
    filters: HybridSearchFilters,
    *,
    result_type: str,
    source_type: str | None,
    source_ids: Sequence[str | None],
    source_paths: Sequence[str | None],
    tags: Sequence[str],
    security: Mapping[str, Any],
    trust_score: float,
    timestamp: str | None,
    created_at: str | None,
    updated_at: str | None,
) -> bool:
    if filters.result_types and result_type not in filters.result_types:
        return False

    normalized_source_type = str(source_type or "").strip().lower()
    if filters.source_types and normalized_source_type not in filters.source_types:
        return False

    normalized_source_ids = {
        str(value).strip().lower()
        for value in source_ids
        if str(value or "").strip()
    }
    if filters.source_ids and normalized_source_ids.isdisjoint(filters.source_ids):
        return False

    normalized_source_paths = {
        str(value).strip().lower()
        for value in source_paths
        if str(value or "").strip()
    }
    if filters.source_paths and not any(
        wanted in path
        for wanted in filters.source_paths
        for path in normalized_source_paths
    ):
        return False

    tag_set = set(tags)
    if filters.tags and tag_set.isdisjoint(filters.tags):
        return False
    if filters.exclude_tags and not tag_set.isdisjoint(filters.exclude_tags):
        return False

    security_status = str(security.get("status") or "allowed").strip().lower()
    requires_review = bool(security.get("requires_review"))
    if requires_review and not filters.include_quarantined:
        return False
    if filters.security_statuses and security_status not in filters.security_statuses:
        return False

    if filters.min_trust_score is not None and trust_score < filters.min_trust_score:
        return False

    if not _timestamp_passes(timestamp, filters.time_after, filters.time_before):
        return False
    if not _timestamp_passes(created_at, filters.created_after, filters.created_before):
        return False
    if not _timestamp_passes(updated_at, filters.updated_after, filters.updated_before):
        return False
    return True


def _timestamp_passes(value: str | None, after: str | None, before: str | None) -> bool:
    if after is None and before is None:
        return True
    timestamp = _parse_datetime(value)
    if timestamp is None:
        return False
    after_dt = _parse_datetime(after)
    before_dt = _parse_datetime(before)
    if after_dt is not None and timestamp < after_dt:
        return False
    if before_dt is not None and timestamp > before_dt:
        return False
    return True


def _validate_filter_times(filters: HybridSearchFilters) -> None:
    for field_name in (
        "time_after",
        "time_before",
        "created_after",
        "created_before",
        "updated_after",
        "updated_before",
    ):
        value = getattr(filters, field_name)
        if value and _parse_datetime(value) is None:
            raise ValueError(f"Invalid hybrid search timestamp filter: {field_name}")


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        timestamp = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            timestamp = datetime.fromisoformat(text)
        except ValueError:
            return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _sort_datetime(value: str | None) -> datetime:
    return _parse_datetime(value) or datetime.min.replace(tzinfo=timezone.utc)


def _frontmatter_value(frontmatter: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = frontmatter.get(key)
        if value is not None:
            return value
    return None


def _frontmatter_sequence(frontmatter: Mapping[str, Any], *keys: str) -> tuple[str, ...]:
    return _normalize_strings(_frontmatter_value(frontmatter, *keys))


def _frontmatter_mapping_sequence(
    frontmatter: Mapping[str, Any],
    *keys: str,
) -> tuple[dict[str, Any], ...]:
    value = _frontmatter_value(frontmatter, *keys)
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return (dict(value),)
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


def _normalize_result_types(value: Any) -> tuple[str, ...]:
    normalized: list[str] = []
    for item in _normalize_strings(value):
        if item in {"all", "*"}:
            continue
        candidate = _RESULT_TYPE_ALIASES.get(item, item)
        if candidate not in _VALID_RESULT_TYPES:
            raise ValueError(f"Unsupported hybrid search result type: {item}")
        if candidate not in normalized:
            normalized.append(candidate)
    return tuple(normalized)


def _normalize_strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items: Iterable[Any] = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = (value,)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_item in items:
        text = str(raw_item).strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _normalize_tags(value: Any) -> tuple[str, ...]:
    tags: list[str] = []
    seen: set[str] = set()
    for item in _normalize_strings(value):
        tag = " ".join(item.lstrip("#").replace("-", "_").split()).replace(" ", "_").strip("_")
        if not tag or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return tuple(tags)


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_present_value(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _first_string(value: Any) -> str | None:
    values = _normalize_strings(value)
    return values[0] if values else None


def _json_payload(value: str | Mapping[str, Any] | None) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def _security_metadata_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized_metadata = payload.get("normalized_metadata")
    if not isinstance(normalized_metadata, Mapping):
        return {}
    return {
        key: normalized_metadata[key]
        for key in (
            THOTH_SECURITY_FINDINGS_KEY,
            THOTH_SECURITY_FINDING_COUNT_KEY,
            THOTH_SECURITY_PATTERN_IDS_KEY,
            THOTH_SECURITY_POLICY_KEY,
            THOTH_SECURITY_AUDIT_KEY,
            THOTH_REDACTION_METADATA_KEY,
        )
        if normalized_metadata.get(key)
    }


def _wiki_security(frontmatter: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping_or_empty(
        _frontmatter_value(frontmatter, THOTH_SECURITY_POLICY_KEY, "security_policy")
    )
    status = str(policy.get("status") or "allowed").strip().lower()
    metadata = dict(frontmatter)
    requires_review = (
        prompt_security_requires_review(metadata)
        or status in _BLOCKING_SECURITY_STATUSES
    )
    findings = _frontmatter_value(frontmatter, THOTH_SECURITY_FINDINGS_KEY, "security_findings")
    pattern_ids = _normalize_strings(
        policy.get("pattern_ids")
        or _frontmatter_value(frontmatter, THOTH_SECURITY_PATTERN_IDS_KEY)
    )
    return _compact_mapping(
        {
            "status": status,
            "requires_review": requires_review,
            "policy": policy,
            "finding_count": _finding_count(findings),
            "pattern_ids": list(pattern_ids),
        }
    )


def _artifact_security(
    entry: IngestionQueueEntry,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _mapping_or_empty(metadata.get(THOTH_SECURITY_POLICY_KEY))
    policy_status = str(policy.get("status") or "allowed").strip().lower()
    entry_status = str(entry.status or "").strip().lower()
    status = (
        entry_status
        if entry_status in _BLOCKING_SECURITY_STATUSES
        else policy_status
    )
    requires_review = (
        entry_status in _BLOCKING_SECURITY_STATUSES
        or prompt_security_requires_review(metadata)
    )
    findings = metadata.get(THOTH_SECURITY_FINDINGS_KEY)
    pattern_ids = _normalize_strings(
        policy.get("pattern_ids") or metadata.get(THOTH_SECURITY_PATTERN_IDS_KEY)
    )
    return _compact_mapping(
        {
            "status": status,
            "requires_review": requires_review,
            "policy": policy,
            "finding_count": _finding_count(findings),
            "pattern_ids": list(pattern_ids),
            "queue_status": entry.status,
        }
    )


def _capture_security(
    event: CaptureEvent,
    findings: tuple[SecurityFinding, ...],
) -> dict[str, Any]:
    status = str(event.status or "captured").strip().lower()
    open_findings = tuple(
        finding
        for finding in findings
        if _finding_is_open(finding)
    )
    if status in _BLOCKING_SECURITY_STATUSES:
        security_status = status
    elif open_findings:
        security_status = "needs_review"
    else:
        security_status = "allowed"
    return _compact_mapping(
        {
            "status": security_status,
            "requires_review": security_status in _BLOCKING_SECURITY_STATUSES,
            "event_status": event.status,
            "finding_count": len(findings),
            "open_finding_count": len(open_findings),
            "max_severity": _max_severity(findings),
            "finding_ids": [finding.finding_id for finding in findings],
            "pattern_ids": [
                str(finding.details.get("pattern_id"))
                for finding in findings
                if finding.details.get("pattern_id")
            ],
        }
    )


def _capture_security_findings(
    event_store: CaptureEventStore,
    event: CaptureEvent,
    raw_refs: tuple[RawArtifactRef, ...],
) -> tuple[SecurityFinding, ...]:
    findings: list[SecurityFinding] = list(
        event_store.list_security_findings(event_id=event.event_id)
    )
    for raw_ref in raw_refs:
        findings.extend(event_store.list_security_findings(raw_ref_id=raw_ref.raw_ref_id))
    return tuple(_dedupe_by_attr(findings, "finding_id"))


def _finding_is_open(finding: SecurityFinding) -> bool:
    status = str(finding.status or "").strip().lower()
    return status in _OPEN_FINDING_STATUSES or status not in _CLOSED_FINDING_STATUSES


def _max_severity(findings: tuple[SecurityFinding, ...]) -> str | None:
    order = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
    max_finding = None
    max_score = -1
    for finding in findings:
        score = order.get(str(finding.severity or "").lower(), 0)
        if score > max_score:
            max_finding = finding.severity
            max_score = score
    return max_finding


def _finding_count(findings: Any) -> int:
    if isinstance(findings, Sequence) and not isinstance(findings, (str, bytes)):
        return len(findings)
    return 0


def _wiki_trust(
    security: Mapping[str, Any],
    influence_sources: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    policy = _mapping_or_empty(security.get("policy"))
    status = str(security.get("status") or "allowed")
    if bool(security.get("requires_review")):
        score = 0.0 if status == "blocked" else 0.25
        reason = f"prompt_security_{status}"
    elif status == "override_approved":
        score = 0.9
        reason = "prompt_security_override_approved"
    else:
        score = 1.0
        reason = f"prompt_security_{policy.get('reason') or 'allowed'}"
    return _compact_mapping(
        {
            "score": score,
            "reason": reason,
            "influence_sources": list(influence_sources),
        }
    )


def _artifact_trust(
    entry: IngestionQueueEntry,
    security: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    explicit_score = _first_present_value(
        payload,
        "source_trust_score",
        "trust_score",
    )
    explicit_reason = payload.get("source_trust_reason") or payload.get("trust_reason")
    if explicit_score is not None:
        try:
            score = float(explicit_score)
        except (TypeError, ValueError):
            score = 0.0
    elif bool(security.get("requires_review")):
        score = 0.0 if security.get("status") == "blocked" else 0.25
    else:
        score = 1.0
    reason = str(
        explicit_reason
        or _mapping_or_empty(security.get("policy")).get("reason")
        or f"queue_status_{entry.status}"
    )
    return {
        "score": score,
        "reason": reason,
        "influence_sources": [],
    }


def _capture_trust(
    event: CaptureEvent,
    security: Mapping[str, Any],
) -> dict[str, Any]:
    explicit_score = _first_present_value(
        event.provenance,
        "source_trust_score",
        "trust_score",
    )
    explicit_reason = event.provenance.get("source_trust_reason") or event.provenance.get(
        "trust_reason"
    )
    if explicit_score is not None:
        try:
            score = float(explicit_score)
        except (TypeError, ValueError):
            score = 0.0
    elif bool(security.get("requires_review")):
        score = 0.0 if security.get("status") == "blocked" else 0.25
    else:
        score = 1.0
    reason = str(explicit_reason or f"capture_security_{security.get('status') or 'allowed'}")
    return {"score": score, "reason": reason, "influence_sources": []}


def _trust_score(trust: Mapping[str, Any]) -> float:
    try:
        return float(trust.get("score", 0.0))
    except (TypeError, ValueError):
        return 0.0


def _artifact_title(entry: IngestionQueueEntry, payload: Mapping[str, Any]) -> str:
    for key in ("title", "repo_name", "full_name", "name", "display_name"):
        text = _optional_string(payload.get(key))
        if text:
            return text
    return _optional_string(payload.get("id")) or f"{entry.artifact_type}:{entry.artifact_id}"


def _artifact_summary(
    entry: IngestionQueueEntry,
    payload: Mapping[str, Any],
    *,
    title: str,
) -> str:
    for key in ("description", "summary", "abstract", "text", "full_text"):
        text = _optional_string(payload.get(key))
        if text:
            return text
    return f"{entry.artifact_type} artifact from {entry.source}: {title}"


def _event_title(event: CaptureEvent, source: CaptureSource | None) -> str:
    for key in ("title", "name", "summary", "text"):
        text = _optional_string(event.payload.get(key))
        if text:
            return text
    source_name = source.source_name if source else event.source_id
    native_id = event.native_event_id or event.event_id
    return f"{event.event_type} from {source_name}: {native_id}"


def _event_summary(event: CaptureEvent) -> str:
    for key in ("summary", "description", "text", "title", "content"):
        text = _optional_string(event.payload.get(key))
        if text:
            return text
    return f"Capture event {event.event_type} ({event.event_id})"


def _artifact_link_payload(link: ArtifactLink) -> dict[str, Any]:
    return _compact_mapping(
        {
            "artifact_link_id": link.artifact_link_id,
            "artifact_id": link.artifact_id,
            "artifact_type": link.artifact_type,
            "raw_ref_id": link.raw_ref_id,
            "link_type": link.link_type,
        }
    )


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _compact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if item not in (None, "", [], {}, ())
    }


def _dedupe_by_attr(items: Iterable[Any], attr_name: str) -> list[Any]:
    seen: set[Any] = set()
    deduped: list[Any] = []
    for item in items:
        key = getattr(item, attr_name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped
