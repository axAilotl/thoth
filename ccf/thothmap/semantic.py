"""Entities/assertions -> semantic candidate Records (checklist 4, row 6).

Two Thoth concepts:

- ``CanonicalEntityRecord`` (``core.metadata_db`` / ``canonical_entities``,
  built by ``core.canonical_identity``) becomes a ``semantic.entity``
  Record. The Thoth ``canonical_id`` (``entity_type:key_type:slug``) is the
  origin native ID. Adjudication between entities is a separate
  ``semantic.entity_resolution`` concern, not part of this mapping.
- ``SemanticMemoryCandidate`` (``core.semantic_memory`` /
  ``semantic_memory_candidates``) becomes a *candidate* ``semantic.assertion``
  Record with ``machine_inference`` authority. Thoth predicates are free
  text; they are normalized into the ``thoth.<candidate_type>.<slug>``
  predicate namespace (original text preserved in qualifiers), because the
  pinned predicate registry only covers ccf.* work/identity predicates.

Each candidate assertion carries ``ccf.evidence_for`` Links from the mapped
evidence objects (artifacts/utterances the caller has already mapped) to
the assertion. Candidates without mapped evidence are still convertible
(Thoth allows promotion gating elsewhere) but the caller should prefer
passing evidence — the promotion policy requires it.
"""

from __future__ import annotations

from ccf.ids import parse_id
from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    claims,
    literal,
    normalize_predicate,
    optional_str,
    origin,
    require_str,
    require_urn,
)


def entity_submission(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    revision: str | int | None = "1",
    aliases: list[str] | None = None,
    description: str | None = None,
) -> MappedSubmissions:
    """Convert a ``CanonicalEntityRecord`` snapshot to a ``semantic.entity``.

    Snapshot keys: ``canonical_id``, ``entity_type``, ``display_name``.
    Optional ``metadata.aliases`` list feeds the payload aliases.
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    canonical_id = require_str(snapshot, "canonical_id", what="canonical entity")
    entity_type = require_str(snapshot, "entity_type", what="canonical entity")
    display_name = require_str(snapshot, "display_name", what="canonical entity")
    if aliases is None:
        metadata = snapshot.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise ThothMapError("canonical entity 'metadata' must be an object")
        raw_aliases = metadata.get("aliases") or []
        if not isinstance(raw_aliases, list):
            raise ThothMapError("canonical entity aliases must be a list")
        aliases = [str(alias) for alias in raw_aliases]

    record = producer.new_record(
        type="semantic.entity",
        claims=claims(ctx, basis="deterministic_derivation", asserted_by=producer.producer_id),
        origin=origin(source_ccf_id, canonical_id, revision),
        payload={
            "entity_kind": entity_type,
            "label": display_name,
            "aliases": list(dict.fromkeys(aliases)),
            "description": description or f"Thoth canonical {entity_type} {canonical_id}",
            "extensions": {
                "thoth_canonical_id": canonical_id,
                "thoth_primary_artifact_id": snapshot.get("primary_artifact_id"),
                "thoth_wiki_slug": snapshot.get("wiki_slug"),
            },
        },
    )
    return MappedSubmissions(records=[record])


def assertion_submissions(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    subject_ccf_id: str | None = None,
    evidence_ccf_ids: list[str] | None = None,
    revision: str | int | None = "1",
) -> MappedSubmissions:
    """Convert a ``SemanticMemoryCandidate`` to a candidate ``semantic.assertion``.

    Snapshot keys: ``candidate_id``, ``candidate_type``, ``status``,
    ``subject``, ``predicate``, ``object_value`` (the
    ``object_text`` column), ``text``, ``confidence``.

    ``subject_ccf_id`` is the mapped ``semantic.entity`` URN when the
    candidate's subject has been mapped; otherwise the subject text is
    carried as a literal. Object values map to string literals (Thoth
    stores them as text). Evidence Links follow the example direction:
    evidence object -> assertion (``ccf.evidence_for``).
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    candidate_id = require_str(snapshot, "candidate_id", what="memory candidate")
    candidate_type = require_str(snapshot, "candidate_type", what="memory candidate")
    predicate_raw = require_str(snapshot, "predicate", what="memory candidate")
    subject_text = optional_str(snapshot, "subject")
    object_text = optional_str(snapshot, "object_value")
    if object_text is None:
        object_text = optional_str(snapshot, "object_text")
    if object_text is None:
        raise ThothMapError("memory candidate requires 'object_value'")
    if subject_ccf_id is not None:
        require_urn(subject_ccf_id, "record", field="subject_ccf_id")
        subject = {"ref": subject_ccf_id}
    elif subject_text:
        subject = literal(subject_text)
    else:
        raise ThothMapError(
            "memory candidate requires a mapped subject_ccf_id or snapshot 'subject' text"
        )

    evidence_ccf_ids = list(evidence_ccf_ids or [])
    for evidence_id in evidence_ccf_ids:
        try:
            parse_id(evidence_id)
        except Exception as exc:
            raise ThothMapError(f"invalid evidence URN: {evidence_id!r}") from exc

    qualifiers: dict = {
        "thoth_predicate": predicate_raw,
        "thoth_candidate_status": snapshot.get("status") or "proposed",
    }
    confidence = snapshot.get("confidence")
    if confidence is not None:
        qualifiers["thoth_confidence"] = float(confidence)

    record = producer.new_record(
        type="semantic.assertion",
        claims=claims(ctx, basis="machine_inference", asserted_by=producer.producer_id),
        origin=origin(source_ccf_id, candidate_id, revision),
        payload={
            "subject": subject,
            "predicate": normalize_predicate(predicate_raw, candidate_type=candidate_type),
            "object": literal(object_text),
            "scope": {},
            "qualifiers": qualifiers,
            "extensions": {
                "thoth_candidate_id": candidate_id,
                "thoth_candidate_type": candidate_type,
                "thoth_text": snapshot.get("text"),
                "thoth_entity_id": snapshot.get("entity_id"),
            },
        },
    )
    links = [
        producer.new_link(
            type="ccf.evidence_for",
            from_id=evidence_id,
            to_id=record["id"],
            claims=claims(ctx),
            selector={},
        )
        for evidence_id in dict.fromkeys(evidence_ccf_ids)
    ]
    return MappedSubmissions(records=[record], links=links)
