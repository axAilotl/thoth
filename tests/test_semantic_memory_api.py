import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api
from core.metadata_db import MetadataDB
from core.semantic_memory import (
    SEMANTIC_MEMORY_PROMOTION_METADATA_KEY,
    SemanticMemoryCandidate,
    SemanticMemoryEvidence,
    SemanticMemoryStore,
)
from core.semantic_memory_review import (
    SEMANTIC_MEMORY_REVIEW_METADATA_KEY,
    SemanticMemoryReviewService,
)


def _patch_background_tasks(monkeypatch):
    def noop(*args, **kwargs):
        return None

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", noop)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "archivist_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(thoth_api, "resolve_x_api_sync_config", lambda: None)
    thoth_api._shutdown_event = asyncio.Event()


def _add_candidate(
    store: SemanticMemoryStore,
    candidate_id: str,
    *,
    text: str,
    candidate_type: str = "preference",
    entity_id: str | None = None,
    evidence_id: str | None = None,
) -> None:
    evidence = ()
    if evidence_id:
        evidence = (
            SemanticMemoryEvidence(
                candidate_id=candidate_id,
                evidence_id=evidence_id,
                source_path=f"notes/{candidate_id}.md",
                evidence_text=f"Evidence for {candidate_id}",
            ),
        )
    store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id=candidate_id,
            candidate_type=candidate_type,
            text=text,
            entity_id=entity_id,
        ),
        evidence=evidence,
    )


def test_semantic_memory_api_lists_details_and_reviews(monkeypatch, tmp_path: Path):
    _patch_background_tasks(monkeypatch)
    db = MetadataDB(str(tmp_path / "meta.db"))
    store = SemanticMemoryStore(db)
    _add_candidate(
        store,
        "candidate-api-1",
        text="Ada prefers written planning.",
        entity_id="person:ada",
        evidence_id="evidence-api-1",
    )
    _add_candidate(
        store,
        "candidate-api-reject",
        text="Ada prefers unsupported claims.",
        candidate_type="claim",
    )
    _add_candidate(
        store,
        "candidate-api-old",
        text="Ada prefers planning notes.",
        evidence_id="evidence-api-old",
    )
    _add_candidate(
        store,
        "candidate-api-replacement",
        text="Ada prefers async written planning.",
    )
    service = SemanticMemoryReviewService(store=store)
    monkeypatch.setattr(
        thoth_api,
        "open_api_semantic_memory_review_service",
        lambda: service,
    )

    with TestClient(thoth_api.app) as client:
        list_response = client.get(
            "/api/memory/candidates",
            params={"status": "proposed", "entity_id": "person:ada"},
        )
        detail_response = client.get("/api/memory/candidates/candidate-api-1")
        confirm_response = client.post(
            "/api/memory/candidates/candidate-api-1/confirm",
            json={
                "actor": "operator",
                "reason": "source reviewed",
                "reviewed_at": "2026-06-26T12:00:00",
                "metadata": {"ticket": "thoth-zps.3"},
            },
        )
        promote_response = client.post(
            "/api/memory/candidates/candidate-api-1/promote",
            json={
                "actor": "operator",
                "reason": "confirmed by review",
                "reviewed_at": "2026-06-26T12:05:00",
            },
        )
        invalid_transition_response = client.post(
            "/api/memory/candidates/candidate-api-1/reject",
            json={"actor": "operator"},
        )
        reject_response = client.post(
            "/api/memory/candidates/candidate-api-reject/reject",
            json={"actor": "operator", "reason": "not supported"},
        )
        supersede_response = client.post(
            "/api/memory/candidates/candidate-api-old/supersede",
            json={
                "superseded_by_candidate_id": "candidate-api-replacement",
                "actor": "operator",
                "reason": "better wording",
            },
        )
        missing_response = client.get("/api/memory/candidates/missing-candidate")

    assert list_response.status_code == 200
    listed = list_response.json()
    assert listed["total"] == 1
    assert listed["candidates"][0]["candidate_id"] == "candidate-api-1"
    assert listed["candidates"][0]["evidence_count"] == 1

    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["candidate"]["text"] == "Ada prefers written planning."
    assert detail["evidence"][0]["evidence_text"] == "Evidence for candidate-api-1"

    assert confirm_response.status_code == 200
    confirmed = confirm_response.json()["candidate"]
    assert confirmed["status"] == "confirmed"
    review = confirmed["metadata"][SEMANTIC_MEMORY_REVIEW_METADATA_KEY]
    assert review["action"] == "confirm"
    assert review["actor"] == "operator"
    assert review["metadata"] == {"ticket": "thoth-zps.3"}
    transition = confirmed["write_provenance"]["last_status_transition"]
    assert transition["write_provenance"][SEMANTIC_MEMORY_REVIEW_METADATA_KEY][
        "reason"
    ] == "source reviewed"

    assert promote_response.status_code == 200
    promoted = promote_response.json()["candidate"]
    assert promoted["status"] == "promoted"
    assert promoted["metadata"][SEMANTIC_MEMORY_PROMOTION_METADATA_KEY]["reason"] == (
        "explicit_confirmation"
    )
    assert [
        item["to"] for item in promoted["write_provenance"]["status_transitions"]
    ] == ["confirmed", "promoted"]

    assert invalid_transition_response.status_code == 400
    assert "cannot transition" in invalid_transition_response.json()["detail"]

    assert reject_response.status_code == 200
    assert reject_response.json()["candidate"]["status"] == "rejected"

    assert supersede_response.status_code == 200
    superseded = supersede_response.json()
    assert superseded["candidate"]["status"] == "superseded"
    assert superseded["candidate"]["superseded_by_candidate_id"] == (
        "candidate-api-replacement"
    )
    assert superseded["evidence"][0]["evidence_id"] == "evidence-api-old"

    assert missing_response.status_code == 404
