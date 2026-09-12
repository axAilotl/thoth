"""Human-facing ingestion review; no raw payload rendering or automatic approval."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit, quote

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, StrictBool, field_validator

from .artifact_review_policy import review_revision, INGESTION_ACTIVE_REVIEW_STATUSES
from .artifact_review_queue import ArtifactReviewQueueService, ArtifactReviewQueueError
from .classification_review import entry_has_classification_review
from .artifacts.web_clipper import WebClipperArtifact
from .document_enrichment import diagnose_document_source
from .prompt_security import prompt_security_requires_review
from .sensitive_redaction import redact_sensitive_text


class ReviewDecision(BaseModel):
    artifact_id: str = Field(min_length=1, max_length=4096)
    revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    action: Literal["retry", "approve_security", "reject"]
    # Identify the console, not an authenticated person or a claimed inspection.
    actor: str = Field(default="review-console", min_length=1, max_length=120)
    reason: str = Field(default="Decision submitted through the review console.", min_length=1, max_length=2000)
    security_acknowledged: StrictBool = False

    @field_validator("actor", "reason")
    @classmethod
    def not_blank(cls, value):
        if not value.strip():
            raise ValueError("Actor and reason must not be blank")
        return value.strip()


def _object(value):
    try:
        result = json.loads(value or "{}")
    except (ValueError, TypeError):
        return {}
    return result if isinstance(result, dict) else {}


def _text(value, limit=2000):
    return redact_sensitive_text(str(value or "")[:limit]).redacted_text


def _source_url(value):
    """Only explicit web links; never executable schemes or embedded credentials."""
    if not isinstance(value, str) or len(value) > 4096:
        return None
    if any(char.isspace() or ord(char) < 32 or char == "\\" for char in value):
        return None
    try:
        parsed = urlsplit(value)
        if parsed.scheme in ("http", "https") and parsed.hostname and parsed.username is None and parsed.password is None:
            return value
    except ValueError:
        return None
    return None


def review_item(entry, service, layout):
    """Allowlist display fields; a source document is never HTML or an instruction."""
    payload = _object(entry.payload_json)
    review = _object(entry.review_json)
    metadata = payload.get("normalized_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    state = review.get("state") or {}
    source_diagnostic = dict(state.get("metadata") or {})
    security = prompt_security_requires_review(metadata)
    classification = entry_has_classification_review(entry)
    active = entry.status in INGESTION_ACTIVE_REVIEW_STATUSES
    findings = metadata.get("thoth_security_findings") or []
    source_relative = str(payload.get("source_relative_path") or "")
    category = state.get("category")
    exhausted = active and category == "processing_failed"
    reason = _text((entry.last_error or state.get("error")) if exhausted else state.get("reason"))
    reason = reason or _text(entry.last_error)
    if exhausted and entry.artifact_type == "web_clipper":
        source_diagnostic = diagnose_document_source(
            WebClipperArtifact.from_queue_payload(payload), service.config, layout, probe_pdf=True,
        )
        category = source_diagnostic.get("category", category)
        reason = _text(source_diagnostic.get("reason")) or reason
        if source_diagnostic.get("diagnostic_error"):
            reason = _text(f"{reason}\nSource check: {source_diagnostic['diagnostic_error']}")
    ocr_required = (
        entry.artifact_type == "web_clipper" and payload.get("file_type") == "attachment"
        and category == "ocr_required"
    )
    action_note = "Classification routing requires the classification CLI." if classification else ""
    if ocr_required and not classification:
        action_note = (
            "Retry repeats PDF text extraction within the configured page limit; it does not run OCR. "
            "For scanned pages, add a text layer with an external OCR tool, then rescan the changed PDF "
            "to capture its new version before processing."
        )
    reason_summary = _text(source_diagnostic.get("reason_summary")) or " ".join(reason.split())
    if ocr_required:
        reason_summary = "No PDF text; scanned pages need OCR."
    elif security:
        patterns = [_text(f.get("pattern_id"), 120).replace("_", " ")
                    for f in findings[:2] if isinstance(f, dict) and f.get("pattern_id")]
        reason_summary = "Security flag: " + (", ".join(patterns) or "potentially unsafe source instructions")
    if len(reason_summary) > 160:
        reason_summary = reason_summary[:157] + "…"
    # Local Obsidian uses the vault-relative path, never the server's home path.
    obsidian_url = None
    if source_relative and not Path(source_relative).is_absolute() and ".." not in Path(source_relative).parts:
        # The content root may be knowledge_vault inside the Obsidian vault.
        root = layout.vault_root
        obsidian_root = next((p for p in (root, *root.parents) if (p / '.obsidian').is_dir()), None)
        configured_name = service.config.get("review_ui.obsidian_vault_name")
        if configured_name:
            prefix = str(service.config.get("review_ui.obsidian_content_prefix", ""))
            if Path(prefix).is_absolute() or '..' in Path(prefix).parts:
                raise ValueError("review_ui.obsidian_content_prefix must be vault-relative")
            file_path = str(Path(prefix) / source_relative)
            obsidian_url = f"obsidian://open?vault={quote(str(configured_name), safe='')}&file={quote(file_path, safe='')}"
        elif obsidian_root is not None:
            file_path = str(root.relative_to(obsidian_root) / source_relative)
            obsidian_url = f"obsidian://open?vault={quote(obsidian_root.name, safe='')}&file={quote(file_path, safe='')}"
    return {
        "artifact_id": entry.artifact_id, "revision": review_revision(entry),
        "title": _text(payload.get("title") or source_relative or entry.artifact_id, 500),
        "source": entry.source, "status": entry.status,
        "source_path": _text(payload.get("source_path")),
        "source_relative_path": source_relative, "obsidian_url": obsidian_url,
        "source_url": _source_url(payload.get("source_url")),
        "source_checksum": payload.get("source_checksum"),
        "attempts": entry.attempts, "category": _text(category),
        "reason": reason, "reason_summary": reason_summary, "ocr_required": ocr_required,
        "source_status": _text(source_diagnostic.get("source_status")) or "unchecked",
        "last_error": _text(entry.last_error), "security_required": security,
        "findings": [{"pattern_id": _text(f.get("pattern_id"), 120),
                      "severity": _text(f.get("severity"), 30)}
                     for f in findings[:100] if isinstance(f, dict)],
        "history": [{key: _text(event.get(key)) for key in ("at", "action", "actor", "reason", "from", "to")}
                    for event in review.get("events", [])[-50:] if isinstance(event, dict)],
        "actions": ([] if not active or classification else
                    ["approve_security" if security else "retry", "reject"]),
        "action_note": action_note,
    }


def require_review_write(request: Request):
    """Same-origin browser mutation guard for the existing trusted-LAN console.

    This is CSRF protection, not authentication. Deployment must remain behind
    the same trusted-network/access boundary as the rest of the settings UI.
    """
    if request.headers.get("x-thoth-review") != "1":
        raise HTTPException(403, "Review requests require the console header")
    if request.headers.get("sec-fetch-site") not in (None, "same-origin", "none"):
        raise HTTPException(403, "Cross-origin review is not allowed")
    origin = request.headers.get("origin")
    if origin:
        parsed = urlsplit(origin)
        if parsed.scheme not in ("http", "https") or parsed.netloc != request.headers.get("host"):
            raise HTTPException(403, "Cross-origin review is not allowed")


def create_review_router(runtime_provider):
    router = APIRouter()

    def service():
        runtime = runtime_provider()
        return ArtifactReviewQueueService(runtime.db, config=runtime.config)

    @router.get("/review", include_in_schema=False)
    def page():
        return RedirectResponse("/settings#review", status_code=307,
                                headers={"Cache-Control": "no-store"})

    @router.get("/api/review")
    def listing(
        status: Literal["active", "needs_review", "blocked", "failed", "reviewed", "rejected", "decided"] = "active",
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        current = service()
        entries = current.db.list_ingestion_review_entries(
            status=None if status == "active" else status, limit=limit + 1,
            offset=offset, raise_errors=True,
        )
        return {"items": [review_item(e, current, runtime_provider().layout) for e in entries[:limit]],
                "has_more": len(entries) > limit, "offset": offset}

    @router.post("/api/review/decision", dependencies=[Depends(require_review_write)])
    def decide(body: ReviewDecision):
        current = service()
        entry = current.db.get_ingestion_entry(body.artifact_id, raise_errors=True)
        if entry is None:
            raise HTTPException(404, "Review item not found")
        try:
            if body.action != "reject" and entry.artifact_type == "web_clipper":
                from .document_enrichment import validate_document_source
                runtime = runtime_provider()
                artifact = WebClipperArtifact.from_queue_payload(_object(entry.payload_json))
                validate_document_source(artifact, runtime.config, runtime.layout)
                state = _object(entry.review_json).get("state") or {}
                if state.get("category") == "source_malformed_pdf":
                    raise ValueError(state.get("reason") or "Malformed PDF; restore the PDF and rescan.")
                if state.get("category") == "processing_failed":
                    diagnostic = diagnose_document_source(artifact, runtime.config, runtime.layout, probe_pdf=True)
                    if diagnostic.get("category", "").startswith("source_"):
                        raise ValueError(diagnostic["reason"])
            updated = current.decide(
                body.artifact_id, action=body.action, actor=body.actor,
                reason=body.reason, expected_revision=body.revision,
                security_acknowledged=body.security_acknowledged,
            )
        except (ArtifactReviewQueueError, ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(409, _text(exc)) from exc
        return {"item": review_item(updated, current, runtime_provider().layout)}

    return router
