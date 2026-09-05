"""Incremental dump-folder intake through the shared capture/ingestion queue."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import time
import uuid

from core.artifacts import MarkdownArtifact
from core.config import config
from core.connector_budgets import start_connector_budget_run
from core.connector_capture import ConnectorCaptureQueue
from core.document_enrichment import extract_document_abstract
from core.metadata_db import FileMetadata, IngestionQueueEntry, get_metadata_db
from core.path_layout import build_path_layout
from core.pdf_text import PDFTextExtractionError
from core.prompt_security import prompt_security_requires_review

from .inbox_files import InboxFileError, extract_snapshot, fingerprint, immutable_write, read_snapshot, sha256


DESTINATIONS = {".md": "documents", ".markdown": "documents", ".txt": "documents",
                ".docx": "documents", ".pdf": "pdfs"}
FINAL_QUEUE_STATES = {"pending", "processing", "processed", "skipped"}


class InboxConnector:
    """Observe twice, snapshot, preserve, queue, then optionally archive the input.

    Receipt state lives in the existing metadata database, not in the vault.
    Extension-based routing is intentionally deterministic, not a semantic guess.
    """

    def __init__(self, runtime_config=None, *, layout=None, db=None):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()
        self.queue = ConnectorCaptureQueue(self.config, layout=self.layout, db=self.db)

    def collect(self, *, limit: int | None = None) -> dict:
        if self.config.get("sources.inbox.enabled", False) is not True:
            raise ValueError("File inbox is disabled; enable sources.inbox.enabled explicitly")
        consume = self.config.get("sources.inbox.consume", False)
        if not isinstance(consume, bool):
            raise ValueError("sources.inbox.consume must be a boolean")
        directory = self.config.get("sources.inbox.directory")
        if not isinstance(directory, str) or not directory.strip():
            raise ValueError("sources.inbox.directory must be an explicit absolute directory")
        inbox = Path(directory)
        if not inbox.is_absolute() or any(part.is_symlink() for part in (inbox, *inbox.parents)):
            raise ValueError("Inbox must be an absolute directory without symlinks")
        inbox = inbox.resolve()
        if not inbox.is_dir():
            raise ValueError("Configured inbox directory does not exist")
        system = self.layout.system_root.resolve()
        vault = self.layout.vault_root.resolve()
        if system.is_relative_to(vault) or system.is_relative_to(inbox) or inbox.is_relative_to(system):
            raise ValueError("Inbox control/archive directory must be outside the vault and inbox")
        destinations = [vault / folder for folder in set(DESTINATIONS.values())]
        if inbox == vault or inbox.is_relative_to(self.layout.wiki_root.resolve()) or any(
            root.is_relative_to(inbox) or inbox.is_relative_to(root) for root in destinations
        ):
            raise ValueError("Inbox must be separate from wiki and managed destination folders")
        maximum = self._positive("max_files_per_run", 25) if limit is None else limit
        if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 1:
            raise ValueError("Inbox limit must be a positive integer")
        stable_seconds = self._positive("stable_seconds", 60)
        max_bytes = self._positive("max_source_bytes", 50 * 1024 * 1024)
        self._positive("max_text_chars", 500000)
        self._positive("pdf_max_pages", 40)
        control = system / "inbox"
        control.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(control / "intake.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(lock_fd)
            return {"queued_count": 0, "busy": True, "records": []}
        try:
            result = {"queued_count": 0, "review_count": 0, "deferred_count": 0,
                      "reused_count": 0, "consumed_count": 0, "records": []}
            budget = start_connector_budget_run(self.config, "inbox")
            # Walk does not follow linked directories. Surface them instead of silently ignoring them.
            candidates = []
            def traversal_error(error):
                raise error
            for root, dirs, files in os.walk(inbox, followlinks=False, onerror=traversal_error):
                candidates.extend(Path(root) / name for name in files)
                candidates.extend(Path(root) / name for name in dirs if (Path(root) / name).is_symlink())
            with self.queue.lifecycle() as lifecycle:
                processed = 0
                for path in sorted(candidates):
                    record = self._collect_one(path, inbox, control, lifecycle,
                                               stable_seconds, max_bytes, consume, budget)
                    result["records"].append(record)
                    status = record["status"]
                    result[f"{status}_count"] += 1
                    if record.get("consumed"):
                        result["consumed_count"] += 1
                    if status in {"queued", "review"}:
                        processed += 1
                    if processed >= maximum:
                        break
            result["budget"] = budget.summary()
            return result
        finally:
            os.close(lock_fd)

    def _positive(self, name, default):
        value = self.config.get(f"sources.inbox.{name}", default)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"sources.inbox.{name} must be a positive integer")
        return value

    def _collect_one(self, path, inbox, control, lifecycle, stable_seconds, max_bytes, consume, budget):
        source = str(path.relative_to(inbox))
        key = "inbox:file:" + sha256(str(path).encode())
        previous = self.db.get_automation_state(key) or {}
        now = time.time()
        try:
            observed = fingerprint(path)
        except (InboxFileError, OSError) as exc:
            return self._review(path, key, previous, None, str(exc), lifecycle)
        if previous.get("fingerprint") != observed:
            self.db.upsert_automation_state(key, {"fingerprint": observed, "observed_at": now,
                                                  "source_path": str(path), "status": "observing"})
            return {"source": source, "status": "deferred"}
        if now - previous.get("observed_at", now) < stable_seconds or now - observed[3] / 1e9 < stable_seconds:
            return {"source": source, "status": "deferred"}
        if previous.get("status") == "review":
            return {"source": source, "status": "reused", "artifact_id": previous.get("artifact_id")}
        if not consume and previous.get("status") in {"accepted", "archived"}:
            # The exact observed source revision was already durably captured.
            # Retained inbox copies must not spend every run's read budget before
            # a newly dropped file can be reached. Managed edits have their own
            # corpus/Web Clipper reconciliation path.
            return {"source": source, "status": "reused", "artifact_id": previous.get("artifact_id"),
                    "destination": previous.get("destination"), "consumed": False}
        if path.suffix.lower() not in DESTINATIONS:
            return self._review(path, key, previous, observed,
                                f"Unsupported inbox file extension: {path.suffix or '(none)'}", lifecycle)
        if observed[2] > max_bytes:
            return self._review(path, key, previous, observed,
                                "Source exceeds configured max_source_bytes", lifecycle)
        # Budget failures remain explicit connector-run failures, not misleading
        # per-document review decisions. Already-persisted receipts resume safely.
        budget.add_file(path, count_input_tokens=False)
        try:
            payload = read_snapshot(path, observed, max_bytes)
            digest = sha256(payload)
            artifact_id = "inbox-" + digest
            suffix = path.suffix.lower()
            if suffix not in DESTINATIONS:
                raise InboxFileError(f"Unsupported inbox file extension: {suffix or '(none)'}")
            existing = self.db.get_ingestion_entry(artifact_id)
            reused = existing is not None
            if existing is not None:
                saved = json.loads(existing.payload_json)
                destination = Path(saved["source_path"])
                destination.resolve().relative_to(self.layout.vault_root.resolve())
                immutable_write(destination, payload)
            else:
                text, extraction = extract_snapshot(
                    payload, suffix, temp_root=control / "tmp",
                    max_text_chars=self._positive("max_text_chars", 500000),
                    pdf_max_pages=self._positive("pdf_max_pages", 40),
                )
                if extraction["text_truncated"]:
                    raise InboxFileError("Extracted text exceeds max_text_chars; increase the bound or review manually")
                if fingerprint(path) != observed:
                    raise InboxFileError("Source changed during extraction; rescan required")
                slug = re.sub(r"[^\w.-]+", "-", path.stem, flags=re.UNICODE).strip(".-")
                slug = slug.encode("utf-8")[:100].decode("utf-8", errors="ignore") or "document"
                destination = self.layout.vault_root / DESTINATIONS[suffix] / f"{slug}-{digest}{suffix}"
                immutable_write(destination, payload)
                artifact = MarkdownArtifact(
                    id=artifact_id, source_type="inbox", title=path.stem,
                    source_path=str(destination),
                    source_relative_path=destination.relative_to(self.layout.vault_root).as_posix(),
                    file_type=suffix.lstrip("."), source_checksum=digest,
                    source_size_bytes=len(payload), raw_content=text, body=text,
                    custom_metadata={"source_kind": "inbox", "inbox_source_path": str(path),
                                     "document_text": text, "document_extraction": extraction,
                                     "document_abstract": {"text": extract_document_abstract(text),
                                                           "method": "explicit_abstract_heading_v1"}},
                )
                self.queue.queue_artifact(
                    lifecycle, artifact, artifact_type="markdown", raw_path=destination,
                    source={"source_name": "inbox", "source_type": "inbox", "native_source_id": digest},
                    event={"event_type": "inbox_file", "native_event_id": digest,
                           "provenance": {"inbox_source_path": str(path), "sha256": digest,
                                          "raw_preserved": True}},
                )
                existing = self.db.get_ingestion_entry(artifact_id)
                if existing is None:
                    raise RuntimeError("Inbox artifact was not durably queued")
            receipt = {**previous, "fingerprint": observed, "source_path": str(path),
                       "artifact_id": artifact_id, "sha256": digest, "destination": str(destination),
                       "status": "accepted", "accepted_at": now}
            if not self.db.upsert_file(FileMetadata(
                path=str(destination), file_type="attachment" if suffix in {".pdf", ".docx"} else "note",
                source_id=artifact_id, hash=digest, size_bytes=len(payload),
            )):
                raise RuntimeError("Could not persist inbox source-to-queue association")
            self.db.upsert_automation_state(key, receipt)
            consumed = False
            stored_metadata = json.loads(existing.payload_json).get("normalized_metadata", {})
            if consume and existing.status in FINAL_QUEUE_STATES and not prompt_security_requires_review(stored_metadata):
                consumed = self._consume(path, observed, payload, destination, control, key, receipt)
            return {"source": source, "status": "reused" if reused else "queued",
                    "artifact_id": artifact_id, "destination": str(destination), "consumed": consumed,
                    "queue_status": existing.status}
        except (UnicodeError, OSError, ValueError, KeyError, PDFTextExtractionError) as exc:
            return self._review(path, key, previous, observed, str(exc), lifecycle)

    def _consume(self, path, observed, payload, destination, control, key, receipt):
        archive = control / "archive" / (receipt["sha256"] + path.suffix.lower())
        immutable_write(archive, payload)
        # No destructive step until archive, destination, queue and source all verify.
        if read_snapshot(destination, fingerprint(destination), len(payload)) != payload:
            raise InboxFileError("Destination changed before inbox consumption")
        if read_snapshot(path, observed, len(payload)) != payload:
            raise InboxFileError("Source changed before inbox consumption")
        receipt.update(archive_path=str(archive), status="archived")
        self.db.upsert_automation_state(key, receipt)
        # Recheck after the durable receipt boundary (sync can race the DB write).
        if read_snapshot(path, observed, len(payload)) != payload:
            raise InboxFileError("Source changed after archive receipt")
        # Claim the directory entry before removing it: a new sync revision
        # arriving at the original name after this rename must never be deleted.
        # A crash leaves a visibly named preserved file, not an invisible loss.
        basename = path.name.encode("utf-8")[:140].decode("utf-8", errors="ignore")
        claimed = path.with_name(basename + ".thoth-preserved-" + uuid.uuid4().hex)
        receipt.update(claim_path=str(claimed), status="claiming")
        self.db.upsert_automation_state(key, receipt)
        path.rename(claimed)
        try:
            claim_observed = fingerprint(claimed)
            # Rename itself changes ctime, but not device/inode/size/mtime.
            if claim_observed[:4] != observed[:4] or read_snapshot(claimed, claim_observed, len(payload)) != payload:
                raise InboxFileError("Source changed while claiming for archive; original retained")
            claimed.unlink()
        except (OSError, InboxFileError):
            if claimed.exists() and not path.exists():
                # Exclusive link avoids overwriting a revision that appears
                # between the existence check and restoration.
                try:
                    os.link(claimed, path, follow_symlinks=False)
                except FileExistsError:
                    pass  # Both revisions remain; the preserved one is reviewable.
                else:
                    claimed.unlink()
            raise
        receipt.update(status="consumed", consumed_at=time.time())
        self.db.upsert_automation_state(key, receipt)
        return True

    def _review(self, path, key, previous, observed, reason, lifecycle):
        artifact_id = "inbox-review-" + sha256((str(path) + repr(observed)).encode())
        reused = self.db.get_ingestion_entry(artifact_id) is not None
        if not reused:
            artifact = MarkdownArtifact(id=artifact_id, source_type="inbox", title=path.name,
                                        source_path=str(path), raw_content="", body="",
                                        custom_metadata={"inbox_review_reason": reason})
            # Seed a held row before the shared queue boundary: no worker can race
            # this unsupported input into processing between capture and review.
            if not self.db.upsert_ingestion_entry(IngestionQueueEntry(
                artifact_id=artifact_id, artifact_type="markdown", source="inbox",
                payload_json=json.dumps(artifact.to_dict()), status="needs_review",
            )):
                raise RuntimeError("Could not persist inbox review item")
            self.queue.queue_artifact(lifecycle, artifact, artifact_type="markdown", source="inbox")
            if self.db.mark_ingestion_review_required(
                artifact_id, category="inbox_input", reason=reason,
                metadata={"source_path": str(path), "original_retained": True},
            ) is None:
                raise RuntimeError("Could not mark inbox input for review")
        self.db.upsert_automation_state(key, {**previous, "fingerprint": observed,
                                              "artifact_id": artifact_id, "status": "review",
                                              "source_path": str(path), "reason": reason})
        return {"source": str(path), "status": "reused" if reused else "review",
                "artifact_id": artifact_id, "reason": reason}
