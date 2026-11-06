"""Scoped wiki proposals and Cissa publication acknowledgements.

Only Cissa applies proposals to the selected vault. Preparing a proposal never
advances the compiler's successful input snapshot or writes wiki files.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass, fields
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from .archivist_retrieval.inventory import resolve_archivist_root_spec
from .archivist_retrieval.service import select_archivist_candidates_async
from .archivist_state import (ArchivistSourceSnapshot, evaluate_archivist_dirty_check,
                             record_archivist_topic_snapshot, snapshot_archivist_candidates)
from .managed_execution import utc_time
from .wiki_change_provenance import resolve_source_path
from .wiki_publication import PublicationSnapshot


@dataclass(frozen=True)
class ManagedWikiContext:
    account_id: str
    archive_id: str
    job_id: str
    lease_generation: int
    author: str
    topic_ids: tuple[str, ...]
    source_cutoff: str
    source_manifest: Mapping[str, Mapping[str, Any]]
    expected_pages: Mapping[str, Mapping[str, Any]]
    account_root: Path

    def identity(self):
        return {name: getattr(self, name) for name in ("account_id", "archive_id", "job_id", "lease_generation")}

    def validate_runtime(self, layout, db, gateway):
        if not all((self.account_id, self.archive_id, self.job_id, self.author, self.topic_ids)) or type(self.lease_generation) is not int or self.lease_generation < 1:
            raise ValueError("Managed wiki identity, author, topics and lease are required")
        utc_time(self.source_cutoff)
        root = self.account_root.resolve()
        if not self.account_root.is_absolute():
            raise ValueError("Managed wiki root must be absolute")
        for field in fields(layout):
            path = getattr(layout, field.name)
            if isinstance(path, Path) and not path.resolve().is_relative_to(root):
                raise ValueError("Managed wiki path escapes account root")
        if Path(db.db_path).resolve() != layout.database_path.resolve():
            raise ValueError("Managed wiki database does not match layout")
        if gateway.identity != self.identity():
            raise ValueError("Managed model gateway identity does not match wiki job")
        self.allowed_files(layout)

    def allowed_files(self, layout):
        allowed = {}
        for name, source in self.source_manifest.items():
            if Path(name).is_absolute() or ".." in Path(name).parts:
                raise ValueError("Invalid managed wiki source path")
            if source.get("account_id") != self.account_id or source.get("archive_id") != self.archive_id:
                raise ValueError("Managed wiki source belongs to another account/archive")
            if not all(source.get(key) for key in ("source_id", "revision", "content_hash", "file_sha256", "observed_at")):
                raise ValueError("Managed wiki source requires revision/hash/time provenance")
            if utc_time(source["observed_at"]) > utc_time(self.source_cutoff):
                continue
            path = resolve_source_path(layout, name)
            if path is None or not path.resolve().is_relative_to(self.account_root.resolve()):
                raise ValueError("Managed wiki source escapes account root")
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != source["file_sha256"]:
                raise ValueError("Managed wiki source revision changed or disappeared")
            allowed[path.resolve()] = (name, dict(source))
        return allowed


async def propose_topic(compiler, topic, *, force, dry_run):
    from .archivist_compiler import ArchivistCompileResult
    context = compiler.managed_context
    if topic.id not in context.topic_ids:
        raise ValueError("Wiki topic is outside job grant")
    path = topic.output_path_for_root(compiler.layout.wiki_root)
    if not path.resolve().is_relative_to(compiler.layout.wiki_root.resolve()) or "06ChatGPT" in Path(topic.output_path).parts:
        raise ValueError("Wiki output is outside writable scope")
    expected = context.expected_pages.get(topic.output_path)
    if expected is None or "revision" not in expected or "content_hash" not in expected:
        raise ValueError("Wiki proposal needs an expected page revision")
    current = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    if current != expected["content_hash"]:
        return ArchivistCompileResult(topic.id, "blocked", "page_revision_changed", None, 0, (), None, None)
    for root_spec in (*topic.include_roots, *topic.exclude_roots):
        if not resolve_archivist_root_spec(root_spec, layout=compiler.layout).path.resolve().is_relative_to(context.account_root.resolve()):
            raise ValueError("Wiki retrieval root escapes account scope")
    allowed = context.allowed_files(compiler.layout)
    route = compiler._resolve_archivist_route()
    selection = await select_archivist_candidates_async(topic, config=compiler.config,
        layout=compiler.layout, db=compiler.db, llm_interface=compiler.llm_interface,
        path_filter=lambda candidate: candidate.resolve() in allowed)
    candidates = selection.candidates
    context.allowed_files(compiler.layout)
    dirty = evaluate_archivist_dirty_check(topic, candidates, route=route, db=compiler.db)
    if not candidates or (not force and not dirty.should_run):
        return ArchivistCompileResult(topic.id, "skipped", "no_candidates" if not candidates else dirty.reason, None,
                                      len(candidates), (), route[0], route[1])
    if dry_run:
        return ArchivistCompileResult(topic.id, "dry_run", dirty.reason, None, len(candidates), (), route[0], route[1])
    sources = []
    for index, candidate in enumerate(candidates, start=1):
        file_path = compiler._absolute_path_for_candidate(candidate).resolve()
        name, source = allowed[file_path]
        sources.append({**source, "source_path": name, "label": f"S{index}"})
    body = await compiler._generate_topic_body(topic, candidates, route=route)
    if not re.search(r"\[S\d+\]", body):
        raise ValueError("Managed wiki output must cite its supplied evidence")
    # Validate again after model execution; stale evidence is never publishable.
    context.allowed_files(compiler.layout)
    current = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    if current != expected["content_hash"]:
        return ArchivistCompileResult(topic.id, "blocked", "page_revision_changed", None, len(candidates), (), route[0], route[1])
    content, metadata = compiler._render_topic_page(topic, candidates, body=body, page_path=path,
        publication=PublicationSnapshot(topic.output_path, current, "clean" if current else "new"))
    proposal = {**context.identity(), "topic_id": topic.id, "path": topic.output_path,
        "author": context.author, "source_cutoff": context.source_cutoff,
        "expected_revision": expected["revision"], "expected_content_hash": expected["content_hash"],
        "content": content, "content_hash": hashlib.sha256(content.encode()).hexdigest(),
        "source_manifest": sources, "metadata": metadata}
    proposal_id = hashlib.sha256(json.dumps(proposal, sort_keys=True).encode()).hexdigest()
    proposal["proposal_id"] = proposal_id
    compiler.db.upsert_automation_state("managed_wiki:" + proposal_id,
        {"proposal": proposal, "snapshot": asdict(snapshot_archivist_candidates(candidates)), "route": list(route), "status": "proposed"})
    return ArchivistCompileResult(topic.id, "proposed", dirty.reason, None, len(candidates),
                                  tuple(source["source_path"] for source in sources), route[0], route[1], proposal)


def acknowledge_wiki_proposal(*, context, layout, db, proposal_id, receipt):
    """Called only by the authenticated Cissa publication boundary."""
    state = db.get_automation_state("managed_wiki:" + proposal_id)
    if state is None:
        raise ValueError("Unknown wiki proposal")
    proposal = state["proposal"]
    for key, value in context.identity().items():
        if proposal.get(key) != value or receipt.get(key) != value:
            raise ValueError("Wiki publication receipt identity mismatch")
    if receipt.get("proposal_id") != proposal_id:
        raise ValueError("Wiki publication receipt proposal mismatch")
    if receipt.get("status") != "applied":
        return {"status": "unapplied", "proposal_id": proposal_id}
    if receipt.get("content_hash") != proposal["content_hash"] or not receipt.get("revision"):
        raise ValueError("Wiki publication receipt revision/hash mismatch")
    for source in proposal["source_manifest"]:
        current = context.source_manifest.get(source["source_path"])
        if current != {key: value for key, value in source.items() if key not in {"source_path", "label"}}:
            raise ValueError("Wiki publication source revision no longer authorized")
    context.allowed_files(layout)
    if state["status"] != "applied":
        snapshot = ArchivistSourceSnapshot(**state["snapshot"])
        record_archivist_topic_snapshot(proposal["topic_id"], snapshot, route=tuple(state["route"]), db=db)
        state.update(status="applied", receipt=dict(receipt))
        db.upsert_automation_state("managed_wiki:" + proposal_id, state)
    return {"status": "acknowledged", "proposal_id": proposal_id, "revision": receipt["revision"]}
