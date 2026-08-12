"""Obsidian vault segment -> CCF archive import (checklist section 9).

Imports an Obsidian vault segment through the canonical producer ->
admission path, reusing the thothmap converters wherever their shape
fits:

- each top-level vault directory -> one ``core.source`` (plus one root
  source for the vault itself); git repository directories become source
  records too and are never blob-dumped;
- the import pass -> one ``core.session`` + one ``process.run``;
- each markdown note -> ``experience.artifact`` (``obsidian_note``) +
  embedded text Blob + ``ccf.has_blob`` / ``ccf.captured_in`` Links, with
  the vault-relative path as origin native ID and the content SHA-256 as
  origin revision;
- ``[[wikilinks]]`` between notes -> ``ccf.about`` Links between the note
  artifacts (the only non-acyclic registry type that fits a mutual
  reference; ``derived_from`` would reject Obsidian's cyclic graph).
  Mutual links land in the same atomic batch whenever the notes do;
- binary attachments referenced from notes -> Blob + ``experience.artifact``
  via :func:`ccf.thothmap.artifacts.media_submissions` plus a
  ``ccf.has_source_media`` Link from the note to the attachment Blob.
  Files up to ``embed_cap_bytes`` carry their bytes into the archive;
  larger files are admitted as manifest-only Blobs (no bytes), the
  archive-side form of a spec 2.5 external dependency;
- frontmatter parse failures are malformed documents: the note is
  skipped, recorded in the report with the parse error, and the import
  continues. Nothing is fabricated for missing attachments or
  unresolvable links — they are reported, never invented.

The importer keeps per-file object IDs in its report so unchanged files
re-map to byte-identical submissions (idempotent retry) and dishonest
re-submissions surface as ``origin_revision_conflict``.
"""

from __future__ import annotations

import mimetypes
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ccf.archive import Archive
from ccf.producer import Producer

from ccf.obsidian.notes import NoteParseError, parse_note
from ccf.obsidian.vault import VaultFile, scan_vault
from ccf.thothmap import artifacts as thothmap_artifacts
from ccf.thothmap import sessions as thothmap_sessions
from ccf.thothmap import sources as thothmap_sources
from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    claims,
    occurred_at,
    origin,
)

DEFAULT_EMBED_CAP_BYTES = 32 * 1024 * 1024  # embed cap default (spec 2.5)
DEFAULT_NOTES_PER_BATCH = 250
ATTACHMENTS_PER_BATCH = 50

_MEDIA_OVERRIDES = {
    ".md": "text/markdown",
    ".pdf": "application/pdf",
    ".json": "application/json",
}

_ATTACHMENT_SUFFIXES = {
    ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".mp3", ".mp4", ".wav", ".zip", ".epub",
}

_OK_OUTCOMES = {"admitted", "existing"}


class ObsidianImportError(RuntimeError):
    """Raised when an import pass cannot proceed safely."""


def media_type_for(relpath: str) -> str:
    """Best-effort MIME type for a vault file."""
    suffix = Path(relpath).suffix.lower()
    if suffix in _MEDIA_OVERRIDES:
        return _MEDIA_OVERRIDES[suffix]
    guessed, _encoding = mimetypes.guess_type(relpath)
    return guessed or "application/octet-stream"


def _json_text(text: str) -> str:
    """Make derived text storable in Postgres jsonb.

    jsonb rejects NUL code points and unpaired surrogates. Derived fields
    (title, excerpt, frontmatter values) are search summaries, never
    evidence — the note's raw bytes stay verbatim in its Blob — so
    offending code points become U+FFFD here rather than crashing an
    import run halfway through.
    """
    return "".join(
        "\ufffd" if ch == "\x00" or 0xD800 <= ord(ch) <= 0xDFFF else ch
        for ch in text
    )


def _json_safe(value: object, *, _depth: int = 0) -> object:
    """Convert YAML-parsed frontmatter into JSON-canonical values.

    YAML produces ``datetime``/``date`` scalars that JCS and jsonb cannot
    represent; they become ISO-8601 strings. Anything else exotic becomes
    its ``str`` form — frontmatter is metadata; the note bytes remain the
    authoritative content. Strings pass through :func:`_json_text` so
    hostile code points cannot crash admission, and nesting is capped so
    a pathological document cannot exhaust the call stack.
    """
    if value is None or isinstance(value, (int, bool)):
        return value
    if isinstance(value, float):
        # JCS rejects NaN/Infinity outright (spec 4.2); a non-finite
        # frontmatter scalar becomes its text form, never an admission
        # crash.
        import math

        return value if math.isfinite(value) else str(value)
    if isinstance(value, str):
        return _json_text(value)
    if _depth >= 100:
        return _json_text(str(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            _json_text(str(key)): _json_safe(item, _depth=_depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item, _depth=_depth + 1) for item in value]
    return _json_text(str(value))


@dataclass
class NoteRecord:
    """Importer state for one imported note."""

    relpath: str
    segment: str
    artifact_id: str
    blob_id: str
    has_blob_id: str
    captured_in_id: str
    revision: str  # content sha256
    title: str
    excerpt: str
    submissions: dict = field(default_factory=dict)  # original signed submissions
    batch_id: str = ""


@dataclass
class ImportReport:
    """Everything one import pass committed — and everything it skipped."""

    vault_root: str
    run_tag: str
    embed_cap_bytes: int
    sources: dict[str, str] = field(default_factory=dict)  # segment -> core.source URN
    session_id: str = ""
    run_id: str = ""
    notes: dict[str, NoteRecord] = field(default_factory=dict)  # relpath -> record
    attachment_blobs: dict[str, str] = field(default_factory=dict)  # relpath -> blob URN
    wikilink_edges: list[dict] = field(default_factory=list)
    attachment_links: list[dict] = field(default_factory=list)
    missing_attachments: list[dict] = field(default_factory=list)
    unresolved_links: list[dict] = field(default_factory=list)
    malformed: list[dict] = field(default_factory=list)
    admission_errors: list[dict] = field(default_factory=list)
    batches: list[dict] = field(default_factory=list)
    signed_batches: dict[str, dict] = field(default_factory=dict)
    bytes_embedded: int = 0
    bytes_external: int = 0
    objects_committed: int = 0

    @property
    def incomplete(self) -> bool:
        """True when anything was skipped (spec 2.5 honesty, no fabrication)."""
        return bool(self.missing_attachments or self.malformed or self.unresolved_links)

    def summary(self) -> dict:
        return {
            "vault_root": self.vault_root,
            "run_tag": self.run_tag,
            "sources": len(self.sources),
            "notes_imported": len(self.notes),
            "attachment_blobs": len(self.attachment_blobs),
            "wikilink_edges": len(self.wikilink_edges),
            "attachment_links": len(self.attachment_links),
            "missing_attachments": len(self.missing_attachments),
            "unresolved_links": len(self.unresolved_links),
            "malformed": len(self.malformed),
            "admission_errors": len(self.admission_errors),
            "batches": len(self.batches),
            "objects_committed": self.objects_committed,
            "bytes_embedded": self.bytes_embedded,
            "bytes_external": self.bytes_external,
            "incomplete": self.incomplete,
        }


class ObsidianImporter:
    """One producer+archive-bound importer for one vault tree."""

    def __init__(
        self,
        *,
        producer: Producer,
        archive: Archive,
        ctx: MapContext,
        vault_root: str | Path,
        embed_cap_bytes: int = DEFAULT_EMBED_CAP_BYTES,
        notes_per_batch: int = DEFAULT_NOTES_PER_BATCH,
        run_tag: str | None = None,
    ) -> None:
        self._producer = producer
        self._archive = archive
        self._ctx = ctx
        self.vault_root = Path(vault_root)
        if not self.vault_root.is_dir():
            raise ObsidianImportError(f"vault root is not a directory: {vault_root}")
        self.embed_cap_bytes = int(embed_cap_bytes)
        if self.embed_cap_bytes <= 0:
            raise ObsidianImportError("embed_cap_bytes must be positive")
        self.notes_per_batch = int(notes_per_batch)
        if self.notes_per_batch <= 0:
            raise ObsidianImportError("notes_per_batch must be positive")
        self.run_tag = run_tag or producer.clock()
        self.report = ImportReport(
            vault_root=str(self.vault_root),
            run_tag=self.run_tag,
            embed_cap_bytes=self.embed_cap_bytes,
        )
        self._session_id: str | None = None
        self._segment_of_fn = self._segment_of
        self._note_targets: dict[str, list[str]] = {}
        self._binary_index: dict[str, VaultFile] = {}
        self._pending_sources: list[dict] = []
        self._pending_edges: list[dict] = []
        self._pending_attachments = MappedSubmissions()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def ctx(self) -> MapContext:
        """The producer-side identity context this importer maps with."""
        return self._ctx

    def import_vault(self) -> ImportReport:
        """Run the full import pass: sources, session/run, notes, blobs."""
        layout = scan_vault(self.vault_root)
        self._segment_of_fn = self._segment_of
        self._index_layout(layout)
        self._admit_preamble(layout)
        self._import_notes(layout.notes)
        self._import_binaries(layout.binaries)
        self._flush_deferred_links()
        return self.report

    def import_probe_tree(self, segment: str, path: str | Path) -> None:
        """Import an extra directory as one segment (malformed-doc probes).

        Runs against the same archive after the main pass; probe outcomes
        land in the same report so a skipped document is visible next to
        the real corpus results.
        """
        if not self._session_id:
            raise ObsidianImportError("import_vault must run before probe trees")
        layout = scan_vault(path)
        source_id = self._ensure_source(segment, kind="obsidian_vault_segment")
        self._flush_sources()
        self._segment_of_fn = lambda _f: (segment, source_id)
        self._index_layout(layout)
        self._import_notes(layout.notes)
        self._import_binaries(layout.binaries)
        self._flush_deferred_links()
        self._segment_of_fn = self._segment_of

    def remap_note(
        self,
        relpath: str,
        *,
        reuse_ids: bool,
        revision: str | None = None,
        text_override: str | None = None,
    ) -> MappedSubmissions:
        """Rebuild one imported note's submissions (retry/conflict probes).

        ``reuse_ids=True`` replays the note's original signed submissions
        verbatim (the crash-retry granularity: identical submissions get
        idempotent ``existing`` outcomes). ``revision`` forces the origin
        revision while ``text_override`` changes the signed content under
        fresh object IDs — together they build the same-tuple /
        different-hash dishonest-replay case.
        """
        record = self.report.notes.get(relpath)
        if record is None:
            raise ObsidianImportError(f"note was not imported: {relpath}")
        path = self.vault_root / relpath
        if reuse_ids:
            if revision is not None or text_override is not None:
                raise ObsidianImportError(
                    "identical replay cannot also alter revision or content"
                )
            data = path.read_bytes()
            subs = record.submissions
            return MappedSubmissions(
                records=[dict(subs["artifact"])],
                links=[dict(subs["has_blob"]), dict(subs["captured_in"])],
                blobs=[dict(subs["blob"])],
                blob_data={subs["blob"]["id"]: data},
            )
        source_id = self.report.sources[record.segment]
        data = (
            text_override.encode("utf-8")
            if text_override is not None
            else path.read_bytes()
        )
        parsed = parse_note(data.decode("utf-8"), fallback_title=Path(relpath).stem)
        return self._note_submissions(
            record.segment,
            source_id,
            relpath,
            data=data,
            parsed=parsed,
            sha256=record.revision,
            revision=revision or record.revision,
            occurred=datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc),
            reuse=None,
        )

    # ------------------------------------------------------------------
    # Sources, session, run
    # ------------------------------------------------------------------

    def _segment_of(self, vfile: VaultFile) -> tuple[str, str]:
        parts = Path(vfile.relpath).parts
        segment = parts[0] if len(parts) > 1 else "_vault_root"
        return segment, self._ensure_source(segment, kind="obsidian_vault_segment")

    def _ensure_source(self, segment: str, *, kind: str) -> str:
        existing = self.report.sources.get(segment)
        if existing is not None:
            return existing
        if "_vault" not in self.report.sources:
            vault = thothmap_sources.source_submission(
                self._producer,
                self._ctx,
                {
                    "source_name": self.vault_root.name or "obsidian-vault",
                    "source_type": "obsidian_vault",
                    "collector": "ccf.obsidian",
                    "native_source_id": str(self.vault_root),
                },
                trust_class="trusted",
            )
            self._pending_sources.extend(vault.records)
            self.report.sources["_vault"] = vault.records[0]["id"]
        mapped = thothmap_sources.source_submission(
            self._producer,
            self._ctx,
            {
                "source_name": segment,
                "source_type": kind,
                "collector": "ccf.obsidian",
                "native_source_id": f"{self.vault_root}/{segment}",
            },
            trust_class="trusted",
        )
        self._pending_sources.extend(mapped.records)
        self.report.sources[segment] = mapped.records[0]["id"]
        return mapped.records[0]["id"]

    def _flush_sources(self) -> None:
        if self._pending_sources:
            self._admit(MappedSubmissions(records=self._pending_sources), purpose="sources")
            self._pending_sources = []

    def _index_layout(self, layout) -> None:
        for vfile in layout.notes:
            keys = {
                vfile.stem.lower(),
                vfile.relpath.lower(),
                vfile.relpath[: -len(".md")].lower(),
            }
            for key in keys:
                bucket = self._note_targets.setdefault(key, [])
                if vfile.relpath not in bucket:
                    bucket.append(vfile.relpath)
                    bucket.sort()
        for binary in layout.binaries:
            self._binary_index[binary.relpath.lower()] = binary
            self._binary_index.setdefault(binary.abspath.name.lower(), binary)

    def _admit_preamble(self, layout) -> None:
        """First batch: sources, the import session, and the process run."""
        for vfile in layout.notes + layout.binaries:
            self._segment_of(vfile)
        for repo_relpath in layout.repos:
            mapped = thothmap_sources.source_submission(
                self._producer,
                self._ctx,
                {
                    "source_name": Path(repo_relpath).name,
                    "source_type": "git_repository",
                    "collector": "ccf.obsidian",
                    "native_source_id": f"{self.vault_root}/{repo_relpath}",
                },
                trust_class="trusted",
            )
            self._pending_sources.extend(mapped.records)
            self.report.sources[f"repo:{repo_relpath}"] = mapped.records[0]["id"]

        vault_source = self.report.sources["_vault"]
        started = self._producer.clock()
        session = thothmap_sessions.session_submission(
            self._producer,
            self._ctx,
            {
                "session_id": f"obsidian-import:{self.run_tag}",
                "started_at": started,
                "session_type": "import",
                "status": "completed",
                "metadata": {"capture_mode": "import"},
            },
            source_ccf_id=vault_source,
        )
        run = thothmap_sessions.run_submission(
            self._producer,
            self._ctx,
            {
                "run_id": f"obsidian-import-run:{self.run_tag}",
                "status": "completed",
                "started_at": started,
                "finished_at": self._producer.clock(),
                "connector_name": "ccf.obsidian",
            },
            source_ccf_id=vault_source,
            run_kind="ingestion",
            task=f"Obsidian vault import {self.vault_root}",
        )
        self.report.session_id = session.records[0]["id"]
        self.report.run_id = run.records[0]["id"]
        self._session_id = session.records[0]["id"]
        self._admit(
            MappedSubmissions(
                records=self._pending_sources + session.records + run.records
            ),
            purpose="sources",
        )
        self._pending_sources = []

    # ------------------------------------------------------------------
    # Notes
    # ------------------------------------------------------------------

    def _import_notes(self, notes: list[VaultFile]) -> None:
        for offset in range(0, len(notes), self.notes_per_batch):
            chunk = notes[offset : offset + self.notes_per_batch]
            combined = MappedSubmissions()
            chunk_relpaths = {vfile.relpath for vfile in chunk}
            for vfile in chunk:
                segment, source_id = self._segment_of_fn(vfile)
                data = vfile.abspath.read_bytes()
                try:
                    parsed = parse_note(data.decode("utf-8"), fallback_title=vfile.stem)
                except (NoteParseError, UnicodeDecodeError) as exc:
                    self.report.malformed.append(
                        {"relpath": vfile.relpath, "segment": segment, "error": str(exc)}
                    )
                    continue
                mapped = self._note_submissions(
                    segment,
                    source_id,
                    vfile.relpath,
                    data=data,
                    parsed=parsed,
                    sha256=vfile.sha256,
                    revision=vfile.sha256,
                    occurred=datetime.fromtimestamp(
                        vfile.abspath.stat().st_mtime, tz=timezone.utc
                    ),
                    reuse=None,
                )
                self.report.notes[vfile.relpath] = NoteRecord(
                    relpath=vfile.relpath,
                    segment=segment,
                    artifact_id=mapped.records[0]["id"],
                    blob_id=mapped.blobs[0]["id"],
                    has_blob_id=mapped.links[0]["id"],
                    captured_in_id=mapped.links[1]["id"],
                    revision=vfile.sha256,
                    title=parsed.title,
                    excerpt=parsed.excerpt,
                    submissions={
                        "artifact": mapped.records[0],
                        "blob": mapped.blobs[0],
                        "has_blob": mapped.links[0],
                        "captured_in": mapped.links[1],
                    },
                )
                combined.extend(mapped)
                self.report.bytes_embedded += len(data)
                self._queue_note_edges(vfile, parsed, mapped.records[0]["id"])
            self._collect_chunk_edges(combined, chunk_relpaths)
            # Attachment blobs referenced by this chunk's links must be
            # admitted before the notes batch that references them.
            self._flush_attachments()
            self._admit(combined, purpose="notes")
            for vfile in chunk:
                record = self.report.notes.get(vfile.relpath)
                if record is not None and not record.batch_id:
                    record.batch_id = self.report.batches[-1]["batch_id"]

    def _note_submissions(
        self,
        segment: str,
        source_id: str,
        relpath: str,
        *,
        data: bytes,
        parsed,
        sha256: str,
        revision: str,
        occurred: datetime,
        reuse: dict | None,
    ) -> MappedSubmissions:
        """One note -> artifact + embedded text Blob + provenance Links."""
        note_claims = claims(self._ctx, data_classes=["document_content"])
        blob_sub, blob_bytes = self._producer.new_blob(
            data=data,
            media_type="text/markdown",
            claims=note_claims,
            origin=origin(source_id, relpath, revision),
            blob_id=reuse["blob_id"] if reuse else None,
        )
        external_uri = parsed.frontmatter.get("source")
        if not isinstance(external_uri, str) or not external_uri.strip():
            external_uri = None
        else:
            external_uri = _json_text(external_uri)
        tags = parsed.frontmatter.get("tags")
        if not isinstance(tags, list):
            tags = [tags] if isinstance(tags, str) else []
        artifact = self._producer.new_record(
            type="experience.artifact",
            claims=note_claims,
            occurred_at=occurred_at(occurred),
            origin=origin(source_id, relpath, revision),
            object_id=reuse["artifact_id"] if reuse else None,
            payload={
                "name": _json_text(parsed.title),
                "media_type": "text/markdown",
                "description": _json_text(parsed.excerpt or parsed.title),
                "external_uri": external_uri,
                "artifact_role": "obsidian_note",
                "extensions": {
                    "obsidian_relpath": relpath,
                    "obsidian_segment": segment,
                    "obsidian_sha256": sha256,
                    "obsidian_frontmatter": _json_safe(parsed.frontmatter),
                    "obsidian_tags": [_json_text(str(tag)) for tag in tags],
                },
            },
        )
        has_blob = self._producer.new_link(
            type="ccf.has_blob",
            from_id=artifact["id"],
            to_id=blob_sub["id"],
            claims=claims(self._ctx),
            selector={},
            link_id=reuse["has_blob_id"] if reuse else None,
        )
        captured_in = self._producer.new_link(
            type="ccf.captured_in",
            from_id=artifact["id"],
            to_id=self._session_id,
            claims=claims(self._ctx),
            selector={},
            link_id=reuse["captured_in_id"] if reuse else None,
        )
        return MappedSubmissions(
            records=[artifact],
            links=[has_blob, captured_in],
            blobs=[blob_sub],
            blob_data={blob_sub["id"]: blob_bytes},
        )

    # ------------------------------------------------------------------
    # Edges: wikilinks and note->attachment links
    # ------------------------------------------------------------------

    def _queue_note_edges(self, vfile: VaultFile, parsed, artifact_id: str) -> None:
        for link in parsed.links:
            target_relpath = self._resolve_note_target(link.target)
            if target_relpath is not None and target_relpath != vfile.relpath:
                self._pending_edges.append(
                    {
                        "kind": "wikilink",
                        "from_relpath": vfile.relpath,
                        "to_relpath": target_relpath,
                        "from_id": artifact_id,
                        "to_id": None,
                        "link_id": None,
                    }
                )
                continue
            if target_relpath is not None:
                continue  # self-link: not a graph edge
            binary = self._resolve_binary_target(link.target)
            if binary is not None:
                blob_id = self._blob_for_attachment(binary)
                self._pending_edges.append(
                    {
                        "kind": "attachment",
                        "from_relpath": vfile.relpath,
                        "to_relpath": binary.relpath,
                        "from_id": artifact_id,
                        "to_id": blob_id,
                        "link_id": None,
                    }
                )
                continue
            entry = {"note": vfile.relpath, "target": link.target, "embed": link.embed}
            if link.embed or self._looks_like_attachment(link.target):
                self.report.missing_attachments.append(entry)
            else:
                self.report.unresolved_links.append(entry)

    def _resolve_note_target(self, target: str) -> str | None:
        key = target.strip().lower().replace("\\", "/")
        for candidate in (key, f"{key}.md"):
            bucket = self._note_targets.get(candidate)
            if bucket:
                return bucket[0]
        return None

    def _resolve_binary_target(self, target: str) -> VaultFile | None:
        return self._binary_index.get(target.strip().lower().replace("\\", "/"))

    def _looks_like_attachment(self, target: str) -> bool:
        return Path(target).suffix.lower() in _ATTACHMENT_SUFFIXES

    def _collect_chunk_edges(self, combined: MappedSubmissions, chunk_relpaths) -> None:
        """Build edges whose targets are in this chunk or already admitted.

        Wikilinks to notes admitted in later chunks defer to the closure
        batch; links inside one chunk land in its single atomic batch
        (checklist 9: same-batch object graph).
        """
        remaining = []
        for edge in self._pending_edges:
            if edge["to_id"] is None:
                target = self.report.notes.get(edge["to_relpath"])
                if target is None:
                    if edge["to_relpath"] in chunk_relpaths:
                        # Target note was skipped (malformed): the link is
                        # unresolvable, never fabricated.
                        self.report.unresolved_links.append(
                            {"note": edge["from_relpath"], "target": edge["to_relpath"]}
                        )
                    else:
                        remaining.append(edge)
                    continue
                edge["to_id"] = target.artifact_id
            link = self._build_edge(edge)
            combined.links.append(link)
        self._pending_edges = remaining

    def _flush_deferred_links(self) -> None:
        if not self._pending_edges:
            return
        combined = MappedSubmissions()
        for edge in self._pending_edges:
            if edge["to_id"] is None:
                target = self.report.notes.get(edge["to_relpath"])
                if target is None:
                    self.report.unresolved_links.append(
                        {"note": edge["from_relpath"], "target": edge["to_relpath"]}
                    )
                    continue
                edge["to_id"] = target.artifact_id
            combined.links.append(self._build_edge(edge))
        self._pending_edges = []
        if combined.links:
            self._admit(combined, purpose="links")

    def _build_edge(self, edge: dict) -> dict:
        link = self._producer.new_link(
            type="ccf.about" if edge["kind"] == "wikilink" else "ccf.has_source_media",
            from_id=edge["from_id"],
            to_id=edge["to_id"],
            claims=claims(self._ctx),
            selector={},
        )
        edge["link_id"] = link["id"]
        (
            self.report.wikilink_edges
            if edge["kind"] == "wikilink"
            else self.report.attachment_links
        ).append(edge)
        return link

    # ------------------------------------------------------------------
    # Attachments and unreferenced binaries
    # ------------------------------------------------------------------

    def _blob_for_attachment(self, binary: VaultFile) -> str:
        blob_id = self.report.attachment_blobs.get(binary.relpath)
        if blob_id is None:
            blob_id = self._attachment_submissions(binary)
        return blob_id

    def _attachment_submissions(self, binary: VaultFile) -> str:
        """Binary file -> attachment artifact + Blob (embedded or external)."""
        segment, source_id = self._segment_of_fn(binary)
        data = binary.abspath.read_bytes()
        embedded = binary.size_bytes <= self.embed_cap_bytes
        mapped = thothmap_artifacts.media_submissions(
            self._producer,
            self._ctx,
            {
                "raw_ref_id": binary.relpath,
                "path": binary.relpath,
                "sha256": binary.sha256,
                "size_bytes": binary.size_bytes,
                "mime_type": media_type_for(binary.relpath),
                "created_at": datetime.fromtimestamp(
                    binary.abspath.stat().st_mtime, tz=timezone.utc
                ),
            },
            data=data,
            source_ccf_id=source_id,
            session_ccf_id=self._session_id,
            revision=binary.sha256,
            artifact_role="vault_attachment",
            description=f"Vault attachment {binary.relpath}",
        )
        blob_id = mapped.blobs[0]["id"]
        if embedded:
            self.report.bytes_embedded += binary.size_bytes
            self._pending_attachments.blob_data.update(mapped.blob_data)
        else:
            # External per spec 2.5: manifest-only Blob, no bytes transfer.
            self.report.bytes_external += binary.size_bytes
        self.report.attachment_blobs[binary.relpath] = blob_id
        self._pending_attachments.extend(
            MappedSubmissions(
                records=mapped.records, links=mapped.links, blobs=mapped.blobs
            )
        )
        return blob_id

    def _import_binaries(self, binaries: list[VaultFile]) -> None:
        """Every unreferenced vault binary still gets a Blob manifest."""
        self._flush_attachments()
        for binary in binaries:
            if binary.relpath in self.report.attachment_blobs:
                continue
            self._attachment_submissions(binary)
            if len(self._pending_attachments.blobs) >= ATTACHMENTS_PER_BATCH:
                self._flush_attachments()
        self._flush_attachments()

    def _flush_attachments(self) -> None:
        pending = self._pending_attachments
        if pending.records or pending.links or pending.blobs:
            self._pending_attachments = MappedSubmissions()
            self._admit(pending, purpose="attachments")

    # ------------------------------------------------------------------
    # Admission
    # ------------------------------------------------------------------

    def _admit(self, mapped: MappedSubmissions, *, purpose: str) -> dict:
        if not (mapped.records or mapped.links or mapped.blobs):
            return {"status": "skipped", "admissions": []}
        batch = self._producer.create_batch(
            records=mapped.records,
            links=mapped.links,
            blobs=mapped.blobs,
            blob_data=mapped.blob_data or None,
        )
        result = self._archive.admit_batch(batch, blob_bytes=mapped.blob_data or None)
        batch_id = batch["batch_id"]
        object_ids = [
            sub["id"]
            for group in (mapped.records, mapped.links, mapped.blobs)
            for sub in group
        ]
        self.report.batches.append(
            {
                "batch_id": batch_id,
                "purpose": purpose,
                "status": result["status"],
                "commit_sequence": result.get("commit_sequence"),
                "object_ids": object_ids,
            }
        )
        self.report.signed_batches[batch_id] = batch
        for admission in result.get("admissions", []):
            status = admission.get("status")
            if status in _OK_OUTCOMES:
                if status == "admitted":
                    self.report.objects_committed += 1
                continue
            self.report.admission_errors.append(
                {"batch_id": batch_id, "purpose": purpose, **admission}
            )
        return result
