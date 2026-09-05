"""Explicit hash-approved condensation of generated topic metadata."""
from __future__ import annotations

from pathlib import Path
import re

from .source_records import SourceRecordStore
from .vault_maintenance import _contained, digest
from .wiki_feedback import split_feedback
from .wiki_io import read_document, render_frontmatter
from .wiki_publication import WikiPublicationStore, minimal_wiki_frontmatter


def compact_topic_pages(layout, *, db, obsidian_root: Path, archive_root: Path,
                        expected_hashes: dict[str, str]) -> dict:
    """Keep prose and citations; move full metadata into revision/control stores.

    The supplied hashes are the operator's approved snapshot, not proof that an
    arbitrary page was machine-authored. Unknown or already-owned pages are held.
    """
    if archive_root.resolve().is_relative_to(obsidian_root.resolve()):
        raise ValueError("Topic archive must be outside Obsidian")
    publications = WikiPublicationStore(db, layout.wiki_root)
    archives = SourceRecordStore(db)
    migrated, skipped = [], []
    for relative, expected in expected_hashes.items():
        path = _contained(obsidian_root / relative, layout.wiki_root)
        if not path.is_file() or digest(path) != expected:
            skipped.append({"path": relative, "reason": "changed or missing"})
            continue
        doc = read_document(path)
        meta = doc.frontmatter
        current_topic = meta.get("thoth_kind") == "topic" and meta.get("thoth_input_manifest")
        # One-time migration of the earlier renderer, not a runtime fallback.
        earlier_topic = (meta.get("kind") == "topic" and meta.get("slug") == path.stem
                         and isinstance(meta.get("source_paths"), list) and meta["source_paths"]
                         and meta.get("created_at") and meta.get("updated_at"))
        if not ((current_topic or earlier_topic) and meta.get("thoth_type") == "wiki_page"):
            skipped.append({"path": relative, "reason": "not a generated topic with metadata"})
            continue
        if publications.inspect(path).status != "unowned":
            skipped.append({"path": relative, "reason": "already tracked; use guarded publication"})
            continue
        content = path.read_bytes()
        archive = archive_root / "topic-pages" / f"{expected}.md"
        archive.parent.mkdir(parents=True, exist_ok=True)
        _contained(archive, archive_root)
        if not archive.exists():
            with archive.open("xb") as stream:
                stream.write(content)
                stream.flush()
                import os
                os.fsync(stream.fileno())
            archive.chmod(0o600)
        if digest(archive) != expected:
            raise ValueError("Topic archive checksum mismatch")
        archives.archive(original_path=str(path), archive_path=str(archive),
                         document=content.decode(), metadata=meta)
        if earlier_topic and not current_topic:
            meta = dict(meta, thoth_id=meta["slug"], thoth_kind="topic",
                        thoth_updated_at=meta["updated_at"])
        snapshot = publications.adopt_baseline(path, expected_hash=expected, metadata=meta)
        body, _ = split_feedback(doc.body)
        prefix, separator, sources = body.rpartition("\n## Sources\n")
        if separator:
            sources = re.sub(r"^  - (?:Path|Type|Tags|Updated|Trust|Retrieval):[^\n]*(?:\n|$)",
                             "", sources, flags=re.M)
            body = prefix + separator + sources
        # The only changes to body are removal of known per-citation metadata.
        # Source links, findings and prose are retained exactly.
        minimal = render_frontmatter(minimal_wiki_frontmatter(meta)) + "\n" + body.lstrip("\n")
        publications.publish(path, minimal, snapshot=snapshot, metadata=meta, feedback_included=False)
        migrated.append(relative)
    return {"compacted": migrated, "skipped": skipped}
