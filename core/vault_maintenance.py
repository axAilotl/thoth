"""Explicit, recoverable migration of generated source pages out of Obsidian.

No prefix-only deletion: operator-approved plans pin complete document hashes,
require source identities, and retain records referenced by other vault notes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import unquote

from .source_records import SourceRecordStore
from .wiki_io import read_document


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _contained(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    if path.is_symlink() or not resolved.is_relative_to(root.resolve()):
        raise ValueError(f"Unsafe migration path: {path}")
    return resolved


def plan_source_pages(layout, *, obsidian_root: Path) -> dict:
    wiki = _contained(layout.wiki_root, obsidian_root)
    candidates = []
    for path in sorted((wiki / "pages").glob("*.md")):
        _contained(path, wiki)
        if path.stat().st_size > 16 * 1024 * 1024:
            continue
        doc = read_document(path)
        meta = doc.frontmatter
        artifact_id = meta.get("thoth_artifact_id")
        if not (meta.get("thoth_type") == "wiki_page" and artifact_id
                and meta.get("thoth_kind") != "topic"
                and meta.get("thoth_id") == path.stem
                and path.name.startswith(("clip-", "paper-", "repo-", "video-", "transcript-"))):
            continue
        sources = meta.get("thoth_source_paths", [])
        if not isinstance(sources, list) or not sources:
            continue
        missing = []
        resolved_sources = []
        for source in sources:
            candidate = layout.vault_root / source
            try:
                _contained(candidate, layout.vault_root)
                if not candidate.is_file():
                    # A cross-platform rename may change case. Only reconcile
                    # a unique case-equivalent name with the recorded checksum.
                    expected = next((m.get("sha256") for m in meta.get("thoth_input_manifest", [])
                                     if m.get("source_path") == source), None)
                    matches = [p for p in candidate.parent.iterdir()
                               if p.name.casefold() == candidate.name.casefold()] if candidate.parent.is_dir() else []
                    if len(matches) == 1 and expected:
                        _contained(matches[0], layout.vault_root)
                        if matches[0].is_file() and digest(matches[0]) == expected:
                            candidate = matches[0]
                        else:
                            missing.append(source)
                    else:
                        missing.append(source)
                resolved_sources.append(str(candidate.relative_to(layout.vault_root)))
            except ValueError:
                missing.append(source)
        candidates.append({"path": str(path.relative_to(obsidian_root)), "sha256": digest(path),
                           "artifact_id": artifact_id, "source_paths": sources,
                           "resolved_source_paths": resolved_sources,
                           "blocked_by": [f"missing/unsafe source: {s}" for s in missing]})
    by_slug = {Path(item["path"]).stem: item for item in candidates}
    # Check every Markdown note, not just generated wiki pages. Leave linked
    # wrappers in place; an intentional link rewrite needs a separate decision.
    import re
    pattern = re.compile("|".join(re.escape(slug) for slug in by_slug)) if by_slug else None
    if pattern:
        for path in obsidian_root.rglob("*.md"):
            if path == wiki / "index.md":
                continue
            _contained(path, obsidian_root)
            text = unquote(path.read_text(encoding="utf-8"))
            if path == wiki / "log.md" and text.lstrip().startswith("# Wiki Maintenance Log\n"):
                continue
            for slug in set(pattern.findall(text)):
                if path == obsidian_root / by_slug[slug]["path"]:
                    continue
                by_slug[slug]["blocked_by"].append(f"referenced by: {path.relative_to(obsidian_root)}")
    return {"schema": "thoth.source-page-migration/v1", "obsidian_root": str(obsidian_root.resolve()),
            "wiki_root": str(wiki), "vault_root": str(layout.vault_root.resolve()), "pages": candidates}


def apply_source_page_plan(plan: dict, *, archive_root: Path, db, layout) -> dict:
    if plan.get("schema") != "thoth.source-page-migration/v1":
        raise ValueError("Unsupported migration plan")
    obsidian = Path(plan["obsidian_root"])
    if archive_root.resolve().is_relative_to(obsidian.resolve()):
        raise ValueError("Archive must be outside the Obsidian vault")
    if str(layout.vault_root.resolve()) != plan["vault_root"] or str(layout.wiki_root.resolve()) != plan["wiki_root"]:
        raise ValueError("Migration plan belongs to another layout")
    # Re-evaluate links and source availability before any removals.
    current = {item["path"]: item for item in plan_source_pages(layout, obsidian_root=obsidian)["pages"]}
    store = SourceRecordStore(db)
    removed, skipped = [], []
    for item in plan["pages"]:
        path = _contained(obsidian / item["path"], obsidian)
        archive = archive_root / "source-pages" / f"{item['sha256']}.md"
        if not path.exists():
            if archive.is_file() and digest(archive) == item["sha256"]:
                skipped.append({"path": item["path"], "reason": "already archived"})
                continue
            raise ValueError(f"Missing source page without verified archive: {path}")
        fresh = current.get(item["path"])
        if not fresh or fresh["blocked_by"] or item["blocked_by"]:
            skipped.append({"path": item["path"], "reason": "referenced or source unavailable"})
            continue
        if digest(path) != item["sha256"]:
            skipped.append({"path": item["path"], "reason": "changed since plan"})
            continue
        content = path.read_bytes()
        archive.parent.mkdir(parents=True, exist_ok=True)
        _contained(archive, archive_root)
        if not archive.exists():
            with archive.open("xb") as out:
                out.write(content)
                out.flush()
                import os
                os.fsync(out.fileno())
            archive.chmod(0o600)
        if digest(archive) != item["sha256"]:
            raise ValueError(f"Archive checksum mismatch: {archive}")
        doc = read_document(path)
        store.archive(original_path=str(path), archive_path=str(archive),
                      document=content.decode("utf-8"), metadata=doc.frontmatter)
        entry = db.get_ingestion_entry(item["artifact_id"])
        if store.get(item["artifact_id"]) is None:
            payload = json.loads(entry.payload_json) if entry else {
                "id": item["artifact_id"], "source_paths": item["source_paths"],
                "imported_from_generated_page": True,
            }
            store.record_payload(item["artifact_id"], doc.frontmatter.get("thoth_source_type", "unknown"),
                payload, metadata={"imported_page": doc.frontmatter, "archive_sha256": item["sha256"],
                                   "resolved_source_paths": fresh["resolved_source_paths"]},
                canonical_id=doc.frontmatter.get("thoth_canonical_id"))
        # Preserve the exact prior record, not a re-render that drops unknown keys.
        if digest(path) != item["sha256"]:
            raise ValueError(f"Page changed during archive; original retained: {path}")
        path.unlink()
        removed.append(item["path"])
    return {"archived": removed, "skipped": skipped, "archive_root": str(archive_root)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "apply", "compact-topics", "export"))
    parser.add_argument("--obsidian-root", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--archive-root", type=Path)
    args = parser.parse_args()
    from .config import Config
    from .path_layout import build_path_layout
    from .metadata_db import MetadataDB
    config = Config()
    config.reload()
    layout = build_path_layout(config)
    if args.action == "plan":
        if not args.obsidian_root:
            parser.error("plan requires --obsidian-root")
        result = plan_source_pages(layout, obsidian_root=args.obsidian_root)
    elif args.action == "apply":
        if not args.plan or not args.archive_root:
            parser.error("apply requires --plan and --archive-root; back up the DB first")
        result = apply_source_page_plan(json.loads(args.plan.read_text()), archive_root=args.archive_root,
                                       db=MetadataDB(str(layout.database_path)), layout=layout)
    elif args.action == "compact-topics":
        if not args.obsidian_root or not args.plan or not args.archive_root:
            parser.error("compact-topics requires --obsidian-root, --plan (relative-path/hash JSON map), --archive-root")
        from .wiki_metadata_migration import compact_topic_pages
        result = compact_topic_pages(layout, db=MetadataDB(str(layout.database_path)),
            obsidian_root=args.obsidian_root, archive_root=args.archive_root,
            expected_hashes=json.loads(args.plan.read_text()))
    else:
        result = SourceRecordStore(MetadataDB(str(layout.database_path))).export()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
