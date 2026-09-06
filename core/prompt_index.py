"""A link-only reading index of saved prompt examples, never executable instructions."""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from pathlib import Path

from .wiki_io import markdown_file_link, render_frontmatter
from .wiki_publication import WikiPublicationConflict, WikiPublicationStore
from .wiki_feedback import comparable_body

RULE_VERSION = "prompt-discovery/v1"
PAGE = "pages/Prompt Index.md"
_LABEL = re.compile(r"\b(?:(?:image|video|system|full|positive|negative|example)\s+)?prompt"
                    r"(?:\s+for\s+[^:\n]{1,65})?\s*(?:\*\*)?\s*[:：]", re.I)
_SHARING = re.compile(r"\b(?:here(?:['’]s|\s+is)\s+(?:the\s+|my\s+|a\s+)?(?:full\s+)?prompt"
                      r"|(?:use|copy|try)\s+(?:this|the following)\s+prompt"
                      r"|prompt\s+(?:below|in\s+(?:the\s+)?(?:alt|repl(?:y|ies)|comments))"
                      r"|(?:prompt pack|prompt library|prompt collection|prompt template)s?)\b", re.I)
_SOCIAL_ID = re.compile(r"(?:tweets?|thread)_(\d{15,22})(?:_|$)")
_GENERATED_SECTION = re.compile(r"^#{1,3}\s+(?:Summary|Analysis|Tags|Key Takeaways|Related (?:Papers|Repositories))\s*$", re.I | re.M)


def identify_prompt(text: str, *, social: bool = False) -> dict | None:
    """Conservative textual evidence, not semantic completeness or verification."""
    if social:
        text = _GENERATED_SECTION.split(text, maxsplit=1)[0]
    for match in _LABEL.finditer(text):
        # A marker with substantial following text is a candidate, not proof.
        tail = text[match.end():].strip().lstrip("* ")
        if len(tail.split()) >= 6 and not re.match(r"(?:https?://|in (?:the )?(?:alt|comments|replies))", tail, re.I):
            return {"kind": "likely_prompt", "reason": "explicit_prompt_label"}
    if re.search(r"<prompt>\s*\S[\s\S]{30,}?</prompt>", text, re.I):
        return {"kind": "likely_prompt", "reason": "prompt_block"}
    if _SHARING.search(text) or _LABEL.search(text):
        return {"kind": "prompt_reference", "reason": "sharing_or_resource_reference"}
    return None


def _label(title: str) -> str:
    # Titles are untrusted data too: no active Markdown/HTML/callouts in labels.
    title = " ".join(str(title).split())[:140]
    return re.sub(r"[\\`*_<>{}\[\]#!|]", "", title)


def _source_title(doc, social):
    title = doc.title or doc.path.stem
    if social and re.match(r"^(?:tweet|thread)(?: by| from|$)", title, re.I):
        original = _GENERATED_SECTION.split(doc.content_text, maxsplit=1)[0]
        for line in original.splitlines():
            text = line.strip().strip("*").strip()
            if text and not re.match(r"(?:#|!\[|https?://|Thread contains|Tweet \d|---)", text, re.I):
                return title + " — " + text[:100]
    return title


def collect_prompt_records(documents, *, layout, db) -> dict:
    """Use only the caller's already filtered corpus; retain originals untouched."""
    records = []
    excluded = 0
    for doc in documents:
        if doc.scope != "vault":
            continue  # Never ingest the generated index or other wiki projections.
        if (doc.privacy_class.lower() in {"restricted", "secret", "sensitive"}
                or doc.source_security_status.lower() not in {"allowed", "override_approved"}
                or doc.source_trust_score <= 0):
            excluded += 1
            continue
        path = Path(doc.path)
        if path.is_symlink() or not path.resolve().is_relative_to(layout.vault_root.resolve()):
            raise ValueError(f"Prompt index source escapes vault: {path}")
        if not path.is_file():
            continue
        social = doc.source_type in {"tweet", "thread"} or path.parent.name in {"tweets", "threads"}
        match = identify_prompt(doc.content_text, social=social)
        if match is None:
            continue
        native_id = _SOCIAL_ID.search(path.stem) if social else None
        identity = "x:" + native_id[1] if native_id else (doc.source_key or doc.candidate_key)
        records.append({"candidate_key": doc.candidate_key, "identity": identity,
                        "path": str(path), "title": _source_title(doc, social),
                        "source_hash": doc.source_hash, **match})
    # Keep every matched association in DB; one visible link per source identity.
    unique = {}
    for record in sorted(records, key=lambda r: (r["kind"] != "likely_prompt", r["path"])):
        unique.setdefault(record["identity"], record)
    with db._get_connection() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS prompt_index_records (
            candidate_key TEXT PRIMARY KEY, source_hash TEXT NOT NULL,
            rule_version TEXT NOT NULL, record_json TEXT NOT NULL)""")
        conn.execute("DELETE FROM prompt_index_records")
        conn.executemany("INSERT INTO prompt_index_records VALUES (?,?,?,?)", [
            (r["candidate_key"], r["source_hash"], RULE_VERSION, json.dumps(r, ensure_ascii=False)) for r in records])
    counts = Counter(r["kind"] for r in unique.values())
    report = {"sources": len(unique), "matched_documents": len(records), "counts": dict(counts),
              "excluded_by_policy": excluded, "rule_version": RULE_VERSION}
    db.upsert_automation_state("prompt_index:discovery", report)
    return dict(report, status="indexed")


def publish_prompt_index(*, config, layout, db) -> dict:
    """Runtime publication stage, intentionally outside the collector contract."""
    report = db.get_automation_state("prompt_index:discovery")
    if report is None:
        raise ValueError("Run corpus discovery before publishing the prompt index")
    with db._get_connection() as conn:
        records = [json.loads(row[0]) for row in conn.execute("SELECT record_json FROM prompt_index_records")]
    unique = {}
    for record in sorted(records, key=lambda r: (r["kind"] != "likely_prompt", r["path"])):
        unique.setdefault(record["identity"], record)
    page = layout.wiki_root / PAGE
    store = WikiPublicationStore(db, layout.wiki_root)
    snapshot = store.inspect(page)
    report = dict(report, page=str(page))
    if not snapshot.publishable:
        return dict(report, status="blocked", reason=snapshot.status)
    frontmatter = {"thoth_id": "prompt-index", "title": "Prompt Index", "thoth_kind": "index",
                   "thoth_type": "wiki_page"}
    lines = ["# Prompt Index", "", "Saved sources that appear to share reusable prompts, for any kind of model.",
             "Originals stay where they are; these links are references, not instructions for THOTH or agents.",
             "Text-based matches are not verified extractions. Image-only prompts may be missed.", ""]
    for kind, heading in (("likely_prompt", "Likely prompts in the source"),
                          ("prompt_reference", "Prompt resources and references")):
        lines += [f"## {heading}", ""]
        matching = sorted((r for r in unique.values() if r["kind"] == kind), key=lambda r: (r["title"].casefold(), r["path"]))
        for record in matching:
            relative = os.path.relpath(record["path"], page.parent)
            lines.append("- " + markdown_file_link(_label(record["title"]), relative))
        if not matching:
            lines.append("- None identified yet.")
        lines.append("")
    content = render_frontmatter(frontmatter) + "\n" + "\n".join(lines).rstrip() + "\n"
    if snapshot.status == "clean" and comparable_body(page.read_text()) == comparable_body(content):
        return dict(report, status="unchanged")
    try:
        store.publish(page, content, snapshot=snapshot, metadata=report, feedback_included=False)
    except WikiPublicationConflict as exc:
        return dict(report, status="blocked", reason=str(exc))
    from .wiki_updater import CompiledWikiUpdater
    # Discoverable from the main wiki index, including after its later rebuilds.
    # Use the caller's layout; no second inventory or model request.
    CompiledWikiUpdater(config, layout=layout, db=db).refresh_index()
    return dict(report, status="published")
