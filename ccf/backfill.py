"""Build CCF metadata from native Thoth state without changing that state."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3

import yaml


def _decoded(row):
    result = dict(row)
    for key in tuple(result):
        if key.endswith("_json"):
            result[key[:-5]] = json.loads(result.pop(key) or "{}")
    if "object_text" in result:
        result["object_value"] = result.pop("object_text")
    return result


def _frontmatter(path):
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        return {}
    end = content.find("\n---", 4)
    if end < 0:
        raise ValueError("unterminated frontmatter")
    metadata = yaml.safe_load(content[4:end]) or {}
    if not isinstance(metadata, dict):
        raise ValueError("frontmatter must be an object")
    return metadata


def backfill(service, database_path, *, wiki_root=None):
    """Idempotently index all native rows; every skipped row has a reason.

    The native SQLite database is opened read-only and remains authoritative.
    No ingestion, LLM call, source rewrite, or review transition is performed.
    """
    report = {"counts": {}, "errors": []}
    problems_before = len(service.problems)

    def process(family, native_id, function):
        try:
            function()
            report["counts"][family] = report["counts"].get(family, 0) + 1
        except Exception as exc:
            # This is an item boundary, not an operational retry or fallback.
            report["errors"].append({"family": family, "native_id": str(native_id), "reason": f"{type(exc).__name__}: {exc}"})

    uri = Path(database_path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "ingestion_queue" not in tables:
            raise ValueError("native database has no ingestion_queue; check the configured database path")
        for row in connection.execute("SELECT * FROM ingestion_queue ORDER BY id"):
            def capture(row=row):
                payload = json.loads(row["payload_json"])
                service.capture_queue(artifact_id=row["artifact_id"], artifact_type=row["artifact_type"], payload=payload)
                if row["review_json"]:
                    review = json.loads(row["review_json"])
                    if not isinstance(review, dict) or not isinstance(review.get("events", []), list):
                        raise ValueError("review audit must contain an events array")
                    for index, event in enumerate(review.get("events", [])):
                        process("reviews", f"{row['artifact_id']}:{index}", lambda event=event:
                                service.record_artifact_review(artifact_id=row["artifact_id"],
                                    artifact_type=row["artifact_type"], payload=payload, review=event))
            process("captures", row["artifact_id"], capture)
        if "canonical_entities" in tables:
            for row in connection.execute("SELECT * FROM canonical_entities ORDER BY canonical_id"):
                process("entities", row["canonical_id"], lambda row=row: service.record_entity(_decoded(row)))
        if "semantic_memory_candidates" in tables:
            for row in connection.execute("SELECT * FROM semantic_memory_candidates ORDER BY candidate_id"):
                def candidate(row=row):
                    evidence = [_decoded(item) for item in connection.execute(
                        "SELECT * FROM semantic_memory_evidence WHERE candidate_id=? ORDER BY evidence_id", (row["candidate_id"],))]
                    service.record_candidate(_decoded(row), evidence=evidence)
                process("candidates", row["candidate_id"], candidate)
        if "files_index" in tables:
            for row in connection.execute("SELECT * FROM files_index ORDER BY path"):
                path = Path(row["path"])
                if not path.is_absolute():
                    path = service.vault_root / path
                process("files", row["path"], lambda path=path, row=row: service.record_file(path, artifact_type=row["type"]))
    if wiki_root is not None and Path(wiki_root).is_dir():
        for path in sorted(Path(wiki_root).rglob("*.md")):
            def wiki(path=path):
                metadata = _frontmatter(path)
                service.record_wiki(page_path=path, slug=str(metadata.get("thoth_slug") or path.stem), metadata=metadata,
                                    source_paths=metadata.get("thoth_source_paths") or [])
            process("wiki", path, wiki)
    report["store"] = service.store.stats()
    report["errors"].extend(service.problems[problems_before:])
    return report
