"""CCF dual-write zero-mismatch harness (checklist section 10a).

Reconciles the authoritative legacy inventory — the SQLite ``metadata_db``
ingestion queue (plus, optionally, the raw files it references) — against
the CCF archive the dual-write mirror admitted into, using the shared
conventions in ``ccf.dualwrite.conventions``:

- every legacy capture source / session / run / artifact / blob / finding
  must have exactly one CCF object under the expected origin tuple (or, for
  origin-root sources, the expected deterministic Record URN);
- mirrored content commitments must match the legacy content (artifact
  payload ``thoth_sha256`` and Blob bytes vs the legacy sha256);
- every CCF object under a dual-written source must trace back to a legacy
  item (no extras);
- unresolved dual-write ledger entries count as mismatches.

Class-by-class report (JSON); exit code 1 unless there are ZERO
mismatches. Usage:

    python scripts/ccf_dualwrite_check.py \
        --metadata-db .thoth_system/meta.db \
        --dsn "$THOTH_CCF_POSTGRES_DSN" --schema ccf \
        [--error-log .thoth_system/ccf_dualwrite_errors.jsonl] \
        [--verify-files] [--out report.json]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ccf.db import DEFAULT_CCF_DSN_ENV, DEFAULT_CCF_SCHEMA, CcfPostgresSettings, open_ccf_connection  # noqa: E402
from ccf.dualwrite.conventions import (  # noqa: E402
    FINDING_REVISION,
    SESSION_REVISION,
    finding_origin_native_id,
    findings_from_metadata,
    raw_ref_id_for,
    run_native_id,
    source_record_id,
)
from ccf.dualwrite.ledger import read_errors  # noqa: E402


class HarnessError(RuntimeError):
    """The harness itself cannot run (missing inputs, empty archive)."""


# ---------------------------------------------------------------------------
# Legacy inventory
# ---------------------------------------------------------------------------


def load_legacy_inventory(
    metadata_db_path: str | Path, *, vault_root: str | Path | None = None
) -> dict:
    """Rebuild the dual-written legacy inventory from the ingestion queue.

    Queue payloads store ``raw_payload.path`` either absolute (as captured)
    or relative to the vault root (artifact-provided); the raw_ref_id
    convention needs the absolute form, so ``--vault-root`` is required
    whenever relative paths appear.
    """
    path = Path(metadata_db_path)
    if not path.is_file():
        raise HarnessError(f"metadata DB not found: {path}")
    root = (
        Path(vault_root).expanduser().resolve() if vault_root is not None else None
    )
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT artifact_id, artifact_type, source, payload_json "
            "FROM ingestion_queue"
        ).fetchall()
    finally:
        conn.close()

    inventory = {
        "sources": {},  # thoth source_id -> {source_name, source_type}
        "sessions": {},  # session_id -> thoth source_id
        "media": {},  # raw_ref_id -> snapshot
        "findings": {},  # fingerprint -> snapshot
        "skipped_rows": 0,
        "queue_rows": len(rows),
    }
    for artifact_id, artifact_type, source_name, payload_json in rows:
        try:
            payload = json.loads(payload_json or "{}")
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        metadata = payload.get("normalized_metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        source_id = metadata.get("capture_source_id")
        if not source_id:
            inventory["skipped_rows"] += 1
            continue

        inventory["sources"].setdefault(
            source_id,
            {
                "source_name": payload.get("source") or source_name,
                "source_type": payload.get("source_type"),
            },
        )
        session_id = metadata.get("capture_session_id")
        if session_id:
            inventory["sessions"][session_id] = source_id

        raw = payload.get("raw_payload")
        if isinstance(raw, dict) and raw.get("sha256") and raw.get("path"):
            raw_path = Path(raw["path"])
            if not raw_path.is_absolute():
                if root is None:
                    raise HarnessError(
                        f"queue entry {artifact_id} has a vault-relative raw path "
                        f"({raw['path']}); pass --vault-root to resolve it"
                    )
                raw_path = root / raw_path
            absolute_path = str(raw_path.resolve())
            raw_ref_id = raw_ref_id_for(source_id, raw["sha256"], absolute_path)
            inventory["media"][raw_ref_id] = {
                "queue_artifact_id": artifact_id,
                "artifact_type": artifact_type,
                "source_id": source_id,
                "sha256": raw["sha256"],
                "size_bytes": raw.get("size_bytes"),
                "path": absolute_path,
            }

        for finding in findings_from_metadata(metadata):
            native_id = finding_origin_native_id(finding)
            inventory["findings"].setdefault(
                native_id,
                {
                    "queue_artifact_id": artifact_id,
                    "source_id": source_id,
                    "finding_type": finding["finding_type"],
                    "severity": finding["severity"],
                },
            )
    return inventory


# ---------------------------------------------------------------------------
# CCF side
# ---------------------------------------------------------------------------


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class CcfSnapshot:
    """Read-only view of the mirrored objects in one CCF archive schema."""

    def __init__(self, settings: CcfPostgresSettings) -> None:
        with open_ccf_connection(settings) as conn:
            row = conn.execute("SELECT archive_id FROM archive").fetchone()
            if row is None:
                raise HarnessError("CCF archive store is empty (no genesis)")
            self.archive_id = row[0]
            self.objects = {
                object_id: kind
                for object_id, kind in conn.execute(
                    "SELECT id, object_kind FROM object_header"
                ).fetchall()
            }
            self.record_types = {
                object_id: rtype
                for object_id, rtype in conn.execute(
                    "SELECT object_id, plaintext_json ->> 'type' FROM compartment "
                    "WHERE compartment = 'structural'"
                ).fetchall()
            }
            self.origins = [
                (source_id, native_id, revision, kind, object_id)
                for source_id, native_id, revision, kind, object_id in conn.execute(
                    "SELECT source_id, native_id, revision, object_kind, object_id "
                    "FROM origin_index WHERE archive_id = %s",
                    (self.archive_id,),
                ).fetchall()
            ]
            self.links = [
                (ltype, from_id, to_id)
                for ltype, from_id, to_id in conn.execute(
                    "SELECT plaintext_json ->> 'type', "
                    "       plaintext_json ->> 'from_id', "
                    "       plaintext_json ->> 'to_id' "
                    "FROM compartment WHERE compartment = 'structural' "
                    "AND object_id IN (SELECT id FROM object_header "
                    "                  WHERE object_kind = 'link')"
                ).fetchall()
            ]
            self.artifact_sha = {
                object_id: sha
                for object_id, sha in conn.execute(
                    "SELECT object_id, plaintext_json #>> '{payload,extensions,thoth_sha256}' "
                    "FROM compartment WHERE compartment = 'semantic'"
                ).fetchall()
                if sha
            }
            self.blobs = {
                blob_id: (str(length), bytes(data) if data is not None else None)
                for blob_id, length, data in conn.execute(
                    "SELECT blob_id, byte_length, plaintext_bytes FROM blob_content"
                ).fetchall()
            }
        self.origin_by_key = {}
        for _source_id, native_id, revision, kind, object_id in self.origins:
            self.origin_by_key.setdefault((native_id, revision, kind), []).append(
                object_id
            )


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def reconcile(
    inventory: dict,
    snapshot: CcfSnapshot,
    *,
    ledger_entries: list[dict] | None = None,
    verify_files: bool = False,
) -> dict:
    """Compare legacy inventory against the CCF snapshot, class by class."""
    mismatches: list[dict] = []

    def mismatch(kind: str, detail: dict) -> None:
        mismatches.append({"kind": kind, **detail})

    archive_id = snapshot.archive_id
    expected_source_ids = {
        thoth_id: source_record_id(archive_id, thoth_id)
        for thoth_id in inventory["sources"]
    }
    dual_source_ccf_ids = set(expected_source_ids.values())
    consumed_origin_keys: set[tuple] = set()

    classes: dict[str, dict] = {}

    def report_class(name: str, expected: int, matched: int) -> None:
        classes[name] = {"expected": expected, "matched": matched}

    # --- sources: deterministic origin-root Records ----------------------
    matched = 0
    for thoth_id, ccf_id in expected_source_ids.items():
        if snapshot.objects.get(ccf_id) != "record":
            mismatch(
                "missing_object",
                {"class": "sources", "legacy_id": thoth_id, "expected_ccf_id": ccf_id},
            )
        elif snapshot.record_types.get(ccf_id) != "core.source":
            mismatch(
                "content_drift",
                {
                    "class": "sources",
                    "legacy_id": thoth_id,
                    "ccf_id": ccf_id,
                    "reason": f"type is {snapshot.record_types.get(ccf_id)!r}, expected 'core.source'",
                },
            )
        else:
            matched += 1
    ccf_source_records = {
        object_id
        for object_id, rtype in snapshot.record_types.items()
        if rtype == "core.source"
    }
    for extra in sorted(ccf_source_records - dual_source_ccf_ids):
        mismatch(
            "extra_object",
            {"class": "sources", "ccf_id": extra, "reason": "CCF core.source traces to no legacy source"},
        )
    report_class("sources", len(expected_source_ids), matched)

    # --- sessions and their paired runs ----------------------------------
    matched_sessions = matched_runs = 0
    for session_id in sorted(inventory["sessions"]):
        key = (session_id, SESSION_REVISION, "record")
        hits = snapshot.origin_by_key.get(key, [])
        consumed_origin_keys.add(key)
        if len(hits) != 1:
            mismatch(
                "missing_object" if not hits else "content_drift",
                {
                    "class": "sessions",
                    "legacy_id": session_id,
                    "reason": f"{len(hits)} CCF objects for origin tuple",
                },
            )
        else:
            matched_sessions += 1
        run_key = (run_native_id(session_id), SESSION_REVISION, "record")
        run_hits = snapshot.origin_by_key.get(run_key, [])
        consumed_origin_keys.add(run_key)
        if len(run_hits) != 1:
            mismatch(
                "missing_object" if not run_hits else "content_drift",
                {
                    "class": "runs",
                    "legacy_id": run_native_id(session_id),
                    "reason": f"{len(run_hits)} CCF objects for origin tuple",
                },
            )
        else:
            matched_runs += 1
    report_class("sessions", len(inventory["sessions"]), matched_sessions)
    report_class("runs", len(inventory["sessions"]), matched_runs)

    # --- media: artifact Records + Blobs + has_blob Links ----------------
    matched_artifacts = matched_blobs = matched_links = 0
    link_index = {(from_id, to_id) for ltype, from_id, to_id in snapshot.links if ltype == "ccf.has_blob"}
    for raw_ref_id, item in sorted(inventory["media"].items()):
        sha256 = item["sha256"]
        artifact_key = (raw_ref_id, sha256, "record")
        blob_key = (raw_ref_id, sha256, "blob")
        consumed_origin_keys.update({artifact_key, blob_key})
        artifacts = snapshot.origin_by_key.get(artifact_key, [])
        blobs = snapshot.origin_by_key.get(blob_key, [])

        if len(artifacts) != 1:
            mismatch(
                "missing_object" if not artifacts else "content_drift",
                {
                    "class": "artifacts",
                    "legacy_id": raw_ref_id,
                    "queue_artifact_id": item["queue_artifact_id"],
                    "reason": f"{len(artifacts)} CCF artifact Records for origin tuple",
                },
            )
        else:
            artifact_id = artifacts[0]
            ccf_sha = snapshot.artifact_sha.get(artifact_id)
            if ccf_sha != sha256:
                mismatch(
                    "content_drift",
                    {
                        "class": "artifacts",
                        "legacy_id": raw_ref_id,
                        "ccf_id": artifact_id,
                        "reason": f"payload thoth_sha256 {ccf_sha!r} != legacy {sha256!r}",
                    },
                )
            else:
                matched_artifacts += 1

        if len(blobs) != 1:
            mismatch(
                "missing_object" if not blobs else "content_drift",
                {
                    "class": "blobs",
                    "legacy_id": raw_ref_id,
                    "queue_artifact_id": item["queue_artifact_id"],
                    "reason": f"{len(blobs)} CCF Blobs for origin tuple",
                },
            )
        else:
            blob_id = blobs[0]
            byte_length, data = snapshot.blobs.get(blob_id, (None, None))
            if data is None:
                mismatch(
                    "content_drift",
                    {
                        "class": "blobs",
                        "legacy_id": raw_ref_id,
                        "ccf_id": blob_id,
                        "reason": "Blob has no plaintext bytes",
                    },
                )
            elif _sha256_bytes(data) != sha256:
                mismatch(
                    "content_drift",
                    {
                        "class": "blobs",
                        "legacy_id": raw_ref_id,
                        "ccf_id": blob_id,
                        "reason": "Blob bytes sha256 differ from legacy content",
                    },
                )
            elif item["size_bytes"] is not None and byte_length != str(item["size_bytes"]):
                mismatch(
                    "content_drift",
                    {
                        "class": "blobs",
                        "legacy_id": raw_ref_id,
                        "ccf_id": blob_id,
                        "reason": f"byte_length {byte_length} != legacy {item['size_bytes']}",
                    },
                )
            else:
                matched_blobs += 1

        if len(artifacts) == 1 and len(blobs) == 1:
            if (artifacts[0], blobs[0]) in link_index:
                matched_links += 1
            else:
                mismatch(
                    "missing_object",
                    {
                        "class": "links",
                        "legacy_id": raw_ref_id,
                        "reason": f"no ccf.has_blob Link {artifacts[0]} -> {blobs[0]}",
                    },
                )

        if verify_files:
            path = Path(item["path"])
            if not path.is_file():
                mismatch(
                    "content_drift",
                    {
                        "class": "blobs",
                        "legacy_id": raw_ref_id,
                        "reason": f"legacy raw file missing: {path}",
                    },
                )
            elif _sha256_bytes(path.read_bytes()) != sha256:
                mismatch(
                    "content_drift",
                    {
                        "class": "blobs",
                        "legacy_id": raw_ref_id,
                        "reason": f"legacy raw file content changed: {path}",
                    },
                )
    report_class("artifacts", len(inventory["media"]), matched_artifacts)
    report_class("blobs", len(inventory["media"]), matched_blobs)
    report_class("links", len(inventory["media"]), matched_links)

    # --- security findings -------------------------------------------------
    matched_findings = 0
    for native_id, item in sorted(inventory["findings"].items()):
        key = (native_id, FINDING_REVISION, "record")
        consumed_origin_keys.add(key)
        hits = snapshot.origin_by_key.get(key, [])
        if len(hits) != 1:
            mismatch(
                "missing_object" if not hits else "content_drift",
                {
                    "class": "findings",
                    "legacy_id": native_id,
                    "queue_artifact_id": item["queue_artifact_id"],
                    "reason": f"{len(hits)} CCF finding Records for origin tuple",
                },
            )
        else:
            matched_findings += 1
    report_class("findings", len(inventory["findings"]), matched_findings)

    # --- extras: CCF objects under dual-written sources with no legacy item
    # Sessions/runs are run-scoped evidence: the legacy queue payload keeps
    # only the LATEST session reference per artifact, while the archive
    # keeps every run's session. A session/run the legacy store no longer
    # references is reported as ``superseded`` (itemized, not a mismatch);
    # content-bearing classes (artifacts, blobs, findings) stay hard
    # mismatches — per checklist 10a the reconciled classes are sources,
    # artifacts, blobs, transcripts, and findings.
    superseded: list[dict] = []
    for source_id, native_id, revision, kind, object_id in snapshot.origins:
        if source_id not in dual_source_ccf_ids:
            continue  # other producers/sources are out of scope
        if (native_id, revision, kind) in consumed_origin_keys:
            continue
        record_type = snapshot.record_types.get(object_id)
        if record_type in ("core.session", "process.run"):
            superseded.append(
                {
                    "class": "sessions" if record_type == "core.session" else "runs",
                    "ccf_id": object_id,
                    "origin": [native_id, revision, kind],
                    "reason": "run-scoped record no longer referenced by the legacy queue",
                }
            )
            continue
        mismatch(
            "extra_object",
            {
                "class": "objects",
                "ccf_id": object_id,
                "reason": f"origin ({native_id}, {revision}, {kind}) traces to no legacy item",
            },
        )

    # --- unresolved dual-write failures ------------------------------------
    ledger_entries = ledger_entries or []
    for entry in ledger_entries:
        mismatch("dual_write_error", {"class": "errors", "detail": entry})

    return {
        "archive_id": archive_id,
        "queue_rows": inventory["queue_rows"],
        "skipped_rows": inventory["skipped_rows"],
        "classes": classes,
        "superseded": superseded,
        "mismatches": mismatches,
        "summary": {
            "mismatch_count": len(mismatches),
            "dual_write_errors": len(ledger_entries),
            "superseded_run_records": len(superseded),
            "ok": not mismatches,
        },
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--metadata-db", required=True, help="legacy SQLite metadata DB")
    parser.add_argument(
        "--vault-root",
        default=None,
        help="vault root for resolving relative raw_payload paths",
    )
    parser.add_argument("--dsn", default=None, help="CCF Postgres DSN (default: env)")
    parser.add_argument("--schema", default=DEFAULT_CCF_SCHEMA, help="CCF Postgres schema")
    parser.add_argument("--dsn-env", default=DEFAULT_CCF_DSN_ENV)
    parser.add_argument("--error-log", default=None, help="dual-write error ledger path")
    parser.add_argument(
        "--verify-files",
        action="store_true",
        help="also re-hash every legacy raw file referenced by the queue",
    )
    parser.add_argument("--out", default=None, help="write the JSON report here")
    args = parser.parse_args(argv)

    import os

    dsn = args.dsn or os.environ.get(args.dsn_env)
    if not dsn:
        print(f"error: no CCF DSN (pass --dsn or set {args.dsn_env})", file=sys.stderr)
        return 2

    try:
        inventory = load_legacy_inventory(args.metadata_db, vault_root=args.vault_root)
        settings = CcfPostgresSettings(enabled=True, dsn=dsn, schema=args.schema)
        snapshot = CcfSnapshot(settings)
    except HarnessError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    ledger_entries = read_errors(args.error_log) if args.error_log else []
    report = reconcile(
        inventory,
        snapshot,
        ledger_entries=ledger_entries,
        verify_files=args.verify_files,
    )
    report["metadata_db"] = str(args.metadata_db)
    report["schema"] = args.schema

    for name, stats in report["classes"].items():
        print(f"{name:10s} expected={stats['expected']:5d} matched={stats['matched']:5d}")
    print(f"mismatches: {report['summary']['mismatch_count']}")
    if report["mismatches"]:
        for item in report["mismatches"][:25]:
            print(f"  - {item['kind']}: {json.dumps(item, default=str)[:300]}")

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        print(f"report written: {out}")

    return 0 if report["summary"]["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
