"""Synthetic scale hammer for the CCF Obsidian import path (thoth-abq).

The real vault corpus tops out at a few thousand objects; this runner
generates a seeded synthetic vault of arbitrary size and drives it
through the canonical producer -> admission path in four phases:

1. fresh import of the whole corpus (volume + throughput);
2. cross-instance re-import: a brand-new importer must reuse every
   admitted object through stable source URNs and the origin index
   (thoth-doz at scale), committing only its own session/run;
3. revised-note re-import: 2% of notes change on disk and must re-admit
   under their new content revisions while everything else is reused;
4. projection destruction and full rebuild at scale.

Opt-in via pytest (``THOTH_CCF_SCALE=1``, see
``tests/test_ccf_scale.py``) or standalone::

    .venv/bin/python scripts/ccf_scale.py --notes 10000 --attachments 1000

Generation is seeded and deterministic: the same parameters produce the
same vault, so a failure can be reproduced byte-for-byte.
"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _extra in (str(REPO_ROOT), str(REPO_ROOT / "tests")):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)

from scripts.ccf_stage9 import (  # noqa: E402
    DEFAULT_EMBED_CAP,
    PROJECTION_DATA_TABLES,
    ScenarioFailure,
    Stage9Context,
    build_context,
    check,
    start_ephemeral_postgres,
)
from ccf.db import open_ccf_connection  # noqa: E402
from ccf.obsidian import ObsidianImporter  # noqa: E402
from ccf.thothmap.context import MapContext  # noqa: E402

DEFAULT_NOTES = 10_000
DEFAULT_ATTACHMENTS = 1_000
DEFAULT_SEGMENTS = 20
DEFAULT_EXTERNAL_FILES = 3
DEFAULT_SEED = 20260813
REVISE_EVERY = 50  # scenario 3 revises every 50th note (2%)

_WORDS = (
    "archive memory signal provenance ledger vault note link graph node "
    "edge commit journal hash merkle custody erasure projection segment "
    " corpus import revision blob artifact session runtime policy"
).split()


def generate_vault(
    root: Path,
    *,
    notes: int = DEFAULT_NOTES,
    attachments: int = DEFAULT_ATTACHMENTS,
    segments: int = DEFAULT_SEGMENTS,
    external_files: int = DEFAULT_EXTERNAL_FILES,
    embed_cap_bytes: int = DEFAULT_EMBED_CAP,
    seed: int = DEFAULT_SEED,
) -> dict:
    """Deterministic synthetic vault: seeded RNG, stable file layout."""
    rng = random.Random(seed)
    root.mkdir(parents=True, exist_ok=True)
    seg_dirs = []
    for index in range(segments):
        seg_dir = root / f"seg-{index:02d}"
        seg_dir.mkdir()
        seg_dirs.append(seg_dir)

    attachment_names = []
    for index in range(attachments):
        name = f"att-{index:05d}.png"
        data = rng.randbytes(rng.randint(300, 3000))
        (seg_dirs[index % segments] / name).write_bytes(data)
        attachment_names.append(name)
    for index in range(external_files):
        # Manifest-only Blobs (spec 2.5 external): over the embed cap.
        data = rng.randbytes(embed_cap_bytes + 4096)
        (seg_dirs[index % segments] / f"big-{index:02d}.bin").write_bytes(data)

    stems = [f"note-{index:06d}" for index in range(notes)]
    for index, stem in enumerate(stems):
        links = " ".join(
            f"[[{rng.choice(stems)}]]" for _ in range(rng.randint(0, 4))
        )
        if rng.random() < 0.05:
            links += " [[missing-target-xyz]]"
        embed = ""
        if attachment_names and rng.random() < 0.05:
            embed = f"![[{rng.choice(attachment_names)}]]"
        body = " ".join(rng.choice(_WORDS) for _ in range(rng.randint(40, 120)))
        text = (
            f"---\ntitle: {stem}\n"
            f"tags: [scale, seg-{index % segments:02d}]\n"
            f"created: 2024-01-{rng.randint(1, 28):02d}\n---\n"
            f"{body} token-{index:06d}\n{links}\n{embed}\n"
        )
        (seg_dirs[index % segments] / f"{stem}.md").write_text(
            text, encoding="utf-8"
        )

    # Malformed documents: reported and skipped, never fabricated.
    (seg_dirs[0] / "broken-utf8.md").write_bytes(b"\xff\xfe\x00invalid")
    return {
        "notes": notes,
        "attachments": attachments,
        "external_files": external_files,
        "segments": segments,
        "seed": seed,
    }


def _fresh_importer(ctx: Stage9Context, run_tag: str) -> ObsidianImporter:
    return ObsidianImporter(
        producer=ctx.producer,
        archive=ctx.archive,
        ctx=MapContext(
            person_id=ctx.rig.person_id, policy_hint=ctx.rig.policy_lineage_id
        ),
        vault_root=ctx.vault_root,
        embed_cap_bytes=ctx.importer.embed_cap_bytes,
        run_tag=run_tag,
    )


def _origin_count(ctx: Stage9Context) -> int:
    with open_ccf_connection(ctx.rig.settings) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM origin_index WHERE archive_id = %s",
            (ctx.archive.archive_id,),
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# Phase 1: fresh import at scale
# ---------------------------------------------------------------------------


def scenario_01_scale_import(ctx: Stage9Context) -> dict:
    expected = ctx.details["generated"]
    started = time.monotonic()
    report = ctx.importer.import_vault()
    seconds = round(time.monotonic() - started, 2)
    ctx.report = report
    check(
        not report.admission_errors,
        f"scale import admission errors: {report.admission_errors[:2]}",
    )
    check(
        len(report.notes) == expected["notes"],
        f"imported {len(report.notes)} of {expected['notes']} notes",
    )
    check(
        len(report.malformed) >= 1,
        "the invalid-UTF-8 note was not reported as malformed",
    )
    check(
        len(report.attachment_blobs)
        == expected["attachments"] + expected["external_files"],
        f"attachment count drift: {len(report.attachment_blobs)}",
    )
    origins = _origin_count(ctx)
    return {
        "seconds": seconds,
        "objects_committed": report.objects_committed,
        "origin_rows": origins,
        "wikilink_edges": len(report.wikilink_edges),
        "bytes_embedded": report.bytes_embedded,
        "bytes_external": report.bytes_external,
    }


# ---------------------------------------------------------------------------
# Phase 2: cross-instance re-import (thoth-doz at scale)
# ---------------------------------------------------------------------------


def scenario_02_cross_instance_reimport(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    origins_before = _origin_count(ctx)
    started = time.monotonic()
    second = _fresh_importer(ctx, f"scale-reimport-{int(time.time())}").import_vault()
    seconds = round(time.monotonic() - started, 2)
    check(
        not second.admission_errors,
        f"re-import admission errors: {second.admission_errors[:2]}",
    )
    check(second.sources == report.sources, "source URNs differ across instances")
    check(set(second.notes) == set(report.notes), "note set differs")
    reused = sum(1 for record in second.notes.values() if record.existing)
    check(
        reused == len(second.notes),
        f"re-import re-admitted {len(second.notes) - reused} unchanged notes",
    )
    check(
        second.attachment_blobs == report.attachment_blobs,
        "attachment identity drifted",
    )
    check(
        second.objects_committed == 2,
        f"re-import committed {second.objects_committed} objects "
        "(expected only its session and run)",
    )
    check(
        _origin_count(ctx) == origins_before + 2,
        "re-import claimed origin tuples beyond its session/run",
    )
    return {"seconds": seconds, "notes_reused": reused}


# ---------------------------------------------------------------------------
# Phase 3: revised notes re-admit under new revisions, the rest is reused
# ---------------------------------------------------------------------------


def scenario_03_revised_reimport(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    relpaths = sorted(report.notes)
    revised = set(relpaths[::REVISE_EVERY])
    for relpath in revised:
        path = ctx.vault_root / relpath
        path.write_text(
            path.read_text(encoding="utf-8") + "\nrevision marker\n",
            encoding="utf-8",
        )
    origins_before = _origin_count(ctx)
    started = time.monotonic()
    third = _fresh_importer(ctx, f"scale-revise-{int(time.time())}").import_vault()
    seconds = round(time.monotonic() - started, 2)
    check(
        not third.admission_errors,
        f"revise pass admission errors: {third.admission_errors[:2]}",
    )
    for relpath, record in third.notes.items():
        if relpath in revised:
            check(not record.existing, f"revised note reused: {relpath}")
            check(
                record.artifact_id != report.notes[relpath].artifact_id,
                f"revised note kept its artifact ID: {relpath}",
            )
            check(
                record.revision != report.notes[relpath].revision,
                f"revised note kept its revision: {relpath}",
            )
        else:
            check(record.existing, f"untouched note re-admitted: {relpath}")
    check(
        _origin_count(ctx) == origins_before + 2 + 2 * len(revised),
        "revise pass claimed unexpected origin tuples",
    )
    return {"seconds": seconds, "notes_revised": len(revised)}


# ---------------------------------------------------------------------------
# Phase 4: projection destruction and rebuild at scale
# ---------------------------------------------------------------------------


def scenario_04_projection_rebuild(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    with open_ccf_connection(ctx.rig.settings) as conn:
        with conn.transaction():
            for table in PROJECTION_DATA_TABLES:
                exists = conn.execute(
                    "SELECT 1 FROM pg_tables WHERE tablename = %s "
                    "AND schemaname = current_schema()",
                    (table,),
                ).fetchone()
                if exists:
                    conn.execute(f"TRUNCATE {table}")
    started = time.monotonic()
    rebuilt = ctx.archive.projections.rebuild_all()
    seconds = round(time.monotonic() - started, 2)
    check(
        rebuilt.get("full_text", 0) >= len(report.notes),
        f"full_text rebuild short: {rebuilt}",
    )
    hits = ctx.archive.projections.search_text("token-000042")
    check(hits, "rare token not searchable after rebuild")
    return {"seconds": seconds, "rebuilt": rebuilt}


SCALE_SCENARIOS = (
    ("01 scale import", scenario_01_scale_import),
    ("02 cross-instance re-import", scenario_02_cross_instance_reimport),
    ("03 revised-note re-import", scenario_03_revised_reimport),
    ("04 projection rebuild", scenario_04_projection_rebuild),
)


def run_scale_scenarios(ctx: Stage9Context) -> list[dict]:
    results = []
    for name, fn in SCALE_SCENARIOS:
        started = time.monotonic()
        try:
            detail = fn(ctx)
        except Exception as exc:
            result = {
                "scenario": name,
                "status": "FAIL",
                "seconds": round(time.monotonic() - started, 2),
                "error": f"{type(exc).__name__}: {exc}",
            }
        else:
            result = {
                "scenario": name,
                "status": "PASS",
                "seconds": round(time.monotonic() - started, 2),
                "detail": detail,
            }
        results.append(result)
        # Stream each phase as it completes: at this scale a crash must
        # never take the already-collected evidence down with it.
        print(f"{result['status']}  {name}  ({result['seconds']}s)", flush=True)
    return results


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json
    import subprocess
    import tempfile

    parser = argparse.ArgumentParser(description="CCF synthetic scale hammer")
    parser.add_argument("--notes", type=int, default=DEFAULT_NOTES)
    parser.add_argument("--attachments", type=int, default=DEFAULT_ATTACHMENTS)
    parser.add_argument("--segments", type=int, default=DEFAULT_SEGMENTS)
    parser.add_argument("--external-files", type=int, default=DEFAULT_EXTERNAL_FILES)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--embed-cap", type=int, default=DEFAULT_EMBED_CAP
    )
    parser.add_argument("--workspace", default=None)
    parser.add_argument(
        "--package-root",
        default=str(REPO_ROOT / "spec" / "ccf" / "0.1.2"),
    )
    parser.add_argument("--report", default=None)
    args = parser.parse_args(argv)

    workspace = Path(args.workspace or tempfile.mkdtemp(prefix="ccf-scale-"))
    workspace.mkdir(parents=True, exist_ok=True)
    vault = workspace / "vault"
    generated = generate_vault(
        vault,
        notes=args.notes,
        attachments=args.attachments,
        segments=args.segments,
        external_files=args.external_files,
        embed_cap_bytes=args.embed_cap,
        seed=args.seed,
    )

    container, dsn = start_ephemeral_postgres()
    started = time.monotonic()
    ctx = None
    results: list[dict] = []
    try:
        ctx = build_context(
            dsn=dsn,
            vault_root=vault,
            workspace=workspace,
            package_root=args.package_root,
            embed_cap_bytes=args.embed_cap,
        )
        ctx.details["generated"] = generated
        results = run_scale_scenarios(ctx)
    finally:
        if ctx is not None:
            try:
                ctx.settings_factory.cleanup()
            except Exception as exc:
                # The container is removed next anyway; a dead server at
                # this point invalidates nothing the phases proved.
                print(f"WARN  schema cleanup failed: {exc}", flush=True)
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
    if ctx is None:
        print("FAIL  context build failed before any scenario ran")
        return 1

    passed = sum(1 for r in results if r["status"] == "PASS")
    print("CCF synthetic scale hammer")
    print(f"vault: {vault} ({generated})")
    print("-" * 72)
    for result in results:
        print(f"{result['status']}  {result['scenario']}  ({result['seconds']}s)")
        if result["status"] == "FAIL":
            print(f"      {result['error']}")
        elif result.get("detail"):
            print(f"      {result['detail']}")
    print("-" * 72)
    total = round(time.monotonic() - started, 1)
    print(f"{passed}/{len(SCALE_SCENARIOS)} phases passed in {total}s")
    report_path = args.report or str(workspace / "scale-report.json")
    Path(report_path).write_text(
        json.dumps(
            {
                "generated": generated,
                "seconds": total,
                "passed": passed,
                "scenarios": results,
                "import_summary": ctx.report.summary() if ctx.report else None,
            },
            indent=1,
            default=str,
        )
    )
    print(f"report: {report_path}")
    return 0 if passed == len(SCALE_SCENARIOS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
