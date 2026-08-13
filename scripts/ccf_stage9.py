"""CCF 0.1.2-rc1 stage 9 torture-run support (checklist section 9).

Importable scenario implementations plus the shared run context. The
standalone entrypoint is ``python scripts/ccf_stage9.py`` (see
``scripts/run_ccf_stage9.py`` semantics in ``__main__`` below); the
pytest wrapper lives in ``tests/test_ccf_stage9_obsidian.py``. Both drive
the same scenario functions against the real vault corpus so the runner
and the test suite can never drift apart.

Corpus location is always explicit: ``--vault`` / ``THOTH_CCF_VAULT`` /
the ``vault_root`` argument — never a silent default inside the library
code.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _extra in (str(REPO_ROOT), str(REPO_ROOT / "tests")):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)

import ccf_helpers  # noqa: E402  (tests/ helpers: rig builder)

from ccf.db import CcfPostgresSettings, open_ccf_connection  # noqa: E402
from ccf.ids import generate_id  # noqa: E402
from ccf.obsidian import ObsidianImporter  # noqa: E402
from ccf.thothmap import review as thothmap_review  # noqa: E402
from ccf.thothmap import semantic as thothmap_semantic  # noqa: E402
from ccf.thothmap.context import MapContext  # noqa: E402

DEFAULT_EMBED_CAP = 1024 * 1024  # runner cap: exercises embed + external paths
PROJECTION_DATA_TABLES = (
    "projection_link_state",
    "projection_derivation_closure",
    "projection_entity_cluster",
    "projection_full_text",
    "projection_embedding",
)


class ScenarioFailure(AssertionError):
    """A torture scenario did not hold."""


@dataclass
class Stage9Context:
    """Shared state threaded through the 11 scenarios, in order."""

    rig: object
    importer: ObsidianImporter
    vault_root: Path
    package_root: Path
    workspace: Path
    dsn: str
    settings_factory: object
    report: object = None  # ImportReport after scenario 1
    details: dict = field(default_factory=dict)

    @property
    def archive(self):
        return self.rig.archive

    @property
    def producer(self):
        return self.rig.producer

    def require_report(self):
        if self.report is None:
            raise ScenarioFailure("scenario 1 (fresh import) has not run")
        return self.report


def check(condition, message: str):
    if not condition:
        raise ScenarioFailure(message)


def make_settings_factory(dsn: str):
    """New-schema settings factory over one ephemeral Postgres DSN."""
    import uuid

    import psycopg

    schemas: list[str] = []

    def factory() -> CcfPostgresSettings:
        schema = f"ccf_stage9_{uuid.uuid4().hex[:10]}"
        schemas.append(schema)
        return CcfPostgresSettings(enabled=True, dsn=dsn, schema=schema)

    def cleanup() -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for schema in schemas:
                conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    factory.cleanup = cleanup
    return factory


def build_context(
    *,
    dsn: str,
    vault_root: str | Path,
    workspace: str | Path,
    package_root: str | Path,
    embed_cap_bytes: int = DEFAULT_EMBED_CAP,
) -> Stage9Context:
    """Rig + importer against one fresh archive schema (import not yet run)."""
    from dataclasses import replace

    from ccf.erasure.suppression import generate_suppression_key

    vault_root = Path(vault_root)
    if not vault_root.is_dir():
        raise ScenarioFailure(f"vault corpus not found: {vault_root}")
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    package_root = Path(package_root)

    settings_factory = make_settings_factory(dsn)
    settings = settings_factory()
    suppression_key = generate_suppression_key(workspace / "suppression.key")
    settings = replace(settings, suppression_key_path=str(suppression_key))
    (workspace / "rig").mkdir(exist_ok=True)
    rig = ccf_helpers.make_rig(settings, workspace / "rig", package_root)
    importer = ObsidianImporter(
        producer=rig.producer,
        archive=rig.archive,
        ctx=MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id),
        vault_root=vault_root,
        embed_cap_bytes=embed_cap_bytes,
        run_tag=f"stage9-{int(time.time())}",
    )
    return Stage9Context(
        rig=rig,
        importer=importer,
        vault_root=vault_root,
        package_root=package_root,
        workspace=workspace,
        dsn=dsn,
        settings_factory=settings_factory,
    )


def _object_count(ctx) -> int:
    with open_ccf_connection(ctx.rig.settings) as conn:
        return conn.execute("SELECT COUNT(*) FROM object_header").fetchone()[0]


def _admit_mapped(ctx, mapped, *, links=True) -> dict:
    batch = ctx.producer.create_batch(
        records=mapped.records,
        links=mapped.links if links else [],
        blobs=mapped.blobs,
        blob_data=mapped.blob_data or None,
    )
    return ctx.archive.admit_batch(batch, blob_bytes=mapped.blob_data or None)


# ---------------------------------------------------------------------------
# Scenario 1: fresh import
# ---------------------------------------------------------------------------


def scenario_01_fresh_import(ctx: Stage9Context) -> dict:
    report = ctx.importer.import_vault()
    ctx.report = report
    summary = report.summary()
    check(
        not report.admission_errors,
        f"import had admission errors: {report.admission_errors[:3]}",
    )
    check(summary["notes_imported"] > 1000, f"too few notes: {summary}")
    check(summary["attachment_blobs"] > 100, f"too few attachments: {summary}")
    check(
        summary["bytes_embedded"] > 0 and summary["bytes_external"] > 0,
        f"embed/external split not exercised: {summary}",
    )
    verification = ctx.archive.verify_chain()
    check(
        verification["commits_verified"] >= len(report.batches),
        f"chain verification short: {verification}",
    )

    # Shared fixtures for later scenarios.
    edges = report.wikilink_edges
    forward = {(e["from_relpath"], e["to_relpath"]) for e in edges}
    mutual = [
        (a, b)
        for a, b in forward
        if (b, a) in forward and a < b
    ]
    check(mutual, "corpus has no mutual wikilink pair (scenario 4 needs one)")
    ctx.details["mutual_pair"] = mutual[0]
    check(report.missing_attachments, "corpus has no missing attachment (scenario 5)")
    return {"summary": summary, "verification": verification}


# ---------------------------------------------------------------------------
# Scenario 2: exact retry after crash
# ---------------------------------------------------------------------------


def scenario_02_exact_retry_after_crash(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    notes_batch = next(b for b in report.batches if b["purpose"] == "notes")
    batch = report.signed_batches[notes_batch["batch_id"]]
    head_before = ctx.archive.head()
    objects_before = _object_count(ctx)

    # Simulated crash/restart: the producer reloads the identical signed
    # batch and its durably spooled Blob bytes, then re-admits.
    replay_bytes = ctx.producer.spooled_blob_bytes(batch["batch_id"])
    result = ctx.archive.admit_batch(batch, blob_bytes=replay_bytes)
    check(result["status"] == "committed", f"retry status: {result['status']}")
    check(
        result.get("commit_sequence") == notes_batch["commit_sequence"],
        "retry did not return the stored commit coordinates",
    )
    check(
        len(result.get("admissions", [])) == len(notes_batch["object_ids"]),
        "retry admission count drifted",
    )
    check(ctx.archive.head() == head_before, "retry advanced the archive head")
    check(_object_count(ctx) == objects_before, "retry duplicated objects")
    pending = ctx.producer.pending_batches()
    check(not pending, f"answered batches still pending: {len(pending)}")
    return {
        "batch_id": batch["batch_id"],
        "replayed_objects": len(notes_batch["object_ids"]),
        "commit_sequence": notes_batch["commit_sequence"],
    }


# ---------------------------------------------------------------------------
# Scenario 3: duplicate source and changed source revision
# ---------------------------------------------------------------------------


def scenario_03_duplicate_and_changed_revision(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    relpath = sorted(report.notes)[0]
    record = report.notes[relpath]
    head_before = ctx.archive.head()

    # Same origin tuple + same submission hash -> idempotent existing.
    retry = _admit_mapped(ctx, ctx.importer.remap_note(relpath, reuse_ids=True))
    check(retry["status"] == "committed", f"dup retry status: {retry['status']}")
    statuses = {a["status"] for a in retry["admissions"]}
    check(statuses == {"existing"}, f"dup retry outcomes: {statuses}")
    check(ctx.archive.head() == head_before, "idempotent retry committed new objects")

    # Same origin tuple + different content -> origin_revision_conflict.
    forged = ctx.importer.remap_note(
        relpath,
        reuse_ids=False,
        revision=record.revision,
        text_override="stage-9 dishonest replay: same revision, new content",
    )
    conflict = _admit_mapped(ctx, forged, links=False)
    check(conflict["status"] == "conflict", f"forged batch status: {conflict['status']}")
    outcomes = {a["status"] for a in conflict["admissions"]}
    check(
        outcomes == {"origin_revision_conflict"},
        f"forged outcomes: {conflict['admissions']}",
    )
    for admission in conflict["admissions"]:
        check(
            ctx.archive.get_object(admission["object_id"]) is None,
            "conflicting object was committed anyway",
        )
    check(ctx.archive.head() == head_before, "conflict advanced the archive head")
    return {"relpath": relpath, "retry_outcomes": sorted(statuses), "forged": sorted(outcomes)}


# ---------------------------------------------------------------------------
# Scenario 4: same-batch object graph
# ---------------------------------------------------------------------------


def scenario_04_same_batch_object_graph(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    a_path, b_path = ctx.details["mutual_pair"]
    a = report.notes[a_path]
    b = report.notes[b_path]
    edge_links = {
        (e["from_relpath"], e["to_relpath"]): e["link_id"]
        for e in report.wikilink_edges
    }
    link_ab = edge_links[(a_path, b_path)]
    link_ba = edge_links[(b_path, a_path)]

    host = None
    for batch in report.batches:
        ids = set(batch["object_ids"])
        if {a.artifact_id, b.artifact_id, link_ab, link_ba} <= ids:
            host = batch
            break
    check(
        host is not None,
        f"mutual pair not committed atomically: {a_path} / {b_path}",
    )
    check(host["status"] == "committed", f"graph batch status: {host['status']}")
    check(host["purpose"] == "notes", f"graph batch purpose: {host['purpose']}")
    return {
        "pair": [a_path, b_path],
        "batch_id": host["batch_id"],
        "batch_size": len(host["object_ids"]),
    }


# ---------------------------------------------------------------------------
# Scenario 5: missing attachment and malformed document
# ---------------------------------------------------------------------------


def scenario_05_missing_attachment_and_malformed(ctx: Stage9Context) -> dict:
    report = ctx.require_report()

    # Declared-missing attachment: reported, referenced note imported, and
    # no Blob was fabricated for the absent file.
    missing = report.missing_attachments
    check(missing, "no missing attachments recorded")
    probe = missing[0]
    check(
        probe["note"] in report.notes,
        f"note with missing attachment was not imported: {probe}",
    )
    with open_ccf_connection(ctx.rig.settings) as conn:
        fabricated = conn.execute(
            "SELECT COUNT(*) FROM origin_index WHERE native_id LIKE %s",
            (f"%{probe['target']}",),
        ).fetchone()[0]
    check(fabricated == 0, f"fabricated content for missing file: {probe['target']}")
    check(report.incomplete, "report must flag the import as incomplete")

    # Malformed document: fails the object loudly, never the import.
    probe_dir = ctx.workspace / "malformed_probe"
    probe_dir.mkdir(exist_ok=True)
    (probe_dir / "Broken Frontmatter.md").write_text(
        "---\ntitle: [unterminated\nbad: {yaml\n---\nbody\n", encoding="utf-8"
    )
    (probe_dir / "Healthy.md").write_text(
        "---\ntitle: Healthy probe\n---\nA well-formed probe note.\n", encoding="utf-8"
    )
    malformed_before = len(report.malformed)
    notes_before = len(report.notes)
    ctx.importer.import_probe_tree("malformed_probe", probe_dir)
    new_malformed = report.malformed[malformed_before:]
    check(
        any("Broken Frontmatter.md" in m["relpath"] for m in new_malformed),
        f"malformed probe not recorded: {new_malformed}",
    )
    check(
        "Healthy.md" in report.notes and len(report.notes) == notes_before + 1,
        "healthy probe note was not imported",
    )
    check(
        not report.admission_errors,
        f"probe caused admission errors: {report.admission_errors[:2]}",
    )
    return {
        "missing_attachments": len(missing),
        "example_missing": probe,
        "malformed_recorded": new_malformed,
    }


# ---------------------------------------------------------------------------
# Scenario 6: entity merge/split
# ---------------------------------------------------------------------------


def scenario_06_entity_merge_split(ctx: Stage9Context) -> dict:
    from ccf.projections.invalidation import ProjectionStaleError

    ctx.require_report()
    a_path, b_path = ctx.details["mutual_pair"]
    vault_source = ctx.report.sources["_vault"]
    entity_ids = []
    for relpath in (a_path, b_path):
        note = ctx.report.notes[relpath]
        mapped = thothmap_semantic.entity_submission(
            ctx.producer,
            ctx.importer.ctx,
            {
                "canonical_id": f"obsidian-note:{relpath}",
                "entity_type": "concept",
                "display_name": note.title,
            },
            source_ccf_id=vault_source,
        )
        result = _admit_mapped(ctx, mapped)
        check(result["status"] == "committed", f"entity admit: {result['status']}")
        entity_ids.append(mapped.records[0]["id"])
    e1, e2 = entity_ids

    ctx.archive.projections.rebuild("entity_cluster")
    clusters = ctx.archive.projections.entity_clusters()
    check(len(clusters) >= 2, f"clusters before merge: {len(clusters)}")

    # Merge: same_as Link + human-adjudicated resolution, one batch.
    merge_claims = ctx.rig.claims()
    merge_claims["authority"] = ccf_helpers.authority(
        "person_accepted", ctx.rig.person_id
    )
    same_as = ctx.producer.new_link(
        type="ccf.same_as", from_id=e1, to_id=e2, claims=ctx.rig.claims()
    )
    resolution = ctx.producer.new_record(
        type="semantic.entity_resolution",
        claims=merge_claims,
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "create",
            "valid_from": ctx.rig.clock(),
            "expires_at": None,
        },
        payload={
            "action": "same_as",
            "entity_ids": [e1, e2],
            "canonical_entity_id": e1,
            "reason": "stage 9: two notes cover the same concept",
            "evidence_refs": [],
            "extensions": {},
        },
    )
    result = _admit_mapped(ctx, _combined(records=[resolution], links=[same_as]))
    check(result["status"] == "committed", f"merge admit: {result['status']}")

    # Generation fence: the projection refuses reads until rebuilt.
    stale = False
    try:
        ctx.archive.projections.entity_clusters()
    except ProjectionStaleError:
        stale = True
    check(stale, "entity_cluster served stale reads after merge admission")
    ctx.archive.projections.rebuild("entity_cluster")
    merged = ctx.archive.projections.entity_clusters()
    pair_clusters = [sorted(m) for m in merged.values() if e1 in m or e2 in m]
    check(
        pair_clusters == [sorted([e1, e2])],
        f"merged cluster wrong: {pair_clusters}",
    )

    # Split: human adjudication retracts the same_as Link; distinct_from
    # records the adjudication.
    retract = ctx.producer.new_record(
        type="lineage.link_disposition",
        claims=ctx.rig.claims(),
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "retract",
            "valid_from": ctx.rig.clock(),
            "expires_at": None,
        },
        payload={
            "target_link_id": same_as["id"],
            "action": "retract",
            "reason": "stage 9 split adjudication",
            "previous_disposition_id": None,
            "replacement_link_id": None,
            "extensions": {},
        },
    )
    distinct = ctx.producer.new_link(
        type="ccf.distinct_from", from_id=e1, to_id=e2, claims=ctx.rig.claims()
    )
    result = _admit_mapped(ctx, _combined(records=[retract], links=[distinct]))
    check(result["status"] == "committed", f"split admit: {result['status']}")
    stale = False
    try:
        ctx.archive.projections.entity_clusters()
    except ProjectionStaleError:
        stale = True
    check(stale, "entity_cluster served stale reads after split admission")
    ctx.archive.projections.rebuild("entity_cluster")
    split = ctx.archive.projections.entity_clusters()
    pair_clusters = sorted(
        sorted(m) for m in split.values() if e1 in m or e2 in m
    )
    check(
        pair_clusters == sorted([[e1], [e2]]),
        f"split clusters wrong: {pair_clusters}",
    )
    ctx.details["entity_ids"] = [e1, e2]
    return {"merged": sorted([e1, e2]), "split": [[e1], [e2]]}


def _combined(**kwargs):
    from ccf.thothmap.context import MappedSubmissions

    return MappedSubmissions(**kwargs)


# ---------------------------------------------------------------------------
# Scenario 7: human review survival after projection deletion
# ---------------------------------------------------------------------------


def scenario_07_review_survival(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    relpath = sorted(report.notes)[1]
    note = report.notes[relpath]
    vault_source = report.sources["_vault"]

    candidate = thothmap_semantic.assertion_submissions(
        ctx.producer,
        ctx.importer.ctx,
        {
            "candidate_id": f"stage9-candidate:{relpath}",
            "candidate_type": "memory",
            "status": "proposed",
            "subject": note.title,
            "predicate": "has summary excerpt",
            "object_value": (note.excerpt or note.title)[:200],
            "text": note.excerpt[:500],
            "confidence": 0.9,
        },
        source_ccf_id=vault_source,
        evidence_ccf_ids=[note.artifact_id],
    )
    result = _admit_mapped(ctx, candidate)
    check(result["status"] == "committed", f"candidate admit: {result['status']}")
    candidate_id = candidate.records[0]["id"]

    accepted_payload = {
        "subject": {"value": note.title, "datatype": "string"},
        "predicate": candidate.records[0]["payload"]["predicate"],
        "object": {
            "value": (note.excerpt or note.title)[:200],
            "datatype": "string",
        },
        "scope": {},
        "qualifiers": {"stage9": "accepted successor"},
        "extensions": {},
    }
    review = thothmap_review.review_submissions(
        ctx.producer,
        ctx.importer.ctx,
        {
            "action": "promote",
            "actor": "stage9-operator",
            "at": ctx.rig.clock(),
            "reason": "stage 9 review acceptance",
        },
        source_ccf_id=vault_source,
        target_ccf_ids=[candidate_id],
        reviewer_ccf_id=ctx.rig.person_id,
        accepted_type="semantic.assertion",
        accepted_payload=accepted_payload,
    )
    result = _admit_mapped(ctx, review)
    check(result["status"] == "committed", f"review admit: {result['status']}")
    decision_id = review.records[0]["id"]
    successor_id = review.records[1]["id"]

    # Destroy every projection data table; rebuild; decisions survive.
    ctx.archive.projections.rebuild_all()
    with open_ccf_connection(ctx.rig.settings) as conn:
        before = {
            table: _table_rows(conn, ctx.archive.archive_id, table)
            for table in PROJECTION_DATA_TABLES
            if _table_exists(conn, table)
        }
    check(any(before.values()), "projections empty before destruction")
    # The truncation must commit before rebuild_all opens its own
    # connection — TRUNCATE holds ACCESS EXCLUSIVE until commit.
    with open_ccf_connection(ctx.rig.settings) as conn:
        with conn.transaction():
            for table in before:
                conn.execute(f"TRUNCATE {table}")
    rebuilt = ctx.archive.projections.rebuild_all()
    with open_ccf_connection(ctx.rig.settings) as conn:
        after = {
            table: _table_rows(conn, ctx.archive.archive_id, table)
            for table in before
        }
    check(after == before, "projection rows changed across destroy/rebuild")
    check(rebuilt, "rebuild_all returned nothing")

    decision = ctx.archive.get_object(decision_id)
    check(
        decision and decision["compartments"]["semantic"]["envelope"]["content"]["payload"]["decision"] == "accept",
        "review decision lost or altered",
    )
    successor = ctx.archive.get_object(successor_id)
    check(
        successor and successor["compartments"]["semantic"]["envelope"] is not None,
        "accepted successor lost",
    )
    verification = ctx.archive.verify_chain()
    ctx.details["review"] = {
        "decision_id": decision_id,
        "successor_id": successor_id,
        "candidate_id": candidate_id,
    }
    return {
        "decision_id": decision_id,
        "tables_rebuilt": sorted(before),
        "commits_verified": verification["commits_verified"],
    }


def _table_exists(conn, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = current_schema() AND tablename = %s",
            (table,),
        ).fetchone()
    )


def _table_rows(conn, archive_id: str, table: str) -> list[tuple]:
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY 1, 2", (archive_id,)
    ).fetchall()
    return [tuple(str(value) for value in row) for row in rows]


# ---------------------------------------------------------------------------
# Scenario 8: semantic compartment erasure
# ---------------------------------------------------------------------------


def scenario_08_semantic_erasure(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    relpath, note, term = _pick_searchable_note(report)

    ctx.archive.projections.rebuild("full_text")
    hits = ctx.archive.projections.search_text(term)
    check(
        any(hit["object_id"] == note.artifact_id for hit in hits),
        f"note not searchable before erasure: {term!r}",
    )

    staging = ctx.workspace / "wiki_staging"
    ctx.archive.projections.rebuild_wiki(staging)
    svc = ctx.archive.erasure(wiki_staging_dir=staging)
    targets = [{"object_id": note.artifact_id, "compartments": ["semantic"]}]
    request = svc.submit_request(
        requester_id=ctx.rig.person_id,
        subject_id=ctx.rig.person_id,
        requested_scope={"targets": targets},
        reason="stage 9 semantic erasure",
        authority=ccf_helpers.authority(
            "first_person_statement", ctx.rig.person_id, ctx.rig.person_id
        ),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=targets,
        reasoning="stage 9 approved",
        decided_by=ctx.rig.person_id,
        authority=ccf_helpers.authority(
            "explicit_authorization", ctx.rig.person_id, ctx.rig.person_id
        ),
        authorized_producers=[ctx.producer.producer_id],
    )
    status = svc.execute(decided["operation_id"])
    check(
        status.get("stage") == "receipt",
        f"erasure saga ended in non-receipt stage: {status.get('stage')}",
    )

    obj = ctx.archive.get_object(note.artifact_id)
    check(obj is not None, "erased object header missing")
    check(
        obj["header"]["structural_commitment"] and obj["header"]["semantic_commitment"],
        "commitments did not survive erasure",
    )
    semantic = obj["compartments"]["semantic"]
    check(semantic["state"] == "erased", f"semantic state: {semantic['state']}")
    check(semantic["envelope"] is None, "erased content still readable")
    verification = ctx.archive.verify_chain()
    check(verification["commits_verified"] > 0, "chain broke after erasure")

    ctx.archive.projections.rebuild("full_text")
    hits = ctx.archive.projections.search_text(term)
    check(
        all(hit["object_id"] != note.artifact_id for hit in hits),
        "erased note still searchable after rebuild",
    )

    # Retry of the identical submissions: lifecycle answer, no bytes back.
    retry = _admit_mapped(ctx, ctx.importer.remap_note(relpath, reuse_ids=True))
    artifact_outcome = next(
        a for a in retry["admissions"] if a["object_id"] == note.artifact_id
    )
    check(artifact_outcome["status"] == "existing", f"retry outcome: {artifact_outcome}")
    check(
        artifact_outcome.get("current_lifecycle") == "erased",
        f"retry lifecycle: {artifact_outcome}",
    )
    check(artifact_outcome.get("payload_available") is False, "bytes returned after erasure")

    # Silent reintroduction under a new revision: suppressed.
    forged = ctx.importer.remap_note(
        relpath,
        reuse_ids=False,
        revision=f"{note.revision}-recapture",
        text_override="stage 9 reintroduction attempt",
    )
    result = _admit_mapped(ctx, forged, links=False)
    suppressed = [
        a for a in result["admissions"] if a.get("current_lifecycle") == "suppressed"
    ]
    check(suppressed, f"reintroduction not suppressed: {result['admissions']}")
    for admission in result["admissions"]:
        check(
            ctx.archive.get_object(admission["object_id"]) is None
            or admission["status"] == "existing",
            "suppressed reintroduction was committed",
        )
    ctx.details["erased_note"] = relpath
    return {
        "erased": relpath,
        "saga": status.get("stage"),
        "retry_lifecycle": artifact_outcome.get("current_lifecycle"),
        "suppressed": len(suppressed),
    }


def _rare_term(note) -> str:
    """A distinctive searchable token from the note's imported payload."""
    for token in note.title.split():
        cleaned = "".join(ch for ch in token if ch.isalnum())
        if len(cleaned) >= 8:
            return cleaned
    raise ScenarioFailure(f"no rare term in note title: {note.title!r}")


def _pick_searchable_note(report, *, exclude: set | None = None):
    """First imported note (sorted order) whose title has a rare term."""
    exclude = exclude or set()
    for relpath in sorted(report.notes):
        if relpath in exclude:
            continue
        note = report.notes[relpath]
        try:
            term = _rare_term(note)
        except ScenarioFailure:
            continue
        return relpath, note, term
    raise ScenarioFailure("no imported note has a searchable rare term")


# ---------------------------------------------------------------------------
# Scenario 9: full wiki/search/vector rebuild
# ---------------------------------------------------------------------------


def scenario_09_full_projection_rebuild(ctx: Stage9Context) -> dict:
    report = ctx.require_report()
    erased = ctx.details.get("erased_note")
    candidate, note, term = _pick_searchable_note(
        report, exclude={erased} if erased else None
    )

    with open_ccf_connection(ctx.rig.settings) as conn:
        with conn.transaction():
            for table in PROJECTION_DATA_TABLES:
                if _table_exists(conn, table):
                    conn.execute(f"TRUNCATE {table}")
    rebuilt = ctx.archive.projections.rebuild_all()
    check(
        rebuilt.get("full_text", 0) >= len(report.notes),
        f"full_text rebuild short: {rebuilt}",
    )

    staging = ctx.workspace / "wiki_rebuild"
    wiki_report = ctx.archive.projections.rebuild_wiki(staging)
    check(wiki_report["pages"] >= 2, f"wiki rebuild: {wiki_report}")
    check((staging / "index.md").is_file(), "wiki index.md missing")
    check(list((staging / "pages").glob("*.md")), "wiki pages missing")

    hits = ctx.archive.projections.search_text(term)
    check(
        any(hit["object_id"] == note.artifact_id for hit in hits),
        f"note not found after full rebuild: {term!r}",
    )
    if erased is not None:
        erased_note = report.notes[erased]
        erased_hits = ctx.archive.projections.search_text(_rare_term(erased_note))
        check(
            all(hit["object_id"] != erased_note.artifact_id for hit in erased_hits),
            "erased note reappeared in rebuilt full_text",
        )

    from ccf.projections.vectors import VectorSupportError

    model_id = "stage9-model-v1"
    vector = [0.25, 0.5, 0.75]
    try:
        ctx.archive.projections.put_embedding(
            object_id=note.artifact_id, model_id=model_id, vector=vector
        )
        ctx.archive.projections.put_embedding(
            object_id=ctx.rig.person_id, model_id=model_id, vector=[9.0, 9.0, 9.0]
        )
    except VectorSupportError as exc:
        raise ScenarioFailure(f"pgvector unavailable in torture run: {exc}") from exc
    nearest = ctx.archive.projections.nearest(model_id=model_id, query_vector=vector, limit=1)
    check(
        nearest and nearest[0]["object_id"] == note.artifact_id,
        f"vector round-trip failed: {nearest}",
    )
    return {
        "rebuilt": rebuilt,
        "wiki_pages": wiki_report["pages"],
        "search_hit": note.artifact_id,
        "vector_nearest": nearest[0]["object_id"],
    }


# ---------------------------------------------------------------------------
# Scenario 10: corrupt commit and unsupported catalog
# ---------------------------------------------------------------------------


def scenario_10_corrupt_commit_and_catalog(ctx: Stage9Context) -> dict:
    import psycopg

    from ccf.catalog import CatalogError, SemanticCatalog
    from ccf.journal import JournalError, verify_chain

    ctx.require_report()
    scratch = ctx.settings_factory()
    tables = (
        "archive",
        "archive_head",
        "object_header",
        "compartment",
        "commit_journal",
        "commit_member",
        # verify_chain cross-checks derived admission rows against the
        # journal; the scratch replica needs them too.
        "admission",
    )
    with psycopg.connect(ctx.dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{scratch.schema}" CASCADE')
        conn.execute(f'CREATE SCHEMA "{scratch.schema}"')
        for table in tables:
            conn.execute(
                f'CREATE TABLE "{scratch.schema}"."{table}" AS '
                f'SELECT * FROM "{ctx.rig.settings.schema}"."{table}"'
            )
    with open_ccf_connection(scratch) as conn:
        report = verify_chain(conn, archive_id=ctx.archive.archive_id)
        check(report["commits_verified"] > 0, "scratch copy did not verify")

        # Tamper one commit member hash.
        with conn.transaction():
            conn.execute(
                """
                UPDATE commit_member SET object_hash = 'sha256:' || repeat('0', 64)
                WHERE ctid IN (SELECT ctid FROM commit_member LIMIT 1)
                """
            )
        member_failed = False
        try:
            verify_chain(conn, archive_id=ctx.archive.archive_id)
        except JournalError:
            member_failed = True
        check(member_failed, "tampered commit member went undetected")
        with conn.transaction():
            conn.execute(
                f'DROP TABLE commit_member',
            )
            conn.execute(
                f'CREATE TABLE commit_member AS '
                f'SELECT * FROM "{ctx.rig.settings.schema}".commit_member'
            )

        # Tamper one parent hash.
        with conn.transaction():
            conn.execute(
                """
                UPDATE commit_journal SET parent_commit_hash = 'sha256:' || repeat('f', 64)
                WHERE sequence = (SELECT MAX(sequence) - 1 FROM commit_journal)
                """
            )
        parent_failed = False
        try:
            verify_chain(conn, archive_id=ctx.archive.archive_id)
        except JournalError:
            parent_failed = True
        check(parent_failed, "tampered parent hash went undetected")

    # Unsupported catalog: a tampered package fails closed at load. The
    # victim is a catalog-pinned artifact (verify_artifacts hashes every
    # pinned schema/registry file).
    import shutil

    tampered = ctx.workspace / "tampered_package"
    if tampered.exists():
        shutil.rmtree(tampered)
    shutil.copytree(ctx.package_root, tampered)
    victim = tampered / "schemas" / "payloads" / "experience" / "artifact.schema.json"
    check(victim.is_file(), f"pinned schema missing from package copy: {victim}")
    import json as _json

    artifact = _json.loads(victim.read_text(encoding="utf-8"))
    artifact["title"] = "tampered by stage 9"
    victim.write_text(_json.dumps(artifact, indent=1), encoding="utf-8")
    load_failed = False
    try:
        SemanticCatalog.load(tampered)
    except CatalogError:
        load_failed = True
    check(load_failed, "tampered catalog artifacts loaded without error")

    catalog = SemanticCatalog.load(ctx.package_root)
    bad_document = dict(catalog._document)
    bad_document["root"] = "sha256:" + "0" * 64
    root_failed = False
    try:
        SemanticCatalog.from_document(bad_document)
    except CatalogError:
        root_failed = True
    check(root_failed, "unsupported catalog root accepted")
    return {
        "member_tamper_detected": member_failed,
        "parent_tamper_detected": parent_failed,
        "catalog_tamper_detected": load_failed,
        "catalog_root_rejected": root_failed,
    }


# ---------------------------------------------------------------------------
# Scenario 11: restore and foreign merge
# ---------------------------------------------------------------------------


def scenario_11_restore_and_foreign_merge(ctx: Stage9Context) -> dict:
    from ccf.archive import Archive
    from ccf.sync.restore import restore_mindpack

    ctx.require_report()
    head = ctx.archive.head()
    pack_dir = ctx.workspace / "corpus.mindpack"
    manifest = ctx.archive.sync().export_mindpack(pack_dir)
    check(
        manifest["extensions"]["completeness"]["complete"] is True,
        f"pack incomplete: {manifest['extensions']['completeness']['dangling'][:3]}",
    )
    check(manifest["head_commit_hash"] == head["commit_hash"], "manifest head mismatch")

    settings_b = ctx.settings_factory()
    report = restore_mindpack(
        settings_b,
        package_root=ctx.package_root,
        pack_path=pack_dir,
        trusted_genesis_hash=manifest["genesis_commit_hash"],
        trusted_head_hash=manifest["head_commit_hash"],
    )
    check(report["status"] == "restored", f"restore: {report['status']}")
    check(report["partial"] is False, "restore was partial")
    verification = report["verification"]
    check(
        verification["genesis_commit_hash"] == manifest["genesis_commit_hash"]
        and verification["head_commit_hash"] == head["commit_hash"],
        "restored chain hashes differ",
    )
    replica = Archive.open(
        settings_b,
        package_root=ctx.package_root,
        archive_key_path=ctx.rig.archive_key_path,
    )
    check(replica.head() == head, "replica head differs from source")

    # Foreign merge into an independent second archive.
    from dataclasses import replace as dc_replace

    from ccf.erasure.suppression import generate_suppression_key

    settings_c = ctx.settings_factory()
    key_c = generate_suppression_key(ctx.workspace / "suppression-c.key")
    rig_b_dir = ctx.workspace / "rig_b"
    rig_b_dir.mkdir(exist_ok=True)
    rig_b = ccf_helpers.make_rig(
        dc_replace(settings_c, suppression_key_path=str(key_c)),
        rig_b_dir,
        ctx.package_root,
    )
    merge = rig_b.archive.sync().import_mindpack(pack_dir)
    check(merge["status"] == "merged", f"merge: {merge.get('status')}")
    check(merge["source_archive_id"] == ctx.archive.archive_id, "merge source mismatch")

    sample = ctx.details["review"]["decision_id"]
    for object_id in (sample, ctx.report.session_id, ctx.report.run_id):
        merged_obj = rig_b.archive.get_object(object_id)
        check(merged_obj is not None, f"merged object missing: {object_id}")
        check(
            merged_obj["header"]["object_hash"]
            == ctx.archive.get_object(object_id)["header"]["object_hash"],
            f"object hash changed across merge: {object_id}",
        )
    forks = rig_b.archive.sync().forks()
    check(
        any(
            f["source_archive_id"] == ctx.archive.archive_id
            and f["head_commit_hash"] == head["commit_hash"]
            for f in forks
        ),
        f"custody proof missing: {forks}",
    )
    rig_b.archive.verify_chain()
    return {
        "manifest_head": manifest["head_commit_hash"],
        "restored_head": verification["head_commit_hash"],
        "merge_commit_sequence": merge.get("commit_sequence"),
        "merged_objects": len(merge.get("admitted", [])),
        "custody_proofs": len(forks),
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

SCENARIOS = (
    ("01 fresh import", scenario_01_fresh_import),
    ("02 exact retry after crash", scenario_02_exact_retry_after_crash),
    ("03 duplicate source and changed revision", scenario_03_duplicate_and_changed_revision),
    ("04 same-batch object graph", scenario_04_same_batch_object_graph),
    ("05 missing attachment and malformed document", scenario_05_missing_attachment_and_malformed),
    ("06 entity merge/split", scenario_06_entity_merge_split),
    ("07 human review survival after projection deletion", scenario_07_review_survival),
    ("08 semantic compartment erasure", scenario_08_semantic_erasure),
    ("09 full wiki/search/vector rebuild", scenario_09_full_projection_rebuild),
    ("10 corrupt commit and unsupported catalog", scenario_10_corrupt_commit_and_catalog),
    ("11 restore and foreign merge", scenario_11_restore_and_foreign_merge),
)


def run_scenarios(ctx: Stage9Context) -> list[dict]:
    """Run all 11 scenarios in order; returns per-scenario results."""
    results = []
    for name, fn in SCENARIOS:
        started = time.monotonic()
        try:
            detail = fn(ctx)
        except Exception as exc:
            results.append(
                {
                    "scenario": name,
                    "status": "FAIL",
                    "seconds": round(time.monotonic() - started, 2),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        results.append(
            {
                "scenario": name,
                "status": "PASS",
                "seconds": round(time.monotonic() - started, 2),
                "detail": detail,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Standalone entrypoint: ephemeral Postgres + full 11-scenario report
# ---------------------------------------------------------------------------

_CCF_PG_IMAGES = ("pgvector/pgvector:pg16", "postgres:16-alpine", "postgres:16")


def _docker_output(*args: str) -> str:
    import subprocess

    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def start_ephemeral_postgres() -> tuple[str, str]:
    """Start a tmpfs-backed ephemeral Postgres; returns (container, DSN)."""
    import shutil
    import subprocess
    import time

    if shutil.which("docker") is None:
        raise ScenarioFailure("docker is required for the stage 9 runner")
    image = None
    for candidate in _CCF_PG_IMAGES:
        try:
            _docker_output("image", "inspect", candidate)
            image = candidate
            break
        except subprocess.CalledProcessError:
            continue
    if image is None:
        raise ScenarioFailure(f"no Postgres image among {_CCF_PG_IMAGES}")
    container = _docker_output(
        "run", "--rm", "-d", "--tmpfs", "/var/lib/postgresql/data",
        "-e", "POSTGRES_PASSWORD=ccf-test", "-e", "POSTGRES_DB=ccf_test",
        "-p", "127.0.0.1::5432", image,
    )
    port = _docker_output("port", container, "5432/tcp").split(":")[-1]
    dsn = f"postgresql://postgres:ccf-test@127.0.0.1:{port}/ccf_test"
    import psycopg

    deadline = time.monotonic() + 60
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=2):
                break
        except Exception:
            if time.monotonic() > deadline:
                _docker_output("rm", "-f", container)
                raise
            time.sleep(0.5)
    return container, dsn


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json
    import os
    import subprocess
    import tempfile

    parser = argparse.ArgumentParser(
        description="CCF 0.1.2-rc1 checklist stage 9: Obsidian torture run"
    )
    parser.add_argument(
        "--vault",
        default=os.environ.get("THOTH_CCF_VAULT"),
        help="vault corpus path (or THOTH_CCF_VAULT env var)",
    )
    parser.add_argument(
        "--embed-cap",
        type=int,
        default=DEFAULT_EMBED_CAP,
        help=f"blob embed cap in bytes (default {DEFAULT_EMBED_CAP})",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="scratch workspace (default: a fresh temp dir)",
    )
    parser.add_argument(
        "--package-root",
        default=str(REPO_ROOT / "spec" / "ccf" / "0.1.2-rc1"),
        help="vendored CCF package root",
    )
    parser.add_argument("--report", default=None, help="write JSON report here")
    args = parser.parse_args(argv)
    if not args.vault:
        parser.error("vault corpus path required: --vault or THOTH_CCF_VAULT")

    workspace = Path(args.workspace or tempfile.mkdtemp(prefix="ccf-stage9-"))
    container, dsn = start_ephemeral_postgres()
    started = time.monotonic()
    ctx = None
    try:
        ctx = build_context(
            dsn=dsn,
            vault_root=args.vault,
            workspace=workspace,
            package_root=args.package_root,
            embed_cap_bytes=args.embed_cap,
        )
        results = run_scenarios(ctx)
        ctx.settings_factory.cleanup()
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
    if ctx is None:
        print("FAIL  context build failed before any scenario ran")
        return 1

    passed = sum(1 for r in results if r["status"] == "PASS")
    print("CCF 0.1.2-rc1 stage 9 — Obsidian torture run")
    print(f"vault: {args.vault}")
    print(f"workspace: {workspace}")
    print("-" * 72)
    for result in results:
        line = f"{result['status']}  {result['scenario']}  ({result['seconds']}s)"
        print(line)
        if result["status"] == "FAIL":
            print(f"      {result['error']}")
    print("-" * 72)
    total = round(time.monotonic() - started, 1)
    print(f"{passed}/{len(SCENARIOS)} scenarios passed in {total}s")
    document = {
        "vault": str(args.vault),
        "workspace": str(workspace),
        "embed_cap_bytes": args.embed_cap,
        "seconds": total,
        "passed": passed,
        "scenarios": results,
        "import_summary": ctx.report.summary() if ctx.report else None,
    }
    report_path = args.report or str(workspace / "stage9-report.json")
    Path(report_path).write_text(json.dumps(document, indent=1, default=str))
    print(f"report: {report_path}")
    return 0 if passed == len(SCENARIOS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
