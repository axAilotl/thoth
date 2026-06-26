import json
from pathlib import Path

from core.admin_lint import run_admin_lint
from core.config import Config
from core.legacy_artifact_lint import (
    LegacyArtifactLintRunner,
    legacy_artifact_lint_report_payload,
)
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.prompt_security import THOTH_SECURITY_SCANNED_LENGTH_KEY
from core.wiki_contract import build_wiki_contract
from core.wiki_io import atomic_write_text, render_frontmatter


def _config(tmp_path: Path) -> Config:
    runtime_config = Config()
    runtime_config.data = {}
    runtime_config.set("paths.vault_dir", str(tmp_path / "vault"))
    runtime_config.set("paths.system_dir", ".thoth_system")
    runtime_config.set("paths.cache_dir", "cache")
    runtime_config.set("paths.raw_dir", "raw")
    runtime_config.set("paths.library_dir", "library")
    runtime_config.set("paths.wiki_dir", "wiki")
    runtime_config.set("paths.digests_dir", "_digests")
    runtime_config.set("database.path", "meta.db")
    return runtime_config


def _insert_queue_row(
    db: MetadataDB,
    *,
    artifact_id: str,
    artifact_type: str,
    source: str,
    payload: dict,
) -> None:
    with db._get_connection() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_queue (
                artifact_id, artifact_type, source, priority, status,
                payload_json, capabilities_json, attempts, last_error,
                next_attempt_at, created_at, processed_at, review_json
            ) VALUES (?, ?, ?, 0, 'processed', ?, NULL, 0, NULL, NULL, ?, NULL, NULL)
            """,
            (
                artifact_id,
                artifact_type,
                source,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                "2026-04-04T00:00:00",
            ),
        )


def _queue_payload(db: MetadataDB, artifact_id: str) -> str:
    with db._get_connection() as conn:
        row = conn.execute(
            "SELECT payload_json FROM ingestion_queue WHERE artifact_id = ?",
            (artifact_id,),
        ).fetchone()
    return str(row["payload_json"])


def _write_wiki_page(path: Path, frontmatter: dict) -> None:
    atomic_write_text(
        path,
        render_frontmatter(frontmatter).rstrip() + "\n\n# Legacy Page\n",
    )


def test_legacy_artifact_lint_reports_old_queue_and_wiki_gaps(tmp_path: Path):
    runtime_config = _config(tmp_path)
    layout = build_path_layout(runtime_config, project_root=tmp_path)
    layout.ensure_directories()
    db = MetadataDB(str(layout.database_path))

    _insert_queue_row(
        db,
        artifact_id="old-1",
        artifact_type="note",
        source="manual",
        payload={
            "id": "old-1",
            "source_type": "manual",
            "raw_content": "legacy queue payload",
        },
    )
    _insert_queue_row(
        db,
        artifact_id="modern-1",
        artifact_type="note",
        source="manual",
        payload={
            "id": "modern-1",
            "source_type": "manual",
            "raw_content": "modern queue payload",
            "raw_payload": {
                "path": "raw/manual/modern-1.json",
                "sha256": "abc123",
                "immutable": True,
            },
            "normalized_metadata": {
                "canonical_id": "note:modern-1",
                "capture_event_id": "event-modern-1",
                THOTH_SECURITY_SCANNED_LENGTH_KEY: 20,
            },
        },
    )

    contract = build_wiki_contract(runtime_config, project_root=tmp_path)
    contract.pages_dir.mkdir(parents=True, exist_ok=True)
    _write_wiki_page(
        contract.pages_dir / "legacy-page.md",
        {
            "type": "Entity",
            "id": "legacy-page",
            "thoth_type": "wiki_page",
            "thoth_id": "legacy-page",
            "thoth_slug": "legacy-page",
            "thoth_kind": "entity",
            "title": "Legacy Page",
            "thoth_artifact_id": "old-1",
            "thoth_source_type": "manual",
        },
    )

    before_payload = _queue_payload(db, "old-1")
    report = LegacyArtifactLintRunner(
        runtime_config,
        layout=layout,
        db=db,
        contract=contract,
    ).lint()
    after_payload = _queue_payload(db, "old-1")

    assert before_payload == after_payload
    assert report.artifacts_checked == 2
    assert report.wiki_pages_checked == 1
    assert {
        issue.code for issue in report.issues if issue.subject_type == "artifact"
    } == {
        "artifact-missing-canonical-id",
        "artifact-missing-event-link",
        "artifact-missing-raw-ref",
        "artifact-missing-threat-metadata",
    }
    assert {
        issue.code for issue in report.issues if issue.subject_type == "wiki_page"
    } == {
        "wiki-page-missing-canonical-id",
        "wiki-page-missing-event-link",
        "wiki-page-missing-raw-ref",
        "wiki-page-missing-threat-metadata",
    }

    payload = legacy_artifact_lint_report_payload(report)
    assert payload["status"] == "needs_migration"
    assert payload["mutated"] is False
    assert payload["summary"]["migration_action_count"] == 2
    assert {
        tuple(action["missing_fields"])
        for action in payload["migration_actions"]
    } == {
        ("canonical_id", "event_link", "raw_ref", "threat_metadata"),
    }


def test_admin_legacy_artifact_lint_persists_downloadable_report(tmp_path: Path):
    runtime_config = _config(tmp_path)
    layout = build_path_layout(runtime_config, project_root=tmp_path)
    layout.ensure_directories()
    db = MetadataDB(str(layout.database_path))
    _insert_queue_row(
        db,
        artifact_id="old-1",
        artifact_type="note",
        source="manual",
        payload={"id": "old-1", "source_type": "manual"},
    )

    payload = run_admin_lint(
        runtime_config.data,
        project_root=tmp_path,
        lint_kind="legacy-artifacts",
    )

    assert payload["kind"] == "legacy-artifacts"
    assert payload["status"] == "needs_migration"
    assert payload["download_url"] == "/api/settings/lint/legacy-artifacts/download"
    assert Path(payload["report_path"]).exists()
