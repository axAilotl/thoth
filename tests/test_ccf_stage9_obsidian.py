"""CCF 0.1.2-rc1 checklist stage 9: Obsidian torture run (real vault corpus).

Runs the 11 stage-9 scenarios against the real Obsidian vault segment via
``scripts.ccf_stage9`` — the same scenario functions the standalone runner
(``python scripts/ccf_stage9.py --vault <path>``) executes, so the suite
and the runner can never drift apart.

The full run imports the whole corpus (1.7 GB, ~1 400 notes, ~450 binary
attachments) through the producer -> admission path and exercises
export/restore/merge, so it takes minutes. It is therefore opt-in, like
the other environment-gated integration tests in this suite::

    THOTH_CCF_STAGE9=1 .venv/bin/python -m pytest tests/test_ccf_stage9_obsidian.py

Corpus path: ``THOTH_CCF_VAULT`` (default: the shared segment at
/home/ada/thoth/.CCF/_vault_share). Requires docker (ephemeral Postgres).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("THOTH_CCF_STAGE9") != "1",
    reason="stage 9 torture run is opt-in; set THOTH_CCF_STAGE9=1",
)

DEFAULT_VAULT = Path("/home/ada/thoth/.CCF/_vault_share")


@pytest.fixture(scope="module")
def stage9_ctx(ccf_postgres_dsn, ccf_package_root, tmp_path_factory):
    """Shared context: rig + importer; scenario 1 performs the import."""
    from scripts.ccf_stage9 import build_context

    vault = Path(os.environ.get("THOTH_CCF_VAULT", str(DEFAULT_VAULT)))
    if not vault.is_dir():
        pytest.skip(f"stage 9 vault corpus not present: {vault}")
    workspace = tmp_path_factory.mktemp("ccf-stage9")
    from scripts.ccf_stage9 import DEFAULT_EMBED_CAP

    ctx = build_context(
        dsn=ccf_postgres_dsn,
        vault_root=vault,
        workspace=workspace,
        package_root=ccf_package_root,
        embed_cap_bytes=DEFAULT_EMBED_CAP,
    )
    yield ctx
    ctx.settings_factory.cleanup()


def test_01_fresh_import(stage9_ctx):
    from scripts.ccf_stage9 import scenario_01_fresh_import

    scenario_01_fresh_import(stage9_ctx)


def test_02_exact_retry_after_crash(stage9_ctx):
    from scripts.ccf_stage9 import scenario_02_exact_retry_after_crash

    scenario_02_exact_retry_after_crash(stage9_ctx)


def test_03_duplicate_source_and_changed_revision(stage9_ctx):
    from scripts.ccf_stage9 import scenario_03_duplicate_and_changed_revision

    scenario_03_duplicate_and_changed_revision(stage9_ctx)


def test_04_same_batch_object_graph(stage9_ctx):
    from scripts.ccf_stage9 import scenario_04_same_batch_object_graph

    scenario_04_same_batch_object_graph(stage9_ctx)


def test_05_missing_attachment_and_malformed(stage9_ctx):
    from scripts.ccf_stage9 import scenario_05_missing_attachment_and_malformed

    scenario_05_missing_attachment_and_malformed(stage9_ctx)


def test_06_entity_merge_split(stage9_ctx):
    from scripts.ccf_stage9 import scenario_06_entity_merge_split

    scenario_06_entity_merge_split(stage9_ctx)


def test_07_human_review_survival(stage9_ctx):
    from scripts.ccf_stage9 import scenario_07_review_survival

    scenario_07_review_survival(stage9_ctx)


def test_08_semantic_compartment_erasure(stage9_ctx):
    from scripts.ccf_stage9 import scenario_08_semantic_erasure

    scenario_08_semantic_erasure(stage9_ctx)


def test_09_full_wiki_search_vector_rebuild(stage9_ctx):
    from scripts.ccf_stage9 import scenario_09_full_projection_rebuild

    scenario_09_full_projection_rebuild(stage9_ctx)


def test_10_corrupt_commit_and_unsupported_catalog(stage9_ctx):
    from scripts.ccf_stage9 import scenario_10_corrupt_commit_and_catalog

    scenario_10_corrupt_commit_and_catalog(stage9_ctx)


def test_11_restore_and_foreign_merge(stage9_ctx):
    from scripts.ccf_stage9 import scenario_11_restore_and_foreign_merge

    scenario_11_restore_and_foreign_merge(stage9_ctx)
