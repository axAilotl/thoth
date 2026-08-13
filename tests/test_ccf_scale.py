"""CCF synthetic scale hammer (thoth-abq).

Generates a seeded synthetic vault (default 10k notes, 1k attachments,
dense wikilinks, manifest-only external binaries) and drives it through
import, cross-instance re-import, revised-note re-import, and projection
destruction/rebuild via ``scripts.ccf_scale`` — the same phase functions
the standalone runner executes, so suite and runner cannot drift.

Opt-in because it takes minutes and needs docker (ephemeral Postgres)::

    THOTH_CCF_SCALE=1 .venv/bin/python -m pytest tests/test_ccf_scale.py

Knobs: ``THOTH_CCF_SCALE_NOTES`` / ``THOTH_CCF_SCALE_ATTACHMENTS`` /
``THOTH_CCF_SCALE_SEED``.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("THOTH_CCF_SCALE") != "1",
    reason="scale hammer is opt-in; set THOTH_CCF_SCALE=1",
)


@pytest.fixture(scope="module")
def scale_ctx(ccf_postgres_dsn, ccf_package_root, tmp_path_factory):
    from scripts.ccf_scale import (
        DEFAULT_EMBED_CAP,
        generate_vault,
    )
    from scripts.ccf_stage9 import build_context

    notes = int(os.environ.get("THOTH_CCF_SCALE_NOTES", "10000"))
    attachments = int(os.environ.get("THOTH_CCF_SCALE_ATTACHMENTS", "1000"))
    seed = int(os.environ.get("THOTH_CCF_SCALE_SEED", "20260813"))

    workspace = tmp_path_factory.mktemp("ccf-scale")
    vault = workspace / "vault"
    generated = generate_vault(vault, notes=notes, attachments=attachments, seed=seed)
    ctx = build_context(
        dsn=ccf_postgres_dsn,
        vault_root=vault,
        workspace=workspace,
        package_root=ccf_package_root,
        embed_cap_bytes=DEFAULT_EMBED_CAP,
    )
    ctx.details["generated"] = generated
    yield ctx
    ctx.settings_factory.cleanup()


def test_01_scale_import(scale_ctx):
    from scripts.ccf_scale import scenario_01_scale_import

    scenario_01_scale_import(scale_ctx)


def test_02_cross_instance_reimport(scale_ctx):
    from scripts.ccf_scale import scenario_02_cross_instance_reimport

    scenario_02_cross_instance_reimport(scale_ctx)


def test_03_revised_reimport(scale_ctx):
    from scripts.ccf_scale import scenario_03_revised_reimport

    scenario_03_revised_reimport(scale_ctx)


def test_04_projection_rebuild(scale_ctx):
    from scripts.ccf_scale import scenario_04_projection_rebuild

    scenario_04_projection_rebuild(scale_ctx)
