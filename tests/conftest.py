"""Shared fixtures for the CCF 0.1.2 / 0.2.0 conformance tests.

All fixtures point at the vendored, hash-verified spec packages. Tests must
never write under ``spec/ccf/``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CCF_PACKAGE_ROOT = REPO_ROOT / "spec" / "ccf" / "0.1.2"
CCF_VECTORS = CCF_PACKAGE_ROOT / "vectors"
CCF_EXAMPLES = CCF_PACKAGE_ROOT / "examples" / "personal-archive"
CCF_DRAFT_ROOT = REPO_ROOT / "spec" / "ccf" / "0.2.0"
CCF_CAPSULE_EXAMPLE = CCF_DRAFT_ROOT / "examples" / "capsule"


#: The 0.1.2 package pins the same deterministic TEST-ONLY Ed25519 key
#: material as 0.1.1 (identical public keys; the 0.1.2 SHA256SUMS entries for
#: the private pems match the 0.1.1 files byte-for-byte). The repo's
#: .gitignore excludes ``*-ed25519-private.pem`` except for the vendored
#: ``spec/ccf/**/vectors/TEST-ONLY-*`` copies, so the final tree carries the
#: TEST-ONLY private keys and tests load them directly from the package —
#: read-only; the tree is never modified.
CCF_TEST_ONLY_KEYS = REPO_ROOT / "spec" / "ccf" / "0.1.2" / "vectors"


@pytest.fixture(scope="session")
def ccf_test_only_keys_dir() -> Path:
    return CCF_TEST_ONLY_KEYS


@pytest.fixture(scope="session")
def ccf_package_root() -> Path:
    return CCF_PACKAGE_ROOT


@pytest.fixture(scope="session")
def ccf_draft_root() -> Path:
    return CCF_DRAFT_ROOT


@pytest.fixture(scope="session")
def ccf_capsule_example() -> Path:
    return CCF_CAPSULE_EXAMPLE

@pytest.fixture(scope="session")
def ccf_vectors_dir() -> Path:
    return CCF_VECTORS


@pytest.fixture(scope="session")
def ccf_examples_dir() -> Path:
    return CCF_EXAMPLES


@pytest.fixture(scope="session")
def load_ccf_json():
    def _load(path: Path) -> object:
        return json.loads(path.read_text(encoding="utf-8"))

    return _load


# ---------------------------------------------------------------------------
# Ephemeral Postgres for CCF archive/producer DB tests
# ---------------------------------------------------------------------------

_CCF_PG_IMAGES = ("pgvector/pgvector:pg16", "postgres:16-alpine", "postgres:16")


def _docker_output(*args: str) -> str:
    import subprocess

    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture(scope="session")
def ccf_postgres_dsn():
    """DSN of an ephemeral, tmpfs-backed Postgres container.

    Skips cleanly when docker or a suitable image is unavailable; never
    touches a pre-existing local Postgres.
    """
    import shutil
    import subprocess
    import time

    if shutil.which("docker") is None:
        pytest.skip("docker is not available")
    try:
        _docker_output("info")
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("docker daemon is not available")

    image = None
    for candidate in _CCF_PG_IMAGES:
        try:
            _docker_output("image", "inspect", candidate)
            image = candidate
            break
        except subprocess.CalledProcessError:
            continue
    if image is None:
        try:
            _docker_output("pull", _CCF_PG_IMAGES[0])
            image = _CCF_PG_IMAGES[0]
        except subprocess.CalledProcessError:
            pytest.skip("no Postgres image available and pull failed")

    container = _docker_output(
        "run",
        "--rm",
        "-d",
        "--tmpfs",
        "/var/lib/postgresql/data",
        "-e",
        "POSTGRES_PASSWORD=ccf-test",
        "-e",
        "POSTGRES_DB=ccf_test",
        "-p",
        "127.0.0.1::5432",
        image,
    )
    try:
        port = _docker_output("port", container, "5432/tcp").split(":")[-1]
        dsn = f"postgresql://postgres:ccf-test@127.0.0.1:{port}/ccf_test"
        deadline = time.monotonic() + 60
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                import psycopg

                with psycopg.connect(dsn, connect_timeout=2):
                    break
            except Exception as exc:  # container still starting
                last_error = exc
                time.sleep(0.5)
        else:
            raise RuntimeError(f"ephemeral Postgres never came up: {last_error}")
        yield dsn
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)


@pytest.fixture()
def ccf_settings(ccf_postgres_dsn):
    """Per-test CCF store settings with a unique schema, cleaned up after."""
    import uuid

    import psycopg

    from ccf.db import CcfPostgresSettings

    schema = f"ccf_test_{uuid.uuid4().hex[:12]}"
    settings = CcfPostgresSettings(enabled=True, dsn=ccf_postgres_dsn, schema=schema)
    yield settings
    with psycopg.connect(ccf_postgres_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
