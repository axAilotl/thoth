"""Shared fixtures for the CCF 0.1.1 conformance tests.

All fixtures point at the vendored, hash-verified spec package. Tests must
never write under ``spec/ccf/``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CCF_PACKAGE_ROOT = REPO_ROOT / "spec" / "ccf" / "0.1.1"
CCF_VECTORS = CCF_PACKAGE_ROOT / "vectors"
CCF_EXAMPLES = CCF_PACKAGE_ROOT / "examples" / "thoth-capture"


@pytest.fixture(scope="session")
def ccf_package_root() -> Path:
    return CCF_PACKAGE_ROOT


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
