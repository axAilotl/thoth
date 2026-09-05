"""Persistent container configuration must survive re-runs and fail closed."""

import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "thoth_container", Path(__file__).resolve().parents[1] / "docker/container.py"
)
container = importlib.util.module_from_spec(spec)
spec.loader.exec_module(container)


def test_initialize_preserves_operator_edits_on_rerun(tmp_path):
    container.initialize(tmp_path)
    control = tmp_path / "config/control.json"
    control.write_text('{"operator": "keep me"}')
    with pytest.raises(ValueError, match="Refusing to overwrite"):
        container.initialize(tmp_path)
    assert json.loads(control.read_text()) == {"operator": "keep me"}
    container.validate(tmp_path / "config")


def test_missing_persistent_config_refuses_startup(tmp_path):
    with pytest.raises(ValueError, match="Missing persistent operator file"):
        container.validate(tmp_path)


@pytest.mark.parametrize("contents", ['{"broken":', "[]", "null"])
def test_malformed_operator_config_refuses_startup(tmp_path, contents):
    container.initialize(tmp_path)
    (tmp_path / "config/control.json").write_text(contents)
    with pytest.raises(ValueError):
        container.validate(tmp_path / "config")
