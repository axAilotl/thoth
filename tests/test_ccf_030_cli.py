"""The public CLI works without touching native state or needing model keys."""

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_cli_import_export_status_and_acknowledgements(tmp_path):
    store = tmp_path / "ccf.sqlite3"

    def run(*args):
        result = subprocess.run([sys.executable, str(ROOT / "thoth.py"), "ccf", "--store", str(store), *args],
                                cwd=ROOT, capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    assert run("status")["version"] == "0.3.0"
    imported = run("import", str(ROOT / "spec/ccf/0.3.0/examples/export"))
    assert imported["counts"]["inserted"] > 0
    pending = run("changes", "phone")
    assert pending["objects"]
    assert run("changes", "phone") == pending
    run("ack", "phone", str(pending["cursor"]))
    assert run("changes", "phone")["objects"] == []
    assert run("changes", "other")["objects"] == pending["objects"]
    exported = run("export", str(tmp_path / "export.zip"))
    assert not exported["errors"]
