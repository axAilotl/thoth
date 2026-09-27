"""CCF 0.3.0 portable objects, local storage, and exchange for Thoth."""

from pathlib import Path

CCF_VERSION = "0.3.0"
SPEC_ROOT = Path(__file__).resolve().parent.parent / "spec" / "ccf"
PACKAGE_ROOT = SPEC_ROOT / CCF_VERSION
SCHEMA_BASE = "https://github.com/axAilotl/ccf-schema/spec/0.3.0/schemas"


def schema_id(name: str) -> str:
    """Identifier of a schema loaded from the pinned local package."""
    return f"{SCHEMA_BASE}/{name}.schema.json"
