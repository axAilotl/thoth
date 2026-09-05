"""Initialize persistent operator files, or validate them before exec.

The image's config symlinks keep API and CLI config ownership unchanged while
letting atomic registry updates and settings edits survive image replacement.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys

APP_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILES = ("config.json", "control.json", ".env", "archivist_topics.yaml")


def initialize(runtime_dir: Path) -> None:
    config_dir = runtime_dir / "config"
    if config_dir.exists() and any(config_dir.iterdir()):
        raise ValueError(f"Refusing to overwrite existing operator files in {config_dir}")
    config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    (runtime_dir / "system").mkdir(exist_ok=True, mode=0o700)
    shutil.copyfile(APP_ROOT / "docker/config.container.example.json", config_dir / "config.json")
    shutil.copyfile(APP_ROOT / "archivist_topics.example.yaml", config_dir / "archivist_topics.yaml")
    (config_dir / "control.json").write_text("{}\n", encoding="utf-8")
    (config_dir / ".env").write_text("# Provider credentials, managed by /settings\n", encoding="utf-8")
    for name in CONFIG_FILES:
        (config_dir / name).chmod(0o600)


def validate(config_dir: Path) -> None:
    for name in CONFIG_FILES:
        path = config_dir / name
        if not path.is_file():
            raise ValueError(f"Missing persistent operator file: {path}")
        if not os.access(path, os.R_OK | os.W_OK):
            raise ValueError(f"Operator file must be readable and writable by the container user: {path}")
    for name in ("config.json", "control.json"):
        value = json.loads((config_dir / name).read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"{name} must contain a JSON object")


def main() -> None:
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "init":
        initialize(Path(args[1]))
        return
    if not args:
        raise ValueError("Expected init RUNTIME_DIR or a command to execute")
    validate(Path("/runtime/config"))
    os.execvp(args[0], args)


if __name__ == "__main__":
    main()
