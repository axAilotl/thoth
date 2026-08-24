#!/usr/bin/env python3
"""Build the deterministic, permission-neutral CCF release ZIP."""

from __future__ import annotations

import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT.parent / f"ccf-{ROOT.name}.zip"
FIXED_TIMESTAMP = (2026, 8, 13, 0, 0, 0)


def main() -> None:
    with zipfile.ZipFile(
        OUTPUT, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for source in sorted(path for path in ROOT.rglob("*") if path.is_file()):
            relative = source.relative_to(ROOT.parent).as_posix()
            info = zipfile.ZipInfo(relative, date_time=FIXED_TIMESTAMP)
            info.create_system = 0  # FAT: no Unix executable-bit dependency
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0
            archive.writestr(info, source.read_bytes(), compresslevel=9)
    print(f"release ZIP: {OUTPUT}")


if __name__ == "__main__":
    main()
