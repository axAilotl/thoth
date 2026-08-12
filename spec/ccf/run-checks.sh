#!/usr/bin/env bash
# Run the vendored CCF 0.1.1 package checks (spec/ccf/0.1.1/tools/check-all.sh).
#
# The package's check-all.sh expects `python` on PATH with jsonschema and
# referencing installed. This wrapper provides an isolated venv so the spec
# tooling stays out of Thoth's runtime dependencies.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG="$ROOT/0.1.1"
VENV="$ROOT/.venv"

if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q jsonschema referencing
fi

cd "$PKG"
# Integrity of the vendored copy against the published package manifest.
sha256sum -c SHA256SUMS --quiet
export PATH="$VENV/bin:$PATH"
bash tools/check-all.sh
