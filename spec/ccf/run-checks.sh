#!/usr/bin/env bash
# Run the vendored CCF package checks for every pinned spec version
# (spec/ccf/0.1.1, spec/ccf/0.1.2, and the 0.2.0 working-draft overlay).
#
# Each package's tools/check-all.sh expects `python` with jsonschema and
# referencing installed (see its requirements-checks.txt). This wrapper
# provides one isolated venv so the spec tooling stays out of Thoth's
# runtime dependencies. The 0.1.2 checks additionally need node,
# docker, and psql (the multi-schema pgvector fixture).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"

if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
fi
# Keep the venv aligned with the strictest requirements-checks.txt.
"$VENV/bin/pip" install -q -r "$ROOT/0.1.2/requirements-checks.txt"

cd "$ROOT/0.1.1"
# Integrity of the vendored copy against the published package manifest.
sha256sum -c SHA256SUMS --quiet
export PATH="$VENV/bin:$PATH"
bash tools/check-all.sh

# --- 0.1.2 -------------------------------------------------------------
# The final package vendors its explicitly TEST-ONLY signing keys, so it
# can verify and reproduce itself directly from the extracted tree.
cd "$ROOT/0.1.2"
sha256sum -c SHA256SUMS --quiet
PYTHON="$VENV/bin/python" bash tools/check-all.sh

# --- 0.2.0 working draft ----------------------------------------------
# Exchange is the default draft gate and does not require Docker.
cd "$ROOT/0.2.0"
PYTHON="$VENV/bin/python" python tools/validate-exchange.py
