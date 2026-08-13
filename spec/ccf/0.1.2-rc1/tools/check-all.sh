#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON_BIN="${PYTHON:-python3}"
sha256sum --check --quiet SHA256SUMS
"$PYTHON_BIN" tools/validate-package.py
node tools/verify-vectors.mjs
node tools/verify-example-mindpack.mjs
node tools/verify-conformance.mjs
tools/verify-postgres-fixture.sh
sha256sum --check --quiet SHA256SUMS
echo "CCF 0.1.2-rc1 package checks passed."
