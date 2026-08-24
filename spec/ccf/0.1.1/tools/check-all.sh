#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
node tools/build-catalog.mjs
node tools/build-example.mjs
node tools/build-vectors.mjs
python tools/validate-package.py
node tools/verify-vectors.mjs
node tools/verify-example-mindpack.mjs
echo "CCF 0.1.1 package checks passed."
