#!/usr/bin/env bash
# Run the vendored CCF package checks for every pinned spec version
# (spec/ccf/0.1.1 and spec/ccf/0.1.2-rc1).
#
# Each package's tools/check-all.sh expects `python` with jsonschema and
# referencing installed (see its requirements-checks.txt). This wrapper
# provides one isolated venv so the spec tooling stays out of Thoth's
# runtime dependencies. The 0.1.2-rc1 checks additionally need node,
# docker, and psql (the multi-schema pgvector fixture).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"

if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
fi
# Keep the venv aligned with the strictest requirements-checks.txt.
"$VENV/bin/pip" install -q -r "$ROOT/0.1.2-rc1/requirements-checks.txt"

cd "$ROOT/0.1.1"
# Integrity of the vendored copy against the published package manifest.
sha256sum -c SHA256SUMS --quiet
export PATH="$VENV/bin:$PATH"
bash tools/check-all.sh

# --- 0.1.2-rc1 -------------------------------------------------------------
# The repo's .gitignore excludes *-ed25519-private.pem, so the vendored
# rc1 tree lacks the two TEST-ONLY private keys its own SHA256SUMS pins.
# They are byte-identical to the 0.1.1 test keys (same deterministic
# material; the public pems match). Run the rc1 checks against a temp
# copy with those keys restored — the vendored tree is never modified.
RC1_WORK="$(mktemp -d)"
trap 'rm -rf "$RC1_WORK"' EXIT
cp -a "$ROOT/0.1.2-rc1/." "$RC1_WORK/"
cp "$ROOT/0.1.1/vectors/TEST-ONLY-archive-ed25519-private.pem" \
   "$ROOT/0.1.1/vectors/TEST-ONLY-device-ed25519-private.pem" \
   "$RC1_WORK/vectors/"
cd "$RC1_WORK"
sha256sum -c SHA256SUMS --quiet
PYTHON="$VENV/bin/python" bash tools/check-all.sh
