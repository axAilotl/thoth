#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py archivist --benchmark --limit 0 --json > /tmp/thoth_archivist_benchmark_regression.json
jq -e '
  .tool == "thoth"
  and .surface == "archivist benchmark"
  and .topics == []
' /tmp/thoth_archivist_benchmark_regression.json >/dev/null
