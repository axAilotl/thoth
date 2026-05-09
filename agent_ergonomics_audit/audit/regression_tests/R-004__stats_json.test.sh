#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py stats --json > /tmp/thoth_stats_regression.json
jq -e '
  .tool == "thoth"
  and (.graphql_cache | has("responses_cached"))
  and (.media_files | has("total"))
' /tmp/thoth_stats_regression.json >/dev/null

"${PYTHON:-python3}" thoth.py db stats --json > /tmp/thoth_db_stats_regression.json
jq -e '
  .tool == "thoth"
  and .surface == "db stats"
  and (.database | has("total_records"))
' /tmp/thoth_db_stats_regression.json >/dev/null
