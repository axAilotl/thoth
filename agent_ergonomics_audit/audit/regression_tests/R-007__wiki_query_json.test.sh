#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py wiki-query "unlikely query token" --json > /tmp/thoth_wiki_query_regression.json
jq -e '
  .tool == "thoth"
  and .surface == "wiki-query"
  and (.hits | type) == "array"
' /tmp/thoth_wiki_query_regression.json >/dev/null
