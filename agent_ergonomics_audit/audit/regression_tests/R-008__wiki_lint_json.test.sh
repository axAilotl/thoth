#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
set +e
"${PYTHON:-python3}" thoth.py wiki-lint --json > /tmp/thoth_wiki_lint_regression.json
rc=$?
set -e
case "$rc" in
  0|1) ;;
  *) echo "unexpected wiki-lint exit code: $rc" >&2; exit 1 ;;
esac
jq -e '
  .tool == "thoth"
  and .surface == "wiki-lint"
  and (.report.pages_checked | type) == "number"
  and (.report.issues | type) == "array"
' /tmp/thoth_wiki_lint_regression.json >/dev/null
