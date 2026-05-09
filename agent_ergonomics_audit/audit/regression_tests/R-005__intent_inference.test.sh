#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py stats --jsno >/tmp/thoth_jsno_regression.json 2>/tmp/thoth_jsno_regression.err
jq -e '.tool == "thoth"' /tmp/thoth_jsno_regression.json >/dev/null
grep -F 'Interpreted `--jsno` as `--json`' /tmp/thoth_jsno_regression.err >/dev/null

if "${PYTHON:-python3}" thoth.py stat --json >/tmp/thoth_bad_command.out 2>/tmp/thoth_bad_command.err; then
  echo "expected invalid command to fail" >&2
  exit 1
fi
grep -F 'Did you mean `stats`?' /tmp/thoth_bad_command.err >/dev/null
