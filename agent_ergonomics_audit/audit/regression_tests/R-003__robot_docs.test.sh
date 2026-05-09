#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
output="$("${PYTHON:-python3}" thoth.py robot-docs guide)"
grep -F "python thoth.py --robot-triage" <<<"$output" >/dev/null
grep -F "python thoth.py delete <tweet_id> --dry-run" <<<"$output" >/dev/null
grep -F "JSON commands write JSON to stdout only." <<<"$output" >/dev/null
