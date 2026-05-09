#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py ingest-queue --plan --json --limit 1 > /tmp/thoth_ingest_queue_plan_regression.json
jq -e '
  .tool == "thoth"
  and .surface == "ingest-queue plan"
  and .limit == 1
  and (.entries | type) == "array"
  and .mutation.will_dispatch_artifacts == false
  and .mutation.will_update_queue_status == false
' /tmp/thoth_ingest_queue_plan_regression.json >/dev/null
