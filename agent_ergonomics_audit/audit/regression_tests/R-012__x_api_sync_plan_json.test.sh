#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py x-api-sync --plan --json --max-pages 1 --max-results 10 > /tmp/thoth_x_api_sync_plan_regression.json
jq -e '
  .tool == "thoth"
  and .surface == "x-api-sync plan"
  and .parameters.max_pages == 1
  and .parameters.max_results == 10
  and .mutation.will_contact_x_api == false
  and .mutation.will_update_checkpoint == false
' /tmp/thoth_x_api_sync_plan_regression.json >/dev/null
