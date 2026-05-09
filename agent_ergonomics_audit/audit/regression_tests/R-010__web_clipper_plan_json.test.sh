#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py web-clipper --plan --json > /tmp/thoth_web_clipper_plan_regression.json
jq -e '
  .tool == "thoth"
  and .surface == "web-clipper plan"
  and (.records | type) == "array"
  and .mutation.will_index_files == false
  and .mutation.will_queue_notes == false
  and .mutation.will_stage_attachments == false
' /tmp/thoth_web_clipper_plan_regression.json >/dev/null
