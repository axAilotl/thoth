#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py capabilities --json > /tmp/thoth_capabilities_regression.json
jq -e '
  .tool == "thoth"
  and .agent_surfaces.robot_triage == "python thoth.py --robot-triage"
  and (.exit_codes | length) >= 5
  and (.commands[] | select(.name == "stats"))
' /tmp/thoth_capabilities_regression.json >/dev/null
