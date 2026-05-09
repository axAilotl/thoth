#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
"${PYTHON:-python3}" thoth.py --robot-triage > /tmp/thoth_robot_triage_regression.json
jq -e '
  .tool == "thoth"
  and (.quick_ref | index("python thoth.py stats --json"))
  and (.recommended_next_commands[] | select(.command == "python thoth.py capabilities --json"))
  and (.commands[] | select(.name == "capabilities"))
' /tmp/thoth_robot_triage_regression.json >/dev/null
