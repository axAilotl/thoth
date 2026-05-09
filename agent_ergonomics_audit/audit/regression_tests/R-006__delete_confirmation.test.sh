#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
if "${PYTHON:-python3}" thoth.py delete 1234567890 >/tmp/thoth_delete_guard.out 2>/tmp/thoth_delete_guard.err; then
  echo "delete without --yes unexpectedly succeeded" >&2
  exit 1
fi
grep -F "Refusing to delete artifacts without explicit confirmation" /tmp/thoth_delete_guard.err >/dev/null
grep -F "python thoth.py delete 1234567890 --dry-run" /tmp/thoth_delete_guard.err >/dev/null
grep -F "python thoth.py delete 1234567890 --yes" /tmp/thoth_delete_guard.err >/dev/null
