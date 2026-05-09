# Uplift Diff

This is the first in-tree pass, so there is no prior committed scorecard to diff against.

Practical uplift from the live baseline observed during Phase 0:

- `capabilities --json`: missing -> present, parseable JSON stdout.
- `--robot-triage`: missing -> present, one-call quick-ref and health payload.
- `robot-docs guide`: missing -> present, in-tool agent handbook.
- `stats --json` and `db stats --json`: missing -> present.
- `wiki-query --json`, `wiki-lint --json`, `archivist --benchmark --json`: missing -> present.
- `stats --jsno`: argparse failure -> inferred `--json` with stderr hint.
- `stat --json`: generic invalid-choice error -> nearest-command hint.
- `delete <id>`: immediate destructive operation -> exit 2 safety block with `--dry-run` and `--yes` commands.
