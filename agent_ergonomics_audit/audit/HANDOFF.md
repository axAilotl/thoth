# Agent Ergonomics Pass 1 Handoff

Pass 1 applied the highest-leverage robot and structured-read surfaces to the Thoth CLI.

Implemented:

- `core/cli_agent.py`: capabilities contract, robot triage payload, robot docs, typo normalization, teaching argparse errors, JSON-safe conversion helper.
- `core/cli_agent_stats.py`: structured stats collection and human rendering.
- `thoth.py`: wired robot commands, `--json` read-side outputs, delete confirmation gating, and parser typo handling.
- `tests/test_cli_commands.py`: regression coverage for robot contracts, JSON surfaces, typo hints, and delete safety.
- `README.md` and `SKILL.md`: operator-visible behavior updates.

Validation:

- `validate_scorecard.sh agent_ergonomics_audit/audit/agent_surfaces.jsonl`
- `for test in agent_ergonomics_audit/audit/regression_tests/R-*.sh; do bash "$test"; done`
- `.venv/bin/python -m pytest tests/test_cli_commands.py`
- `verify-stdout-stderr-split.sh ./thoth.py stats`
- `verify-stdout-stderr-split.sh ./thoth.py capabilities`
- `verify-determinism.sh ./thoth.py capabilities`
- `verify-non-tty-discipline.sh ./thoth.py stats`

Deferred to Pass 2:

- Add truthful `--json --dry-run` or `--plan` surfaces to mutating ingestion commands where preview data can be produced without side effects.
- Consider `--json` for `ingest-queue` after separating queue inspection from queue mutation.
- Expand scoring beyond the 11 highest-leverage surfaces to every command and flag in `surface_inventory.jsonl`.
