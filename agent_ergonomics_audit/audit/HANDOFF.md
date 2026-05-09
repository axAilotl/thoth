# Agent Ergonomics Pass 2 Handoff

Pass 2 focused on truthful plan surfaces for mutating ingestion commands.

Implemented:

- `web-clipper --plan --json`: read-only source scan with readiness/issues, counts, record summaries, and explicit mutation=false fields. Disabled or misconfigured Web Clipper now returns a machine-readable plan instead of forcing a source-code read.
- `ingest-queue --plan --json`: due queue preview with entry summaries, counts, and no queue status mutation or dispatch.
- `x-api-sync --plan --json`: no-network sync readiness probe with parameters, scopes, token/checkpoint presence, and no secret material.
- `web-clipper --json`, `ingest-queue --json`, and `x-api-sync --json`: structured run summaries for actual execution.
- `core/cli_plan_surfaces.py`: focused payload/rendering helpers for plan and JSON run summaries.
- `core/cli_agent.py`, `README.md`, and `SKILL.md`: capabilities and operator docs list the new plan probes.
- Regression coverage for all three plan surfaces plus collector-level proof that Web Clipper planning does not write file metadata or queue rows.

Validation to rerun:

- `.venv/bin/python -m pytest tests/test_cli_commands.py tests/test_web_clipper_collector.py tests/test_ingestion_runtime.py`
- `for test in agent_ergonomics_audit/audit/regression_tests/R-*.test.sh; do bash "$test"; done`
- `tools/validate_scorecard.sh agent_ergonomics_audit/audit/agent_surfaces.jsonl`
- `scripts/validate_pass.sh /mnt/samesung/ai/thoth/agent_ergonomics_audit`

Deferred to Pass 3:

- Score the full remaining CLI inventory instead of only the highest-risk mutation probes.
- Add structured previews or guarded JSON summaries to other mutation-heavy commands such as `arxiv --discover`, `social --sync`, media post-processing, and migrations where the command can safely describe intended work.
- Expand error-teaching for command-specific invalid flag combinations.

Generated: 2026-05-09T21:10:37Z
