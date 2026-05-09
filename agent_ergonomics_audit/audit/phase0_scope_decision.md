# Phase 0 Scope Decision

- Target: `/mnt/samesung/ai/thoth`
- Tool entrypoint: `python thoth.py` and executable `./thoth.py`
- Mode: `full`
- Branch policy: stayed on current branch `integration/universal-loop`; no branch created.
- Workspace: `agent_ergonomics_audit/` in-tree; no sibling workspace.
- CASS mining: skipped for this first implementation pass to keep focus on live CLI evidence.
- Scope guardrails: no feature work, no ingestion behavior redesign, no destructive git operations.
- Runtime behavior changes: update `README.md` and `SKILL.md` alongside code changes.
