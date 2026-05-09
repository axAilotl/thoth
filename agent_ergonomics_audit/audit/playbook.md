# Agent Ergonomics Playbook

Pass 1 focused on surfaces an agent reaches first:

1. Add `capabilities --json` so agents can discover commands, options, exit codes, and robot surfaces without external docs.
2. Add `--robot-triage` / `triage --json` as the one-call quick-ref surface.
3. Add `robot-docs guide` as the in-tool agent handbook.
4. Add `--json` to read-side state commands: `stats`, `db stats`, `wiki-query`, `wiki-lint`, and `archivist --benchmark`.
5. Teach common JSON flag typos and invalid command typos with exact corrected commands.
6. Gate `delete` behind `--yes`, with a safe `--dry-run` command in the error.

The next pass should continue across mutating ingestion commands (`web-clipper`, `ingest-queue`, `x-api-sync`) with dry-run/plan JSON where each command can expose a truthful preview without changing feature semantics.
