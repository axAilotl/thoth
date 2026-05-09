# Agent Ergonomics Playbook

## Pass 2 Focus

The highest-risk gap after Pass 1 was that agents had safe read-side probes, but mutating ingestion commands still forced an all-or-nothing choice. Pass 2 adds plan-first surfaces for the commands most likely to alter queue state, source metadata, checkpoints, or external API state.

## Applied Moves

- Add `--plan --json` before mutation for Web Clipper, queue draining, and X API sync.
- Put the intended mutation contract into the payload as explicit `mutation.* == false` fields.
- Keep stdout data-only and let existing config warnings remain on stderr.
- Return readiness and actionable issues for disabled/misconfigured sources instead of failing the plan invocation.
- Pin every applied recommendation with a shell regression test and targeted Python coverage where state mutation matters.

## Next Moves

Continue with commands that discover or mutate large source sets: `arxiv --discover`, `social --sync`, media enrichment, and migrations. Prefer plan surfaces that can be computed from local config and metadata without network calls.
