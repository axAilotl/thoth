# Decision: evidence-free semantic memory promotion is rejected

**Bead:** thoth-13k
**Date:** 2026-08-30
**Status:** Decided

## Context

`core/semantic_memory.py` gates promotion of a semantic-memory candidate from
`confirmed` to `promoted`. The gate evaluates durable evidence items attached to
the candidate, explicit operator confirmation, and trusted structured input.

A previous review (thoth-7v1, finding #22) changed the gate so that candidates
with zero evidence fail promotion even when `explicit_confirmation` or
`trusted_structured_input` is present. This silently revoked a documented
operator path, so thoth-13k was opened to decide whether to keep the hard fail
or restore evidence-free promotion with visible provenance.

## Decision

Keep the hard fail.

`explicit_confirmation` and `trusted_structured_input` are **not evidence** by
themselves. They are gate qualifiers that only apply when at least one durable
evidence item is present. A candidate with zero evidence items is rejected with
reason `missing_evidence` and a `SemanticMemoryTransitionError` is raised on
attempted promotion.

Durable promoted facts must always retain visible source evidence and provenance
so the wiki compiler and any downstream consumer can trace a fact back to its
source captures.

## Consequences

- Promotion requires either:
  - explicit confirmation **plus** at least one evidence item, or
  - trusted structured input **plus** at least one evidence item, or
  - repeated evidence meeting configured thresholds (default ≥2 items from ≥2
    distinct sources).
- Operators cannot promote a fact solely by confirming it or marking it as
  trusted structured input.
- Existing tests and docs are updated to match this behavior.

## References

- `core/semantic_memory.py` promotion gate (`_promotion_decision_in_connection`)
- `core/semantic_memory_promotion.py` policy docstring
- `tests/test_semantic_memory.py` zero-evidence promotion tests
- `docs/project_overview.md` human-in-the-loop section
- `docs/code_review_fix_reference.md` success criteria
