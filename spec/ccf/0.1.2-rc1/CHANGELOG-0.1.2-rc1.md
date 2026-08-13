# CCF 0.1.2-rc1 change log

0.1.2-rc1 is a narrow implementation-informed correction to 0.1.1. It does
not redesign Record, Link, Blob, compartment commitments, or the
canonical/projection boundary.

## Normative corrections

- Origin idempotency now keys on archive, source, native ID, revision, and
  object kind; same-kind multiplicity requires stable native suffixes.
- `admission-authority-classes.registry.json` pins the first reference
  implementation's deterministic authority mapping.
- Foreign merge preserves unavailable compartment state, commitments,
  retention, custody proofs, and erasure/withholding lineage.
- Cryptographically valid producer batches remain chain predecessors after
  content rejection or quarantine; missing exact predecessors remain pending
  and retryable.
- Suppression commitments are canonical, journal-covered erasure lineage;
  suppression lookup rows are rebuildable projections.
- Chain verification proves exact signed-member/admission correspondence.

## Regenerated artifacts

- Semantic catalog and genesis example.
- Mindpack with per-compartment availability declarations.
- Authority and implementation-informed conformance vectors.
- PostgreSQL reference SQL, package inventory, and SHA256SUMS.

## Conformance additions

The executable suite adds the twelve regressions listed in
`docs/13-conformance.md` §13.8,
including cross-kind origin reuse, producer-chain liveness, unavailable foreign
merge, canonical suppression rebuild, admission tamper detection, multi-schema
pgvector discovery, a real three-commit Git fixture, and positive/negative
vectors for every authority class.
