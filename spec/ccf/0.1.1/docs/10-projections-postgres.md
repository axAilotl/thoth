# 10. Projections and the Postgres reference envelope

## 10.1 One operational database

The reference deployment keeps the operational envelope in Postgres:

```text
lexical search  → tsvector
semantic search → pgvector
graph traversal → recursive CTE / closure projection / optional Apache AGE
```

An external graph database is a deployment-owned projection, not a Core dependency.

## 10.2 Required canonical tables

The reference schema separates:

- portable object headers;
- structural and semantic compartment storage state;
- archive-local admissions;
- commit journal and members;
- origin/idempotency index;
- lineage heads;
- producer batch spool and receipts.

Attachments are canonical Links. Any Record–Blob join table is a projection.

## 10.3 Derivation closure

Build the active `derived_from` closure early because policy propagation depends on it. It contains active paths only and stores computation generation and commit sequence.

The correctness fallback is a recursive CTE. If closure growth exceeds deployment bounds, the system may use depth-capped materialization plus on-demand traversal, degrading latency rather than correctness.

## 10.4 Projection invalidation

Canonical mutations create invalidation causes and advance coarse generation fences synchronously. Background workers then compute fine-grained affected descendants.

A projection row is usable only if:

```text
computed_through_sequence >= latest_affecting_sequence
all dependency generations match
no unresolved invalidation covers the row
```

The fast path must determine this from projection metadata, not by replaying history on every read.

## 10.5 Checkpoints

Projection checkpoints are accelerations, not authority. They include projection name, generation, source head, dependency generations, snapshot digest, and storage reference.

Recovery validates the newest checkpoint and replays later commits. A corrupt checkpoint falls back to an older checkpoint or genesis.

Checkpoints containing protected data remain encrypted under the same erasure unit or participate in purge verification.

## 10.6 Cross-projection consistency

A request that uses multiple projections MUST pin one archive head and one compatible dependency-generation vector. Mixing independently current projections without a snapshot contract is nonconformant for consequential results.

## 10.7 Reproducible consequential selections

When a projection selection materially affects a decision or action, preserve:

- exact selected object IDs and selectors;
- archive head and generation vector;
- embedding, ranker, model, and evaluator versions;
- authorization decision or capability;
- generated output provenance.

The projection remains rebuildable; the historical selection becomes canonical evidence.
