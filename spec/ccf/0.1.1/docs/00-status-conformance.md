# 0. Status, scope, and conformance

## 0.1 Version and status

This package defines **Continuity Core Format 0.1.1**.

The following architectural decisions are frozen for this version:

- exactly three portable semantic object kinds: **Record**, **Link**, and **Blob**;
- portable object identity generated before admission;
- a portable header separated from structural and semantic compartments;
- archive-local admission order separated from portable object hashes;
- append-only correction and adjudication;
- one signed canonical head per archive epoch;
- projections are disposable and rebuildable;
- no required agent framework, graph database, companion runtime, device, key manager, or cloud topology.

The protocol is an implementation candidate until at least one independent implementation reproduces the published vectors and passes the mandatory suite.

## 0.2 Mandatory and optional profiles

`ccf-core-0.1.1` is mandatory. Other profiles compose without changing the three-object model:

| Profile | Status | Purpose |
|---|---|---|
| `ccf-core-0.1.1` | required | Objects, compartments, hashing, admission, journal, lineages, baseline governance, mindpack |
| `ccf-local-sync-0.1.1` | recommended | Offline signed producer batches and resumable delta synchronization |
| `ccf-archive-encryption-derived-v1` | optional | One epoch secret wrapped to authorized recipients; derived compartment keys |
| `ccf-object-erasure-v1` | optional | Random per-object keys and selective crypto-erasure |
| `ccf-witnessed-integrity-v1` | optional | Independent head checkpoints and rollback detection |
| `ccf-succession-v1` | optional | Signer rotation and preauthorized successor activation |
| continuity/work/agent packs | optional | Domain-specific semantic types |

A deployment MUST state every profile it claims and MUST NOT imply a stronger property than those profiles provide.

## 0.3 Scope

CCF standardizes:

1. portable IDs and references;
2. portable headers and governed compartments;
3. source submissions, producer claims, archive resolution, and idempotency;
4. archive admission order and signed commit history;
5. append-only current-state lineages;
6. provenance, authority, privacy, policy, and valid-time semantics;
7. mindpack restore, foreign merge, and delta transfer;
8. semantic-catalog pinning and unknown-extension preservation;
9. a reference Postgres operational envelope.

CCF does not standardize a model, prompt, persona, UI, agent topology, metaphysical truth, consciousness claim, legal conclusion, or universal hosted-service custody mode.

## 0.4 Normative language and source precedence

`MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, and `MAY` are normative.

Precedence on contradiction:

1. published executable vectors;
2. JSON Schemas and activated registries;
3. normative specification chapters;
4. examples and adoption guidance.

A contradiction at levels 1–3 is a specification defect. Implementations MUST NOT silently invent a local interpretation.

## 0.5 Conformance classes

- **Preserver** — preserves known and unknown objects and compartments without activating unknown semantics.
- **Producer** — creates stable IDs, valid submissions, and signed producer batches.
- **Archive** — resolves submissions, admits canonical objects, maintains one journal head, and validates lineages.
- **Processor** — emits derived objects with exact evidence and declared authority.
- **Importer/Exporter** — performs restore, replica, foreign merge, and completeness reporting.
- **Policy Evaluator** — implements the pinned baseline evaluator and generation-fence rules.
- **Profile Implementer** — implements one named optional profile and its declared claims.

A system that drops unknown objects, rewrites portable IDs, treats inference as acceptance, or bypasses governance is not conformant.
