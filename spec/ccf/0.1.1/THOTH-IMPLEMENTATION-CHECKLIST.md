# Thoth → CCF 0.1.1 implementation checklist

## 0. Preserve a rollback point

- [ ] Snapshot the repository, databases, artifact store, and a representative Obsidian corpus.
- [ ] Keep the existing Thoth stores authoritative until dual-write verification passes.
- [ ] Vendor this package under `spec/ccf/0.1.1/`.
- [ ] Run `./tools/check-all.sh` in CI.

## 1. Canonical primitives

- [ ] Implement UUIDv4 CCF URNs and preserve existing IDs as source-native IDs.
- [ ] Implement `ccf-jcs-sha256-v2` and reproduce every vector.
- [ ] Load and pin `semantic-catalog.json`.
- [ ] Implement Record, Link, and Blob portable headers.
- [ ] Implement structural and semantic compartment storage states.
- [ ] Keep admission metadata outside portable hashes.

## 2. Local producer path

- [ ] Add a durable signed producer-batch spool.
- [ ] Create one Thoth runtime/device credential.
- [ ] Generate object IDs before batch construction.
- [ ] Support same-batch references.
- [ ] Preserve signed producer claims and archive resolution separately.
- [ ] Make retries stable across restart and network loss.
- [ ] Expose provisional local objects without calling them canonical.

## 3. Canonical admission

- [ ] Add origin-tuple uniqueness and submission-hash comparison.
- [ ] Add one serialized archive head transaction.
- [ ] Add numeric commit sequence/position ordering.
- [ ] Add Merkle members and signed commits.
- [ ] Add state-machine compare-and-swap.
- [ ] Add cycle checks for new and restored `derived_from` Links.
- [ ] Return explicit queue, commit, conflict, and lifecycle outcomes.

## 4. Map current Thoth concepts

- [ ] Capture source → `core.source`.
- [ ] Import/capture run → `core.session` and `process.run`.
- [ ] Original files/media → Blob + `experience.artifact` + `has_blob` Link.
- [ ] Transcripts → `experience.utterance` derived from source media.
- [ ] Security scan → `security.finding` with exact evidence.
- [ ] Entities/assertions → semantic candidate Records.
- [ ] Human review → `governance.review_decision` and accepted successor Record.
- [ ] Wiki pages/summaries → projection or generated artifact, never source replacement.

## 5. Projections

- [ ] Current Link state from Link dispositions.
- [ ] Active `derived_from` graph and recursive CTE.
- [ ] Closure table projection and invalidation.
- [ ] Entity clusters from adjudication Records and Links.
- [ ] Full text via `tsvector`.
- [ ] Vectors via pgvector.
- [ ] Optional AGE only after CTEs become unmanageable.
- [ ] Wiki and knowledge-base rebuild from canonical CCF state.

## 6. Governance baseline

- [ ] Implement root default-deny `ccf-deny-overrides-v1`.
- [ ] Add generation fences atomically with governance admission.
- [ ] Return bounded `policy_resolution_pending` states.
- [ ] Add exact positive allow tests.
- [ ] Require fresh local authorization only at consequential egress.
- [ ] Do not make ordinary local reads depend on the network.

## 7. Retention and deletion

- [ ] Declare type and Link retention profiles.
- [ ] Retain `derived_from` endpoints while allowing selectors to erase.
- [ ] Implement logical deletion first and label it honestly.
- [ ] Purge indexes, caches, checkpoints, and generated plaintext.
- [ ] Preserve commitments and erasure receipts.
- [ ] Add optional archive-derived encryption only after Core works.
- [ ] Do not implement millions of random DEKs unless selective crypto-erasure is actually required.

## 8. Sync and packs

- [ ] Exchange archive and producer heads.
- [ ] Create compressed delta packs.
- [ ] Resume Blob ranges.
- [ ] Support HTTP and file/USB import through the same pack semantics.
- [ ] Distinguish restore/replica from foreign merge.
- [ ] Preserve foreign custody proofs.

## 9. Obsidian torture run

- [ ] Fresh import.
- [ ] Exact retry after crash.
- [ ] Duplicate source and changed source revision.
- [ ] Same-batch object graph.
- [ ] Missing attachment and malformed document.
- [ ] Entity merge/split.
- [ ] Human review survival after projection deletion.
- [ ] Semantic compartment erasure.
- [ ] Full wiki/search/vector rebuild.
- [ ] Corrupt commit and unsupported catalog.
- [ ] Restore and foreign merge.

## 10. Cutover

- [ ] Dual-write mismatch rate is zero for the representative corpus.
- [ ] Published vectors pass independently from the package generator.
- [ ] Destroy every projection and recover every canonical decision.
- [ ] Export a complete mindpack and restore it into an empty database.
- [ ] Retain a rollback path through the first production release.
