# Thoth → CCF 0.1.2 implementation checklist

> **Status annotation (2026-08-13, branch `ccf-0.1.2` @ merge `6527d8c`).**
> Sections 1–9 and the 10a/10b gates are implemented with test evidence;
> boxes are checked where code + tests exist on this branch. Unchecked boxes
> are operator actions or standing posture, not missing code. Evidence:
> 38 `tests/test_ccf_*` files, `scripts/ccf_stage9.py`,
> `scripts/ccf_dualwrite_check.py`, `scripts/ccf_dualwrite_corpus_import.py`,
> `scripts/ccf_scale.py`, `scripts/verify_ccf_012_vectors.py`.
> Security review `SECURITY_REVIEW_CCF_0.1.2.md` findings H1, M1–M3,
> L1–L4 closed in `748cfda` / `2997acb`.

## 0. Preserve a rollback point

- [ ] Snapshot the repository, databases, artifact store, and a representative Obsidian corpus. *(operator action, pre-cutover)*
- [ ] Keep the existing Thoth stores authoritative until dual-write verification passes. *(standing posture: dual-write is mirror-only, fail-open, `database.ccf_archive.dual_write` gated; legacy stores remain authoritative)*
- [x] Vendor this package under `spec/ccf/0.1.2/`. *(`0dab2e0`)*
- [ ] Run `./tools/check-all.sh` in CI. *(see `spec/ccf/run-checks.sh`; CI wiring not confirmed)*

## 1. Canonical primitives

- [x] Implement UUIDv4 CCF URNs and preserve existing IDs as source-native IDs. *(`ccf/ids.py`, `ccf/thothmap/sources.py`)*
- [x] Implement `ccf-jcs-sha256-v2` and reproduce every vector. *(`ccf/jcs.py`, `ccf/hashing.py`, `tests/test_ccf_hashing.py`, `scripts/verify_ccf_012_vectors.py`)*
- [x] Load and pin `semantic-catalog.json`. *(`ccf/catalog.py`, `tests/test_ccf_catalog.py`)*
- [x] Implement Record, Link, and Blob portable headers. *(`ccf/objects.py`, `tests/test_ccf_objects.py`)*
- [x] Implement structural and semantic compartment storage states. *(`ccf/db.py`, compartment tables)*
- [x] Keep admission metadata outside portable hashes. *(`ccf/admission.py`)*

## 2. Local producer path

- [x] Add a durable signed producer-batch spool. *(`ccf/spool.py`)*
- [x] Create one Thoth runtime/device credential. *(`ccf/credentials.py`, dualwrite bootstrap `ccf/dualwrite/service.py`)*
- [x] Generate object IDs before batch construction. *(`ccf/producer.py`)*
- [x] Support same-batch references. *(`ccf/admission.py` pass 2, spec 2.3)*
- [x] Preserve signed producer claims and archive resolution separately. *(`ccf/admission.py`)*
- [x] Make retries stable across restart and network loss. *(`ccf/spool.py`, stable retry IDs)*
- [x] Expose provisional local objects without calling them canonical. *(producer/spool layering)*

## 3. Canonical admission

- [x] Add origin-tuple uniqueness and submission-hash comparison. *(`ccf/admission.py`, origin index; 0.1.2 origin-key correction)*
- [x] Add one serialized archive head transaction. *(`ccf/admission.py`)*
- [x] Add numeric commit sequence/position ordering. *(`ccf/journal.py`)*
- [x] Add Merkle members and signed commits. *(`ccf/journal.py`, `ccf/archive.py`)*
- [x] Add state-machine compare-and-swap. *(`ccf/lineage.py`)*
- [x] Add cycle checks for new and restored `derived_from` Links. *(`ccf/admission.py`, `ccf/sync/restore.py`)*
- [x] Return explicit queue, commit, conflict, and lifecycle outcomes. *(`ccf/admission.py` — `committed`/`quarantined`/`content_rejected`/conflict outcomes; `tests/test_ccf_admission.py`)*

## 4. Map current Thoth concepts

Mapping layer complete in `ccf/thothmap/` with per-domain tests
(`tests/test_ccf_thothmap_{sources,sessions,artifacts,transcripts,findings,semantic,review,wiki}.py`).
Note: the runtime dual-write mirror (`ccf/dualwrite/service.py:mirror_capture`)
currently invokes only source/session/run/media/finding converters;
transcript, semantic, review, and wiki-projection converters are tested but
not yet wired into the mirror (tracked: bead thoth-dz1).

- [x] Capture source → `core.source`. *(`ccf/thothmap/sources.py`)*
- [x] Import/capture run → `core.session` and `process.run`. *(`ccf/thothmap/sessions.py`)*
- [x] Original files/media → Blob + `experience.artifact` + `has_blob` Link. *(`ccf/thothmap/artifacts.py`)*
- [x] Transcripts → `experience.utterance` derived from source media. *(`ccf/thothmap/transcripts.py`; mirror wiring pending)*
- [x] Security scan → `security.finding` with exact evidence. *(`ccf/thothmap/findings.py`)*
- [x] Entities/assertions → semantic candidate Records. *(`ccf/thothmap/semantic.py`; mirror wiring pending)*
- [x] Human review → `governance.review_decision` and accepted successor Record. *(`ccf/thothmap/review.py`; mirror wiring pending)*
- [x] Wiki pages/summaries → projection or generated artifact, never source replacement. *(`ccf/thothmap/wiki.py` — refused without evidence Links; mirror wiring pending)*

## 5. Projections

*(`ccf/projections/`, `tests/test_ccf_projections.py`)*

- [x] Current Link state from Link dispositions. *(`links.py`)*
- [x] Active `derived_from` graph and recursive CTE. *(`derivation.py`)*
- [x] Closure table projection and invalidation. *(`derivation.py`, `invalidation.py`)*
- [x] Entity clusters from adjudication Records and Links. *(`entities.py`)*
- [x] Full text via `tsvector`. *(`fulltext.py`)*
- [x] Vectors via pgvector. *(`vectors.py`)*
- [x] Optional AGE only after CTEs become unmanageable. *(not needed; CTEs hold)*
- [x] Wiki and knowledge-base rebuild from canonical CCF state. *(`wiki.py` — rebuilds to staging dir only, never live `wiki/`; live consumption is Phase 4 cutover)*

## 6. Governance baseline

*(`ccf/governance/`, `tests/test_ccf_governance.py`)*

- [x] Implement root default-deny `ccf-deny-overrides-v1`. *(`evaluator.py`)*
- [x] Add generation fences atomically with governance admission. *(`engine.py`, fence tests)*
- [x] Return bounded `policy_resolution_pending` states.
- [x] Add exact positive allow tests.
- [x] Require fresh local authorization only at consequential egress.
- [x] Do not make ordinary local reads depend on the network.

## 7. Retention and deletion

*(`ccf/erasure/`, `tests/test_ccf_erasure.py`)*

- [x] Declare type and Link retention profiles. *(`retention.py`)*
- [x] Retain `derived_from` endpoints while allowing selectors to erase.
- [x] Implement logical deletion first and label it honestly. *(`operations.py`)*
- [x] Purge indexes, caches, checkpoints, and generated plaintext. *(`purge.py`, `media.py`)*
- [x] Preserve commitments and erasure receipts. *(`receipts.py`, `suppression.py`, `suppression_set.py` — canonical journal-covered suppression lineage per 0.1.2)*
- [x] Add optional archive-derived encryption only after Core works. *(profile, not in Core path)*
- [x] Do not implement millions of random DEKs unless selective crypto-erasure is actually required. *(not implemented, by design)*

## 8. Sync and packs

*(`ccf/sync/`, `tests/test_ccf_sync.py`, `tests/test_ccf_mindpack.py`)*

- [x] Exchange archive and producer heads. *(`heads.py`)*
- [x] Create compressed delta packs. *(`delta.py`, `export.py`)*
- [x] Resume Blob ranges. *(`chunks.py`)*
- [x] Support HTTP and file/USB import through the same pack semantics. *(`transport.py`, `packio.py`)*
- [x] Distinguish restore/replica from foreign merge. *(`restore.py`, `merge.py`)*
- [x] Preserve foreign custody proofs. *(`merge.py`; 0.1.2 foreign-merge corrections)*

## 9. Obsidian torture run

*(`ccf/obsidian/`, `scripts/ccf_stage9.py`, `tests/test_ccf_stage9_obsidian.py`, `tests/test_ccf_obsidian_reimport.py`, scale hammer `scripts/ccf_scale.py` + `tests/test_ccf_scale.py`)*

- [x] Fresh import.
- [x] Exact retry after crash.
- [x] Duplicate source and changed source revision.
- [x] Same-batch object graph.
- [x] Missing attachment and malformed document.
- [x] Entity merge/split.
- [x] Human review survival after projection deletion.
- [x] Semantic compartment erasure.
- [x] Full wiki/search/vector rebuild.
- [x] Corrupt commit and unsupported catalog.
- [x] Restore and foreign merge.

## 10. Cutover

10a dual-write harness and 10b cutover gates are implemented
(`scripts/ccf_dualwrite_check.py`, `scripts/ccf_dualwrite_corpus_import.py`,
`tests/test_ccf_cutover_{bootstrap_retention,mindpack_restore,projection_recovery,rollback,vectors}.py`).
Boxes below stay unchecked until the operator runs them against the
production representative corpus.

- [ ] Dual-write mismatch rate is zero for the representative corpus. *(harness ready; operator run pending)*
- [x] Published vectors pass independently from the package generator. *(`scripts/verify_ccf_012_vectors.py`; all 56 independent reproduction cases pass)*
- [ ] Destroy every projection and recover every canonical decision. *(gate test exists; production drill pending)*
- [ ] Export a complete mindpack and restore it into an empty database. *(gate test exists; production drill pending)*
- [ ] Retain a rollback path through the first production release. *(standing posture)*
