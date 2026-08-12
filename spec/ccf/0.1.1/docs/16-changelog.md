# 16. 0.1.1 change log

0.1.1 supersedes the unpublished Draft 1 wire candidate.

## Wire and identity

- UUIDv7 portable IDs replaced by producer-generated UUIDv4 URNs.
- Exact semantic type removed from permanent headers.
- One governed body replaced by structural and semantic compartments.
- Hash profile replaced by `ccf-jcs-sha256-v2`.
- Schema and registry semantics pinned by a semantic-catalog root.

## Local-first operation

- Added signed producer-batch chains and stable same-batch references.
- Added provisional local state and explicit queue/commit outcomes.
- Added resumable delta packs and file/USB-compatible sync semantics.

## Security and custody

- Removed mandatory per-object DEKs from Core.
- Added optional archive-derived encryption and high-assurance object-erasure profiles.
- Added device credentials and wrapped keyset manifests.
- Clarified prefix integrity, rollback witnesses, and operator trust limits.
- Clarified provider-blind storage versus plaintext cloud computation.

## Retention and erasure

- Added registry-level retention profiles.
- Defined structurally retained Link endpoints and commit material.
- Added assurance levels for logical, storage-verified, and cryptographic erasure.
- Kept multi-subject media deletion as a reviewed decision, not an automatic legal conclusion.

## Governance and replay

- Made compare-and-swap mandatory for every stateful lineage.
- Added valid-time semantics separate from admission precedence.
- Added semantic catalog transitions, generation fences, contextual decisions, and fenced egress capabilities.
- Split restore/replica from foreign merge.
