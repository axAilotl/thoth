# Continuity Core Format 0.1.1

CCF 0.1.1 is a local-first canonical backend and interchange specification for person-governed experience, knowledge, continuity, provenance, and derived work.

## Start here

- [`CCF-0.1.1-SPEC.md`](CCF-0.1.1-SPEC.md) — consolidated normative specification.
- [`CHANGELOG-0.1.1.md`](CHANGELOG-0.1.1.md) — Draft 1 to 0.1.1 changes.
- [`docs/14-thoth-adoption.md`](docs/14-thoth-adoption.md) — implementation mapping for Thoth.
- [`THOTH-IMPLEMENTATION-CHECKLIST.md`](THOTH-IMPLEMENTATION-CHECKLIST.md) — staged migration and torture-test plan.
- [`schemas/catalog.json`](schemas/catalog.json) — immutable schema catalog.
- [`semantic-catalog.json`](semantic-catalog.json) — exact activated schema and registry digests.
- [`registries/profiles.registry.json`](registries/profiles.registry.json) — core and optional profiles.
- [`sql/postgres-reference.sql`](sql/postgres-reference.sql) — reference operational envelope.
- [`openapi/ccf-api.openapi.yaml`](openapi/ccf-api.openapi.yaml) — batch, sync, read, and export contract.
- [`vectors/`](vectors/) — executable canonicalization, hashing, signing, and ordering expectations.

## Status

The **Record / Link / Blob algebra** and canonical-versus-projection boundary are frozen for 0.1.1. The package is an implementation candidate: the bytes become a frozen interoperability profile after an independent implementation reproduces the vectors and passes the conformance suite.

## What changed from Draft 1

- Canonical IDs are producer-generated UUIDv4 URNs, not timestamp-bearing UUIDv7.
- Each object has a portable header plus separate structural and semantic compartments.
- Exact types may be sealed; retained structural metadata is minimized and explicitly classified.
- Encryption is no longer a mandatory Core dependency.
- Archive-derived encryption, selective object erasure, witnessed integrity, and succession are separate profiles.
- Offline signed producer-batch chains and resumable packs are first-class.
- Genesis pins an exact semantic-catalog root.
- Core integrity is a signed Merkle journal, not a blockchain or consensus network.
- Governance is local and fail-closed; network round trips are required only by a deployment's chosen action path, not by CCF Core.

## Build order

1. Implement `ccf-jcs-sha256-v2` and pass vectors.
2. Validate the semantic catalog, registries, and JSON Schemas.
3. Implement portable headers and structural/semantic compartment storage.
4. Implement signed producer batches, stable retry IDs, and local admission.
5. Implement commit membership, Merkle roots, and the signed archive journal.
6. Add lineage compare-and-swap and Link dispositions.
7. Add projection generation fences and baseline policy evaluation.
8. Add mindpack restore, foreign merge, and delta packs.
9. Add optional encryption, witnessed-head, succession, or high-assurance erasure profiles only where needed.
10. Build search, vectors, graph views, wiki pages, and agent context as projections.

Run `./tools/check-all.sh` to validate the package.
