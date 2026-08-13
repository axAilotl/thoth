# Continuity Core Format 0.1.2-rc1

CCF 0.1.2-rc1 is a local-first canonical backend and interchange specification for person-governed experience, knowledge, continuity, provenance, and derived work.

## Start here

- [`CCF-0.1.2-rc1-SPEC.md`](CCF-0.1.2-rc1-SPEC.md) — consolidated normative specification.
- [`CHANGELOG-0.1.2-rc1.md`](CHANGELOG-0.1.2-rc1.md) — narrow 0.1.1 to 0.1.2-rc1 corrections.
- [`docs/14-thoth-adoption.md`](docs/14-thoth-adoption.md) — implementation mapping for Thoth.
- [`THOTH-IMPLEMENTATION-CHECKLIST.md`](THOTH-IMPLEMENTATION-CHECKLIST.md) — staged migration and torture-test plan.
- [`schemas/catalog.json`](schemas/catalog.json) — immutable schema catalog.
- [`semantic-catalog.json`](semantic-catalog.json) — exact activated schema and registry digests.
- [`registries/profiles.registry.json`](registries/profiles.registry.json) — core and optional profiles.
- [`sql/postgres-reference.sql`](sql/postgres-reference.sql) — reference operational envelope.
- [`openapi/ccf-api.openapi.yaml`](openapi/ccf-api.openapi.yaml) — batch, sync, read, and export contract.
- [`vectors/`](vectors/) — executable canonicalization, hashing, signing, and ordering expectations.

## Status

The **Record / Link / Blob algebra** and canonical-versus-projection boundary
remain unchanged from 0.1.1. This release candidate packages corrections found
by the first full reference-implementation pressure test. Cut final 0.1.2 only
after an independent clean reproduction of the catalog, genesis, vectors,
mindpack, SQL, inventory, checksums, and conformance suite.

## What changed from 0.1.1

- Origin idempotency includes object kind and stable same-kind native suffixes.
- Admission-authority semantics are an exact catalog-pinned registry.
- Unavailable foreign compartments retain their exact canonical state and proof.
- Content rejection cannot brick a valid producer chain; early batches stay pending.
- Suppression authority is canonical and rebuildable, not a fragile lookup row.
- Journal verification proves membership/admission correspondence.
- Twelve implementation-informed regressions are executable from the package.

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

Run the package in a Python environment containing the exactly pinned check
dependencies in `requirements-checks.txt`. For example:

```bash
uv run --with-requirements requirements-checks.txt ./tools/check-all.sh
```

The full check also runs a disposable PostgreSQL 16 + pgvector container pinned
by registry digest. It installs pgvector outside `public`, removes it from
`search_path`, executes the reference SQL, and verifies the qualified vector
projection. The container is deleted on exit.

`make check` is read-only and verifies the published checksum manifest without
regenerating its oracle. Maintainers use `make rebuild`, then `make check`.
`make reproduce` stages a clean temporary copy, regenerates catalog, genesis,
mindpack, vectors, inventory, and checksums there, and proves the resulting
package is byte-for-byte identical.
