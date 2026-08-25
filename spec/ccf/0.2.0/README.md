# Continuity Core Format 0.2.0 Working Draft

CCF 0.2.0 is an additive working draft for layered conformance. It separates
what an implementation does (roles), how strongly it interoperates (guarantee
level), and which optional security or semantic features it implements
(capabilities).

This draft does not change the published 0.1.2 portable object algebra. It pins
the exact 0.1.2 semantic catalog and reuses `ccf/0.1.2` Record, Link, Blob,
compartment, canonicalization, commitment, and object-hash formats. A 0.1.2
archive remains readable without rewriting any portable object.

The draft adds:

- Exchange, Canonical Store, Verified Archive, and Governed Archive levels;
- independent implementation-role and capability registries;
- a separate semantic-pack registry;
- minimum-level and capability requirements for all 88 inherited registered
  types and predicates;
- separate level-bundle and semantic-pack manifests, keeping continuity, work,
  and agent payload schemas out of the beginner bundles;
- CCF Capsule, uplift receipt, downgrade receipt, and implementation declaration
  schemas;
- tiered conformance entry points.

Start with [CCF-0.2.0-DRAFT.md](CCF-0.2.0-DRAFT.md). The executable Capsule is
under [examples/capsule](examples/capsule/README.md).

## Status

This directory is a **Working Draft**, not a published interoperability release.
CCF 0.1.2 remains the current release and its files and identifiers are frozen.

## Checks

The default draft check proves L1 and L2 behavior:

```bash
make check
```

Run a specific cumulative level suite with:

```bash
make check-exchange
make check-canonical
make check-verified
make check-governed
make check-capability-signed-producer-sync
make check-semantic-pack-continuity
make check-semantic-pack-work
make check-semantic-pack-agent
```

`check-governed` runs the disposable PostgreSQL 16 + pgvector fixture inherited
from 0.1.2 and therefore requires Docker.

Only signed producer sync has a draft capability suite today. The remaining
security capabilities require deployment-specific evidence until dedicated
vectors are published; see [docs/conformance-suites.md](docs/conformance-suites.md).
Each semantic pack has an independent resource/schema/bundle coverage suite.

Regenerate the requirement overlay, draft catalogs, and bundle manifests with:

```bash
make rebuild
```
