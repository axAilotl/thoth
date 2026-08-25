# Continuity Core Format 0.2.0 Working Draft

**Status:** Working Draft
**Published compatibility base:** `ccf-0.1.2`
**Portable object format:** `ccf/0.1.2`

## 1. Scope and compatibility boundary

CCF standardizes four related systems: a semantic interchange format, a
canonical object store, a verifiable archive, and a governance engine. They are
all useful, but they are not one minimum implementation.

This draft replaces the monolithic meaning of `ccf-core-0.1.2` with cumulative
guarantee levels. It does not mutate the meaning of any published 0.1.2 schema,
registry, vector, profile, or object. The 0.2.0 semantic catalog contains the
exact 0.1.2 semantic-catalog root as a base dependency.

Record, Link, and Blob remain the only portable semantic object kinds. Capsule
is a transport container, not a fourth object kind. Archive-local admission
coordinates remain outside portable object hashes.

`MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, and `MAY` are normative.

## 2. Three independent declaration axes

An implementation declaration contains exactly one guarantee `level`, one or
more `roles`, and zero or more `capabilities`. Semantic packs are namespaced
capabilities whose definitions live in a separate registry.

```json
{
  "format": "ccf.implementation-declaration/0.2.0",
  "implementation": "Cissa phone local store",
  "version": "0.1.0",
  "level": "ccf-canonical-store-v1",
  "roles": ["producer", "preserver", "importer-exporter"],
  "capabilities": [
    "ccf-continuity-pack-v1",
    "ccf-archive-encryption-derived-v1"
  ],
  "portable_formats": ["ccf/0.1.2"],
  "semantic_catalog_roots": ["sha256:..."],
  "extensions": {}
}
```

The declaration MUST NOT imply roles, levels, security properties, semantic
activation, or freshness properties that the deployment has not implemented and
tested.

### 2.1 Roles

Roles describe behavior, not assurance strength:

- Preserver
- Producer
- Processor
- Importer/Exporter
- Archive
- Policy Evaluator

Their identifiers and required operations are pinned by
`registries/roles.registry.json`. An application MAY implement any meaningful
combination. For example, a phone can be a Producer and Preserver without being
an Archive or Policy Evaluator.

### 2.2 Capabilities and semantic packs

Security and operational capabilities are orthogonal to levels:

- signed producer sync;
- archive-derived encryption;
- selective per-object erasure;
- witnessed integrity;
- succession;
- external KMS integration.

Continuity, work, and agent vocabularies are semantic packs. They do not raise a
guarantee level or create a security claim. A capability declaration is valid
only at or above the capability's registered minimum level.

Content encryption remains optional. Plaintext compartments MAY be protected by
filesystem, database, or volume encryption. A deployment MUST declare the
specific encryption or erasure capability before making its associated claim.

## 3. Cumulative guarantee levels

The identifiers and machine-readable requirements are in
`registries/levels.registry.json`.

| Rank | Identifier | Meaning |
|---:|---|---|
| 1 | `ccf-exchange-v1` | Stable-ID semantic exchange with provenance, references, dependency reporting, and unknown preservation |
| 2 | `ccf-canonical-store-v1` | L1 plus compartments, JCS, commitments, portable hashes, local atomic storage, idempotency, and exact availability |
| 3 | `ccf-verified-archive-v1` | L2 plus signed genesis/commits, Merkle membership, catalog pinning, restore, and foreign merge |
| 4 | `ccf-governed-archive-v1` | L3 plus pinned policy, bitemporal lineages, generation fences, erasure/suppression, receipts, and destructive rebuild |

Levels are cumulative. A level-N implementation MUST meet the requirements of
every lower level and MUST accept lower-level packages when the referenced types
are supported. Missing higher-level evidence means that property is not claimed;
it does not make lower-level data invalid.

High assurance is not a fifth level. It is Governed Archive plus the security
capabilities the deployment can operate correctly. For example:

```text
ccf-governed-archive-v1
  + ccf-archive-encryption-derived-v1
  + ccf-witnessed-integrity-v1
  + ccf-succession-v1
```

L3 proves valid-prefix integrity, not that a presented head is newest. Rollback
detection requires `ccf-witnessed-integrity-v1` and a trusted checkpoint.

## 4. Registered semantic requirements

`registries/semantic-requirements.registry.json` overlays every Record type,
Link type, Blob type, and predicate inherited from the exact 0.1.2 registries.
Each entry declares:

- a minimum guarantee level;
- a separate minimum level for consequential state effects, when applicable;
- required capabilities;
- an optional semantic pack;
- `preserve_inert_or_refuse` behavior below those requirements.

An implementation MAY preserve an object whose requirements exceed its own
declaration. It MUST NOT activate that object's behavior, apply its state
transition, treat it as authorization, or claim to reproduce its semantics.

Semantic understanding and consequential state are deliberately separate. For
example, an Exchange implementation with the work pack can understand a
`work.project` assertion, while only a Governed Archive may claim to reproduce
its compare-and-swap current-state lineage. Capability-specific state such as
producer credentials, catalog transitions, and signer succession uses the
minimum level registered for that capability.

This distinction lets an Exchange notebook carry a governance lineage without
becoming a governance engine. It also prevents stored content from granting
itself authority. Content is not instruction, and inference is not acceptance.

## 5. Mandatory compatibility rules

The normative texts are also content-addressed in
`registries/compatibility-rules.registry.json`.

1. Higher levels accept supported lower-level packages. Missing signatures or
   history mean “not claimed,” not “invalid data.”
2. Supplied portable IDs survive uplift. Archive-added resolution is returned in
   an uplift receipt.
3. Importers do not silently strengthen unsigned material into authenticated
   producer evidence.
4. Exporters do not silently downgrade. Lossy exports enumerate omitted proofs,
   state, compartments, registries, schemas, and extensions.
5. Unsupported material is preserved opaquely, preserved as a byte-exact
   containing pack, or refused. It is never silently discarded.
6. Registered semantics declare their minimum level and capabilities and remain
   inert below them.

## 6. CCF Capsule

A Capsule is a scoped ZIP-compatible or directory transport for “sideload this
knowledge.” It contains a root Record, membership Links, submission or canonical
streams, exact catalog dependencies, unavailable dependencies, and explicit
custody declarations.

```text
project.capsule/
  manifest.json
  submissions/
    records.ndjson
    links.ndjson
    blobs.ndjson          optional
  objects/                optional L2+ canonical headers
  compartments/           optional L2+ canonical bodies
  blob-data/              optional
  registries/             optional
  schemas/                optional
  integrity/              optional L3+ or opaque proofs
  opaque/                 optional byte-preserved material
```

The manifest declares `complete` or `partial` custody and `lossless` or `lossy`
representation independently. External, withheld, and erased dependencies remain
distinct. Every listed stream has a SHA-256 digest and byte length. A signature
or archive proof is optional and creates no claim unless its declared profile is
supported and verification succeeds.

Every stream also declares its activation requirements and handling. Material
above the recipient's level or capabilities is marked `preserve_inert` or
`preserve_opaque`; an `activate` stream whose requirements are unmet is invalid.
The executable fixture byte-preserves both a known Governed Record and an
unknown future type without loading either as active semantics.

Capsule does not replace mindpack. Capsule is for scoped exchange and import;
mindpack remains the archive-oriented container for restore, replica, complete
foreign merge, journal history, and high-fidelity custody transfer.

## 7. Uplift and downgrade receipts

An uplift receipt maps each source submission ID and submission hash to the same
canonical ID, resulting object hash, disposition, archive resolution, and exact
producer-authentication state. A supplied ID MUST NOT change. `verified`
producer authentication requires a retained proof that actually verified.

Pending uplift entries have null object hashes and make no completed-admission
claim. `admitted` and `existing` entries require an object hash. A
`producer_authentication` value of `verified` requires proof that an applicable
capability verifier actually accepted; a nonempty string alone is not proof.
The base Exchange validator refuses such a claim. The signed-producer-sync
suite resolves the retained batch against an explicit credential trust anchor,
verifies the ordered, fully hash-pinned canonical credential lineage and
revocation state at batch time,
checks the key binding and signature, and recomputes the covered submission
hash before accepting it. A deployment obtains that trust anchor from an
authenticated archive state or an explicitly configured out-of-band trust
store; an arbitrary credential supplied alongside a batch is not trusted.

A downgrade receipt identifies source and target levels, declares the export
`lossless` or `lossy`, enumerates every omission when lossy, and lists higher-level
material retained opaquely. The receipt accompanies the export and does not alter
portable objects.

The receipt pins source and export inventories. Its omission set MUST equal the
exact source-minus-export inventory difference; opaque material listed as
preserved MUST have bytes matching its digest.

Inventories use `schemas/exchange/downgrade-inventory.schema.json`. Physical
artifacts are identified by relative path and raw digest. Assertions that move
between containers are identified logically as `submission:<portable-id>` with
their JCS submission hash, so an exporter cannot make a derived output appear
to have been a pre-existing source file merely by reusing its path.

## 8. Conformance suites

Conformance is cumulative but executable at the claimed boundary:

- `check-exchange`: declarations, registries, schema-valid submissions, stable
  references, Capsule streams, unknown preservation, dependencies, and receipts;
- `check-canonical`: L1 plus inherited 0.1.2 JCS, compartment, object-hash, Blob,
  submission-hash, Capsule replay, atomic write, and exact-availability vectors;
- `check-verified`: L2 plus trusted-genesis signer binding, Merkle, catalog,
  mindpack stream, parent, head, tamper rejection, restore-coordinate,
  downgrade-source authentication, foreign-merge, and destination commit checks;
- `check-governed`: L3 plus the 0.1.2 policy, lineage, suppression, conformance,
  projection, and PostgreSQL fixtures.

CCF schema validation MUST assert the inherited `ccf-uint64` format, including
the inclusive `0` through `18446744073709551615` bound; treating this custom
format as an unknown annotation is not CCF-conformant. Protocol rules may impose
a narrower range, such as the nonzero producer sequence required by signed sync.

Capability-specific suites remain additional to the claimed level. An
implementation is not required to run a suite for a capability it does not
declare.

This Working Draft includes an executable signed-producer-sync capability suite.
The capability registry uses a null suite for encryption, selective erasure,
witnessing, succession, and external KMS until dedicated vectors are published;
the draft package itself therefore makes no conformance claim for them.

## 9. Distribution bundles

`bundles/` contains machine-readable, digest-pinned distribution manifests for
each guarantee level and semantic pack. The level bundles form a dependency
chain from Exchange through Governed Archive. Continuity, work, and agent
payload schemas live only in their semantic-pack bundles and the complete
Governed bundle; validation rejects any accidental inclusion in the Exchange,
Canonical Store, or Verified Archive bundles.

These manifests define the artifact boundary for later release archives without
copying or renumbering inherited schemas. Each artifact identifies its source
package and raw SHA-256 digest.

Distribution bundles are not standalone conformance runners. The draft source
package is the conformance package and retains its tools, vectors, full
fixtures, Makefiles, and private test keys separately from the runtime artifact
sets. A level bundle therefore does not acquire Governed fixtures or test-only
secrets merely because its conformance suite inherits them as an oracle.

## 10. Migration from 0.1.2

`ccf-core-0.1.2` maps to `ccf-governed-archive-v1` without implying optional
sync, encryption, erasure, witnessing, or succession. The remaining 0.1.2
profiles map to either capabilities or semantic packs in
`registries/legacy-profile-mappings.registry.json`.

Migration changes declarations and packaging, not existing object bytes:

1. keep every 0.1.2 object, compartment, hash, admission, and journal entry;
2. pin the original catalog and portable format;
3. publish roles, one level, and exact capabilities;
4. use Capsule for scoped knowledge transfer;
5. continue using mindpack restore or foreign merge for archive transfer.

No object rewrite is required to adopt layered conformance.
