# Continuity Core Format 0.1.2-rc1

**Status:** implementation candidate
**Date:** 2026-08-11
**Reference package:** `ccf-0.1.2-rc1`

CCF is a standalone, local-first canonical backend and interchange protocol for person-governed experience, knowledge, continuity, provenance, governance, and derived work. It is independent of any particular assistant runtime, agent framework, wearable, cloud service, or database projection.


---

# 0. Status, scope, and conformance

## 0.1 Version and status

This package defines **Continuity Core Format 0.1.2-rc1**.

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

Status note (rc1 final-pass review): independent reproduction has been achieved by the thoth implementation — its suite reproduces every published vector (`scripts/verify_ccf_012rc1_vectors.py`, `tests/test_ccf_cutover_vectors.py`). The freeze decision for 0.1.2 final now rests with the spec author; the version remains 0.1.2-rc1 until then.

## 0.2 Mandatory and optional profiles

`ccf-core-0.1.2-rc1` is mandatory. Other profiles compose without changing the three-object model:

| Profile | Status | Purpose |
|---|---|---|
| `ccf-core-0.1.2-rc1` | required | Objects, compartments, hashing, admission, journal, lineages, baseline governance, mindpack |
| `ccf-local-sync-0.1.2-rc1` | recommended | Offline signed producer batches and resumable delta synchronization |
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


---

# 1. Object model and invariants

## 1.1 Three portable object kinds

### Record

A typed thing: person, source, event, assertion, policy, decision, process run, receipt, or integrity operation.

### Link

An independently addressable typed relationship between two CCF objects. Attachments are Links. A bad relationship is corrected through an append-only disposition Record.

### Blob

A commitment and governed manifest for bytes. The bytes may be present, external, withheld, disclosed, published, or erased.

No fourth canonical object kind is introduced for attachments, batches, entity clusters, policy caches, or projections.

## 1.2 Four layers of a stored object

Each portable object participates in four layers:

1. **Portable header** — object kind, globally stable ID, cryptographic profile, compartment commitments, object hash.
2. **Structural compartment** — minimum type, lineage, endpoint, or integrity material needed by the declared retention profile.
3. **Semantic compartment** — person/perspective, source claims, archive resolution, privacy, policy, authority, selectors, and payload.
4. **Archive-local admission** — commit sequence, position, admission time, local storage, caches, and indexes.

Only the first three are portable object material. Admission metadata is authenticated by commit membership but excluded from the portable object hash.

## 1.3 Structural retention is type-declared

The type and Link registries declare one of four profiles:

- `erasable` — both compartments may be removed; the header and commitments may remain.
- `payload_erasable` — structural compartment remains; semantic compartment may be removed.
- `structural_retention_required` — type-specific structural data needed for lineage or replay remains.
- `epoch_lifetime_required` — required structural material remains for the archive epoch.

A retained compartment remains policy-governed and may be encrypted at rest. Retained does not mean public.

## 1.4 Canonical versus projection state

Canonical information consists of admitted Records, Links, Blobs, their available compartments, and journal history.

Disposable projections include:

- current-state views;
- entity clusters;
- derivation closure;
- effective-policy caches;
- full-text and vector indexes;
- graph caches;
- summaries and wiki pages unless preserved as generated artifacts;
- prompt/context selections;
- archive-head caches;
- invalidation queues and checkpoints.

> If a human decided it, it is a Record. If a machine can recompute it without losing human judgment, it may be a projection.

## 1.5 Foundational invariants

```text
Evidence is not belief.
Content is not instruction.
Inference is not acceptance.
Preference is not authority to act.
Consent is not delegated agency.
A Runtime is not the Person.
A model of a Person is not the Person.
A generated summary never replaces its source.
Every derived object links to exact evidence.
Every object has a governance binding while its governed content is usable.
Admission order is the state-precedence clock.
Valid time determines when admitted state applies.
Corrections append; they do not rewrite admitted history.
Erasure may remove protected content while preserving permitted commitments and lineage.
Unknown semantics survive round trips but activate no behavior.
Canonical data is Records, Links, and Blobs.
Everything optimized for one application is a projection.
```

## 1.6 N-ary decisions

Links remain binary. An n-ary operation is a Record with authoritative membership Links:

```text
ErasureDecision
  ├── ccf.covers → Record A
  ├── ccf.covers → Blob B
  ├── ccf.invalidates → Link C
  └── ccf.destroys_key → key receipt D
```

Payload lists are presentation aids only. Active membership Links define canonical membership.

## 1.7 Unknown and sealed semantics

An unknown type or predicate MUST be preserved but MUST NOT grant authority or behavior.

A sensitive object MAY use `sealed.record` or `sealed.link` in its structural compartment. The exact type and, for a sealed Link, endpoints live in the semantic compartment. If that compartment is erased, the exact semantics intentionally become unavailable. Sealed Links cannot participate in structural derivation or governance propagation after erasure.


---

# 2. Identifiers and references

## 2.1 Producer-generated canonical IDs

Every object receives its canonical ID before submission. The producer generates it and uses it for all same-batch references.

```text
urn:ccf:record:<uuidv4>
urn:ccf:link:<uuidv4>
urn:ccf:blob:<uuidv4>
urn:ccf:archive:<uuidv4>
urn:ccf:lineage:<uuidv4>
urn:ccf:key:<uuidv4>
urn:ccf:credential:<uuidv4>
urn:ccf:batch:<uuidv4>
urn:ccf:pack:<uuidv4>
```

UUID text is lowercase canonical hyphenated UUIDv4. UUIDv7 remains valid inside an application as a source-native ID, but it is not the portable CCF identifier profile because creation time is already explicit and timestamp-bearing IDs leak information after erasure.

## 2.2 Admission behavior

The Archive MUST either admit the supplied ID unchanged or reject it. It MUST NOT silently substitute a new canonical ID.

- Existing ID + identical object hash: idempotent existing result.
- Existing ID + different object hash: hard collision/conflict.
- Same origin tuple + same submission hash: idempotent existing result.
- Same origin tuple + different submission hash: `origin_revision_conflict`.

The canonical origin key is:

```text
(archive_id, source_id, native_id, revision, object_kind)
```

`object_kind` is `record`, `link`, or `blob`. One native item may therefore
produce a Record and a Blob under the same source identity and revision. A
source that emits more than one object of the same kind for one native item and
revision MUST assign stable distinct native IDs, normally by appending a native
component suffix such as `segment-1842/utterance-1` or
`segment-1842/thumbnail`.

## 2.3 Same-batch references

Because IDs exist before submission, Records, Links, and Blobs in one producer batch may refer to one another directly. A batch is rejected if a required referenced ID is neither already admitted nor present in the atomic batch.

## 2.4 Import and local aliases

Portable IDs are never remapped on restore or foreign merge. A destination may create local database keys, aliases, or identity-cluster mappings as projections.

If two IDs are adjudicated as the same real-world entity, an entity-resolution Record and active `same_as` Links express that decision. Original IDs remain.

## 2.5 Reference completeness

A reference is complete when its target is:

- included in the archive or pack;
- declared as an external dependency;
- declared withheld;
- declared erased with a resolvable receipt; or
- preserved as a foreign custody proof.

An undeclared dangling reference makes a pack incomplete.

## 2.6 ID privacy

A random ID reveals no creation time. The remaining existence of an ID and commitment after erasure is still metadata. Exports and retained structures remain governed and SHOULD be encrypted as a container when disclosure of existence is sensitive.


---

# 3. Compartments, retention, encryption, and erasure

## 3.1 Portable header

The header is intentionally minimal:

```json
{
  "spec": "ccf/0.1.2-rc1",
  "object_kind": "record",
  "id": "urn:ccf:record:550e8400-e29b-41d4-a716-446655440000",
  "hash_profile": "ccf-jcs-sha256-v2",
  "structural_commitment": "sha256:...",
  "semantic_commitment": "sha256:...",
  "object_hash": "sha256:..."
}
```

The exact type is not permanently exposed by the header.

## 3.2 Structural and semantic compartments

Each compartment is independently committed:

```json
{
  "format": "ccf.record-structural/0.1.2-rc1",
  "salt": "base64url-32-byte-secret",
  "content": {}
}
```

The salt is stored with the compartment while it is available. An erased compartment removes both content and salt, preventing cheap dictionary tests against low-entropy semantic content.

Structural content declares type, schema digest, retention profile, optional lineage, and type-specific replay material. Semantic content carries person/perspective, provenance, producer claims, archive resolution, privacy, policy, authority, selectors, and payload.

## 3.3 Core does not require content encryption

`ccf-core-0.1.2-rc1` permits plaintext compartments protected by the deployment's filesystem, database, or volume encryption. Core still defines commitments, availability state, erasure receipts, and honest security claims.

A Core-only implementation may claim logical or storage-verified deletion only. It MUST NOT claim selective cryptographic erasure unless it implements the corresponding profile.

## 3.4 Optional encryption profiles

### Archive-derived encryption

`ccf-archive-encryption-derived-v1` uses:

```text
random archive epoch secret
  ├── wrapped to user recovery public key
  ├── wrapped to each authorized device public key
  ├── optionally wrapped to a service or enterprise KMS recipient
  └── HKDF(epoch secret, object ID, compartment, key version)
         → symmetric compartment key
```

This avoids a stored random key per object and permits fully offline encryption and decryption by authorized devices. Destroying every recoverable epoch secret provides epoch-wide crypto-erasure. It does not provide selective per-object cryptographic erasure.

### Per-object erasure

`ccf-object-erasure-v1` uses a random DEK per erasable compartment or Blob content, wrapped under a custodian key. Destroying every wrapped copy of one DEK can make that object selectively unrecoverable. This profile requires stronger key inventory, backup, KMS, and recovery controls and is not mandatory for Thoth or local CCF use.

## 3.5 Public-key identity versus content encryption

Public-key credentials authenticate devices and wrap or encapsulate archive secrets. Bulk content remains symmetrically encrypted. Signing keys, key-wrapping keys, and data-encryption keys MUST be purpose-separated.

A deployment MAY support customer-controlled wrapping keys, external KMS keys, or user-only recovery keys. It MUST accurately state whether any service runtime possesses a usable decryption path.

## 3.6 Availability states

Each structural compartment, semantic compartment, and Blob content has an operational state:

```text
plaintext
 encrypted
 withheld
 erased
```

The object header remains portable. A pack reports unavailable compartments without fabricating empty content.

Compartment unavailability is canonical state, not absence of canonical state.
Every transfer and foreign merge MUST preserve the object header, structural
and semantic commitments, retention profile, exact availability state, source
custody proof, and erasure or withholding lineage even when plaintext bytes are
unavailable. Portable availability states are `available`, `withheld`,
`erased`, and `external`; importers MUST NOT collapse one into another.

## 3.7 Erasure assurance levels

- **Logical** — content is removed from active stores and projections; no cryptographic claim.
- **Storage-verified** — controlled replicas, indexes, caches, checkpoints, and backups are verified purged under deployment policy.
- **Cryptographic** — required keys and recoverable key copies are destroyed under the selected encryption profile.

External disclosure and publication leave the archive's control boundary. CCF records receipts and obligations but cannot guarantee deletion of an independent plaintext copy.

## 3.8 Erasure saga

Erasure is a durable state machine:

1. authenticate request;
2. decide scope, holds, competing obligations, and assurance profile;
3. block ordinary reads immediately;
4. destroy or purge content and keys;
5. verify controlled copies, indexes, WAL/PITR, checkpoints, replicas, caches, and exports;
6. append a receipt and membership Links.

A crash after irreversible key destruction but before the receipt must resume from durable operational state and inspect the custody system. It MUST NOT report content as recoverable when the key is gone.

## 3.9 Multi-subject media

CCF does not pretend arbitrary mixed audio can be surgically deidentified. A decision may erase the whole Blob, restrict it pending review, or create a reviewed replacement Blob containing permitted spans. Short capture segments and utterance-level derived Records reduce blast radius.

Subject propagation is conservative. Derived content inherits source subjects unless an explicit reviewed redaction or deidentification transformation narrows the set.

## 3.10 Structural retention after erasure

- Commit, signer, catalog, and succession structures remain for the archive epoch.
- `derived_from` endpoints remain where the Link registry requires lineage retention.
- Selectors and explanatory text may be erased independently.
- An erased semantic compartment leaves its commitment but is no longer inspectable.
- An object whose required provenance becomes unavailable is withheld when policy requires an inspectable source.


---

# 4. Canonicalization and hashing: `ccf-jcs-sha256-v2`

## 4.1 JSON profile

Before hashing:

- UTF-8 only; no BOM;
- no duplicate keys;
- schema validation succeeds;
- RFC 8785 JCS serialization;
- no implicit Unicode normalization;
- unpaired surrogates, NaN, Infinity, and negative zero are rejected;
- optional fields are omitted unless their schema explicitly permits `null`.

## 4.2 Canonical strings

- Timestamp: `YYYY-MM-DDTHH:mm:ss.SSSZ`, UTC `Z`, exactly three fractional digits.
- UUID: lowercase canonical UUIDv4.
- Digest: lowercase `sha256:<64 hex>`.
- Base64url: URL-safe, no padding.
- Integers that may exceed `2^53-1`: canonical unsigned decimal strings.

Commit sequences are decimal strings on the wire but are compared numerically. Lexicographic ordering is forbidden.

## 4.3 Compartment commitments

For a compartment envelope with a 32-byte base64url salt and `content`:

```text
record structural = SHA256("ccf:record-structural:v2\0" || salt || JCS(content))
record semantic   = SHA256("ccf:record-semantic:v2\0"   || salt || JCS(content))
link structural   = SHA256("ccf:link-structural:v2\0"   || salt || JCS(content))
link semantic     = SHA256("ccf:link-semantic:v2\0"     || salt || JCS(content))
blob structural   = SHA256("ccf:blob-structural:v2\0"   || salt || JCS(content))
blob semantic     = SHA256("ccf:blob-semantic:v2\0"     || salt || JCS(content))
```

An absent semantic compartment is represented by `semantic_commitment: null`, not by a hash of an empty object.

## 4.4 Blob content commitment

Blob bytes use a separate 32-byte content salt stored in the governed Blob semantic compartment:

```text
SHA256("ccf:blob-content:v2\0" || content_salt || bytes)
```

When Blob content is erased, the content salt is erased with it unless a profile declares an equivalent keyed commitment. The retained digest is then a historical commitment, not an oracle for guessed low-entropy content.

## 4.5 Portable object hash

The exact hash input is the complete portable header excluding only `object_hash`:

```text
spec
object_kind
id
hash_profile
structural_commitment
semantic_commitment
```

Domain separators:

```text
ccf:record:v2
ccf:link:v2
ccf:blob:v2
```

Type, lineage, endpoint, and semantic details are committed indirectly through the compartment commitments. Admission coordinates, storage location, availability state, projection state, and local aliases are excluded.

## 4.6 Submission hash

The producer submission hash commits to the complete canonical producer-controlled submission, including its stable ID, source timestamps, origin tuple, claims, payload, and extensions. Archive resolution, compartment salts, policy resolution, admission coordinates, object hashes, and signatures are excluded.

```text
SHA256("ccf:submission:v2\0" || JCS(submission))
```

The archive preserves the submission hash and producer evidence in the admitted semantic compartment.

## 4.7 Producer batch hash and signature

Remove `batch_hash` and `signature`, canonicalize the rest, and compute:

```text
batch_hash = SHA256("ccf:producer-batch:v1\0" || JCS(unsigned_batch))
signature  = Ed25519.sign(raw_32_byte_batch_hash)
```

The credential identifies the verification key. A modified object, order, previous batch hash, catalog root, or sequence invalidates the signature.

## 4.8 Commit leaves and Merkle root

Each admitted object produces a member containing numeric sequence, numeric position, admitted time, kind, ID, and object hash. Leaves are sorted by numeric `commit_position` and hashed with `ccf:commit-leaf:v2`.

The tree uses the published deterministic split algorithm and `ccf:merkle-node:v2`. Empty member batches use `ccf:merkle-empty:v2`.

## 4.9 Commit signature and hash

The `integrity.commit` Record is special:

1. Build the portable header fields that do not depend on the structural signature.
2. Build structural content without `signature`.
3. Compute the signing digest over `{header_without_commitments_and_hash, structural_content_without_signature}` using `ccf:commit-sig:v2`.
4. Add the signature.
5. Compute the structural commitment.
6. Build the completed header and object hash.
7. Define `commit_hash` as the completed commit Record's `object_hash`.

The next commit's `parent_commit_hash` is that value. The commit is excluded from its own member root. Its sequence is inside signed structural content.

## 4.10 Semantic catalog root

`semantic-catalog.json` contains sorted entries binding each activated schema, registry, evaluator, and profile to an exact digest. Its root is:

```text
SHA256("ccf:semantic-catalog:v1\0" || JCS(catalog_without_root))
```

Genesis pins the root. A catalog change requires an authorized `integrity.catalog_transition` at a defined sequence. An implementation MUST NOT silently substitute local executable semantics while claiming replay equivalence.


---

# 5. Portable envelopes and field ownership

## 5.1 Record example

Header:

```json
{
  "spec": "ccf/0.1.2-rc1",
  "object_kind": "record",
  "id": "urn:ccf:record:550e8400-e29b-41d4-a716-446655440000",
  "hash_profile": "ccf-jcs-sha256-v2",
  "structural_commitment": "sha256:...",
  "semantic_commitment": "sha256:...",
  "object_hash": "sha256:..."
}
```

Structural compartment:

```json
{
  "format": "ccf.record-structural/0.1.2-rc1",
  "salt": "...",
  "content": {
    "type": "experience.utterance",
    "type_version": 1,
    "type_visibility": "clear",
    "schema_digest": "sha256:...",
    "registry_entry_digest": "sha256:...",
    "retention_profile": "payload_erasable",
    "structural_payload": {},
    "extensions": {}
  }
}
```

Semantic compartment:

```json
{
  "format": "ccf.record-semantic/0.1.2-rc1",
  "salt": "...",
  "content": {
    "person_id": "urn:ccf:record:...",
    "perspective_id": "urn:ccf:record:...",
    "recorded_by": "urn:ccf:record:...",
    "recorded_at": "2026-08-11T21:42:18.331Z",
    "occurred_at": {"start": "2026-08-11T21:41:48.000Z"},
    "origin": {
      "source_id": "urn:ccf:record:...",
      "native_id": "boot-8891/segment-1842/utterance-3",
      "revision": "1",
      "submission_hash": "sha256:..."
    },
    "claimed": {},
    "privacy": {},
    "policy_ref": {},
    "authority": {},
    "epistemic": {},
    "producer_evidence": {},
    "payload": {},
    "extensions": {}
  }
}
```

## 5.2 Producer versus archive ownership

### Producer-controlled

- object ID;
- type and version claim;
- source origin and source-recorded times;
- occurrence time;
- raw payload;
- claimed person, perspective, subjects, authority, and policy hint;
- source epistemic and capture-fidelity metadata;
- source extensions.

### Archive-resolved or stamped

- authenticated producer and credential;
- selected registry entry and schema digests;
- final retention profile and type visibility;
- resolved person/perspective and privacy classification;
- exact policy reference;
- archive authority interpretation;
- producer evidence block;
- compartment salts, commitments, and object hash;
- admission coordinates.

The original claims remain inspectable. Archive resolution never makes it impossible to determine what the producer actually asserted.

## 5.3 Links

A Link structural compartment contains endpoints when the Link registry says `endpoints_location: structural`. Selectors and explanatory material remain semantic and may be erased independently.

A `sealed.link` stores exact type and endpoints in the semantic compartment and cannot be used as a retained structural lineage edge.

## 5.4 Blobs

A Blob header commits to its structural and semantic compartments. Structural content includes media type, size, content commitment, availability class, and retention profile. The bytes are transferred separately. Blob identity is a random UUID, not a plaintext digest or ciphertext digest.

## 5.5 Validation

Object validation requires:

1. header schema and hash;
2. every available compartment commitment;
3. semantic-catalog entry and schema digest;
4. registry rules for retention, visibility, endpoints, lineages, and authority;
5. payload schema for the resolved exact type;
6. policy and profile checks required for the requested operation.

A Preserver may store an unknown object without validating its unknown payload semantics, but must verify portable hashes and preserve bytes or ciphertext.


---

# 6. Local-first producer batches, admission, and synchronization

## 6.1 Normal operation is offline-capable

CCF does not require a network call for local capture, local read, local search, or local derivation. A device or runtime may operate from its local replica, cached credentials, and local keys.

A network freshness check is a deployment concern for consequential external actions, not a prerequisite for opening a locally available file or recording new evidence.

## 6.2 Signed producer-batch chain

A Producer maintains its own append-only chain:

```text
batch sequence
previous batch hash
stable object submissions
Blob transfer manifest
semantic catalog root
credential ID
batch hash
signature
```

A disconnected producer may create many batches. On reconnect, the Archive verifies the credential, signature, sequence, previous hash, object schemas, and source revisions.

Producer batches establish authenticated source order and provenance. They do not independently advance the canonical archive head.

Producer-chain continuity is determined by cryptographically valid producer
batches, not only batches whose objects are all admitted. A batch that passes
credential, signature, batch-hash, resource-bound, and producer-sequence checks
receives a durable terminal disposition: `accepted`, `partially_accepted`,
`content_rejected`, or `quarantined`. Its batch hash remains a valid predecessor
even when every content item is rejected. Rejected objects do not thereby become
canonical. An invalid signature, chain hash, credential, resource bound, or
outer envelope does not become a predecessor.

A producer batch MAY be admitted whenever its exact producer-chain predecessor
has been durably verified, regardless of arrival order relative to other
producers or unrelated archive commits. When that predecessor is absent, the
batch remains pending with `predecessor_missing`; it is retryable after the exact
predecessor arrives and MUST NOT receive a permanent content rejection solely
for early arrival. Archive admission order and producer sequence are distinct.

## 6.3 Provisional local state

A producer MAY expose locally created objects as **provisional** before canonical admission. Provisional state:

- is durable in the producer spool;
- has stable IDs and submission hashes;
- may feed local projections under a visibly provisional status;
- has no archive admission order;
- MUST NOT be represented as canonically committed.

The Archive returns `queued`, `accepted`, `partially_accepted`,
`content_rejected`, `quarantined`, or `conflict`.

## 6.4 Atomic admission

The Archive may combine submissions from one or more producer batches into one commit. Within one commit:

1. validate credentials and catalog compatibility;
2. enforce ID and origin uniqueness;
3. resolve claims and policy;
4. validate all same-batch references;
5. validate lineage compare-and-swap and graph cycle rules;
6. construct canonical compartments and object hashes;
7. allocate numeric commit positions;
8. write objects, admission rows, generation fences, and commit atomically;
9. sign and advance the head before acknowledging success.

There is no durable-but-secretly-canonical intermediate state.

## 6.5 Idempotency

Primary key for source-backed submissions:

```text
(archive_id, source_id, native_id, revision, object_kind)
```

Retry comparison uses the producer submission hash, not archive-resolved fields.

- same tuple + same hash: return existing ID and current lifecycle;
- same tuple + different hash: conflict;
- same content from a different source: distinct provenance Records;
- same ID + different content: collision/conflict.

A retry after erasure returns an authorized lifecycle result without restoring bytes. Unauthorized callers receive an indistinguishable generic response.

## 6.6 Stateful conflicts

Every stateful lineage transition carries `previous_head_id`. On conflict, the caller:

1. reads the current head;
2. reevaluates its intended transition;
3. explicitly rebases;
4. submits a new transition.

The Archive never silently rebases a human or machine decision.

## 6.7 Sync packs

Sync is batch- and pack-oriented, not one request per object:

```text
exchange archive and producer heads
negotiate missing ranges and Blob chunks
transfer compressed delta pack
resume interrupted byte ranges
verify signatures and digests
return compact admission receipt
```

The same delta pack may travel over HTTP, LAN, USB, or a file copy. Long latency changes transfer time, not semantics.

## 6.8 Offline revocation limit

A disconnected device cannot learn a newly issued revocation. It may continue signing evidence and using locally possessed keys until reconnecting. The Archive may quarantine later batches based on revocation state. CCF MUST NOT claim impossible retroactive remote revocation.


---

# 7. Signed archive journal and semantic catalog

## 7.1 Not a blockchain network

CCF uses a single-authority signed Merkle journal. It requires no mining, token, proof of work, distributed consensus, smart contract, or public replication.

The useful analogy is a signed Git history for a continuity archive.

## 7.2 Genesis

Genesis is an `integrity.commit` Record with sequence `0` and no parent. It pins:

- archive and epoch IDs;
- `ccf-jcs-sha256-v2`;
- `ed25519-jcs-v1`;
- initial signer key ID and public key;
- semantic-catalog root;
- active profiles;
- creation time.

A verifier must obtain the trusted genesis hash through an owner bootstrap, prior device, trusted export, or explicit trust-on-first-use decision.

## 7.3 Commit contents

Each commit structurally retains:

- numeric sequence;
- parent commit hash;
- member Merkle root and count;
- semantic-catalog root;
- active profiles;
- signer key ID and signature profile;
- committed time;
- signature.

The commit Record is excluded from its own member root. Commit Records form the chain through `parent_commit_hash`.

Verification MUST prove correspondence between signed commit membership and
archive-local admission state, not merely parent hashes and signatures. For each
verified range, every member resolves to exactly one admission with matching
sequence, position, object kind, object ID, object hash, and admission time; and
every non-commit admission appears in exactly one membership set. Duplicate
membership, missing admission rows, extra admissions, or mutated coordinates
fail verification. The commit Record's explicit self-exclusion is the sole
exception.

## 7.4 Assurance levels

### Prefix integrity

The presented chain verifies from a trusted genesis. This detects corruption, object mutation, omission inside the presented history, and unauthorized writes by actors without the active signing key.

It does not prove that the presented head is the newest possible head.

### Rollback detecting

A deployment compares the current head with an independent monotonic checkpoint: another device, HSM state, witness service, transparency log, or equivalent trust domain.

### Operator resistant

A stronger deployment uses independent witness signatures or external anchoring such that the active archive operator alone cannot silently replace history.

The base commit chain does not claim protection from a malicious holder of the active signing key.

## 7.5 Head witnesses

`ccf-witnessed-integrity-v1` defines `integrity.head_witness` Records or external receipts binding archive, epoch, sequence, and commit hash. Witnesses do not create consensus. They provide an independently held expected head.

## 7.6 Catalog transitions

Genesis pins exact executable semantics. A catalog transition:

- is a compare-and-swap lineage;
- names previous and new roots;
- provides the catalog artifact;
- activates at a specific commit sequence;
- is signed by an authorized catalog actor;
- never rewrites old objects or hashes.

An unsupported transition causes preservation and fail-closed behavior, not silent fallback.

## 7.7 Archive epochs

A cryptographic hash-profile change creates a new epoch linked to the final head of the previous epoch. Historical objects retain historical hashes. 0.1.2-rc1 does not permit an unannounced in-place hash-profile switch.

## 7.8 Batching and acknowledgement

The Archive chooses batch membership subject to deployment bounds. A client receives committed success only after object writes, journal signature, and head advancement complete atomically. Queue delay and maximum batch size are deployment metrics and MUST be observable.


---

# 8. Lineages, current state, valid time, and graph correction

## 8.1 Total order

Current-state precedence uses numeric:

```text
(commit_sequence, commit_position)
```

Source times and wall clocks are evidence, not precedence authorities.

## 8.2 Compare-and-swap is universal for stateful lineages

Every registered stateful type declares a state machine and requires:

```text
lineage_id
previous_head_id
transition
valid_from
expires_at
```

The submitted predecessor must equal the current admitted head. This applies to policy, consent, legal basis determinations, grants, restrictions, entity adjudication, Link disposition, erasure decisions, credentials, keysets, catalog transitions, and succession.

Last-writer-wins is permitted only for a type explicitly registered as a non-authoritative observation stream.

## 8.3 Admission time versus valid time

For a query at archive head `H` and effective time `T`:

1. consider only transitions known by `H`;
2. follow valid state-machine transitions in admission order;
3. apply the latest admitted applicable transition whose valid interval contains `T`;
4. fail closed on invalid overlaps unless the registered state machine defines precedence.

Backdated transitions do not rewrite what was known at an earlier head. Historical evaluation asks, “what state effective at T was known at H?”

## 8.4 Link dispositions

Links are immutable; their current use is governed by `lineage.link_disposition` Records:

```text
retract
restore
supersede
invalidate_selector
tombstone
```

The target Link ID, action, predecessor, replacement ID, and terminal flag are structurally retained. Human and machine dispositions use the same compare-and-swap rule.

A physical or cryptographic erasure tombstone is terminal. A logical retraction may be restored if the state machine permits.

## 8.5 Entity resolution

An entity decision is a Record plus authoritative membership Links admitted atomically. The Record declares the operation; active `same_as` or `distinct_from` Links declare membership.

If the Record and Link set disagree, admission fails. Entity clusters remain projections.

A merge or split advances the entity-generation fence and invalidates dependent consent and policy decisions.

## 8.6 Derivation graph

Active `derived_from` Links form a DAG. New or restored edges are cycle-checked inside the serialized admission transaction. Other relation types may be cyclic.

Recursive CTEs are the correctness baseline. A closure table is a rebuildable acceleration projection.

## 8.7 Human decisions survive projection loss

Accepted candidates, entity decisions, quarantine releases, manual corrections, policy exceptions, fold decisions, and deletion approvals are Records. Destroying every projection must not erase them.


---

# 9. Governance and contextual authorization

## 9.1 Baseline evaluator

Admission authority classes are pinned by
`registries/admission-authority-classes.registry.json`. That registry
normatively defines accepted actor/envelope kinds, authority bases, person
acceptance behavior, canonical state consulted, stable failure reasons, and the
`ccf-admission-authority-v1` evaluator involvement for every class referenced by
the type registry. Unknown classes and bases fail closed. Implementations MUST
not reinterpret a class outside the catalog-pinned mapping.

Every Archive implements `ccf-deny-overrides-v1`:

- root default is deny;
- applicable deny overrides allow;
- mandatory obligations accumulate;
- unknown required context yields deny or pending;
- destination overlays may tighten imported policy but may not silently widen it;
- legal bases may be explicit alternatives rather than all required simultaneously;
- an impossible mandatory obligation denies or remains pending.

Jurisdiction-specific modules are optional pinned inputs. CCF represents decisions and evidence; it does not certify legal sufficiency.

## 9.2 Policy closure

Applicable inputs are deterministically collected from:

- direct object policy;
- active `governed_by` Links;
- active derivation ancestors where policy propagates;
- current resolved data-subject identities;
- consent, restriction, objection, legal-basis, and hold lineages;
- archive governance;
- destination-local tightening overlays.

The exact predicates and evaluator version are pinned in the semantic catalog.

## 9.3 Decision context

There is no context-free “effective policy.” A decision includes:

```text
operation
purpose
requester
recipient
runtime
destination
jurisdiction
requested time
object set
archive head
```

The result includes allow/deny/pending, obligations, reason codes, closure hash, context hash, evaluator version, generation vector, and expiry.

## 9.4 Local reads versus consequential egress

CCF Core does not require a remote governance call for local owner access to a local replica. The local runtime evaluates against its synchronized head and cached policy inputs.

A deployment MUST require fresh authorization at the point of consequential external action such as:

- disclosure outside the archive control domain;
- publication;
- message sending;
- spending;
- destructive remote action;
- model training or adaptation using protected data.

## 9.5 Generation fences

A governance mutation atomically advances relevant generation fences in the same transaction as admission. Cached decisions record the generations used.

A cached decision is usable only when every required generation matches. Fine-grained dirty discovery may run asynchronously; the fence closes the unsafe window immediately.

Widening changes may remain conservatively denied while recomputing. Tightening or unknown-direction changes block stale allows.

## 9.6 Pending behavior

When dependencies are dirty, the API returns structured pending information with dirty sequence, dependency estimate, retry hint, and request ID. The implementation must prioritize requested objects and eventually return allow, deny, or a documented terminal error. An implementation that permanently returns pending or denies everything fails positive conformance tests.

## 9.7 Fenced external capabilities

For external egress, an authorization capability binds:

- operation and purpose;
- exact objects;
- requester, recipient, runtime, and destination;
- archive head and generation vector;
- availability state;
- short expiry and use count.

The key-unwrapping or egress boundary consumes it and rechecks generations. A second ordinary read alone is not full linearization.

## 9.8 Consequential receipts

Consequential disclosures and actions create canonical receipts. High-volume internal reads may use chained audit segments stored as governed Blobs with periodically committed roots, avoiding one global commit per prompt-context lookup.


---

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


---

# 11. Mindpacks, restore, foreign merge, and evolution

## 11.1 Boring transport

A mindpack is a ZIP-compatible container with manifests, NDJSON streams, compartments, Blob bytes or external references, registries, schemas, and integrity proofs.

```text
archive.mindpack/
  manifest.json
  objects/records.ndjson
  objects/links.ndjson
  objects/blobs.ndjson
  compartments/records/*.structural.json
  compartments/records/*.semantic.json
  compartments/links/*.structural.json
  compartments/links/*.semantic.json
  compartments/blobs/*.structural.json
  compartments/blobs/*.semantic.json
  blob-data/*
  integrity/commits.ndjson
  integrity/members.ndjson
  schemas/*
  registries/*
  semantic-catalog.json
```

The entire container MAY be encrypted independently of object-level encryption.

## 11.2 Restore/replica

Restore preserves archive ID, epoch, original admission coordinates, journal, and head. The trusted genesis and head are verified. It does not re-admit objects into a new archive history.

## 11.3 Foreign merge

Foreign merge preserves source portable objects and custody proofs, then re-admits them into the destination archive under destination admission order and policy.

- portable IDs and object hashes remain unchanged;
- source commit proofs remain foreign evidence;
- source commit Records are ordinary destination members if imported as objects;
- only the destination's newly created commit is excluded from its own member root;
- destination overlays may tighten, not silently widen, imported policy.
- unavailable compartments retain their exact `withheld`, `erased`, or
  `external` state, commitments, retention, custody proof, and lineage even when
  their plaintext is absent.

## 11.4 Delta packs

A delta pack contains a bounded commit range, missing objects, compartments, and Blob chunks. It may be transferred by HTTP, local network, USB, or file. Resume uses byte ranges and verified chunk digests.

## 11.5 Completeness

The manifest reports:

- included object and commit counts;
- stream digests;
- available, withheld, erased, disclosed, and external material;
- unknown extensions;
- active profiles;
- semantic-catalog root;
- genesis and head hashes;
- foreign custody proofs.

The manifest is unsigned and non-authoritative:

- the verifier MUST independently reconstruct counts and availability from
  verified streams and journal membership;
- manifest values MUST NOT determine iteration bounds;
- every mismatch fails BEFORE any destination mutation;
- `mode` is only an exporter claim and cannot authorize restore or foreign
  merge.

Manifest-tamper conformance vectors are required at final (see §13.8).

Unknown semantics must round-trip in canonical form. Byte-identical original JSON is not required unless preserved separately as a Blob.

### 11.5.1 Complete versus partial-custody export

```text
complete export:
    MUST fail if required material is withheld, erased without adequate
    lineage, external, or otherwise unavailable
partial-custody export:
    MAY be supported as an explicit, separately authorized mode
    MUST declare complete=false
    MUST declare restore_capable=false
    MUST preserve commitments, availability, and custody proofs
```

A fail-closed implementation that does not support partial-custody export remains conformant.

## 11.6 Evolution

Published 0.1.2-rc1 schemas and profile semantics do not change in place.

- new type versions coexist with old;
- catalog transitions activate exact new digests;
- unknown later types are preservable but inactive;
- a hash-profile change starts a new archive epoch;
- old object hashes and old epoch verification remain intact;
- migrations append new objects and lineage rather than rewriting historical bytes.

0.1.1 to 0.1.2 suppression migration:

```text
derive 0.1.2 suppression sets from canonical historical erasure lineage
when all required preimages remain available
otherwise:
    report suppression migration incomplete
    do not silently discard old suppression state
    do not claim full 0.1.2 suppression conformance
```

Development archives may be recreated instead of migrated, but recreation is not a lossless migration.

## 11.7 Forks

Two independently writable descendants from one head are explicit forks. Imports preserve both heads and do not invent a winner. Automatic merge of first-person or governance state is outside 0.1.2-rc1; a reviewed destination process may create new adjudication Records.


---

# 12. Security, credentials, custody, and trust claims

## 12.1 Threat statement

The native journal detects corruption and unauthorized alteration by actors who do not control the active archive signing key. It does not prevent a malicious active signer from creating a replacement valid history. Witnessed integrity is a separate profile.

## 12.2 Archive and device keys

Recommended key roles:

- offline archive root signing key;
- active archive admission signing key;
- one signing key per device/runtime;
- optional encryption/wrapping key per device;
- random archive epoch encryption secret where the encryption profile is used;
- optional random per-object DEKs only for the high-assurance erasure profile.

Key roles MUST remain cryptographically distinct.

## 12.3 Device credentials

A `core.device_credential` Record binds a device/runtime to:

- signing public key;
- optional encryption public key;
- issuer key;
- scopes;
- validity interval;
- optional offline grace interval.

CCF does not require X.509. A canonical archive-issued credential is sufficient. Deployments may map to X.509, WebAuthn, TPM, HSM, or enterprise PKI.

## 12.4 Consumer and hosted custody modes

A hosted service may offer distinct, honestly named modes:

- user-only/private vault — provider has no decryption recipient;
- assisted recovery — threshold or trusted-device recovery;
- managed service — service runtime can decrypt;
- customer-managed or external key — enterprise wrapping authority controls access;
- attested compute — key release limited to a measured workload.

Encrypted storage is not encrypted computation. If ordinary hosted transcription or model inference sees plaintext, the service has a plaintext processing path during that workload.

## 12.5 Offline keys

Authorized devices may cache credentials and archive epoch keys locally. This permits local reads and capture without a network. Short-lived action capabilities may expire separately from long-lived evidence-capture credentials.

Loss of every authorized device and every recovery path makes a provider-blind archive unrecoverable by design.

## 12.6 Cognitive security

External content is evidence, not authority. Ingress preserves source identity, trust class, transformation history, screening findings, and exact derivation. No stored text can grant itself permission to change policy, identity, preferences, disclosure, or action authority.

## 12.7 Suppression after erasure

Suppression lookup tables are projections. The commitments that authorize
rejection of reintroduced erased content MUST be covered by canonical erasure
lineage and the signed admission journal. `lineage.suppression_set` commits to a
governed Blob containing exact keyed commitments. The erasure receipt commits
to the suppression profile, suppression-set Record and Blob IDs, entry count,
Merkle root, key/profile identifier, and scope commitment. A destroyed lookup
projection is rebuilt from this canonical state; deletion of a lookup row is
detectable and cannot silently permit reintroduction. Suppression tokens remain
sensitive governed metadata and MUST NOT be exposed in public headers. Keyed
PRF/HMAC profiles require rotation and recovery policy; plain unsalted
fingerprints are insufficient for low-entropy content.

`ccf-hmac-sha256-suppression-v1` is pinned by
`registries/suppression-profiles.registry.json`. Its preimage is a JCS object
validated by the closed catalog schema
`urn:ccf:schema:0.1.2-rc1:security.suppression-preimage`, with
`format: "ccf.suppression-preimage/1"` and either an origin tuple
(`kind`, `source_id`, `native_id`, `revision`, `object_kind`) or a content
commitment (`kind`, `content_commitment`). Tokens are HMAC-SHA-256 over the
profile domain plus canonical preimage, encoded as lowercase hexadecimal with
the `hmac-sha256:` prefix. Tokens sort by complete string, duplicates reject,
and the registry pins leaf, node, empty-root, and tree-split construction. The
key is at least 32 bytes and is governed outside the public token Blob.

The `scope_commitment` construction is pinned to the reference implementation.
The schema types it as an opaque digest and the rc1 registry does not pin its
bytes; conforming implementations MUST construct it exactly as follows:

- domain separator: the UTF-8 bytes of `ccf:suppression-scope:v1`, followed by
  a single `0x00` byte;
- preimage: the JCS (RFC 8785) canonical serialization of the JSON array of
  erased object ID strings, sorted in ascending Unicode code-point order;
- duplicates: object IDs are not deduplicated and duplicates are not rejected;
  each erasure plan contributes exactly one array element;
- empty scope: an empty plan list hashes the canonical empty array `[]`,
  yielding a well-defined digest rather than an error;
- digest encoding: SHA-256 over the domain separator, the `0x00` byte, and the
  preimage, encoded as lowercase hexadecimal with the `sha256:` prefix.

The suppression-profiles registry entry gains a field pinning this construction
at final; registry bytes are frozen for rc1.

## 12.8 Side channels

Unauthorized existence queries, retry responses, and erasure status must be response-shaped and rate-limited. Authorized source owners may receive richer lifecycle information. Core does not require identical network timing but requires deployments to document observable differences.


---

# 13. Conformance suite

The package divides tests into executable vectors and end-to-end scenarios.

## 13.1 Canonicalization and hashing

Required vectors cover:

- timestamps, UUIDv4, digests, base64url, omission versus null;
- unsafe integers, decimal-string ordering across digit boundaries;
- negative zero, NaN, Infinity, duplicate keys, hostile Unicode, literal U+0000;
- structural and semantic commitments for all object kinds;
- absent semantic compartment;
- Blob content commitment and salt deletion behavior;
- exact object-hash field membership;
- producer submission and batch hash;
- mixed-kind Merkle leaves, odd counts, empty batches;
- commit signing and completed `commit_hash`.

## 13.2 Admission and concurrency

- exact retry and source revision conflict;
- same content from two sources remains provenance-distinct;
- same-batch cross references;
- two transactions racing for one archive head;
- concurrent same-origin admission;
- two transitions using one predecessor;
- stale-head rebase and resubmit;
- cycle creation through concurrent or restored derivation edges;
- crash before and after commit signing.

## 13.3 Structural retention and erasure

- erase ordinary Record semantic content and retain valid header/structure;
- erase Link selector while retaining required endpoints;
- destroy every projection and rebuild lineage;
- verify commit and catalog structures remain;
- logical, storage-verified, and cryptographic claims remain distinct;
- crash after key destruction before receipt;
- purge checkpoints, caches, indexes, WAL/PITR, replicas, and controlled exports where claimed;
- multi-subject media decision and blast-radius behavior;
- authorized suppression prevents silent republishing.

## 13.4 Governance

- exact positive allow vectors;
- deny override and obligations;
- alternative legal basis versus consent;
- valid-time gaps, overlaps, expiry, and backdating;
- entity merge propagates consent state;
- generation fence blocks stale allow immediately;
- permission widening remains conservatively denied until recomputed;
- pending eventually completes or reaches a documented terminal result;
- stale egress capability fails at key/egress boundary.

## 13.5 Integrity and import

- mutate every canonical header, compartment, member, parent, and current head field;
- valid-prefix rollback with and without a trusted witness;
- schema, registry, evaluator, and private-extension substitution;
- unsupported catalog or hash profile;
- restore preserves original chain;
- foreign merge preserves source proofs and creates local admission;
- missing references and external dependencies;
- forked heads remain explicit;
- new epoch links to prior final head.

## 13.6 Positive interoperability

A deny-all, pending-forever, or preserve-nothing implementation is nonconformant. Independent implementations must produce identical canonical bytes and decisions for published positive vectors.

The 0.1.2 conformance package additionally requires these implementation-informed regressions:

1. Record and Blob sharing an origin tuple succeed because `object_kind` differs.
2. Two same-kind objects conflict unless stable native IDs differ.
3. Erased and withheld compartments survive foreign merge unchanged.
4. Bootstrap semantic compartments survive reload and projection destruction.
5. A cryptographically valid content-rejected batch does not brick successors.
6. An early batch remains pending and succeeds after its predecessor arrives.
7. A deleted suppression lookup row is detected and canonically reconstructed.
8. Erased-content reintroduction remains blocked after all projections are destroyed.
9. Deleted or mutated admission rows fail chain verification.
10. A multi-schema PostgreSQL fixture detects pgvector without `public` or `search_path` assumptions.
11. A three-commit Git fixture covers evolution, rename, delete, binary content, and retry.
12. Every admission authority class has positive and negative vectors.

## 13.7 Nightly destructive test

Delete every projection and restore only from canonical objects and a validated checkpoint or genesis replay. No human decision, lineage transition, policy state, consent state, Link disposition, erasure receipt, or integrity proof may be lost.

## 13.8 0.1.2 implementation-informed regressions

The executable package covers cross-kind and same-kind origin behavior;
unavailable foreign compartments; bootstrap rebuild; content-rejection chain
liveness; out-of-order predecessor retry; suppression detection, reconstruction,
and reintroduction blocking; admission/member correspondence; multi-schema
pgvector discovery; three-commit Git evolution/rename/delete/binary/retry; and
positive plus negative vectors for every authority class.

Manifest-tamper conformance vectors are required at final: forged counts,
stream digests, availability declarations, and `mode` claims against the
unsigned-manifest rules of §11.5. They are stated here as a requirement and are
not part of the frozen rc1 vector set.


---


# Profile summaries


---

# Profile: ccf-archive-encryption-derived-v1

Defines one random archive epoch secret wrapped to authorized public-key or KMS recipients and locally derived symmetric compartment keys. It supports provider-blind storage when the provider has no recipient slot. It provides epoch-wide, not selective object, crypto-erasure.


---

# Profile: ccf-core-0.1.2-rc1

Mandatory portable objects, compartments, canonicalization, signed archive journal, lineages, baseline governance, mindpack, and projection rules. It requires no network KMS and makes no selective crypto-erasure or rollback-freshness claim.


---

# Profile: ccf-local-sync-0.1.2-rc1

Defines archive-issued device credentials, signed producer-batch chains, provisional local state, delta packs, resumable Blob transfer, conflict receipts, and offline operation. Canonical archive order remains single-headed.


---

# Profile: ccf-object-erasure-v1

Defines random per-compartment or per-Blob DEKs, wrapped-key inventory, destruction verification, and backup/KMS coverage for selective cryptographic erasure. It is intentionally optional because of its operational cost.


---

# Profile: ccf-succession-v1

Defines live signer rotation and preauthorized successor activation. Historical keys remain valid before their activation boundary. Successor verification is performed against the active authorization at the parent head.


---

# Profile: ccf-witnessed-integrity-v1

Defines independent head checkpoints, witness signatures, or transparency receipts. It detects rollback relative to a trusted witness but does not create distributed consensus.


---

# 15. Glossary

**Admission** — Archive act that places an object at a numeric commit sequence and position.

**Archive epoch** — Period governed by one immutable hash/signature profile and linked commit chain.

**Blob** — Portable commitment and manifest for bytes whose availability is governed separately.

**Commit hash** — Completed object hash of an `integrity.commit` Record.

**Compartment** — Salted committed structural or semantic object content stored separately from the portable header.

**Continuity Archive** — Person-governed canonical CCF dataset and integrity history.

**Foreign merge** — Re-admission of portable foreign objects into a destination archive while preserving source custody proofs.

**Generation fence** — Synchronously advanced counter invalidating stale authorization or projection caches.

**Lineage** — Append-only compare-and-swap sequence representing current state without rewriting older Records.

**Mindpack** — ZIP-compatible complete or partial CCF transfer container.

**Person** — Continuity-bearing subject distinct from any Runtime.

**Producer batch** — Offline-capable signed chain element containing stable object submissions.

**Projection** — Rebuildable index, cache, graph, embedding, summary, or context view.

**Restore** — Reconstitution of the same archive identity, epoch, journal, and head.

**Runtime** — System through which a Person captures, retrieves, reasons, communicates, or acts.

**Semantic catalog** — Content-addressed set of activated schemas, registries, evaluator semantics, and profiles.

**Structural compartment** — Minimum type, endpoint, lineage, and integrity material retained according to registry rules.

**Semantic compartment** — Governed person, provenance, privacy, authority, selector, and payload material.

**Witness** — Independent holder of an expected archive head or signature over that head.
