# 3. Compartments, retention, encryption, and erasure

## 3.1 Portable header

The header is intentionally minimal:

```json
{
  "spec": "ccf/0.1.2",
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
  "format": "ccf.record-structural/0.1.2",
  "salt": "base64url-32-byte-secret",
  "content": {}
}
```

The salt is stored with the compartment while it is available. An erased compartment removes both content and salt, preventing cheap dictionary tests against low-entropy semantic content.

Structural content declares type, schema digest, retention profile, optional lineage, and type-specific replay material. Semantic content carries person/perspective, provenance, producer claims, archive resolution, privacy, policy, authority, selectors, and payload.

## 3.3 Core does not require content encryption

`ccf-core-0.1.2` permits plaintext compartments protected by the deployment's filesystem, database, or volume encryption. Core still defines commitments, availability state, erasure receipts, and honest security claims.

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

Compartment unavailability is canonical state. Transfers and foreign merges
preserve header, structural and semantic commitments, retention, exact
availability (`available`, `withheld`, `erased`, or `external`), source custody
proof, and erasure/withholding lineage even when plaintext is unavailable.

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
