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

Verification also proves a bijection between signed membership and archive-local
admission state. Sequence, position, kind, object ID, object hash, and admission
time must match exactly. Duplicate membership, missing or extra admissions, and
mutated coordinates fail. The commit Record self-exclusion is the sole special
case.

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
