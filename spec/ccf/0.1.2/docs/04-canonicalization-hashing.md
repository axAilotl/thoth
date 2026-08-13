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
