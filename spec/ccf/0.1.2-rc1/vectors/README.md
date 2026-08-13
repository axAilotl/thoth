# CCF 0.1.2-rc1 test vectors

These vectors exercise canonical JSON, compartment commitments, portable object
hashes, submission hashes, signed producer batches, Merkle roots, commit
signatures, semantic-catalog hashing, numeric admission ordering, every
admission-authority class, and the twelve 0.1.2 implementation-informed
conformance cases.

The private keys in this directory are deterministic **test material only**. They MUST NOT be used for a real archive or device.

Run:

```bash
node tools/verify-vectors.mjs
```
