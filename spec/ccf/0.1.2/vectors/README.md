# CCF 0.1.2 test vectors

These vectors exercise canonical JSON, compartment commitments, portable object
hashes, submission hashes, signed producer batches, Merkle roots, commit
signatures, semantic-catalog hashing, numeric admission ordering, every
admission-authority class, the implementation-informed conformance cases, and
unsigned-manifest tamper rejection.

The private keys in this directory are deterministic **test material only**. They MUST NOT be used for a real archive or device.

Run:

```bash
node tools/verify-vectors.mjs
```
