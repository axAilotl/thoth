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
unsigned-manifest rules of `docs/11-import-export-evolution.md` §11.5. They are
stated here as a requirement and are not part of the frozen rc1 vector set.
