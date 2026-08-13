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
- unavailable compartments retain exact state, commitments, retention, custody
  proof, and erasure/withholding lineage; erased MUST NOT collapse to withheld.

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

Unknown semantics must round-trip in canonical form. Byte-identical original JSON is not required unless preserved separately as a Blob.

## 11.6 Evolution

Published 0.1.2-rc1 schemas and profile semantics do not change in place.

- new type versions coexist with old;
- catalog transitions activate exact new digests;
- unknown later types are preservable but inactive;
- a hash-profile change starts a new archive epoch;
- old object hashes and old epoch verification remain intact;
- migrations append new objects and lineage rather than rewriting historical bytes.

## 11.7 Forks

Two independently writable descendants from one head are explicit forks. Imports preserve both heads and do not invent a winner. Automatic merge of first-person or governance state is outside 0.1.2-rc1; a reviewed destination process may create new adjudication Records.
