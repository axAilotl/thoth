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

Cryptographically valid producer batches receive a durable terminal disposition
(`accepted`, `partially_accepted`, `content_rejected`, or `quarantined`) and
remain valid predecessor anchors even when contents are rejected. Rejected
objects do not become canonical. Invalid credentials, signatures, batch hashes,
resource bounds, or outer envelopes never anchor the chain.

An exact missing predecessor yields retryable `predecessor_missing`. Once that
predecessor is durably verified, the early batch may be retried and admitted.
Arrival order across producers and canonical archive admission order are not
producer sequence.

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
