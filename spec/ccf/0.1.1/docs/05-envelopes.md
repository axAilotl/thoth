# 5. Portable envelopes and field ownership

## 5.1 Record example

Header:

```json
{
  "spec": "ccf/0.1.1",
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
  "format": "ccf.record-structural/0.1.1",
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
  "format": "ccf.record-semantic/0.1.1",
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
