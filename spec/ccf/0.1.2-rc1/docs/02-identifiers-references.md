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

The canonical origin key is `(archive_id, source_id, native_id, revision,
object_kind)`, where `object_kind` is `record`, `link`, or `blob`. A native item
may therefore produce one Record and one Blob without collision. Multiple
same-kind objects for one native item and revision MUST use stable distinct
native IDs, normally with component suffixes.

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
