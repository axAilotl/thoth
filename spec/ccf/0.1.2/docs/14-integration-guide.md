# 14. Integration guide

This chapter describes an application-neutral path for adopting CCF in a
personal knowledge system, companion application, capture tool, or archive.

## 14.1 Boundary

Keep application behavior outside the canonical format:

```text
capture and application features
  -> signed local producer batches
  -> CCF Record / Link / Blob archive
  -> rebuildable search, graph, vector, wiki, and summary projections
```

A companion runtime or agent framework may use CCF through an adapter. CCF
does not define that runtime's interface, personality, prompts, or model.

## 14.2 Safe first migration

1. Generate canonical UUIDv4 IDs at first capture and preserve existing IDs as `origin.native_id`.
2. Introduce portable headers and split structural and semantic compartments.
3. Keep existing files and databases behind Blob and compartment storage adapters.
4. Convert provenance and artifact relations into typed Links.
5. Treat wiki pages, embeddings, clusters, and summaries as projections or generated artifacts with exact provenance.
6. Preserve producer claims separately from archive resolution.
7. Dual-write the existing store and CCF objects until verification supports cutover.

## 14.3 Local-first batch spool

Write a durable producer batch before remote synchronization. A local archive
may admit that batch into its canonical journal immediately. Remote systems
receive signed delta packs instead of one request per object.

## 14.4 Untrusted content boundary

Ingress processing should preserve these rules:

- raw material remains evidence;
- security findings are Records;
- quarantine and release decisions are reviewed Records;
- derived summaries link to exact sources;
- stored content cannot grant itself instruction or action authority;
- source invalidation marks descendants for rebuilding.

## 14.5 Start with the core profile

Begin with Core and local sync. Plaintext compartments on an encrypted local
volume or one archive-derived encryption profile are sufficient for many local
applications. Per-object wrapped DEKs, HSM recovery, and jurisdiction modules
can be added when their operational requirements are understood.

## 14.6 Migration corpus

Test with a copied notebook or archive containing links, attachments,
duplicates, renamed files, conflicting notes, private text, and malformed
content. Exercise:

- fresh and repeated import;
- offline interruption and retry;
- source revision changes;
- projection destruction and rebuild;
- entity merge and split;
- semantic erasure;
- foreign merge and replica restore;
- corrupt commits, missing Blobs, and unsupported types;
- regenerated search, graph, vector, wiki, and summary projections.

## 14.7 Cutover rule

Cut over when every human review, provenance decision, source artifact, and
accepted semantic record survives deletion of the legacy projections and a
rebuild from the CCF archive.
