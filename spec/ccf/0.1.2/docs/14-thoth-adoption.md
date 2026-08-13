# 14. Thoth adoption guide

This chapter is implementation guidance, not a requirement that other CCF systems use Thoth.

## 14.1 Boundary

Thoth should evolve into a reference CCF backend while retaining its own application layer:

```text
connectors and capture
  → signed local producer batches
  → CCF Record / Link / Blob archive
  → Thoth projections: wiki, graph, vectors, search, summaries
```

CCF does not require any companion runtime or agent framework. Other agent systems consume the same backend through adapters.

## 14.2 Safe first migration

1. Generate canonical UUIDv4 IDs at first capture; preserve existing IDs as `origin.native_id`.
2. Introduce portable headers and split structural/semantic compartments.
3. Keep current artifact files and databases as storage implementations behind Blob and compartment storage adapters.
4. Turn existing provenance and artifact relations into typed Links.
5. Treat wiki pages, embeddings, clusters, and summaries as projections or generated artifacts with exact provenance.
6. Preserve producer claims and archive resolution separately.
7. Dual-write existing Thoth tables and CCF objects before cutover.

## 14.3 Local-first batch spool

Thoth should write a durable producer batch before any remote sync. The local archive may immediately admit it into its own canonical journal. A remote service later receives signed delta packs rather than individual object calls.

## 14.4 Content firewall

Thoth's ingress screening becomes a processing profile over CCF:

- raw material remains evidence;
- security findings are Records;
- quarantine and release are reviewed Records;
- derived summaries link to exact sources;
- malicious content never becomes an instruction or authority;
- source invalidation dirties all descendants.

## 14.5 Do not begin with enterprise key management

For the first Thoth conversion, implement Core plus local sync. Use plaintext compartments on an encrypted local volume or one archive-derived encryption profile. Do not block the schema migration on per-object wrapped DEKs, HSM recovery, or jurisdiction modules.

## 14.6 Obsidian torture corpus

Use a copied notebook containing links, attachments, duplicates, renamed files, conflicting notes, private text, and malformed content. Exercise:

- fresh and repeated import;
- offline interruption and retry;
- source revision changes;
- projection destruction and rebuild;
- entity merge/split;
- semantic erasure;
- foreign merge and replica restore;
- corrupt commit, missing Blob, and unsupported type;
- wiki regeneration from canonical CCF state.

## 14.7 Cutover rule

Thoth is ready to cut over when every human review, provenance decision, source artifact, and semantic candidate survives deletion of the legacy projections and rebuild from the CCF archive.
