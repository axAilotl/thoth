# Security review — `ccf-0.1.2-rc1`

Review date: 2026-02-13
Scope: branch `ccf-0.1.2-rc1`, production code only (~21k LOC: `ccf/` package,
core wiring in `core/`, `scripts/`). Diff base: `main` at merge-base `0ed653b`.

Areas read end to end: crypto/integrity core (`hashing.py`, `jcs.py`,
`keys.py`, `ids.py`), untrusted-input surfaces (`sync/packio.py`,
`sync/restore.py`, `sync/transport.py`, `sync/chunks.py`), admission
(`admission.py`), journal (`journal.py`), governance (`engine.py`,
`capabilities.py`, `evaluator.py`, `authority.py`), erasure
(`suppression.py`, `purge.py`, `media.py`, `operations.py`), credentials
(`credentials.py`), obsidian importer (`vault.py`, `notes.py`), and the
core wiring diffs (`ccf_dualwrite.py`, `capture_lifecycle.py`,
`connector_capture.py`, `postgres_migrations.py`).

Overall assessment: the cryptographic core and governance/erasure engines
are unusually disciplined — fail-closed everywhere, default-deny policy
engine, signed member Merkle roots, strict RFC 8785 canonicalization with
duplicate-key and surrogate rejection, 0600 key handling, anchored delta
apply. The findings below are the gaps that survive that discipline,
concentrated in the seams: bootstrap anchoring, a file re-read race,
resource limits, and credential lifecycle enforcement.

## High

### H1. Restore has no identity anchor by default — a fully forged archive passes verification

`ccf/sync/restore.py:405-425`: `restore_mindpack(...,
trusted_genesis_hash=None, trusted_head_hash=None)` — both optional, and no
production caller passes them (`git grep restore_mindpack` → only
`scripts/ccf_stage9.py`, which *does* pass them). The pack's chain is
signed, but by a key carried *inside the pack*. An attacker who generates
their own Ed25519 key and signs a malicious, self-consistent archive
passes every check (stream digests, commitment recomputation, chain,
in-database `verify_chain`) unless the operator supplies an out-of-band
trusted hash. The whole CCF integrity model collapses to nothing at the
bootstrap point.

Fix: require `trusted_genesis_hash` (or an explicit
`bootstrap_new_archive=True` flag that prints the new genesis hash for
out-of-band pinning). Fail closed otherwise.

## Medium

### M1. TOCTOU on the pack file during restore — operational streams re-read unverified

`ccf/sync/restore.py:434-447`: the pack is fully verified through one
`PackReader`, closed, then `archive.json`, `lineage-heads.ndjson`,
`origin-index.ndjson`, `producer-heads.ndjson`, and `producer-batches/*`
are re-read through a *second* `PackReader` on the same path **without
digest re-verification**. A local attacker who can swap the pack file/dir
between the two opens injects unverified operational state (epoch, signer
key id, origins, producer heads). The epoch/genesis swap mostly DoSes
(final `verify_chain` catches it), but origin/producer-head rows land as
given.

Fix: keep one reader open for the whole restore, or re-check the manifest
digests on every re-read.

### M2. Zip bomb / unbounded decompression

`ccf/sync/packio.py:146-161` (`PackReader.read`) and
`verify_stream_digests` read each entry fully into memory **before**
checking the manifest's `byte_length` or digest, with no caps on entry
count, per-entry compressed size, or total uncompressed size. Packs arrive
over HTTP (`ccf/sync/transport.py`) — a malicious `.mindpack` OOMs the host
before any integrity check runs. Same pattern in `sync/chunks.py`
(`build_sidecar`, `verify_file`) and `make_pack_app` (full `read_bytes` per
request, no auth — conformance tool, keep it off the network).

Fix: check `ZipInfo.file_size` against the manifest `byte_length` and a
total-uncompressed cap before reading; stream-hash with the cap enforced.

### M3. Credential expiry and scopes are never enforced at admission

`ccf/credentials.py:resolve_credential_public_key` checks only that the
credential record exists, is unambiguous, and its lineage head isn't
`revoke`. The payload's `valid_from`, `expires_at`, `offline_grace_until`,
and `scopes` are declared (`ccf/dualwrite/service.py` issues
`scopes=["capture","sync","derive"]`) but never consulted by
`_verify_batch_envelope` (`ccf/admission.py:480-505`). An expired,
out-of-scope, or not-yet-valid device credential keeps signing admissible
batches until explicitly revoked.

Fix: enforce `valid_from ≤ now < expires_at` and check the operation
against `scopes` in `resolve_credential_public_key` (or return the full
payload and enforce at the envelope).

## Low

### L1. Suppression key written with default umask

`ccf/erasure/suppression.py:generate_suppression_key` uses
`Path.write_text` → typically 0644, world-readable HMAC key protecting
suppression-after-erasure. `ccf/keys.py` does this correctly (0600 +
`O_EXCL`); mirror it.

### L2. Exported packs are world-readable

`PackWriter.write_bytes` → `Path.write_bytes` (0644) into a 0755 dir
(`ccf/sync/packio.py`). Mindpacks contain plaintext compartments and blob
bytes. Recommend 0700/0600 (or an explicit `chmod` flag) on export.

### L3. Manifest availability lists are unauthenticated

`withheld`, `erased`, `external_dependencies` (and `mode`, `counts`) in
`manifest.json` are not bound by any digest in the signed chain. Traced:
an attacker can inflate these lists, but cannot forge content — headers
are still bound by member `object_hash` → Merkle root → signed payload, so
the effect is limited to availability semantics, withheld rows, and DoS.
Still: integrity-relevant metadata traveling unsigned. Bind the full
manifest into a signed commitment in the next spec revision.

### L4. Malformed member fields propagate raw KeyError/ValueError

`verify_commit_chain`/`merkle_root` raise raw `KeyError`/`ValueError` on
malformed member fields (missing `commit_position`, non-numeric
`member_count`) instead of `PackVerificationError`
(`ccf/sync/verify.py:130-160`). Fails closed (transaction rollback), but
leaks raw stack traces and breaks the error contract. Wrap in `PackError`.

## Notes (not vulnerabilities)

- `StreamEntry.from_dict` coerces `required` via `bool()` — `"false"` →
  `True` — but the manifest schema pins `"type": "boolean"` before this
  runs (`spec/.../mindpack-manifest.schema.json:90-92`), so it's dead code.
  Fail-closed direction anyway.
- The branch ships a full `spec/ccf/0.1.2-rc1/` package (schemas, URNs
  `0.1.2-rc1:*`, format `ccf.mindpack/0.1.2-rc1`), but **zero runtime code
  references it** — everything is pinned to the 0.1.1 package
  (`SCHEMA_MINDPACK_MANIFEST = "urn:ccf:schema:0.1.1:..."` in
  `ccf/sync/export.py:27`). Nothing breaks (runtime stays self-consistent
  on 0.1.1), but the RC package is unwired, and anyone pointing
  `package_root` at 0.1.2-rc1 will get restore rejections from the
  `format != "ccf.mindpack/0.1.1"` check in `restore.py`. Decide: wire the
  RC package or document it as spec-only.
- HTTP transport is only as trustworthy as the sidecar channel:
  `fetch_sidecar_http` takes `pack_digest` at face value, so MITM wins
  unless the restore-side trusted-genesis anchor (H1) exists. H1's fix also
  fixes this.
- `_verify_batch_envelope` correctly refuses to persist rejected envelopes
  (anti-chain-poisoning), and the suppression response-shaping avoids
  oracle leaks — both good.

## Bottom line

The cryptographic core and governance/erasure engines are solid; the
failures are in the seams: bootstrap anchoring (H1), a file re-read race
(M1), resource limits (M2), and credential lifecycle enforcement (M3). Fix
H1 first; it is one guard and it makes the rest of the system actually
mean something.
