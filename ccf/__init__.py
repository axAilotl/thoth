"""CCF 0.1.1 for Thoth.

Implements the vendored spec package at ``spec/ccf/0.1.1/``:

- canonical primitives (phase 1): identifiers, RFC 8785 (JCS)
  canonicalization, the ``ccf-jcs-sha256-v2`` hash profile, portable object
  envelopes, and semantic catalog pinning;
- the local producer path (phase 2): explicit Ed25519 key storage, the
  Thoth device credential, signed producer-batch chains, and the durable
  Postgres spool with provisional local objects;
- canonical admission (phase 3): genesis, the serialized archive-head
  transaction with origin-tuple idempotency, lineage compare-and-swap and
  derivation cycle checks, signed Merkle commits, and prefix-integrity
  chain verification.

The vendored package is authoritative; published executable vectors take
precedence over prose on any contradiction (spec section 0.4).
"""

CCF_SPEC = "ccf/0.1.1"
CCF_HASH_PROFILE = "ccf-jcs-sha256-v2"
CCF_SIGNATURE_PROFILE = "ed25519-jcs-v1"
