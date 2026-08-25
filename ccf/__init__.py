"""CCF 0.2.0 layered conformance for Thoth, over frozen CCF 0.1.2 objects.

Implements the vendored spec packages at ``spec/ccf/0.1.2/`` and
``spec/ccf/0.2.0/``:

- canonical primitives (phase 1): identifiers, RFC 8785 (JCS)
  canonicalization, the ``ccf-jcs-sha256-v2`` hash profile, portable object
  envelopes, and semantic catalog pinning;
- the local producer path (phase 2): explicit Ed25519 key storage, the
  Thoth device credential, signed producer-batch chains, and the durable
  Postgres spool with provisional local objects;
- canonical admission (phase 3): genesis, the serialized archive-head
  transaction with origin-tuple idempotency, lineage compare-and-swap and
  derivation cycle checks, signed Merkle commits, and prefix-integrity
  chain verification;
- projections (phase 5, :mod:`ccf.projections`): current Link state, the
  active ``derived_from`` closure, entity clusters, tsvector full-text,
  caller-supplied pgvector embeddings, wiki rebuild, generation-fence
  invalidation, checkpoints, and the cross-projection snapshot pin;
- the governance baseline (phase 6, ``ccf.governance``): the pinned
  ``ccf-deny-overrides-v1`` evaluator over a deterministic policy closure,
  ``governance.*`` generation fences advanced in the admission
  transaction, bounded pending results, fenced egress capabilities,
  consequential receipts, and registry ``required_authority``
  enforcement at admission;
- sync and packs (phase 8, :mod:`ccf.sync`): sync-head exchange and
  negotiation, mindpack export/restore, foreign merge with custody
  proofs, fork preservation, compressed delta packs with verified-chunk
  resume over file or HTTP, and the durable producer-side Blob spool;
- layered conformance (0.2.0): implementation declarations, guarantee
  levels, roles, capabilities, semantic packs, Capsule scoped exchange,
  and uplift/downgrade receipts. Portable Record/Link/Blob bytes stay
  ``ccf/0.1.2``.

0.1.2 deltas: the pinned admission authority classes with their
normative ``failure_reason`` strings, producer-chain dispositions
(``accepted`` / ``partially_accepted`` / ``content_rejected`` /
``quarantined``, with ``predecessor_missing`` as the retryable pending
outcome), foreign merge preserving unavailable compartments, and
canonical, journal-covered suppression lineage
(``lineage.suppression_set``) with the suppression lookup table as a
rebuildable projection (spec section 12.7).

Conformance label: CCF 0.2.0 Governed Archive over CCF 0.1.2 Core,
including canonical erasure suppression (spec 12.7). Known gaps:
archive-derived encryption and per-object DEKs are unimplemented
(``ccf-archive-encryption-derived-v1`` / ``ccf-object-erasure-v1`` stay
undeclared); erasure assurance is honestly ``logical`` only.

The vendored packages are authoritative; published executable vectors
take precedence over prose on any contradiction (spec section 0.4).
"""

from pathlib import Path

CCF_SPEC = "ccf/0.1.2"
CCF_VERSION = "0.1.2"
CCF_LAYER = "0.2.0"
CCF_LEVEL = "ccf-governed-archive-v1"
CCF_HASH_PROFILE = "ccf-jcs-sha256-v2"
CCF_SIGNATURE_PROFILE = "ed25519-jcs-v1"

SPEC_ROOT = Path(__file__).resolve().parent.parent / "spec" / "ccf"
PACKAGE_ROOT = SPEC_ROOT / "0.1.2"
DRAFT_ROOT = SPEC_ROOT / "0.2.0"


def schema_urn(name: str) -> str:
    """Pinned schema URN for the implemented portable-object package version."""
    return f"urn:ccf:schema:{CCF_VERSION}:{name}"


def draft_schema_urn(name: str) -> str:
    """Pinned schema URN for a 0.2.0 layered-conformance document."""
    return f"urn:ccf:schema:{CCF_LAYER}:{name}"
