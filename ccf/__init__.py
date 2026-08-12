"""CCF 0.1.1 canonical primitives for Thoth.

Implements the portable, hash-relevant core of the vendored spec package at
``spec/ccf/0.1.1/``: identifiers, RFC 8785 (JCS) canonicalization, the
``ccf-jcs-sha256-v2`` hash profile, portable object envelopes, and semantic
catalog pinning.

The vendored package is authoritative; published executable vectors take
precedence over prose on any contradiction (spec section 0.4).
"""

CCF_SPEC = "ccf/0.1.1"
CCF_HASH_PROFILE = "ccf-jcs-sha256-v2"
CCF_SIGNATURE_PROFILE = "ed25519-jcs-v1"
