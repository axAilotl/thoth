"""Retention and deletion (checklist phase 7; spec sections 3.6-3.10, 12.7).

This package owns:

- :mod:`ccf.erasure.retention` — registry-declared retention-profile
  enforcement (spec 1.3): what may be erased per object, fail closed;
- :mod:`ccf.erasure.operations` — the durable erasure saga state machine
  (spec 3.8), one ``erasure_operation`` row per operation;
- :mod:`ccf.erasure.purge` — destroy/verify of controlled copies:
  projection tables, checkpoints, egress capabilities, and generated
  plaintext (wiki staging);
- :mod:`ccf.erasure.suppression` — the keyed (HMAC) suppression store
  (spec 12.7) blocking silent reintroduction of erased origins/content;
- :mod:`ccf.erasure.receipts` — canonical ``lineage.erasure_receipt``
  Records with ``ccf.covers`` membership Links (spec 1.6, 3.8);
- :mod:`ccf.erasure.media` — multi-subject media decision shapes
  (spec 3.9): whole-blob erasure, restriction pending review, or a
  reviewed replacement Blob — never surgical deidentification;
- :mod:`ccf.erasure.service` — the :class:`ErasureService` facade exposed
  as ``Archive.erasure()``.

Assurance honesty (spec 3.7): this implementation records ``logical``
erasure only. Storage-verified would require verifying WAL/PITR, replicas,
and backups outside the controlled envelope, and cryptographic erasure
requires a per-object DEK profile (``ccf-object-erasure-v1``) that Core
deliberately does not implement.
"""
