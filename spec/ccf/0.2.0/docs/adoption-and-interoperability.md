# Adoption and interoperability matrix

## Cissa and Thoth target declarations

| Component | CCF boundary |
|---|---|
| Cissa pendant firmware | Reliable capture transport and device/session identity; not a CCF implementation |
| Cissa phone | Exchange Producer and Preserver with durable batching |
| Cissa phone-only mode | Optional Canonical Store |
| Cissa Home or Cloud | Verified Archive, plus only the capabilities it operates |
| Thoth reference archive | Governed Archive |
| Scoped knowledge transfer | CCF Capsule |
| Backup, replica, or archive migration | mindpack restore or foreign merge |

Full governance is claimed only after policy evaluation, deletion operations,
suppression behavior, generation fences, and destructive projection rebuild have
passed operational tests.

## Planned cross-application cases

1. Exchange Capsule round trip preserves IDs, types, source claims, references,
   and unknown extensions.
2. Canonical round trip preserves object hashes and available compartments.
3. Duplicate Capsule import remains idempotent.
4. Same origin and revision with changed content produces a conflict.
5. Unsupported semantic packs remain preserved and inert.
6. Withheld, erased, and external states remain distinguishable.
7. Foreign merge preserves source proofs and creates destination admission
   history without rewriting portable objects.
8. Audio capture survives termination at every boundary without duplicate or
   missing semantic artifacts.
9. Governed erasure transfer cannot silently resurrect or relabel material.

Round trips between two applications that share one serializer, verifier, or
archive kernel prove integration but not independent interoperability. A full
independent claim needs a separately written generator or verifier. That second
implementation can target Exchange and Canonical Store before implementing an
archive or governance engine.

These cases are an application-integration roadmap, not claims made by this
schema repository. The package directly exercises Capsule preservation,
canonical uplift/idempotency, Verified foreign-merge invariants, and Governed
erasure fixtures. Real Thoth/Cissa round trips and process-interruption tests
require those application repositories and remain external release gates.
