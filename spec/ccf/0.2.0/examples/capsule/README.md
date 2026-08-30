# Example CCF Capsule

This fixture is a partial, lossless Exchange Capsule for a small project
knowledge transfer. It contains:

- a `semantic.entity` root Record for the Cissa hardware project;
- an `experience.observation` design note;
- the exporting runtime Record;
- `ccf.part_of` membership Links;
- one byte-preserved opaque stream containing a known Governed Record and an
  unknown future type, neither activated by the Exchange recipient;
- one external, one withheld, and one erased dependency;
- a pending L1-to-L3 uplift receipt that preserves every supplied ID, pins each
  JCS submission hash, and makes no premature object-hash or
  producer-authentication claim;
- a completed L1-to-L2 uplift receipt backed by published deterministic
  conformance salts, commitments, object hashes, and archive resolution;
- an independent L3-to-L1 lossy downgrade fixture built from the real 0.1.2
  Verified mindpack: its selected source inventory contains corresponding
  portable objects, the selected assertions' committed compartments, origin
  rows, a pinned source-archive identity, journal membership evidence, signed
  commit compartments, and producer batch, while the L1 export retains one
  logically inventoried source assertion and exactly enumerates the omitted
  archive material in both its receipt and target Capsule manifest.

The `org.example.future_context` extension on the root and the
`org.example.future-governance` type in the opaque stream are deliberately
unknown to CCF. Exchange implementations preserve them without activating them.
The same opaque stream carries `lineage.erasure_receipt`, proving that known L4
material can survive an L1 transfer as bytes without becoming active semantics.
That Capsule preservation example is separate from the downgrade fixture; the
downgrade never pairs its export with evidence from an unrelated archive.
Exchange conformance proves the byte-exact inventory subtraction. Verified
conformance additionally recomputes every selected object/compartment
commitment, producer-evidence submission hash, and journal binding; an L1-only
recipient is not expected to authenticate L3 proof.

The pending receipt deliberately contains null object hashes because this
Capsule does not include the destination's canonical compartments. Completed
uplift identity, fixed conformance salts, commitments, and expected hashes are
tested for the complete Capsule by `check-canonical`.
