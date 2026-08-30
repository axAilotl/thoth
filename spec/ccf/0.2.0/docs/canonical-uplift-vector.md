# Canonical Capsule uplift vector

`ccf-canonical-uplift-vector-v1` is a conformance-only resolution profile for
the example Capsule. It makes the L1-to-L2 operation reproducible without
turning deterministic salts into a production rule.

For each Record or Link submission, the vector constructs a 0.1.2 structural
compartment with the submitted type, version, visibility, retention hint, an
empty `structural_payload`, and empty extensions. `schema_digest` is the pinned
0.1.2 semantic-schema digest and `registry_entry_digest` is the
`ccf:registry-entry:v1` canonical digest. A Link whose registry entry places
endpoints structurally also carries its submitted `from_id` and `to_id` there.

The semantic compartment carries `recorded_by`, `recorded_at`, optional
`occurred_at`, the submitted claims, authority, payload, and extensions. An
origin claim is extended only with the JCS submission hash. Semantic Link
endpoints and selectors are included when their registry/submission requires
them. The vector adds no policy, authentication, admission coordinate, or
governance claim.

The exact structural and semantic salts, resulting commitments, and object
hashes are pinned per ID in `vectors/canonical-store-operations.json`. Real
Canonical Stores MUST generate fresh random salts; they use these fixed salts
only when executing this known-answer vector. The completed uplift receipt
under `examples/capsule/` records the expected stable IDs, submission hashes,
object hashes, and resolution profile.
