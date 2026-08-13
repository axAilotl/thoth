# CCF 0.1.2-rc1 Registries

The semantic catalog binds these registries and all activated schemas to exact SHA-256 digests. Names and versions are immutable once published. Unknown entries may be preserved but must not activate behavior.

The type registry declares each Record type's semantic payload schema, optional structural payload schema, retention profile, lineage behavior, and owning conformance profile. The Link registry declares endpoint location, retention, transitivity, cycle rules, and policy propagation.

The admission-authority-class registry pins the deterministic interpretation of
every `required_authority` value used by the type registry, including actor
kinds, accepted bases, person-acceptance behavior, consulted canonical state,
failure reason, and evaluator profile.

The suppression-profile registry pins keyed-token preimages and derivation,
encoding, entry order, duplicate rejection, key bounds, and the
suppression-specific Merkle tree construction.
