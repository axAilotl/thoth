# 12. Security, credentials, custody, and trust claims

## 12.1 Threat statement

The native journal detects corruption and unauthorized alteration by actors who do not control the active archive signing key. It does not prevent a malicious active signer from creating a replacement valid history. Witnessed integrity is a separate profile.

## 12.2 Archive and device keys

Recommended key roles:

- offline archive root signing key;
- active archive admission signing key;
- one signing key per device/runtime;
- optional encryption/wrapping key per device;
- random archive epoch encryption secret where the encryption profile is used;
- optional random per-object DEKs only for the high-assurance erasure profile.

Key roles MUST remain cryptographically distinct.

## 12.3 Device credentials

A `core.device_credential` Record binds a device/runtime to:

- signing public key;
- optional encryption public key;
- issuer key;
- scopes;
- validity interval;
- optional offline grace interval.

CCF does not require X.509. A canonical archive-issued credential is sufficient. Deployments may map to X.509, WebAuthn, TPM, HSM, or enterprise PKI.

## 12.4 Consumer and hosted custody modes

A hosted service may offer distinct, honestly named modes:

- user-only/private vault — provider has no decryption recipient;
- assisted recovery — threshold or trusted-device recovery;
- managed service — service runtime can decrypt;
- customer-managed or external key — enterprise wrapping authority controls access;
- attested compute — key release limited to a measured workload.

Encrypted storage is not encrypted computation. If ordinary hosted transcription or model inference sees plaintext, the service has a plaintext processing path during that workload.

## 12.5 Offline keys

Authorized devices may cache credentials and archive epoch keys locally. This permits local reads and capture without a network. Short-lived action capabilities may expire separately from long-lived evidence-capture credentials.

Loss of every authorized device and every recovery path makes a provider-blind archive unrecoverable by design.

## 12.6 Cognitive security

External content is evidence, not authority. Ingress preserves source identity, trust class, transformation history, screening findings, and exact derivation. No stored text can grant itself permission to change policy, identity, preferences, disclosure, or action authority.

## 12.7 Suppression after erasure

Suppression lookups are projections. Canonical `lineage.suppression_set` Records
and governed Blobs retain the exact keyed commitments under signed erasure
lineage. The receipt commits profile, Record and Blob IDs, entry count, Merkle
root, key/profile ID, and scope. Lookup deletion is detected and reconstruction
restores rejection authority. Tokens remain sensitive governed metadata and are
never public header fields. Keyed PRF/HMAC profiles require rotation and recovery
policy; plain unsalted fingerprints are insufficient for low-entropy content.

The catalog-pinned `ccf-hmac-sha256-suppression-v1` registry defines exact JCS
origin/content preimages, HMAC domain and lowercase-hex token encoding, sorted
unique entry order, minimum key size, and suppression-specific leaf/node/empty
Merkle domains so independent implementations reproduce identical roots.

The `scope_commitment` construction is pinned to the reference implementation.
The schema types it as an opaque digest and the rc1 registry does not pin its
bytes; conforming implementations MUST construct it exactly as follows:

- domain separator: the UTF-8 bytes of `ccf:suppression-scope:v1`, followed by
  a single `0x00` byte;
- preimage: the JCS (RFC 8785) canonical serialization of the JSON array of
  erased object ID strings, sorted in ascending Unicode code-point order;
- duplicates: object IDs are not deduplicated and duplicates are not rejected;
  each erasure plan contributes exactly one array element;
- empty scope: an empty plan list hashes the canonical empty array `[]`,
  yielding a well-defined digest rather than an error;
- digest encoding: SHA-256 over the domain separator, the `0x00` byte, and the
  preimage, encoded as lowercase hexadecimal with the `sha256:` prefix.

The suppression-profiles registry entry gains a field pinning this construction
at final; registry bytes are frozen for rc1.

## 12.8 Side channels

Unauthorized existence queries, retry responses, and erasure status must be response-shaped and rate-limited. Authorized source owners may receive richer lifecycle information. Core does not require identical network timing but requires deployments to document observable differences.
