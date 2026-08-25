"""Thoth runtime/device credentials (spec sections 3.5 and 6.2).

One Thoth runtime or device signs producer batches with an Ed25519 keypair
stored at an explicit path (see :mod:`ccf.keys`). The archive learns the
verification key from an admitted ``core.device_credential`` Record whose
structural payload carries the credential ID, subject, scopes, and raw
base64url public key, and whose lifecycle is a compare-and-swapped
``ccf.state.credential-v1`` lineage.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ccf.keys import CcfKeyError, load_signing_key, public_key_from_b64url, public_key_text


class CredentialError(RuntimeError):
    """Raised when a credential is unknown, revoked, or malformed."""


@dataclass(frozen=True)
class DeviceCredential:
    """A producer-side signing credential: keypair plus its CCF record IDs."""

    credential_id: str  # urn:ccf:credential:...
    key_id: str  # urn:ccf:key:...
    private_key: Ed25519PrivateKey

    @classmethod
    def load(cls, key_path: str | Path, *, credential_id: str, key_id: str) -> "DeviceCredential":
        """Load the signing key from an explicit path; fail closed if absent."""
        return cls(
            credential_id=credential_id,
            key_id=key_id,
            private_key=load_signing_key(key_path),
        )

    @property
    def public_key_b64url(self) -> str:
        return public_key_text(self.private_key)

    def sign(self, digest: bytes) -> bytes:
        from ccf.hashing import sign_digest

        return sign_digest(self.private_key, digest)


def device_credential_structural_payload(
    credential: DeviceCredential,
    *,
    subject_id: str,
    issuer_key_id: str,
    scopes: list[str],
    valid_from: str,
    expires_at: str | None = None,
) -> dict:
    """Structural payload of a ``core.device_credential`` Record.

    Mirrors ``spec/ccf/0.1.2/examples/personal-archive`` record material: the
    archive resolves batch signature verification keys from this payload.
    """
    return {
        "credential_id": credential.credential_id,
        "subject_id": subject_id,
        "issuer_key_id": issuer_key_id,
        "signing_key": {
            "profile": "ed25519",
            "public_key": credential.public_key_b64url,
            "key_id": credential.key_id,
        },
        "encryption_key": None,
        "scopes": scopes,
        "valid_from": valid_from,
        "expires_at": expires_at,
        "offline_grace_until": None,
        "extensions": {},
    }


def resolve_credential_public_key(
    conn,
    credential_id: str,
    *,
    archive_id: str,
    subject_id: str,
    required_scope: str,
    now: str,
) -> str:
    """Resolve an admitted credential's raw base64url public key.

    Fails closed when the credential is unknown, ambiguous, malformed, or
    its credential lineage head is in the terminal ``revoke`` state. The
    payload's lifecycle fields are enforced as well: the credential must
    carry ``required_scope`` in ``scopes``, and ``now`` must satisfy
    ``valid_from <= now < expires_at`` (a null ``expires_at`` never
    expires).
    """
    from ccf.governance.context import parse_timestamp

    rows = conn.execute(
        """
        WITH matching_lineages AS (
            SELECT DISTINCT
                   oh.archive_id,
                   c.plaintext_json -> 'lineage' ->> 'lineage_id' AS lineage_id
            FROM object_header oh
            JOIN compartment c
              ON c.object_id = oh.id AND c.compartment = 'structural'
            WHERE oh.archive_id = %s
              AND oh.object_kind = 'record'
              AND c.state = 'plaintext'
              AND c.plaintext_json ->> 'type' = 'core.device_credential'
              AND c.plaintext_json -> 'structural_payload' ->> 'credential_id' = %s
        )
        SELECT lh.state,
               head.plaintext_json -> 'structural_payload' AS payload
        FROM matching_lineages matched
        JOIN lineage_head lh
          ON lh.archive_id = matched.archive_id
         AND lh.lineage_id = matched.lineage_id
        JOIN compartment head
          ON head.object_id = lh.head_record_id
         AND head.compartment = 'structural'
         AND head.state = 'plaintext'
        """,
        (archive_id, credential_id),
    ).fetchall()
    if not rows:
        raise CredentialError(f"unknown credential: {credential_id}")
    if len(rows) > 1:
        raise CredentialError(
            f"ambiguous credential {credential_id}: {len(rows)} canonical lineages"
        )
    state, payload = rows[0]
    if state == "revoke" or payload.get("credential_id") != credential_id:
        raise CredentialError(f"credential is revoked: {credential_id}")

    scopes = payload.get("scopes")
    if not isinstance(scopes, list) or required_scope not in scopes:
        raise CredentialError(
            f"credential {credential_id} lacks the required scope "
            f"{required_scope!r}"
        )
    if payload.get("subject_id") != subject_id:
        raise CredentialError(
            f"credential {credential_id} subject does not match producer {subject_id}"
        )
    try:
        now_dt = parse_timestamp(now)
        valid_from = parse_timestamp(payload["valid_from"])
        expires_at = payload.get("expires_at")
        expires_dt = parse_timestamp(expires_at) if expires_at is not None else None
    except (KeyError, TypeError, ValueError) as exc:
        raise CredentialError(
            f"credential {credential_id} has malformed validity fields: {exc}"
        ) from exc
    if now_dt < valid_from:
        raise CredentialError(
            f"credential {credential_id} is not yet valid "
            f"(valid_from {payload['valid_from']})"
        )
    if expires_dt is not None and now_dt >= expires_dt:
        raise CredentialError(
            f"credential {credential_id} expired at {expires_at}"
        )

    try:
        key_text = payload["signing_key"]["public_key"]
        if payload["signing_key"].get("profile") != "ed25519":
            raise KeyError("signing_key.profile")
    except (KeyError, TypeError) as exc:
        raise CredentialError(
            f"credential {credential_id} lacks an ed25519 signing key: {exc}"
        ) from exc
    try:
        public_key_from_b64url(key_text)
    except CcfKeyError as exc:
        raise CredentialError(
            f"credential {credential_id} has a malformed public key: {exc}"
        ) from exc
    return key_text
