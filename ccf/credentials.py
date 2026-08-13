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

    Mirrors ``spec/ccf/0.1.2-rc1/examples/thoth-capture`` record material: the
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


def _credential_payloads(row) -> tuple[str, dict]:
    return row[0], row[1]


def resolve_credential_public_key(conn, credential_id: str) -> str:
    """Resolve an admitted credential's raw base64url public key.

    Fails closed when the credential is unknown, ambiguous, malformed, or
    its credential lineage head is in the terminal ``revoke`` state.
    """
    rows = conn.execute(
        """
        SELECT oh.id, c.plaintext_json -> 'structural_payload' AS payload
        FROM object_header oh
        JOIN compartment c
          ON c.object_id = oh.id AND c.compartment = 'structural'
        WHERE oh.object_kind = 'record'
          AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = 'core.device_credential'
          AND c.plaintext_json -> 'structural_payload' ->> 'credential_id' = %s
        """,
        (credential_id,),
    ).fetchall()
    if not rows:
        raise CredentialError(f"unknown credential: {credential_id}")

    active: list[tuple[str, dict]] = []
    for record_id, payload in (_credential_payloads(row) for row in rows):
        state_row = conn.execute(
            """
            SELECT state FROM lineage_head
            WHERE head_record_id = %s
            """,
            (record_id,),
        ).fetchone()
        if state_row is not None and state_row[0] == "revoke":
            continue
        active.append((record_id, payload))

    if not active:
        raise CredentialError(f"credential is revoked: {credential_id}")
    if len(active) > 1:
        raise CredentialError(
            f"ambiguous credential {credential_id}: {len(active)} active records"
        )

    _, payload = active[0]
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
