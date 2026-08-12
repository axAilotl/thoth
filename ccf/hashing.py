"""CCF 0.1.1 hash profile ``ccf-jcs-sha256-v2`` (spec section 4).

Semantic port of ``spec/ccf/0.1.1/tools/ccf-jcs.mjs``. Every hash is

    SHA256(<domain-separator> || 0x00 || <parts...>)

with the exact domain separators and field membership defined by the spec.
Digests are lowercase ``sha256:<64 hex>`` strings; Ed25519 signs the raw
32-byte digest, never the hex text.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_pem_private_key,
    load_pem_public_key,
)

from ccf.jcs import canonical_bytes

OBJECT_KINDS: frozenset[str] = frozenset({"record", "link", "blob"})
COMPARTMENTS: frozenset[str] = frozenset({"structural", "semantic"})

_SALT_LENGTH = 32
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class CcfHashError(ValueError):
    """Raised when hash-profile input is malformed."""


def sha256_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def digest_string(data: bytes) -> str:
    return "sha256:" + sha256_bytes(data).hex()


def parse_digest(digest: str) -> bytes:
    """Validate a ``sha256:<64 hex>`` digest and return its raw 32 bytes."""
    if not isinstance(digest, str) or _DIGEST_RE.match(digest) is None:
        raise CcfHashError(f"invalid SHA-256 digest: {digest!r}")
    return bytes.fromhex(digest[7:])


def domain_hash_bytes(domain: str, *parts: bytes) -> bytes:
    hasher = hashlib.sha256()
    hasher.update(domain.encode("utf-8"))
    hasher.update(b"\x00")
    for part in parts:
        hasher.update(part)
    return hasher.digest()


def domain_digest(domain: str, *parts: bytes) -> str:
    return "sha256:" + domain_hash_bytes(domain, *parts).hex()


def canonical_digest(domain: str, value: object) -> str:
    """Digest of a domain separator over the JCS serialization of ``value``."""
    return domain_digest(domain, canonical_bytes(value))


def decode_b64url(text: str) -> bytes:
    """Decode unpadded base64url, failing closed on non-canonical input."""
    if not isinstance(text, str) or not re.fullmatch(r"[A-Za-z0-9_-]*", text):
        raise CcfHashError(f"invalid base64url text: {text!r}")
    try:
        return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except binascii.Error as exc:
        raise CcfHashError(f"invalid base64url text: {text!r}") from exc


def encode_b64url(data: bytes) -> str:
    """Encode bytes as base64url without padding (spec section 4.2)."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _decode_salt(text: str, what: str) -> bytes:
    salt = decode_b64url(text)
    if len(salt) != _SALT_LENGTH:
        raise CcfHashError(f"{what} must be 32 bytes, got {len(salt)}")
    return salt


# ---------------------------------------------------------------------------
# Section 4.3 / 4.4: compartment and blob-content commitments
# ---------------------------------------------------------------------------


def compartment_commitment(
    object_kind: str, compartment: str, envelope: dict
) -> str:
    """Salted compartment commitment for one of the 6 kind/compartment pairs."""
    if object_kind not in OBJECT_KINDS:
        raise CcfHashError(f"unsupported object kind: {object_kind!r}")
    if compartment not in COMPARTMENTS:
        raise CcfHashError(f"unsupported compartment: {compartment!r}")
    if not isinstance(envelope, dict) or "salt" not in envelope or "content" not in envelope:
        raise CcfHashError("compartment envelope requires 'salt' and 'content'")
    salt = _decode_salt(envelope["salt"], "compartment salt")
    domain = f"ccf:{object_kind}-{compartment}:v2"
    return domain_digest(domain, salt, canonical_bytes(envelope["content"]))


def blob_content_commitment(content_salt: str, data: bytes) -> str:
    """Commitment over raw blob bytes with a separate 32-byte content salt."""
    salt = _decode_salt(content_salt, "Blob content salt")
    return domain_digest("ccf:blob-content:v2", salt, data)


# ---------------------------------------------------------------------------
# Section 4.5 / 4.6: portable object hash and submission hash
# ---------------------------------------------------------------------------


def object_hash(header: dict) -> str:
    """Portable object hash over the complete header minus ``object_hash``."""
    kind = header.get("object_kind") if isinstance(header, dict) else None
    if kind not in OBJECT_KINDS:
        raise CcfHashError(f"unsupported object kind: {kind!r}")
    hashed = {key: value for key, value in header.items() if key != "object_hash"}
    return canonical_digest(f"ccf:{kind}:v2", hashed)


def submission_hash(submission: dict) -> str:
    """Producer submission hash (spec section 4.6)."""
    return canonical_digest("ccf:submission:v2", submission)


# ---------------------------------------------------------------------------
# Section 4.7: producer batch hash and Ed25519 signature
# ---------------------------------------------------------------------------


def producer_batch_hash(batch: dict) -> str:
    """Batch hash over the batch minus ``batch_hash`` and ``signature``."""
    unsigned = {
        key: value
        for key, value in batch.items()
        if key not in ("batch_hash", "signature")
    }
    return canonical_digest("ccf:producer-batch:v1", unsigned)


def producer_batch_signing_digest(batch_hash: str) -> bytes:
    """The raw 32-byte batch hash is the Ed25519 message."""
    return parse_digest(batch_hash)


# ---------------------------------------------------------------------------
# Section 4.8: commit leaves and the deterministic-split Merkle tree
# ---------------------------------------------------------------------------


def commit_leaf(member: dict) -> bytes:
    """Raw leaf hash for one commit member."""
    return domain_hash_bytes("ccf:commit-leaf:v2", canonical_bytes(member))


def _largest_power_of_two_less_than(n: int) -> int:
    k = 1
    while (k << 1) < n:
        k <<= 1
    return k


def merkle_root_from_leaf_bytes(leaves: list[bytes]) -> bytes:
    """Deterministic-split Merkle root (RFC 6962 split rule)."""
    if not leaves:
        return domain_hash_bytes("ccf:merkle-empty:v2")
    if len(leaves) == 1:
        return leaves[0]
    split = _largest_power_of_two_less_than(len(leaves))
    left = merkle_root_from_leaf_bytes(leaves[:split])
    right = merkle_root_from_leaf_bytes(leaves[split:])
    return domain_hash_bytes("ccf:merkle-node:v2", left, right)


def merkle_root(members: list[dict]) -> str:
    """Merkle root over commit members sorted by numeric commit position.

    Positions must be unique and contiguous from zero (fail closed).
    """
    ordered = sorted(members, key=lambda m: int(m["commit_position"]))
    for index, member in enumerate(ordered):
        position = int(member["commit_position"])
        if position < index:
            raise CcfHashError(f"duplicate commit position: {position}")
        if position != index:
            raise CcfHashError("commit positions must be contiguous from zero")
    return "sha256:" + merkle_root_from_leaf_bytes(
        [commit_leaf(member) for member in ordered]
    ).hex()


# ---------------------------------------------------------------------------
# Section 4.9: commit signing digest (commit_hash is the commit's object_hash)
# ---------------------------------------------------------------------------


def commit_signing_digest(
    header_without_commitments: dict, structural_content_without_signature: dict
) -> bytes:
    """Raw 32-byte signing digest for an ``integrity.commit`` Record."""
    signing_input = {
        "header": header_without_commitments,
        "structural_content": structural_content_without_signature,
    }
    return domain_hash_bytes("ccf:commit-sig:v2", canonical_bytes(signing_input))


# ---------------------------------------------------------------------------
# Section 4.10 / catalog artifacts: semantic catalog root and entry digests
# ---------------------------------------------------------------------------


def semantic_catalog_root(catalog_without_root: dict) -> str:
    """Root digest of a semantic catalog with its ``root`` field removed."""
    if "root" in catalog_without_root:
        raise CcfHashError("catalog input must not contain 'root'")
    return canonical_digest("ccf:semantic-catalog:v1", catalog_without_root)


def schema_artifact_digest(schema: dict) -> str:
    """Pinned digest of a parsed schema artifact (catalog entry value)."""
    return canonical_digest("ccf:schema-artifact:v1", schema)


def registry_artifact_digest(registry: dict) -> str:
    """Pinned digest of a parsed registry artifact (catalog entry value)."""
    return canonical_digest("ccf:registry-artifact:v1", registry)


def registry_entry_digest(entry: dict) -> str:
    """Pinned digest of one registry entry (structural compartment binding).

    Structural compartments bind an object to the exact type/Link registry
    entry the archive resolved, via ``ccf:registry-entry:v1`` (see
    ``spec/ccf/0.1.1/tools/build-example.mjs``).
    """
    return canonical_digest("ccf:registry-entry:v1", entry)


# ---------------------------------------------------------------------------
# Ed25519 (ed25519-jcs-v1): sign/verify over raw 32-byte digests
# ---------------------------------------------------------------------------


def load_private_key(pem: bytes | str | Path) -> Ed25519PrivateKey:
    """Load an unencrypted PEM Ed25519 private key; fail closed otherwise."""
    if isinstance(pem, Path):
        pem = pem.read_bytes()
    if isinstance(pem, str):
        pem = pem.encode("utf-8")
    key = load_pem_private_key(pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise CcfHashError("not an Ed25519 private key")
    return key


def load_public_key(pem: bytes | str | Path) -> Ed25519PublicKey:
    """Load a PEM Ed25519 public key; fail closed otherwise."""
    if isinstance(pem, Path):
        pem = pem.read_bytes()
    if isinstance(pem, str):
        pem = pem.encode("utf-8")
    key = load_pem_public_key(pem)
    if not isinstance(key, Ed25519PublicKey):
        raise CcfHashError("not an Ed25519 public key")
    return key


def sign_digest(private_key: Ed25519PrivateKey, digest: bytes) -> bytes:
    """Ed25519-sign a raw 32-byte digest; return the 64-byte signature."""
    if len(digest) != 32:
        raise CcfHashError(f"Ed25519 message must be a 32-byte digest, got {len(digest)}")
    return private_key.sign(digest)


def verify_digest(
    public_key: Ed25519PublicKey, signature: bytes, digest: bytes
) -> None:
    """Verify an Ed25519 signature over a raw digest; raise on failure."""
    if len(digest) != 32:
        raise CcfHashError(f"Ed25519 message must be a 32-byte digest, got {len(digest)}")
    public_key.verify(signature, digest)  # raises InvalidSignature


def public_key_b64url(public_key: Ed25519PublicKey) -> str:
    """Raw 32-byte public key as unpadded base64url (header key material)."""
    raw = public_key.public_bytes(Encoding.Raw, PublicFormat.Raw)
    return encode_b64url(raw)


def private_key_from_raw(raw: bytes) -> Ed25519PrivateKey:
    """Build an Ed25519 private key from its 32-byte raw seed."""
    if len(raw) != 32:
        raise CcfHashError(f"Ed25519 seed must be 32 bytes, got {len(raw)}")
    return Ed25519PrivateKey.from_private_bytes(raw)
