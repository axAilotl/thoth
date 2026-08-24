"""Explicit Ed25519 key-file management for CCF signers (spec section 3.5).

Key storage is always an explicit filesystem path resolved from config or
environment — there is no implicit default location and no in-repo key
material. Loading fails closed when the file is missing, unreadable, or not
an unencrypted PEM Ed25519 key. Generation refuses to overwrite an existing
file and writes with owner-only permissions. Private keys must never be
committed to git.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from ccf.hashing import CcfHashError, load_private_key, load_public_key, public_key_b64url


class CcfKeyError(RuntimeError):
    """Raised when a signing key is missing, unsafe, or malformed."""


def _check_private_key_file(path: Path) -> None:
    try:
        mode = path.stat().st_mode
    except OSError as exc:
        raise CcfKeyError(f"cannot stat signing key file {path}: {exc}") from exc
    if not stat.S_ISREG(mode):
        raise CcfKeyError(f"signing key path is not a regular file: {path}")
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise CcfKeyError(
            f"signing key file {path} is accessible by group/other; "
            "chmod 600 required"
        )


def load_signing_key(path: str | Path) -> Ed25519PrivateKey:
    """Load an Ed25519 private key from an explicit path; fail closed."""
    key_path = Path(path)
    if not key_path.is_file():
        raise CcfKeyError(
            f"signing key not found: {key_path}. Configure an explicit key "
            "path (config database.ccf_archive.*_key_path or the matching "
            "environment variable) and generate the key out-of-band."
        )
    _check_private_key_file(key_path)
    try:
        return load_private_key(key_path)
    except (ValueError, CcfHashError) as exc:
        raise CcfKeyError(f"invalid Ed25519 signing key at {key_path}: {exc}") from exc


def generate_signing_key(path: str | Path) -> Ed25519PrivateKey:
    """Generate a new Ed25519 keypair at ``path`` (mode 0600, no overwrite)."""
    key_path = Path(path)
    if key_path.exists():
        raise CcfKeyError(f"refusing to overwrite existing key file: {key_path}")
    key = Ed25519PrivateKey.generate()
    pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, pem)
    finally:
        os.close(fd)
    return key


def load_verification_key(path: str | Path) -> Ed25519PublicKey:
    """Load an Ed25519 public key from an explicit path; fail closed."""
    key_path = Path(path)
    if not key_path.is_file():
        raise CcfKeyError(f"verification key not found: {key_path}")
    try:
        return load_public_key(key_path)
    except (ValueError, CcfHashError) as exc:
        raise CcfKeyError(
            f"invalid Ed25519 public key at {key_path}: {exc}"
        ) from exc


def public_key_text(key: Ed25519PublicKey | Ed25519PrivateKey) -> str:
    """Raw 32-byte public key as unpadded base64url."""
    if isinstance(key, Ed25519PrivateKey):
        key = key.public_key()
    return public_key_b64url(key)


def public_key_from_b64url(text: str) -> Ed25519PublicKey:
    """Rebuild an Ed25519 public key from its raw base64url encoding."""
    from ccf.hashing import decode_b64url

    raw = decode_b64url(text)
    if len(raw) != 32:
        raise CcfKeyError(f"Ed25519 public key must be 32 bytes, got {len(raw)}")
    return Ed25519PublicKey.from_public_bytes(raw)


# Re-export for callers that only need PEM parsing helpers.
__all__ = [
    "CcfKeyError",
    "generate_signing_key",
    "load_signing_key",
    "load_verification_key",
    "public_key_from_b64url",
    "public_key_text",
]
