"""Suppression after erasure (spec 12.7, 6.5).

A protected suppression store retains keyed (HMAC-SHA256) commitments for
erased origin tuples and erased content, so a later submission that would
silently reintroduce them is caught at admission. Commitments are keyed —
never plain unsalted fingerprints — because erased content may be
low-entropy (spec 12.7).

Two commitment kinds:

- ``origin``: HMAC over ``{source_id, native_id}`` — catches re-capture of
  an erased source item under a new revision or object ID;
- ``content``: HMAC over the producer submission hash — catches the same
  erased content reappearing under a fresh origin tuple.

Response shaping (spec 6.5, 12.8): an authorized source owner (a producer
listed in the entry's ``authorized_producers``) receives a lifecycle
result — status ``existing`` with ``current_lifecycle: "suppressed"`` and
no bytes restored. Any other caller receives a generic per-object
rejection that does not reveal whether the tuple was suppressed, held, or
simply refused: deployments must document this observable difference
(spec 12.8).

The key comes from an explicit path (``database.ccf_archive.
suppression_key_path`` or ``THOTH_CCF_SUPPRESSION_KEY``); there is no
fallback. Once an archive holds suppression entries, admission without the
key fails closed rather than skipping the check.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from pathlib import Path

from ccf.db import CcfPostgresSettings
from ccf.erasure.errors import SuppressionKeyError
from ccf.hashing import decode_b64url, encode_b64url
from ccf.jcs import canonical_bytes

_ORIGIN_DOMAIN = b"ccf:suppression:origin:v1\0"
_CONTENT_DOMAIN = b"ccf:suppression:content:v1\0"


def generate_suppression_key(path: str | Path) -> Path:
    """Generate a fresh 32-byte suppression key at ``path`` (base64url)."""
    path = Path(path)
    path.write_text(encode_b64url(secrets.token_bytes(32)), encoding="utf-8")
    return path


def load_suppression_key(settings: CcfPostgresSettings) -> bytes | None:
    """Load the suppression key; ``None`` when no path is configured.

    Fails closed when a path is configured but the key is unreadable or is
    not exactly 32 bytes (raw or base64url-encoded).
    """
    if not settings.suppression_key_path:
        return None
    path = Path(settings.suppression_key_path)
    try:
        raw = path.read_bytes().strip()
    except OSError as exc:
        raise SuppressionKeyError(f"suppression key unreadable at {path}: {exc}") from exc
    if len(raw) == 32:
        return raw
    try:
        decoded = decode_b64url(raw.decode("ascii"))
    except Exception as exc:
        raise SuppressionKeyError(
            f"suppression key at {path} is neither 32 raw bytes nor base64url"
        ) from exc
    if len(decoded) != 32:
        raise SuppressionKeyError(f"suppression key at {path} must be 32 bytes")
    return decoded


def _commitment(key: bytes, domain: bytes, payload: bytes) -> str:
    return "hmac-sha256:" + hmac.new(key, domain + payload, hashlib.sha256).hexdigest()


def origin_commitment(key: bytes, source_id: str, native_id: str) -> str:
    """Keyed commitment for an erased source item (any revision)."""
    return _commitment(
        key,
        _ORIGIN_DOMAIN,
        canonical_bytes({"source_id": source_id, "native_id": native_id}),
    )


def content_commitment(key: bytes, submission_hash: str) -> str:
    """Keyed commitment for erased content (via its submission hash)."""
    return _commitment(key, _CONTENT_DOMAIN, submission_hash.encode("utf-8"))


def record_suppression(
    conn,
    *,
    archive_id: str,
    operation_id: str,
    key: bytes,
    plans: list[dict],
    authorized_producers: list[str],
    created_at: str,
) -> int:
    """Insert suppression commitments for erased plans; returns the count."""
    from psycopg.types.json import Jsonb

    commitments: list[tuple[str, str]] = []
    for plan in plans:
        origin = plan.get("origin")
        if origin is not None:
            commitments.append(
                (
                    "origin",
                    origin_commitment(key, origin["source_id"], origin["native_id"]),
                )
            )
        if plan.get("submission_hash"):
            commitments.append(
                ("content", content_commitment(key, plan["submission_hash"]))
            )
    for kind, commitment in commitments:
        conn.execute(
            """
            INSERT INTO suppression_entry (
                archive_id, commitment, kind, operation_id,
                authorized_producers, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (archive_id, commitment) DO NOTHING
            """,
            (
                archive_id,
                commitment,
                kind,
                operation_id,
                Jsonb(list(authorized_producers)),
                created_at,
            ),
        )
    return len(commitments)


def store_has_entries(conn, archive_id: str) -> bool:
    """True when the archive holds any suppression entries."""
    row = conn.execute(
        "SELECT 1 FROM suppression_entry WHERE archive_id = %s LIMIT 1",
        (archive_id,),
    ).fetchone()
    return row is not None


def lookup(
    conn,
    *,
    archive_id: str,
    key: bytes,
    origin: dict | None,
    submission_hash: str | None,
) -> dict | None:
    """The suppression entry matching a submission, if any.

    Content commitments are checked first (exact content), then the origin
    commitment (same source item, any revision).
    """
    candidates: list[tuple[str, str]] = []
    if submission_hash:
        candidates.append(("content", content_commitment(key, submission_hash)))
    if origin is not None:
        candidates.append(
            ("origin", origin_commitment(key, origin["source_id"], origin["native_id"]))
        )
    for kind, commitment in candidates:
        row = conn.execute(
            """
            SELECT commitment, kind, authorized_producers FROM suppression_entry
            WHERE archive_id = %s AND commitment = %s
            """,
            (archive_id, commitment),
        ).fetchone()
        if row is not None:
            return {
                "commitment": row[0],
                "kind": row[1],
                "authorized_producers": list(row[2]),
            }
    return None


def admission_outcome(
    conn,
    *,
    archive_id: str,
    key: bytes | None,
    origin: dict | None,
    submission_hash: str,
    object_id: str,
    producer_id: str,
) -> dict | None:
    """Suppression check for one submission at admission (spec 6.5, 12.7).

    Returns ``None`` when the submission is not suppressed. Otherwise
    returns the per-object outcome: an authorized lifecycle result for a
    listed producer, a generic indistinguishable rejection for everyone
    else. Fails closed when entries exist but no key is configured.
    """
    if not store_has_entries(conn, archive_id):
        return None
    if key is None:
        raise SuppressionKeyError(
            "suppression entries exist but no suppression key is configured; "
            "refusing admission rather than skipping the check"
        )
    entry = lookup(
        conn,
        archive_id=archive_id,
        key=key,
        origin=origin,
        submission_hash=submission_hash,
    )
    if entry is None:
        return None
    if producer_id in entry["authorized_producers"]:
        # Authorized lifecycle result (spec 6.5): no bytes are restored.
        return {
            "object_id": object_id,
            "status": "existing",
            "object_hash": None,
            "commit_sequence": None,
            "commit_position": None,
            "payload_available": False,
            "current_lifecycle": "suppressed",
        }
    # Generic response: indistinguishable from an ordinary per-object
    # refusal; reveals neither erasure nor suppression (spec 12.8).
    return {
        "object_id": object_id,
        "status": "rejected",
        "object_hash": None,
        "commit_sequence": None,
        "commit_position": None,
        "payload_available": False,
        "reason": "origin unavailable for admission",
    }
