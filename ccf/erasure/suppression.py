"""Suppression after erasure (spec 12.7, 6.5) — profile
``ccf-hmac-sha256-suppression-v1``.

A protected suppression store retains keyed (HMAC-SHA256) commitments for
erased origin tuples and erased content, so a later submission that would
silently reintroduce them is caught at admission. Commitments are keyed —
never plain unsalted fingerprints — because erased content may be
low-entropy (spec 12.7).

0.1.2-rc1 makes suppression canonical: the tokens committed here live in a
governed Blob referenced by a canonical ``lineage.suppression_set`` Record
(:mod:`ccf.erasure.suppression_set`); the ``suppression_entry`` table is a
rebuildable projection of that canonical state. This module owns the
pinned token derivation:

- the preimage is a JCS object validated by the closed catalog schema
  ``urn:ccf:schema:0.1.2-rc1:security.suppression-preimage``, either an
  origin tuple (``kind``, ``source_id``, ``native_id``, ``revision``,
  ``object_kind``) or a content commitment (``kind``,
  ``content_commitment``);
- the token is ``HMAC-SHA-256(key, UTF8('ccf:suppression-token:v1\\0') ||
  UTF8(JCS(preimage)))`` encoded as ``hmac-sha256:<lowercase hex>``.

Response shaping (spec 6.5, 12.8): an authorized source owner (a producer
listed in the entry's ``authorized_producers``) receives a lifecycle
result — status ``existing`` with ``current_lifecycle: "suppressed"`` and
no bytes restored. Any other caller receives a generic per-object
rejection that does not reveal whether the tuple was suppressed, held, or
simply refused: deployments must document this observable difference
(spec 12.8).

The key comes from an explicit path (``database.ccf_archive.
suppression_key_path`` or ``THOTH_CCF_SUPPRESSION_KEY``); there is no
fallback. Once an archive holds canonical suppression sets, admission
without the key fails closed rather than skipping the check.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from pathlib import Path

from ccf.db import CcfPostgresSettings
from ccf.erasure.errors import SuppressionKeyError
from ccf.hashing import (
    canonical_digest,
    decode_b64url,
    domain_digest,
    encode_b64url,
)
from ccf.jcs import canonical_bytes

#: The pinned suppression profile (registries/suppression-profiles).
SUPPRESSION_PROFILE = "ccf-hmac-sha256-suppression-v1"
PREIMAGE_FORMAT = "ccf.suppression-preimage/1"
SCHEMA_PREIMAGE = "urn:ccf:schema:0.1.2-rc1:security.suppression-preimage"

_TOKEN_DOMAIN = b"ccf:suppression-token:v1\0"
_TOKEN_PREFIX = "hmac-sha256:"


def generate_suppression_key(path: str | Path) -> Path:
    """Generate a fresh 32-byte suppression key at ``path`` (base64url).

    Written mode 0600 with ``O_EXCL`` (same discipline as
    :func:`ccf.keys.generate_signing_key`): the key protects
    suppression-after-erasure, so it is never world-readable and never
    silently overwritten.
    """
    path = Path(path)
    if path.exists():
        raise SuppressionKeyError(
            f"refusing to overwrite existing suppression key: {path}"
        )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(encode_b64url(secrets.token_bytes(32)))
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


# ---------------------------------------------------------------------------
# Preimages and token derivation (ccf-hmac-sha256-suppression-v1)
# ---------------------------------------------------------------------------


def origin_preimage(
    *, source_id: str, native_id: str, revision: str, object_kind: str
) -> dict:
    """The origin-tuple suppression preimage (closed schema shape)."""
    return {
        "format": PREIMAGE_FORMAT,
        "kind": "origin",
        "source_id": source_id,
        "native_id": native_id,
        "revision": revision,
        "object_kind": object_kind,
    }


def content_preimage(*, content_commitment: str) -> dict:
    """The content suppression preimage (closed schema shape)."""
    return {
        "format": PREIMAGE_FORMAT,
        "kind": "content",
        "content_commitment": content_commitment,
    }


def suppression_token(key: bytes, preimage: dict) -> str:
    """Keyed token for one preimage, per the pinned profile."""
    mac = hmac.new(key, _TOKEN_DOMAIN + canonical_bytes(preimage), hashlib.sha256)
    return _TOKEN_PREFIX + mac.hexdigest()


def token_for_origin(
    key: bytes, *, source_id: str, native_id: str, revision: str, object_kind: str
) -> str:
    """Token for an erased source item (exact origin tuple + object kind)."""
    return suppression_token(
        key,
        origin_preimage(
            source_id=source_id,
            native_id=native_id,
            revision=revision,
            object_kind=object_kind,
        ),
    )


#: Domain for the content commitment inside a content preimage. The
#: commitment must be reproducible from equivalent content — the same
#: payload (Records/Links) or the same bytes (Blobs) — so that erased
#: content reappearing under a fresh origin tuple, revision, or object ID
#: is still caught (spec 12.7). A submission hash would not serve: it is
#: unique per submission envelope, so any recapture would slip past.
_CONTENT_DOMAIN = "ccf:suppression-content:v1"


def content_commitment_for_payload(payload: dict) -> str:
    """Content commitment for a Record/Link semantic payload."""
    return canonical_digest(_CONTENT_DOMAIN, payload)


def content_commitment_for_bytes(data: bytes) -> str:
    """Content commitment for raw Blob bytes."""
    return domain_digest(_CONTENT_DOMAIN, data)


def token_for_content(key: bytes, content_commitment: str) -> str:
    """Token for erased content (via its content commitment)."""
    return suppression_token(key, content_preimage(content_commitment=content_commitment))


def tokens_for_plan(key: bytes, plan: dict) -> list[tuple[str, str]]:
    """``(kind, token)`` pairs for one retention-checked erasure plan."""
    tokens: list[tuple[str, str]] = []
    origin = plan.get("origin")
    if origin is not None:
        tokens.append(
            (
                "origin",
                token_for_origin(
                    key,
                    source_id=origin["source_id"],
                    native_id=origin["native_id"],
                    revision=origin["revision"],
                    object_kind=plan["object_kind"],
                ),
            )
        )
    if plan.get("content_commitment"):
        tokens.append(("content", token_for_content(key, plan["content_commitment"])))
    return tokens


# ---------------------------------------------------------------------------
# Projection row writes (the table is a rebuildable projection; spec 12.7)
# ---------------------------------------------------------------------------


def record_suppression(
    conn,
    *,
    archive_id: str,
    operation_id: str,
    set_record_id: str,
    blob_id: str,
    key_profile_id: str,
    kind_tokens: list[tuple[str, str]],
    authorized_producers: list[str],
    created_at: str,
) -> int:
    """Insert projection rows for one canonical suppression set.

    Every row resolves to its chain-covered ``lineage.suppression_set``
    Record and governed Blob (spec 12.7); deleting the table never removes
    canonical suppression authority — :func:`ccf.erasure.suppression_set.
    rebuild_projection` restores it.
    """
    from psycopg.types.json import Jsonb

    for kind, token in kind_tokens:
        conn.execute(
            """
            INSERT INTO suppression_entry (
                archive_id, suppression_set_record_id, commitment, kind,
                operation_id, suppression_blob_id, key_profile_id,
                authorized_producers, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (archive_id, suppression_set_record_id, commitment)
            DO NOTHING
            """,
            (
                archive_id,
                set_record_id,
                token,
                kind,
                operation_id,
                blob_id,
                key_profile_id,
                Jsonb(list(authorized_producers)),
                created_at,
            ),
        )
    return len(kind_tokens)


# ---------------------------------------------------------------------------
# Admission-time check
# ---------------------------------------------------------------------------


class SuppressionState:
    """Per-batch suppression view: canonical tokens plus authorized producers.

    Constructed by :func:`prepare_admission_check`, which loads the
    canonical suppression sets (verifying each Blob against its pinned
    Merkle root) and audits the projection rows against them — any drift
    fails closed before any object is admitted.
    """

    def __init__(self, tokens: set[str], authorized: dict[str, list[str]]) -> None:
        self._tokens = tokens
        self._authorized = authorized

    def match(self, token: str) -> dict | None:
        if token not in self._tokens:
            return None
        return {"commitment": token, "authorized_producers": self._authorized.get(token, [])}


def prepare_admission_check(
    conn, *, archive_id: str, key: bytes | None
) -> SuppressionState | None:
    """Load and audit suppression state for one admission batch.

    Returns ``None`` when the archive holds no canonical suppression sets.
    Fails closed when sets exist but no key is configured, and when the
    rebuildable projection drifts from canonical state (deleted or
    tampered rows are detected here, before they could silently permit
    reintroduction — spec 12.7).
    """
    from ccf.erasure import suppression_set

    sets = suppression_set.load_canonical_sets(conn, archive_id)
    if not sets:
        return None
    if key is None:
        raise SuppressionKeyError(
            "canonical suppression sets exist but no suppression key is "
            "configured; refusing admission rather than skipping the check"
        )
    suppression_set.audit_projection(conn, archive_id)
    tokens: set[str] = set()
    for canonical in sets:
        tokens.update(canonical.tokens)
    authorized: dict[str, list[str]] = {}
    if tokens:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (commitment) commitment, authorized_producers
            FROM suppression_entry
            WHERE archive_id = %s AND commitment = ANY(%s)
            """,
            (archive_id, sorted(tokens)),
        ).fetchall()
        for commitment, producers in rows:
            authorized[commitment] = list(producers)
    return SuppressionState(tokens, authorized)


def admission_outcome(
    state: SuppressionState,
    *,
    key: bytes,
    origin: dict | None,
    content_commitment: str | None,
    object_kind: str,
    object_id: str,
    producer_id: str,
) -> dict | None:
    """Suppression check for one submission at admission (spec 6.5, 12.7).

    Returns ``None`` when the submission is not suppressed. Otherwise
    returns the per-object outcome: an authorized lifecycle result for a
    listed producer, a generic indistinguishable rejection for everyone
    else.
    """
    candidates: list[str] = []
    if content_commitment is not None:
        # Exact content first, then the origin tuple (spec 6.5 retry).
        candidates.append(token_for_content(key, content_commitment))
    if origin is not None:
        candidates.append(
            token_for_origin(
                key,
                source_id=origin["source_id"],
                native_id=origin["native_id"],
                revision=origin["revision"],
                object_kind=object_kind,
            )
        )
    entry = None
    for token in candidates:
        entry = state.match(token)
        if entry is not None:
            break
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
