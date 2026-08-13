"""CCF 0.1.2 portable object envelopes (spec sections 3 and 5).

Models the portable layers of a stored object (spec section 1.2):

- :class:`PortableHeader` — kind, stable ID, hash profile, compartment
  commitments, object hash;
- :class:`CompartmentEnvelope` — format, 32-byte base64url salt, content;
- :class:`CompartmentStorage` — the envelope plus its operational
  availability state (section 3.6);
- :class:`AdmissionMetadata` — archive-local admission coordinates, modeled
  separately and never fed into portable hashes (section 4.5).
"""

from __future__ import annotations

import enum
import re
import secrets
from dataclasses import dataclass

from ccf import CCF_HASH_PROFILE, CCF_SPEC
from ccf.hashing import (
    CcfHashError,
    compartment_commitment,
    decode_b64url,
    encode_b64url,
    object_hash,
    parse_digest,
)
from ccf.ids import parse_id

_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
)
_DECIMAL_RE = re.compile(r"^(0|[1-9][0-9]*)$")


class CcfObjectError(ValueError):
    """Raised when a portable object or envelope is malformed."""


class AvailabilityState(enum.Enum):
    """Operational state of a compartment or Blob content (section 3.6)."""

    PLAINTEXT = "plaintext"
    ENCRYPTED = "encrypted"
    WITHHELD = "withheld"
    ERASED = "erased"


def compartment_format(object_kind: str, compartment: str) -> str:
    """Canonical compartment format label, e.g. ``ccf.record-structural/0.1.2``."""
    return f"ccf.{object_kind}-{compartment}/0.1.2"


def new_salt() -> str:
    """Generate a fresh 32-byte salt as unpadded base64url."""
    return encode_b64url(secrets.token_bytes(32))


def validate_timestamp(text: str) -> str:
    """Validate the canonical timestamp form ``YYYY-MM-DDTHH:mm:ss.SSSZ``."""
    if not isinstance(text, str) or _TIMESTAMP_RE.match(text) is None:
        raise CcfObjectError(f"non-canonical timestamp: {text!r}")
    return text


def now_timestamp() -> str:
    """Current UTC time as a canonical CCF timestamp (millisecond precision)."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def validate_decimal_string(text: str) -> str:
    """Validate a canonical unsigned decimal string (section 4.2)."""
    if not isinstance(text, str) or _DECIMAL_RE.match(text) is None:
        raise CcfObjectError(f"non-canonical decimal string: {text!r}")
    return text


def admission_order_key(commit_sequence: str, commit_position: int) -> tuple[int, int]:
    """Numeric admission ordering key (section 4.2 / 8.1).

    Commit sequences are decimal strings on the wire but compared
    numerically; lexicographic ordering is forbidden.
    """
    return (int(validate_decimal_string(commit_sequence)), int(commit_position))


@dataclass(frozen=True)
class CompartmentEnvelope:
    """A structural or semantic compartment envelope (section 3.2)."""

    format: str
    salt: str
    content: dict

    def __post_init__(self) -> None:
        if not isinstance(self.format, str) or not self.format:
            raise CcfObjectError("compartment envelope requires a format label")
        if len(decode_b64url(self.salt)) != 32:
            raise CcfObjectError("compartment salt must be 32 bytes")
        if not isinstance(self.content, dict):
            raise CcfObjectError("compartment content must be a JSON object")

    @classmethod
    def create(cls, object_kind: str, compartment: str, content: dict) -> "CompartmentEnvelope":
        return cls(compartment_format(object_kind, compartment), new_salt(), content)

    @classmethod
    def from_dict(cls, data: dict) -> "CompartmentEnvelope":
        try:
            return cls(data["format"], data["salt"], data["content"])
        except KeyError as exc:
            raise CcfObjectError(f"compartment envelope missing field: {exc}") from exc

    def to_dict(self) -> dict:
        return {"format": self.format, "salt": self.salt, "content": self.content}

    def commitment(self, object_kind: str, compartment: str) -> str:
        expected = compartment_format(object_kind, compartment)
        if self.format != expected:
            raise CcfObjectError(
                f"envelope format {self.format!r} does not match {expected!r}"
            )
        return compartment_commitment(object_kind, compartment, self.to_dict())


@dataclass(frozen=True)
class CompartmentStorage:
    """A compartment plus its availability state (section 3.6).

    ``plaintext``/``encrypted`` compartments carry an envelope (encrypted
    content stays a JSON object of ciphertext material); ``withheld`` and
    ``erased`` compartments carry none, and an erased compartment's salt is
    erased with its content.
    """

    availability: AvailabilityState
    envelope: CompartmentEnvelope | None = None

    def __post_init__(self) -> None:
        if self.availability in (AvailabilityState.PLAINTEXT, AvailabilityState.ENCRYPTED):
            if self.envelope is None:
                raise CcfObjectError(
                    f"{self.availability.value} compartment requires an envelope"
                )
        elif self.envelope is not None:
            raise CcfObjectError(
                f"{self.availability.value} compartment must not carry an envelope"
            )


@dataclass(frozen=True)
class PortableHeader:
    """The portable header of a Record, Link, or Blob (section 3.1).

    ``semantic_commitment`` is ``None`` when the semantic compartment is
    absent — never a hash of an empty object (section 4.3).
    """

    object_kind: str
    id: str
    structural_commitment: str
    semantic_commitment: str | None
    object_hash: str
    spec: str = CCF_SPEC
    hash_profile: str = CCF_HASH_PROFILE

    def __post_init__(self) -> None:
        parsed = parse_id(self.id)
        if parsed.kind != self.object_kind:
            raise CcfObjectError(
                f"header id kind {parsed.kind!r} != object_kind {self.object_kind!r}"
            )
        if self.spec != CCF_SPEC:
            raise CcfObjectError(f"unsupported spec: {self.spec!r}")
        if self.hash_profile != CCF_HASH_PROFILE:
            raise CcfObjectError(f"unsupported hash profile: {self.hash_profile!r}")
        parse_digest(self.structural_commitment)
        if self.semantic_commitment is not None:
            parse_digest(self.semantic_commitment)
        parse_digest(self.object_hash)

    @classmethod
    def build(
        cls,
        object_kind: str,
        object_id: str,
        structural: CompartmentEnvelope,
        semantic: CompartmentEnvelope | None,
    ) -> "PortableHeader":
        """Compute commitments and the object hash for a new object."""
        structural_c = structural.commitment(object_kind, "structural")
        semantic_c = (
            semantic.commitment(object_kind, "semantic") if semantic is not None else None
        )
        fields = {
            "spec": CCF_SPEC,
            "object_kind": object_kind,
            "id": object_id,
            "hash_profile": CCF_HASH_PROFILE,
            "structural_commitment": structural_c,
            "semantic_commitment": semantic_c,
        }
        return cls(
            object_kind=object_kind,
            id=object_id,
            structural_commitment=structural_c,
            semantic_commitment=semantic_c,
            object_hash=object_hash(fields),
        )

    @classmethod
    def from_dict(cls, data: dict) -> "PortableHeader":
        try:
            return cls(
                object_kind=data["object_kind"],
                id=data["id"],
                structural_commitment=data["structural_commitment"],
                semantic_commitment=data["semantic_commitment"],
                object_hash=data["object_hash"],
                spec=data["spec"],
                hash_profile=data["hash_profile"],
            )
        except KeyError as exc:
            raise CcfObjectError(f"header missing field: {exc}") from exc

    def to_dict(self) -> dict:
        return {
            "spec": self.spec,
            "object_kind": self.object_kind,
            "id": self.id,
            "hash_profile": self.hash_profile,
            "structural_commitment": self.structural_commitment,
            "semantic_commitment": self.semantic_commitment,
            "object_hash": self.object_hash,
        }

    def verify(
        self,
        structural: CompartmentEnvelope,
        semantic: CompartmentEnvelope | None,
    ) -> None:
        """Recompute commitments and object hash; raise on any mismatch."""
        try:
            rebuilt = PortableHeader.build(self.object_kind, self.id, structural, semantic)
        except CcfHashError as exc:
            raise CcfObjectError(str(exc)) from exc
        if rebuilt != self:
            raise CcfObjectError(
                f"header verification failed for {self.id}: "
                f"recomputed object_hash {rebuilt.object_hash} != {self.object_hash}"
            )


@dataclass(frozen=True)
class AdmissionMetadata:
    """Archive-local admission coordinates (section 1.2, layer 4).

    Authenticated by commit membership and always excluded from portable
    object hashes.
    """

    commit_sequence: str
    commit_position: int
    admitted_at: str

    def __post_init__(self) -> None:
        validate_decimal_string(self.commit_sequence)
        if not isinstance(self.commit_position, int) or self.commit_position < 0:
            raise CcfObjectError(
                f"commit_position must be a non-negative integer: {self.commit_position!r}"
            )
        validate_timestamp(self.admitted_at)

    def to_member(self, header: PortableHeader) -> dict:
        """Commit-journal member for this object (section 4.8)."""
        return {
            "commit_sequence": self.commit_sequence,
            "commit_position": self.commit_position,
            "admitted_at": self.admitted_at,
            "object_kind": header.object_kind,
            "object_id": header.id,
            "object_hash": header.object_hash,
        }
