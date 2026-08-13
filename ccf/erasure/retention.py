"""Retention-profile enforcement at erasure time (spec 1.3, 3.10, 5.3).

Type and Link registries declare one of four retention profiles
(``ccf.retention-profiles/0.1.2-rc1``):

- ``erasable`` — both compartments (and Blob content) may be removed; the
  header and commitments remain;
- ``payload_erasable`` — structural compartment remains; semantic
  compartment and Blob content may be removed;
- ``structural_retention_required`` — structural material needed for
  lineage or replay remains; semantic material may be removed;
- ``epoch_lifetime_required`` — nothing may be removed for the archive
  epoch.

Enforcement fails closed twice over: a requested compartment outside the
profile is refused, and the effective profile is the *stricter* of the
registry entry and the profile declared in the object's structural
compartment (a producer hint may tighten, never widen).

Structural retention after erasure (spec 3.10) falls out of the profiles:
Link endpoints live in the structural compartment (``ccf.derived_from``
and every other pinned ``ccf.*`` Link is ``structural_retention_required``),
so selectors and explanatory text erase independently while endpoints —
and therefore the derivation closure — are retained.
"""

from __future__ import annotations

from ccf.erasure.errors import ErasureError, RetentionViolation

#: Profiles from most to least restrictive.
_PROFILE_ORDER = (
    "epoch_lifetime_required",
    "structural_retention_required",
    "payload_erasable",
    "erasable",
)

#: Compartments each profile permits to erase.
_ALLOWED = {
    "erasable": frozenset({"structural", "semantic", "content"}),
    "payload_erasable": frozenset({"semantic", "content"}),
    "structural_retention_required": frozenset({"semantic"}),
    "epoch_lifetime_required": frozenset(),
}

_VALID_PARTS = frozenset({"structural", "semantic", "content"})


def effective_profile(declared: str | None, registry_profile: str | None) -> str:
    """The stricter of the declared and registry profiles (fail closed)."""
    candidates = [
        profile
        for profile in (declared, registry_profile)
        if profile is not None
    ]
    if not candidates:
        raise ErasureError("no retention profile declared or registered")
    for profile in candidates:
        if profile not in _PROFILE_ORDER:
            raise ErasureError(f"unknown retention profile {profile!r}")
    return min(candidates, key=_PROFILE_ORDER.index)


def load_target(conn, archive_id: str, object_id: str) -> dict:
    """Current storage facts for one erasure target; fail closed if unknown."""
    from ccf.erasure import suppression

    header = conn.execute(
        "SELECT object_kind, submission_hash FROM object_header WHERE id = %s",
        (object_id,),
    ).fetchone()
    if header is None:
        raise ErasureError(f"erasure target {object_id} is not admitted")
    structural = conn.execute(
        """
        SELECT state, plaintext_json ->> 'type', plaintext_json ->> 'retention_profile'
        FROM compartment
        WHERE object_id = %s AND compartment = 'structural'
        """,
        (object_id,),
    ).fetchone()
    compartments = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT compartment, state FROM compartment WHERE object_id = %s",
            (object_id,),
        ).fetchall()
    }
    blob_state = None
    blob_bytes = None
    if header[0] == "blob":
        row = conn.execute(
            "SELECT state, plaintext_bytes FROM blob_content WHERE blob_id = %s",
            (object_id,),
        ).fetchone()
        blob_state = row[0] if row else None
        if row is not None and row[0] == "plaintext":
            blob_bytes = bytes(row[1])
    origin = conn.execute(
        """
        SELECT source_id, native_id, revision FROM origin_index
        WHERE archive_id = %s AND object_id = %s
        """,
        (archive_id, object_id),
    ).fetchone()
    # Content commitment for the suppression preimage (spec 12.7): the
    # still-plaintext semantic payload (Records/Links) or raw bytes
    # (Blobs), so equivalent content is caught even under a fresh origin
    # tuple. None when the content is already unavailable.
    content_commitment = None
    if header[0] == "blob":
        if blob_bytes is not None:
            content_commitment = suppression.content_commitment_for_bytes(blob_bytes)
    elif compartments.get("semantic") == "plaintext":
        semantic = conn.execute(
            """
            SELECT plaintext_json -> 'payload' FROM compartment
            WHERE object_id = %s AND compartment = 'semantic'
              AND state = 'plaintext'
            """,
            (object_id,),
        ).fetchone()
        if semantic is not None and semantic[0] is not None:
            content_commitment = suppression.content_commitment_for_payload(semantic[0])
    return {
        "object_id": object_id,
        "object_kind": header[0],
        "submission_hash": header[1],
        "type": structural[1] if structural else None,
        "declared_profile": structural[2] if structural else None,
        "structural_state": structural[0] if structural else None,
        "compartments": compartments,
        "blob_state": blob_state,
        "content_commitment": content_commitment,
        "origin": (
            {"source_id": origin[0], "native_id": origin[1], "revision": origin[2]}
            if origin
            else None
        ),
    }


def _registry_profile(target: dict, registries) -> str | None:
    kind = target["object_kind"]
    type_name = target["type"]
    if kind == "blob":
        return registries.blob_entry["retention_profile"]
    if type_name is None:
        return None
    if kind == "record":
        # sealed.record hides the exact type in the erased semantic
        # compartment; only the declared profile is enforceable.
        if type_name == "sealed.record":
            return None
        return registries.type_entry(type_name)["retention_profile"]
    if kind == "link":
        if type_name == "sealed.link":
            return registries.link_entry("sealed.link")["retention_profile"]
        return registries.link_entry(type_name)["retention_profile"]
    raise ErasureError(f"unknown object kind {kind!r}")


def plan_targets(
    conn,
    *,
    archive_id: str,
    targets: list[dict],
    registries,
) -> list[dict]:
    """Retention-checked erasure plans for the requested targets.

    Each requested target is ``{"object_id": ..., "compartments": [...]}``
    with compartments drawn from ``structural`` / ``semantic`` / ``content``
    (Blob bytes). Raises :class:`RetentionViolation` — fail closed — when a
    request exceeds the effective retention profile, and
    :class:`ErasureError` for unknown objects, unknown parts, or parts
    that do not apply to the object kind.
    """
    plans: list[dict] = []
    for request in targets:
        object_id = request.get("object_id")
        parts = request.get("compartments")
        if not object_id or not isinstance(parts, list) or not parts:
            raise ErasureError(
                "erasure targets require an object_id and a non-empty "
                f"compartments list: {request!r}"
            )
        unknown = set(parts) - _VALID_PARTS
        if unknown:
            raise ErasureError(f"unknown erasure parts {sorted(unknown)} for {object_id}")

        target = load_target(conn, archive_id, object_id)
        kind = target["object_kind"]
        if "content" in parts and kind != "blob":
            raise ErasureError(f"'content' erasure applies to Blobs only: {object_id}")

        profile = effective_profile(
            target["declared_profile"], _registry_profile(target, registries)
        )
        refused = set(parts) - _ALLOWED[profile]
        if refused:
            raise RetentionViolation(
                f"retention profile {profile!r} forbids erasing "
                f"{sorted(refused)} of {object_id} ({kind} {target['type']})"
            )
        if kind != "blob" and target["compartments"].get("structural") is None:
            raise ErasureError(f"{object_id} has no structural compartment state")
        plans.append(
            {
                "object_id": object_id,
                "object_kind": kind,
                "type": target["type"],
                "retention_profile": profile,
                "erase_structural": "structural" in parts,
                "erase_semantic": "semantic" in parts,
                "erase_content": "content" in parts,
                "origin": target["origin"],
                "submission_hash": target["submission_hash"],
                "content_commitment": target["content_commitment"],
            }
        )
    return plans
