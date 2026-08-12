"""Multi-subject media decisions (spec 3.9).

CCF does not pretend arbitrary mixed media can be surgically
deidentified. A multi-subject Blob decision has exactly three shapes:

- ``erase_blob`` — erase the whole Blob content (and its salt);
- ``restrict`` — withhold the Blob pending review;
- ``replace`` — a reviewed replacement Blob containing only permitted
  spans supersedes the original, which is then erased; the replacement is
  linked ``ccf.redacted_from`` the original by the caller.

There is deliberately no span-level erasure parameter: surgical editing
of mixed-subject media is not a supported decision shape.
"""

from __future__ import annotations

from ccf.erasure.errors import ErasureError
from ccf.ids import parse_id


def decide_multi_subject(
    *,
    blob_id: str,
    subject_ids: list[str],
    restrict_pending_review: bool = False,
    reviewed_replacement_blob_id: str | None = None,
) -> dict:
    """The decision shape for a multi-subject Blob.

    Fails closed on contradictory inputs (a restriction cannot also name
    a replacement) and on non-Blob identifiers.
    """
    if parse_id(blob_id).kind != "blob":
        raise ErasureError(f"multi-subject media decisions target Blobs: {blob_id!r}")
    if not subject_ids:
        raise ErasureError("a multi-subject decision requires at least one subject")
    if restrict_pending_review and reviewed_replacement_blob_id is not None:
        raise ErasureError(
            "a Blob cannot be both restricted pending review and replaced"
        )
    if reviewed_replacement_blob_id is not None:
        if parse_id(reviewed_replacement_blob_id).kind != "blob":
            raise ErasureError(
                f"replacement must be a Blob: {reviewed_replacement_blob_id!r}"
            )
        return {
            "blob_id": blob_id,
            "action": "replace",
            "subject_ids": list(subject_ids),
            "reviewed_replacement_blob_id": reviewed_replacement_blob_id,
            "note": (
                "replacement must contain permitted spans only and be linked "
                "ccf.redacted_from the original before the original is erased"
            ),
        }
    if restrict_pending_review:
        return {
            "blob_id": blob_id,
            "action": "restrict",
            "subject_ids": list(subject_ids),
            "note": "withhold the Blob until review produces erase_blob or replace",
        }
    return {
        "blob_id": blob_id,
        "action": "erase_blob",
        "subject_ids": list(subject_ids),
        "note": "whole-blob erasure; no surgical deidentification is claimed",
    }
