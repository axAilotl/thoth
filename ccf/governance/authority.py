"""``required_authority`` enforcement at admission (spec section 5.5).

Every type registry entry declares the authority class a Record of that
type must carry. Admission interprets the claimed authority block
(``{basis, asserted_by, accepted_by}``) against that class and fails
closed: a missing claim or an unsatisfied class rejects the Record.

Since 0.1.2-rc1 the class mapping is pinned by
``registries/admission-authority-classes.registry.json`` — this module is
driven by that registry (evaluation mode, claim requirement, acceptable
bases, person-acceptance rule) rather than a parallel hardcoded table,
and rejection reasons are the registry's normative ``failure_reason``
strings verbatim. ``admitted_by_archive`` is True for operator/bootstrap
admission (archive-signed, no producer evidence) and False for
producer-admitted batches.
"""

from __future__ import annotations

from ccf.registry import PinnedRegistries, RegistryError


def check_required_authority(
    required: str | None,
    *,
    claim: dict | None,
    recorded_by: str,
    admitted_by_archive: bool,
    registries: PinnedRegistries,
    lineage_state_machine_passed: bool = True,
) -> str | None:
    """Return None when the claim satisfies ``required``, else a reason.

    The reason is the pinned registry's normative ``failure_reason`` for
    the class, verbatim. Fails closed on an unknown required-authority
    class and on any claim whose basis is not in the pinned
    authority-bases registry.
    """
    if required is None:
        return None
    try:
        entry = registries.authority_class(required)
    except RegistryError:
        return f"unknown required_authority class {required!r}"
    failure = entry["failure_reason"]
    mode = entry["evaluation_mode"]

    if mode == "structural_state_machine":
        # Enforced structurally: the lineage CAS and state-machine checks
        # run before this point in admission.
        return None if lineage_state_machine_passed else failure
    if mode == "archive_admission_only":
        return None if admitted_by_archive else failure

    if entry["claim_required"] and claim is None:
        return failure
    if claim is not None:
        basis = claim.get("basis")
        if basis not in registries.authority_basis_names():
            return failure
    else:
        basis = None
    accepted_by = claim.get("accepted_by") if claim is not None else None
    acceptable = frozenset(entry["acceptable_authority_bases"])

    if mode == "any_pinned_basis":
        # Claim presence and the pinned-basis check above already ran.
        return None
    if mode == "basis_allowlist":
        return None if basis in acceptable else failure
    if mode == "person_or_acceptance":
        # A person basis, or acceptance by the person, satisfies the class.
        if basis in acceptable or accepted_by is not None:
            return None
        return failure
    if mode == "person_acceptance":
        if basis in acceptable or accepted_by is not None:
            return None
        return failure
    if mode == "archive_or_basis":
        # Archive admission overrides the basis allowlist.
        if admitted_by_archive or basis in acceptable:
            return None
        return failure
    # Unknown evaluation mode in a pinned entry: fail closed.
    return failure
