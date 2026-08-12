"""``required_authority`` enforcement at admission (spec section 5.5).

Every type registry entry declares the authority class a Record of that
type must carry. Admission interprets the claimed authority block
(``{basis, asserted_by, accepted_by}``) against that class and fails
closed: a missing claim or an unsatisfied class rejects the Record.

The spec pins the class names in the types registry but leaves their
interpretation to the archive; the mapping below is deliberately small,
deterministic, and documented here as the archive authority interpretation
(spec section 5.2). ``admitted_by_archive`` is True for operator/bootstrap
admission (archive-signed, no producer evidence) and False for
producer-admitted batches.
"""

from __future__ import annotations

from ccf.registry import PinnedRegistries

#: Bases that only an identified person (or their explicit delegate) can claim.
_PERSON_BASES = frozenset(
    {"first_person_statement", "explicit_authorization", "person_accepted"}
)

#: Bases an authenticated runtime or capture process can assert.
_RUNTIME_BASES = frozenset(
    {
        "runtime_import",
        "direct_observation",
        "deterministic_derivation",
        "machine_inference",
        "quoted_statement",
        "third_party_statement",
    }
)


def check_required_authority(
    required: str | None,
    *,
    claim: dict | None,
    recorded_by: str,
    admitted_by_archive: bool,
    registries: PinnedRegistries,
) -> str | None:
    """Return None when the claim satisfies ``required``, else a reason.

    Fails closed on an unknown required-authority class and on any claim
    whose basis is not in the pinned authority-bases registry.
    """
    if required is None:
        return None
    known = {
        "type_state_machine",
        "archive",
        "active_signer",
        "source_or_runtime",
        "runtime_authenticated",
        "authenticated_witness",
        "authorized_governance_actor",
        "grantor_or_authorized_actor",
        "authorized_scheduler",
        "authorized_erasure_worker",
        "authorized_security_actor",
        "authorized_successor",
        "archive_owner_or_catalog_admin",
        "archive_owner_or_key_custodian",
        "subject_or_authorized_representative",
        "person_accepted_or_reviewed",
        "person_or_authorized_agent",
        "person_or_operator",
        "archive_root_or_authorized_issuer",
    }
    if required not in known:
        return f"unknown required_authority class {required!r}"
    if required == "type_state_machine":
        # Enforced structurally: the lineage CAS and state machine checks
        # already ran before this point.
        return None
    if required in ("archive", "active_signer"):
        if admitted_by_archive:
            return None
        return f"required_authority {required!r} needs archive-signed admission"

    if claim is None:
        return f"required_authority {required!r} but no authority claim"
    basis = claim.get("basis")
    if basis not in registries.authority_basis_names():
        return f"authority basis {basis!r} is not in the pinned registry"
    accepted_by = claim.get("accepted_by")

    if required == "source_or_runtime":
        # Any pinned basis asserted by the source or runtime is acceptable.
        return None
    if required == "runtime_authenticated":
        # Authenticated runtime output: a runtime-class basis. The producer
        # itself is already authenticated by its device credential at the
        # batch envelope, so the claim distinguishes runtime assertions
        # from person assertions.
        if basis in _RUNTIME_BASES:
            return None
        return f"required_authority 'runtime_authenticated' rejects basis {basis!r}"
    if required == "authenticated_witness":
        # A witness directly attests what it observed.
        if basis in ("direct_observation", "first_person_statement"):
            return None
        return f"required_authority 'authenticated_witness' rejects basis {basis!r}"
    if required in (
        "authorized_governance_actor",
        "grantor_or_authorized_actor",
        "authorized_scheduler",
        "authorized_erasure_worker",
        "authorized_security_actor",
        "authorized_successor",
        "archive_owner_or_catalog_admin",
        "archive_owner_or_key_custodian",
    ):
        if basis == "explicit_authorization":
            return None
        if required == "authorized_scheduler" and basis == "deterministic_derivation":
            return None
        return f"required_authority {required!r} needs explicit_authorization"
    if required == "subject_or_authorized_representative":
        if basis in _PERSON_BASES or accepted_by is not None:
            return None
        return (
            f"required_authority {required!r} needs a first-person or "
            "accepted claim"
        )
    if required == "person_accepted_or_reviewed":
        if basis == "person_accepted" or accepted_by is not None:
            return None
        return f"required_authority {required!r} needs person acceptance"
    if required == "person_or_authorized_agent":
        if basis in _PERSON_BASES or accepted_by is not None:
            return None
        return f"required_authority {required!r} rejects basis {basis!r}"
    if required == "person_or_operator":
        # A person record may be asserted by the person, or admitted by the
        # operator — directly (bootstrap) or through the operator's
        # authenticated runtime. The claim-exists and pinned-basis checks
        # above have already run, so any pinned basis satisfies this class.
        return None
    if required == "archive_root_or_authorized_issuer":
        if admitted_by_archive or basis == "explicit_authorization":
            return None
        return (
            "required_authority 'archive_root_or_authorized_issuer' needs "
            "archive admission or explicit_authorization"
        )
    raise AssertionError(f"unhandled required_authority class {required!r}")
