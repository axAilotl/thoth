#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT.parent / "0.1.2"
CCF_FORMAT_CHECKER = FormatChecker()


@CCF_FORMAT_CHECKER.checks("ccf-uint64")
def is_ccf_uint64(value) -> bool:
    if not isinstance(value, str):
        return True
    if value != "0" and (not value.isascii() or not value.isdigit() or value.startswith("0")):
        return False
    return value == "0" or len(value) < 20 or (
        len(value) == 20 and value <= "18446744073709551615"
    )


def load_json(path: Path):
    return json.loads(path.read_text())


def _is_canonical_relative_posix(rel: str) -> bool:
    """True iff ``rel`` is a non-empty, relative, normalized POSIX path."""
    if not rel or rel.startswith("/"):
        return False
    return all(part and part not in {".", ".."} for part in rel.split("/"))


def _enumerate_tree(root: Path) -> set[str]:
    """Return every regular file under ``root`` as a relative POSIX path.

    Rejects symlinks and non-regular entries at any depth, matching the
    runtime fail-closed containment primitive.
    """

    def _on_walk_error(exc: OSError) -> None:
        raise SystemExit(f"package tree traversal failed: {exc}") from exc

    root_resolved = root.resolve()
    files: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(
        root_resolved, followlinks=False, onerror=_on_walk_error
    ):
        for name in dirnames + filenames:
            full = Path(dirpath) / name
            if full.is_symlink():
                raise SystemExit(
                    f"package contains symlink: {full.relative_to(root_resolved).as_posix()}"
                )
            if full.is_dir():
                continue
            if not full.is_file():
                raise SystemExit(
                    f"package contains non-regular entry: {full.relative_to(root_resolved).as_posix()}"
                )
            files.add(full.relative_to(root_resolved).as_posix())
    return files


schemas = {}
schema_registry = Registry()
for schema_root in (BASE / "schemas", ROOT / "schemas"):
    for path in schema_root.rglob("*.json"):
        if path.name == "catalog.json":
            continue
        schema = load_json(path)
        schema_id = schema.get("$id")
        if schema_id is None:
            continue
        if schema_id in schemas:
            raise SystemExit(f"duplicate schema id {schema_id}")
        schemas[schema_id] = schema
        schema_registry = schema_registry.with_resource(
            schema_id, Resource.from_contents(schema)
        )


def validate(schema_id: str, instance, label: str):
    if schema_id not in schemas:
        raise SystemExit(f"missing schema {schema_id} for {label}")
    validator = Draft202012Validator(
        schemas[schema_id],
        registry=schema_registry,
        format_checker=CCF_FORMAT_CHECKER,
    )
    errors = sorted(validator.iter_errors(instance), key=lambda error: list(error.path))
    if errors:
        print(f"FAIL {label}: {len(errors)} error(s)")
        for error in errors[:30]:
            print("  ", "/".join(map(str, error.path)), error.message)
        raise SystemExit(1)
    print("OK  ", label)


registry_specs = {
    "levels.registry.json": "urn:ccf:schema:0.2.0:registries.level-registry",
    "roles.registry.json": "urn:ccf:schema:0.2.0:registries.role-registry",
    "capabilities.registry.json": "urn:ccf:schema:0.2.0:registries.capability-registry",
    "semantic-packs.registry.json": "urn:ccf:schema:0.2.0:registries.semantic-pack-registry",
    "semantic-requirements.registry.json": "urn:ccf:schema:0.2.0:registries.semantic-requirements-registry",
    "legacy-profile-mappings.registry.json": "urn:ccf:schema:0.2.0:registries.legacy-profile-mapping-registry",
    "compatibility-rules.registry.json": "urn:ccf:schema:0.2.0:registries.compatibility-rule-registry",
}
registries = {}
for filename, schema_id in registry_specs.items():
    value = load_json(ROOT / "registries" / filename)
    registries[filename] = value
    validate(schema_id, value, filename)


levels = {entry["id"]: entry for entry in registries["levels.registry.json"]["entries"]}
if len(levels) != len(registries["levels.registry.json"]["entries"]):
    raise SystemExit("duplicate guarantee level")
ordered_levels = sorted(levels.values(), key=lambda entry: entry["rank"])
level_rank = {entry["id"]: entry["rank"] for entry in ordered_levels}
if [entry["rank"] for entry in ordered_levels] != [1, 2, 3, 4]:
    raise SystemExit("guarantee levels must have contiguous ranks 1 through 4")
for index, entry in enumerate(ordered_levels):
    expected = [item["id"] for item in ordered_levels[: index + 1]]
    if entry["accepts_levels"] != expected:
        raise SystemExit(f'{entry["id"]} must accept itself and every lower level')

roles = {entry["id"] for entry in registries["roles.registry.json"]["entries"]}
if len(roles) != len(registries["roles.registry.json"]["entries"]):
    raise SystemExit("duplicate implementation role")
capabilities = {
    entry["id"]: entry for entry in registries["capabilities.registry.json"]["entries"]
}
if len(capabilities) != len(registries["capabilities.registry.json"]["entries"]):
    raise SystemExit("duplicate capability")
semantic_packs = {
    entry["id"]: entry for entry in registries["semantic-packs.registry.json"]["entries"]
}
if len(semantic_packs) != len(registries["semantic-packs.registry.json"]["entries"]):
    raise SystemExit("duplicate semantic pack")
for entry in capabilities.values():
    if entry["minimum_level"] not in levels:
        raise SystemExit(f'unknown capability minimum level for {entry["id"]}')
    missing = set(entry["depends_on"]) - capabilities.keys()
    if missing:
        raise SystemExit(f'unknown capability dependencies for {entry["id"]}: {sorted(missing)}')
    suite = entry["conformance_suite"]
    if suite is not None and f"{suite}:" not in (ROOT / "Makefile").read_text():
        raise SystemExit(f'missing declared capability suite for {entry["id"]}: {suite}')
for entry in semantic_packs.values():
    if entry["minimum_level"] not in levels:
        raise SystemExit(f'unknown semantic-pack minimum level for {entry["id"]}')
    missing = set(entry["required_capabilities"]) - capabilities.keys()
    if missing:
        raise SystemExit(f'unknown semantic-pack capabilities for {entry["id"]}: {sorted(missing)}')
    if f'{entry["conformance_suite"]}:' not in (ROOT / "Makefile").read_text():
        raise SystemExit(
            f'missing declared semantic-pack suite for {entry["id"]}: '
            f'{entry["conformance_suite"]}'
        )

base_profiles = {
    entry["name"] for entry in load_json(BASE / "registries" / "profiles.registry.json")["entries"]
}
legacy_mappings = registries["legacy-profile-mappings.registry.json"]["entries"]
mapped_profiles = {entry["legacy_profile"] for entry in legacy_mappings}
if len(mapped_profiles) != len(legacy_mappings) or mapped_profiles != base_profiles:
    raise SystemExit("legacy profile mappings do not cover each 0.1.2 profile exactly once")
for entry in legacy_mappings:
    if entry["level"] is not None and entry["level"] not in levels:
        raise SystemExit(f'legacy profile maps to unknown level: {entry["legacy_profile"]}')
    if entry["capability"] is not None and entry["capability"] not in capabilities:
        raise SystemExit(f'legacy profile maps to unknown capability: {entry["legacy_profile"]}')
    if entry["semantic_pack"] is not None and entry["semantic_pack"] not in semantic_packs:
        raise SystemExit(f'legacy profile maps to unknown semantic pack: {entry["legacy_profile"]}')

compatibility_ids = [
    entry["id"] for entry in registries["compatibility-rules.registry.json"]["entries"]
]
if compatibility_ids != [f"CCF-COMPAT-{index}" for index in range(1, 7)]:
    raise SystemExit("compatibility rules must be ordered and complete")


base_resources = set()
base_registry_entries = {}
for filename, resource_kind in (
    ("types.registry.json", "record_type"),
    ("links.registry.json", "link_type"),
    ("blobs.registry.json", "blob_type"),
    ("predicates.registry.json", "predicate"),
):
    entries = load_json(BASE / "registries" / filename)["entries"]
    base_registry_entries[resource_kind] = {
        (entry["name"], entry["version"]): entry for entry in entries
    }
    for entry in entries:
        base_resources.add((resource_kind, entry["name"], entry["version"]))

requirements = registries["semantic-requirements.registry.json"]["entries"]
declared_resources = {
    (entry["resource_kind"], entry["name"], entry["version"]) for entry in requirements
}
if len(declared_resources) != len(requirements):
    raise SystemExit("duplicate semantic requirement entry")
if declared_resources != base_resources:
    missing = sorted(base_resources - declared_resources)
    extra = sorted(declared_resources - base_resources)
    raise SystemExit(f"semantic requirement coverage mismatch; missing={missing}, extra={extra}")
for entry in requirements:
    if entry["minimum_level"] not in levels:
        raise SystemExit(f'unknown minimum level for {entry["name"]}')
    effects_level = entry["state_effects_level"]
    if effects_level is not None:
        if effects_level not in levels:
            raise SystemExit(f'unknown state-effects level for {entry["name"]}')
        if level_rank[effects_level] < level_rank[entry["minimum_level"]]:
            raise SystemExit(f'state effects precede semantic activation for {entry["name"]}')
    missing = set(entry["required_capabilities"]) - capabilities.keys()
    if missing:
        raise SystemExit(f'unknown required capabilities for {entry["name"]}: {sorted(missing)}')
    pack = entry["semantic_pack"]
    if pack is not None and pack not in semantic_packs:
        raise SystemExit(f'unknown semantic pack for {entry["name"]}: {pack}')
print(f"OK   semantic requirement coverage ({len(requirements)} resources)")

requirement_by_resource = {
    (entry["resource_kind"], entry["name"], entry["version"]): entry
    for entry in requirements
}


def registered_activation_requirement(submission):
    resource_kind = f'{submission["submission_kind"]}_type'
    if submission["submission_kind"] == "record":
        resource_kind = "record_type"
    elif submission["submission_kind"] == "link":
        resource_kind = "link_type"
    key = (
        resource_kind,
        submission.get("type", "blob.manifest"),
        submission.get("type_version", 1),
    )
    requirement = requirement_by_resource.get(key)
    if requirement is None:
        raise ValueError(f'unregistered active semantics: {key}')
    return requirement


def assert_declared_features_fit_level(declaration, label: str):
    declared_level_rank = level_rank[declaration["level"]]
    for feature_id in declaration["capabilities"]:
        feature = capabilities.get(feature_id) or semantic_packs.get(feature_id)
        if feature is None:
            raise SystemExit(f"{label} declares unknown feature {feature_id}")
        if level_rank[feature["minimum_level"]] > declared_level_rank:
            raise SystemExit(f"{label} declares {feature_id} below its minimum level")


bundle_root = ROOT / "bundles"
bundles = {}
for path in sorted(bundle_root.glob("*.json")):
    bundle = load_json(path)
    validate(
        "urn:ccf:schema:0.2.0:declarations.bundle-manifest",
        bundle,
        f"bundle {path.name}",
    )
    if bundle["id"] in bundles:
        raise SystemExit(f'duplicate bundle ID {bundle["id"]}')
    bundles[bundle["id"]] = bundle

source_roots = {"ccf-0.1.2": BASE, "ccf-0.2.0": ROOT}
for bundle in bundles.values():
    missing_dependencies = set(bundle["depends_on"]) - bundles.keys()
    if missing_dependencies:
        raise SystemExit(f'unknown bundle dependency for {bundle["id"]}: {sorted(missing_dependencies)}')
    artifact_keys = set()
    for artifact in bundle["artifacts"]:
        key = (artifact["source_package"], artifact["path"])
        if key in artifact_keys:
            raise SystemExit(f'duplicate artifact in {bundle["id"]}: {key}')
        artifact_keys.add(key)
        path = source_roots[artifact["source_package"]] / artifact["path"]
        actual_digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_digest != artifact["digest"]:
            raise SystemExit(f'stale artifact digest in {bundle["id"]}: {artifact["path"]}')
    if bundle["kind"] == "level":
        if bundle["provides"] not in levels:
            raise SystemExit(f'{bundle["id"]} provides an unknown level')
        if bundle["provides"] != "ccf-governed-archive-v1":
            forbidden = (
                "schemas/payloads/continuity/",
                "schemas/payloads/work/",
                "schemas/payloads/agent/",
            )
            leaking = [
                artifact["path"]
                for artifact in bundle["artifacts"]
                if artifact["source_package"] == "ccf-0.1.2"
                and artifact["path"].startswith(forbidden)
            ]
            if leaking:
                raise SystemExit(f'{bundle["id"]} leaks semantic-pack schemas: {leaking}')
    elif bundle["kind"] == "capability":
        if bundle["provides"] not in capabilities:
            raise SystemExit(f'{bundle["id"]} provides an unknown capability')
    elif bundle["provides"] not in semantic_packs:
        raise SystemExit(f'{bundle["id"]} provides an unknown semantic pack')
if len(bundles) != 8:
    raise SystemExit("expected four level, one capability, and three semantic-pack bundles")
if any(
    artifact["path"] == "requirements-checks.txt"
    for bundle in bundles.values()
    for artifact in bundle["artifacts"]
):
    raise SystemExit("runtime bundles include conformance-only Python dependencies")
signed_sync_bundle = next(
    bundle
    for bundle in bundles.values()
    if bundle["provides"] == "ccf-signed-producer-sync-v1"
)
signed_sync_base_paths = {
    artifact["path"]
    for artifact in signed_sync_bundle["artifacts"]
    if artifact["source_package"] == "ccf-0.1.2"
}
required_credential_envelope_paths = {
    "schemas/common/compartment-envelope.schema.json",
    "schemas/objects/record-header.schema.json",
    "schemas/objects/record-structural-content.schema.json",
    "schemas/objects/record-structural.schema.json",
    "schemas/objects/structural/core-device-credential.schema.json",
    "schemas/security/device-credential.schema.json",
}
if not required_credential_envelope_paths <= signed_sync_base_paths:
    raise SystemExit("signed-sync bundle cannot validate its canonical credential trust input")
print("OK   four level, one capability, and three isolated semantic-pack bundles")


all_capability_ids = capabilities.keys() | semantic_packs.keys()
implementation = load_json(ROOT / "examples" / "implementation-declaration.json")
validate(
    "urn:ccf:schema:0.2.0:declarations.implementation",
    implementation,
    "implementation declaration",
)
if implementation["level"] not in levels:
    raise SystemExit("implementation declares an unknown level")
if not set(implementation["roles"]) <= roles:
    raise SystemExit("implementation declares an unknown role")
if not set(implementation["capabilities"]) <= all_capability_ids:
    raise SystemExit("implementation declares an unknown capability or semantic pack")
assert_declared_features_fit_level(implementation, "implementation")


capsule_root = ROOT / "examples" / "capsule"
manifest = load_json(capsule_root / "manifest.json")
validate("urn:ccf:schema:0.2.0:exchange.capsule-manifest", manifest, "capsule manifest")
if manifest["level"] not in levels:
    raise SystemExit("capsule declares an unknown level")
if not set(manifest["capabilities"]) <= all_capability_ids:
    raise SystemExit("capsule declares an unknown capability or semantic pack")
assert_declared_features_fit_level(manifest, "capsule")

submissions = []
reexported_submissions = []
stream_paths = [stream["path"] for stream in manifest["streams"]]
if len(stream_paths) != len(set(stream_paths)):
    raise SystemExit("duplicate capsule stream path")
for stream in manifest["streams"]:
    path = capsule_root / stream["path"]
    content = path.read_bytes()
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    if digest != stream["digest"] or len(content) != int(stream["byte_length"]):
        raise SystemExit(f'capsule stream metadata mismatch: {stream["path"]}')
    activation = stream["activation_requirements"]
    if activation["minimum_level"] not in levels:
        raise SystemExit(f'unknown stream activation level: {stream["path"]}')
    if not set(activation["capabilities"]) <= all_capability_ids:
        raise SystemExit(f'unknown stream activation capability: {stream["path"]}')
    if stream["handling"] == "activate":
        if level_rank[activation["minimum_level"]] > level_rank[manifest["level"]]:
            raise SystemExit(f'active stream exceeds Capsule level: {stream["path"]}')
        if not set(activation["capabilities"]) <= set(manifest["capabilities"]):
            raise SystemExit(f'active stream lacks Capsule capability: {stream["path"]}')
    if stream["content_role"] == "submissions":
        if stream["handling"] != "activate":
            raise SystemExit(f'Capsule submission stream is not active: {stream["path"]}')
        stream_submissions = [json.loads(line) for line in content.splitlines() if line.strip()]
        with tempfile.TemporaryDirectory(prefix="ccf-submission-preserver-") as temporary:
            export_path = Path(temporary) / stream["path"]
            export_path.parent.mkdir(parents=True)
            export_path.write_text(
                "".join(
                    json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
                    for value in stream_submissions
                )
            )
            reexported_stream = [
                json.loads(line)
                for line in export_path.read_text().splitlines()
                if line.strip()
            ]
        if reexported_stream != stream_submissions:
            raise SystemExit(f'active Capsule stream changed during import/export: {stream["path"]}')
        submissions.extend(stream_submissions)
        reexported_submissions.extend(reexported_stream)
    elif stream["content_role"] == "opaque":
        if stream["handling"] != "preserve_opaque":
            raise SystemExit(f'opaque Capsule stream is not byte-preserved: {stream["path"]}')
        required_level = stream["activation_requirements"]["minimum_level"]
        required_capabilities = set(stream["activation_requirements"]["capabilities"])
        requirements_exceed_declaration = (
            level_rank[required_level] > level_rank[manifest["level"]]
            or not required_capabilities <= set(manifest["capabilities"])
        )
        if not requirements_exceed_declaration:
            raise SystemExit(f'opaque Capsule stream has no unsupported requirement: {stream["path"]}')
        with tempfile.TemporaryDirectory(prefix="ccf-opaque-preserver-") as temporary:
            export_path = Path(temporary) / stream["path"]
            export_path.parent.mkdir(parents=True)
            export_path.write_bytes(content)
            reexported = export_path.read_bytes()
        if reexported != content or hashlib.sha256(reexported).digest() != hashlib.sha256(content).digest():
            raise SystemExit(f'opaque Capsule stream did not round trip byte-for-byte: {stream["path"]}')

ids = [submission["id"] for submission in submissions]
if len(ids) != len(set(ids)):
    raise SystemExit("duplicate object ID in capsule")
if manifest["root_record_id"] not in ids:
    raise SystemExit("capsule root Record is absent")

unknown_active_fixture = dict(submissions[0])
unknown_active_fixture["type"] = "org.example.future-active"
try:
    registered_activation_requirement(unknown_active_fixture)
except ValueError:
    pass
else:
    raise SystemExit("unregistered semantics were allowed to activate")

for submission in submissions:
    validate(
        f'urn:ccf:schema:0.1.2:submissions.{submission["submission_kind"]}',
        submission,
        f'capsule submission {submission["id"]}',
    )
    try:
        requirement = registered_activation_requirement(submission)
    except ValueError as error:
        raise SystemExit(f'capsule attempts to activate unregistered semantics for {submission["id"]}') from error
    if level_rank[requirement["minimum_level"]] > level_rank[manifest["level"]]:
        raise SystemExit(f'capsule activates {submission["id"]} below its minimum level')
    needed = set(requirement["required_capabilities"])
    if requirement["semantic_pack"] is not None:
        needed.add(requirement["semantic_pack"])
    if not needed <= set(manifest["capabilities"]):
        raise SystemExit(f'capsule activates {submission["id"]} without {sorted(needed)}')
    if submission["submission_kind"] == "record" and submission["type_visibility"] == "clear":
        type_entry = base_registry_entries["record_type"].get(
            (submission["type"], submission["type_version"])
        )
        if type_entry is not None:
            validate(
                type_entry["semantic_schema_id"],
                submission["payload"],
                f'capsule payload {submission["id"]}',
            )

producer_batch_fixture = load_json(BASE / "vectors" / "producer-batch.json")["batch"]
blob_submission = producer_batch_fixture["blobs"][0]
validate(
    "urn:ccf:schema:0.1.2:submissions.blob",
    blob_submission,
    "Exchange Blob submission envelope",
)
blob_requirement = registered_activation_requirement(blob_submission)
if blob_requirement["minimum_level"] != "ccf-exchange-v1":
    raise SystemExit("ordinary Blob submissions are not activatable at Exchange")

producer_batch_validator = Draft202012Validator(
    schemas["urn:ccf:schema:0.1.2:sync.producer-batch"],
    registry=schema_registry,
    format_checker=CCF_FORMAT_CHECKER,
)
max_sequence_batch = dict(producer_batch_fixture, producer_sequence=str(2**64 - 1))
if list(producer_batch_validator.iter_errors(max_sequence_batch)):
    raise SystemExit("producer batch schema rejects the maximum uint64 sequence")
overflow_sequence_batch = dict(producer_batch_fixture, producer_sequence=str(2**64))
if not list(producer_batch_validator.iter_errors(overflow_sequence_batch)):
    raise SystemExit("producer batch schema accepts a uint64 overflow sequence")
oversized_sequence_batch = dict(producer_batch_fixture, producer_sequence="9" * 4301)
if not list(producer_batch_validator.iter_errors(oversized_sequence_batch)):
    raise SystemExit("producer batch schema accepts an oversized decimal sequence")
print("OK   CCF uint64 schema format boundary")

included_or_declared = set(ids) | {
    dependency["object_id"] for dependency in manifest["dependencies"]
}
for submission in submissions:
    if submission["submission_kind"] == "link":
        for endpoint in (submission["from_id"], submission["to_id"]):
            if endpoint not in included_or_declared:
                raise SystemExit(f"capsule Link has undeclared endpoint {endpoint}")

member_ids = set(ids) - {manifest["root_record_id"]}
membership_links = {
    submission["from_id"]
    for submission in submissions
    if submission["submission_kind"] == "link"
    and submission["type"] in manifest["membership_link_types"]
    and submission["to_id"] == manifest["root_record_id"]
}
if not member_ids - {submission["id"] for submission in submissions if submission["submission_kind"] == "link"} <= membership_links:
    raise SystemExit("capsule object is not connected to the root by a membership Link")
print(f"OK   capsule membership and streams ({len(submissions)} submissions)")
submission_by_id = {
    submission["id"]: submission for submission in submissions
}
unknown_extension = submission_by_id[manifest["root_record_id"]]["extensions"]
reexported_by_id = {submission["id"]: submission for submission in reexported_submissions}
if reexported_by_id[manifest["root_record_id"]]["extensions"] != unknown_extension:
    raise SystemExit("unknown Capsule extension did not round trip")
opaque_stream = next(stream for stream in manifest["streams"] if stream["content_role"] == "opaque")
opaque_bytes = (capsule_root / opaque_stream["path"]).read_bytes()
opaque_values = [json.loads(line) for line in opaque_bytes.splitlines() if line.strip()]
if {value["type"] for value in opaque_values} != {
    "lineage.erasure_receipt",
    "org.example.future-governance",
}:
    raise SystemExit("opaque fixture does not cover known-governed and unknown semantics")
governed_opaque = next(
    value for value in opaque_values if value["type"] == "lineage.erasure_receipt"
)
validate(
    "urn:ccf:schema:0.1.2:payload.lineage.erasure_receipt",
    governed_opaque["payload"],
    "source governed payload retained opaquely",
)
print("OK   unknown extension and unsupported semantics preserved without activation")

uplift = load_json(capsule_root / "uplift-receipt.json")
validate("urn:ccf:schema:0.2.0:exchange.uplift-receipt", uplift, "uplift receipt")
if (
    uplift["source_pack_id"] != manifest["pack_id"]
    or uplift["source_level"] != manifest["level"]
    or uplift["destination_level"] != "ccf-verified-archive-v1"
):
    raise SystemExit("pending uplift is not bound to its L1 Capsule and L3 destination")
if level_rank[uplift["destination_level"]] < level_rank[uplift["source_level"]]:
    raise SystemExit("uplift receipt moves to a weaker level")
uplift_source_ids = [entry["source_id"] for entry in uplift["objects"]]
if (
    len(uplift_source_ids) != len(submission_by_id)
    or len(uplift_source_ids) != len(set(uplift_source_ids))
    or set(uplift_source_ids) != set(submission_by_id)
):
    raise SystemExit("uplift receipt does not cover every capsule object exactly once")
for admission in uplift["objects"]:
    if admission["source_id"] != admission["canonical_id"]:
        raise SystemExit("uplift changed a supplied portable ID")
    if admission["producer_authentication"] == "verified":
        raise SystemExit(
            "verified producer authentication requires the signed-producer-sync verifier"
        )
    if admission["disposition"] in {"admitted", "existing"} and admission["object_hash"] is None:
        raise SystemExit("completed uplift disposition lacks an object hash")
    if admission["disposition"] in {"pending", "rejected", "conflict"} and admission["object_hash"] is not None:
        raise SystemExit("incomplete uplift disposition claims an object hash")
if uplift["status"] == "pending" and any(
    admission["disposition"] != "pending" for admission in uplift["objects"]
):
    raise SystemExit("pending uplift contains a completed disposition")

completed_uplift = load_json(capsule_root / "completed-uplift-receipt.json")
validate(
    "urn:ccf:schema:0.2.0:exchange.uplift-receipt",
    completed_uplift,
    "completed uplift receipt",
)
if (
    completed_uplift["source_pack_id"] != manifest["pack_id"]
    or completed_uplift["source_level"] != manifest["level"]
    or completed_uplift["destination_level"] != "ccf-canonical-store-v1"
):
    raise SystemExit("completed uplift is not bound to its L1 Capsule and L2 destination")
completed_source_ids = [entry["source_id"] for entry in completed_uplift["objects"]]
if (
    completed_uplift["status"] != "accepted"
    or len(completed_source_ids) != len(submission_by_id)
    or len(completed_source_ids) != len(set(completed_source_ids))
    or set(completed_source_ids) != set(submission_by_id)
):
    raise SystemExit("completed uplift does not accept every Capsule object")
for admission in completed_uplift["objects"]:
    if admission["source_id"] != admission["canonical_id"]:
        raise SystemExit("completed uplift changed a supplied portable ID")
    if admission["producer_authentication"] == "verified":
        raise SystemExit("completed uplift silently strengthened producer authentication")

downgrade = load_json(capsule_root / "downgrade-receipt.json")
validate("urn:ccf:schema:0.2.0:exchange.downgrade-receipt", downgrade, "downgrade receipt")
if downgrade["losslessness"] == "lossy" and not downgrade["omissions"]:
    raise SystemExit("lossy downgrade did not enumerate omissions")
if level_rank[downgrade["target_level"]] >= level_rank[downgrade["source_level"]]:
    raise SystemExit("downgrade receipt does not move to a weaker level")
inventories = {}
inventory_categories = {
    "submission",
    "journal_proof",
    "policy_state",
    "lineage_state",
    "compartment",
    "blob_content",
    "unknown_extension",
    "registry",
    "schema",
    "other",
}
# Preflight the downgrade package trees before any direct read beneath them.
# This rejects symlinked directories (integrity/, producer-batches/, etc.) and
# non-regular entries that would otherwise be followed by direct reads.
downgrade_source = capsule_root / "downgrade-source"
downgrade_export = capsule_root / "downgrade-export"
actual_source_files = _enumerate_tree(downgrade_source)
actual_export_files = _enumerate_tree(downgrade_export)

for name in ("source_inventory", "export_inventory"):
    inventory_ref = downgrade[name]
    inventory_path = capsule_root / inventory_ref["path"]
    content = inventory_path.read_bytes()
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    if digest != inventory_ref["digest"]:
        raise SystemExit(f"downgrade {name} digest mismatch")
    entries = load_json(inventory_path)
    validate(
        "urn:ccf:schema:0.2.0:exchange.downgrade-inventory",
        entries,
        f"downgrade {name}",
    )
    if any(
        set(entry) != {"category", "subject", "digest"}
        or entry["category"] not in inventory_categories
        or not isinstance(entry["subject"], str)
        or not entry["subject"]
        or not isinstance(entry["digest"], str)
        for entry in entries
    ):
        raise SystemExit(f"downgrade {name} contains an invalid entry")
    for entry in entries:
        subject = entry["subject"]
        if entry["category"] == "submission" and subject.startswith("submission:urn:ccf:"):
            continue
        if not _is_canonical_relative_posix(subject):
            raise SystemExit(f"downgrade {name} subject is not a canonical relative path: {subject}")
        if name == "source_inventory" and not subject.startswith("downgrade-source/"):
            raise SystemExit(f"downgrade source inventory references outside source package: {subject}")
        if name == "export_inventory" and not subject.startswith("downgrade-export/"):
            raise SystemExit(f"downgrade export inventory references outside export package: {subject}")
        artifact = (capsule_root / subject).read_bytes()
        artifact_digest = "sha256:" + hashlib.sha256(artifact).hexdigest()
        if artifact_digest != entry["digest"]:
            raise SystemExit(
                f'downgrade {name} artifact digest mismatch: {subject}'
            )
    keys = {(entry["category"], entry["subject"]) for entry in entries}
    if len(keys) != len(entries):
        raise SystemExit(f"downgrade {name} contains duplicate entries")
    inventories[name] = {
        (entry["category"], entry["subject"]): entry["digest"]
        for entry in entries
    }
source_inventory_entries = inventories["source_inventory"]
export_inventory_entries = inventories["export_inventory"]
source_inventory = set(source_inventory_entries)
export_inventory = set(export_inventory_entries)
if not export_inventory <= source_inventory:
    raise SystemExit("downgrade export inventory adds undeclared source material")
if any(
    source_inventory_entries[key] != export_inventory_entries[key]
    for key in export_inventory
):
    raise SystemExit("downgrade changed an exported logical item digest")
omission_keys = {(entry["category"], entry["subject"]) for entry in downgrade["omissions"]}
if len(omission_keys) != len(downgrade["omissions"]):
    raise SystemExit("downgrade receipt contains duplicate omissions")
if omission_keys != source_inventory - export_inventory:
    raise SystemExit("downgrade omissions are not the exact source/export inventory difference")
for item in downgrade["preserved_opaque"]:
    content = (capsule_root / item["path"]).read_bytes()
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    if digest != item["digest"]:
        raise SystemExit(f'downgrade opaque preservation digest mismatch: {item["path"]}')

# The selected downgrade source is a real 0.1.2 Verified fixture. Its journal
# members and producer batch must resolve to the exact portable objects included
# in the pinned source inventory; unrelated valid proofs are not sufficient.
source_identity = load_json(downgrade_source / "source-identity.json")
expected_identity_fields = {
    "format",
    "archive_id",
    "epoch_id",
    "genesis_commit_hash",
    "head_commit_hash",
    "head_sequence",
    "semantic_catalog_root",
    "trusted_genesis_signer_key_id",
    "trusted_genesis_signer_public_key",
}
if set(source_identity) != expected_identity_fields:
    raise SystemExit("downgrade source identity has missing or unknown fields")

physical_source_inventory = {
    subject.removeprefix("downgrade-source/")
    for category, subject in source_inventory
    if category != "submission" and subject.startswith("downgrade-source/")
}
if physical_source_inventory != actual_source_files:
    raise SystemExit("downgrade source inventory is not the exact physical source package")

downgrade_export_manifest = load_json(downgrade_export / "manifest.json")
validate(
    "urn:ccf:schema:0.2.0:exchange.capsule-manifest",
    downgrade_export_manifest,
    "downgrade export Capsule manifest",
)
if downgrade_export_manifest["pack_id"] != downgrade["export_pack_id"]:
    raise SystemExit("downgrade receipt export_pack_id does not bind the exported Capsule")
if (
    downgrade_export_manifest["level"] != downgrade["target_level"]
    or downgrade_export_manifest["custody"]["losslessness"] != downgrade["losslessness"]
    or downgrade_export_manifest["custody"]["omissions"] != downgrade["omissions"]
):
    raise SystemExit("downgrade export Capsule does not carry the receipt's downgrade declaration")
export_streams = downgrade_export_manifest["streams"]
export_stream_paths = [stream["path"] for stream in export_streams]
if len(export_stream_paths) != len(set(export_stream_paths)):
    raise SystemExit("downgrade export Capsule has duplicate stream paths")
expected_export_files = {"manifest.json", *export_stream_paths}
if expected_export_files != actual_export_files:
    raise SystemExit("downgrade export Capsule has unmanifested or missing files")
for stream in export_streams:
    content = (downgrade_export / stream["path"]).read_bytes()
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    if digest != stream["digest"] or len(content) != int(stream["byte_length"]):
        raise SystemExit(f'downgrade export stream metadata mismatch: {stream["path"]}')

source_headers = {}
for kind in ("records", "links", "blobs"):
    for line in (downgrade_source / "objects" / f"{kind}.ndjson").read_text().splitlines():
        if line.strip():
            header = json.loads(line)
            source_headers[header["id"]] = header
source_commits = [
    json.loads(line)
    for line in (downgrade_source / "integrity" / "commits.ndjson").read_text().splitlines()
    if line.strip()
]
source_members = [
    json.loads(line)
    for line in (downgrade_source / "integrity" / "members.ndjson").read_text().splitlines()
    if line.strip()
]
commit_by_sequence = {commit["sequence"]: commit for commit in source_commits}
if len(commit_by_sequence) != len(source_commits):
    raise SystemExit("downgrade source contains duplicate commit sequences")
for commit in source_commits:
    header = source_headers.get(commit["record_id"])
    if header is None or header["object_hash"] != commit["commit_hash"]:
        raise SystemExit("downgrade source commit is not bound to its portable object")
    uuid = commit["record_id"].rsplit(":", 1)[1]
    compartment_path = f"downgrade-source/compartments/records/{uuid}.structural.json"
    if ("compartment", compartment_path) not in source_inventory:
        raise SystemExit("downgrade source omits a signed commit compartment")
for member in source_members:
    header = source_headers.get(member["object_id"])
    if header is None or header["object_hash"] != member["object_hash"]:
        raise SystemExit("downgrade source member is not bound to its portable object")
    if member["commit_sequence"] not in commit_by_sequence:
        raise SystemExit("downgrade source member references an absent commit")
producer_batches = list((downgrade_source / "producer-batches").glob("*.json"))
if len(producer_batches) != 1:
    raise SystemExit("downgrade source must contain exactly one selected producer batch")
source_batch = load_json(producer_batches[0])
batch_submissions = [
    item
    for field in ("records", "links", "blobs")
    for item in source_batch[field]
]
batch_ids = {item["id"] for item in batch_submissions}
if not batch_ids or not batch_ids <= source_headers.keys():
    raise SystemExit("downgrade producer batch does not resolve to its canonical source objects")
submission_streams = [
    stream for stream in export_streams if stream["content_role"] == "submissions"
]
if len(submission_streams) != 1:
    raise SystemExit("downgrade export Capsule must contain exactly one submission stream")
exported_submissions = [
    json.loads(line)
    for line in (downgrade_export / submission_streams[0]["path"]).read_text().splitlines()
    if line.strip()
]
batch_submission_by_id = {submission["id"]: submission for submission in batch_submissions}
if len(exported_submissions) != 1 or any(
    batch_submission_by_id.get(submission["id"]) != submission
    for submission in exported_submissions
):
    raise SystemExit("downgrade Exchange assertions are not exact source batch submissions")
if downgrade_export_manifest["root_record_id"] != exported_submissions[0]["id"]:
    raise SystemExit("downgrade export root is not its selected source assertion")
for submission in exported_submissions:
    validate(
        f'urn:ccf:schema:0.1.2:submissions.{submission["submission_kind"]}',
        submission,
        f'downgrade export submission {submission["id"]}',
    )
    requirement = registered_activation_requirement(submission)
    if requirement["minimum_level"] != "ccf-exchange-v1":
        raise SystemExit("downgrade export activates an assertion above Exchange")
logical_export_ids = {
    subject.removeprefix("submission:")
    for category, subject in export_inventory
    if category == "submission" and subject.startswith("submission:")
}
if logical_export_ids != {submission["id"] for submission in exported_submissions}:
    raise SystemExit("downgrade logical inventory does not exactly cover its Exchange assertions")
origin_rows = {
    row["object_id"]: row
    for line in (downgrade_source / "origin-index.ndjson").read_text().splitlines()
    if line.strip()
    for row in [json.loads(line)]
}
for submission in exported_submissions:
    header = source_headers[submission["id"]]
    uuid = submission["id"].rsplit(":", 1)[1]
    plural_kind = f'{submission["submission_kind"]}s'
    structural_path = f"downgrade-source/compartments/{plural_kind}/{uuid}.structural.json"
    if ("compartment", structural_path) not in source_inventory:
        raise SystemExit(f"downgrade source omits structural compartment for {submission['id']}")
    semantic_path = f"downgrade-source/compartments/{plural_kind}/{uuid}.semantic.json"
    if header["semantic_commitment"] is not None and ("compartment", semantic_path) not in source_inventory:
        raise SystemExit(f"downgrade source omits semantic compartment for {submission['id']}")
    origin = submission.get("origin")
    if origin is not None:
        row = origin_rows.get(submission["id"])
        if row is None or any(row[field] != origin[field] for field in ("source_id", "native_id", "revision")):
            raise SystemExit(f"downgrade source origin tuple mismatch for {submission['id']}")
print("OK   downgrade source journal, objects, and producer batch correspond")


base_catalog = load_json(BASE / "semantic-catalog.json")
draft_catalog = load_json(ROOT / "semantic-catalog.json")
schema_catalog = load_json(ROOT / "schemas" / "catalog.json")
discovered_schema_ids = set(schemas) - {
    schema_id for schema_id in schemas if schema_id.startswith("urn:ccf:schema:0.1.2:")
}
catalog_schema_ids = {entry["id"] for entry in schema_catalog["schemas"]}
if catalog_schema_ids != discovered_schema_ids:
    raise SystemExit("draft schema catalog does not exactly cover draft schema IDs")
if schema_catalog["schemas"] != draft_catalog["schemas"]:
    raise SystemExit("schema and semantic catalogs disagree")
for entry in draft_catalog["schemas"] + draft_catalog["registries"]:
    if not (ROOT / entry["path"]).exists():
        raise SystemExit(f'stale draft catalog path {entry["path"]}')
base_pin = draft_catalog["base_catalogs"][0]
if base_pin["version"] != base_catalog["version"] or base_pin["root"] != base_catalog["root"]:
    raise SystemExit("0.2.0 draft does not pin the exact 0.1.2 semantic catalog")
print("OK   exact 0.1.2 catalog compatibility pin")
print("\nCCF 0.2.0 Exchange conformance checks passed.")
