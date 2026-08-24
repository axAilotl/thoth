# CCF integration (dual-write) — developer guide

Status: implemented on branch `ccf-0.1.2`; **legacy stores remain authoritative**.
CCF (Continuity Core Format) is the canonical record/link/blob store thoth is
migrating to. The normative spec is vendored at `spec/ccf/0.1.2/`; implementation
state per checklist section is annotated in
`spec/ccf/0.1.2/THOTH-IMPLEMENTATION-CHECKLIST.md`.

This document covers the thoth-side wiring. It reflects `ccf-0.1.2` @ `eb5ded4`.

## How it works today

Every ingestion capture flows through `CaptureLifecycleService.capture_to_queue()`
(`core/capture_lifecycle.py`), which commits the legacy queue row and then — when
enabled — mirrors the capture into the CCF archive (`_mirror_capture_to_ccf`).
Nothing reads from CCF yet; the wiki, archivist, and search all still run on the
legacy stores. CCF is a non-authoritative mirror until the cutover gates in
checklist section 10 are run by an operator.

Failure semantics:

- **Config/bootstrap: fail closed.** Missing DSN/keys, contradictory config, or an
  archive this runtime didn't create → `CcfConfigError`, no mirror.
- **Mirror writes: fail open.** A mirror error never breaks the legacy write. It is
  logged and ledgered to `.thoth_system/ccf_dualwrite_errors.jsonl` (per family).

## Enabling it

`database.ccf_archive` (see `config.example.json`):

| Key | Default | Effect |
|---|---|---|
| `enabled` | `false` | Master switch for the CCF archive |
| `dual_write` | `false` | Mirror captures (source → session/run → media → findings) |
| `mirror_transcripts` | `false` | Also mirror `experience.utterance` from transcript artifacts |
| `mirror_semantic` | `false` | Also mirror `semantic.entity` / `semantic.assertion` |
| `mirror_review` | `false` | Also mirror `governance.review_decision` (semantic + artifact review) |
| `mirror_wiki` | `false` | Also mirror wiki pages as evidence-linked projection artifacts |
| `dsn_env` | `THOTH_CCF_POSTGRES_DSN` | Env var holding the Postgres DSN |
| `device_key_path` / `archive_key_path` | `.thoth_system/ccf/*.pem` | Ed25519 keypair paths (0600) |

Turn on `dual_write` first; enable the `mirror_*` families incrementally. Each
family mirrors as its own signed batch and fails independently.

## Lanes: CCF-forward ingestion

Connectors declare their CCF lane in their manifest (`collectors/*.connector.json`
or any dropped-in plugin manifest):

```json
"ccf": {
  "lane": "paper",
  "artifact_role": "raw_capture",
  "extensions": {"thoth.lane": "paper"}
}
```

- `lane` — closed vocabulary: `paper`, `repository`, `transcript`, `video`,
  `web_clipper`, `markdown`, `tweet`, `mixed`. Unknown values fail manifest load.
- `artifact_role` — CCF artifact role token; builtins use `raw_capture`.
- `extensions` — namespaced dotted keys only (`thoth.*`, or your own namespace),
  JSON scalar values.

`mixed` is for multi-lane connectors (`skill_outputs`, `pi_skills`); per-artifact
lanes then come from the envelope (below). The mirror resolves the manifest once
per capture (keyed on the capture source's collector name / `source_aliases`) and
threads lane + provenance (`thoth_source_name`, `thoth_collector`,
`thoth_native_source_id`) into the mirrored artifact's extensions. A connector
with no `ccf` block mirrors byte-identically to before.

### Skill output envelope v1.1

External skill envelopes (JSON/JSONL via `skill_output_connector`, including pi
skills) accept two optional top-level fields:

- `lane` — same closed vocabulary as manifests
- `ccf` — extensions object, same namespaced-key rules

Precedence: envelope fields > ingesting connector's manifest block > legacy
behavior. Validation reuses the exact manifest validators
(`validate_ccf_lane` / `parse_ccf_extensions` in `core/connector_registry.py`);
a malformed override is rejected loudly and ledgered, never silently dropped.
Note: a legacy envelope with stray top-level `lane`/`ccf` keys is now treated as
declaring v1.1 fields (fail-closed) rather than being folded into the payload.

## Writing a drop-in connector

1. Write `<name>.connector.json` into a configured plugin dir
   (`connectors.plugin_dirs`, or `$THOTH_CONNECTOR_PATH`), with an `entrypoint`
   (`module:attr`) following the standard contract: class taking
   `(config, layout=..., db=...)` with `collect(**options)`.
2. Add a `ccf` block to declare its lane.
3. Done — `connectors list` / `GET /api/connectors` shows it, `connectors run`
   executes it, captures mirror with its lane. No core edits.

Direct wiki writes remain contractually forbidden for connectors
(`validate_allowed_side_effects`, `validate_manifest_outputs`) — connectors queue
artifacts; the wiki is a projection.

## Verification

- `scripts/ccf_dualwrite_check.py` — reconciles legacy `metadata_db` inventory vs
  the CCF archive; exit 1 on any mismatch. Flag-gated family objects report as
  `derived`, not mismatches. This is the zero-mismatch gate for cutover.
- `scripts/ccf_dualwrite_corpus_import.py` — drives a representative corpus
  through the real legacy entrypoint with the mirror on.
- `tests/test_ccf_*` — 40+ files: conformance vectors, admission, erasure,
  governance, sync, thothmap per-domain, dual-write families, cutover gates.

## Not yet

- Archivist / wiki / search reading from CCF projections (Phase 4, post-cutover).
  `ccf/projections/wiki.py` can already rebuild a wiki to a **staging dir** as a
  pure function of canonical state.
- Normative per-lane CCF types (would be a spec 0.1.3 change; lanes currently live
  in extension fields by design).
- Capture-event wiki pages (`update_from_capture_events`) are deliberately not
  mirrored: no honest evidence Links exist for them yet.
