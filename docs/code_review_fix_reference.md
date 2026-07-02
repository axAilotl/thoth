# Code Review Fix Reference

Validated on 2026-07-01 against branch `exocortext-update-1`.

This document is the implementation reference for epic `thoth-7v1`
(`Fix validated exocortext review regressions`). It distills the validated
findings from `CODE_REVIEW_FINDINGS.md` into fix beads, evidence, and suggested
ordering. The original review was intentionally adversarial but not fully
verified; the notes below reflect the follow-up validation pass.

## Bead Map

| Bead | Priority | Scope | Review findings |
| --- | --- | --- | --- |
| `thoth-7v1.1` | P0 | LLM cache isolation for redacted inputs | #1 |
| `thoth-7v1.2` | P0 | Capture prompt-security finding persistence and enforcement | #2, #11 |
| `thoth-7v1.3` | P0 | CJK prompt detection and zero trust-score handling | #9, #10 |
| `thoth-7v1.4` | P1 | X bookmark checkpoint delivery safety | #3 |
| `thoth-7v1.5` | P1 | Retention policy enforcement and scoped deletion | #4, #5, #6 |
| `thoth-7v1.6` | P1 | Ingestion cancellation safety and imported markdown ID uniqueness | #7, #8 |
| `thoth-7v1.7` | P1 | Capture ingest capability defaults and markdown routing | #12, #24, #25 |
| `thoth-7v1.8` | P2 | Transcript, YouTube budget, and boolean option behavior | #13, #14, #20 |
| `thoth-7v1.9` | P2 | Collector per-item tolerance and config compatibility | #15, #16, #17 |
| `thoth-7v1.10` | P2 | Wiki lint, canonical URL, timestamp, query, semantic wiki correctness | #18, #19, #21, #22, #23 |
| `thoth-7v1.11` | P2 | Search, retention, LLM usage, and semantic memory performance | #26-#31 |
| `thoth-7v1.12` | P3 | Consolidation after functional fixes | #32-#40 |

## Dependency Shape

- `thoth-7v1.7` depends on `thoth-7v1.6` so markdown processing is not broadly
  enabled before duplicate-title overwrite risk is fixed.
- `thoth-7v1.11` depends on `thoth-7v1.2`, `thoth-7v1.5`, and `thoth-7v1.10`
  so performance work does not optimize around broken security, retention, or
  timestamp semantics.
- `thoth-7v1.12` depends on all preceding child beads. Consolidation should move
  final behavior, not move known bugs.

## Fix Order

1. `thoth-7v1.1`, `thoth-7v1.2`, `thoth-7v1.3`: security and trust boundary
   fixes.
2. `thoth-7v1.4`, `thoth-7v1.5`, `thoth-7v1.6`: data-loss fixes.
3. `thoth-7v1.7`, `thoth-7v1.8`, `thoth-7v1.9`, `thoth-7v1.10`: visible API,
   connector, wiki, and search regressions.
4. `thoth-7v1.11`: performance fixes after correctness behavior is settled.
5. `thoth-7v1.12`: behavior-preserving consolidation.

## Validation Nuances

- Finding #3 is fully valid for the payload-return path. Capture-event queuing
  partially mitigates already-queued page records, but checkpoint advancement
  still occurs before the caller receives returned payloads.
- Finding #13 is valid. The "raw unredacted transcript" concern specifically
  applies to single-chunk validation failures: `_process_single_chunk()` returns
  `None`, then the YouTube writer falls back to raw transcript.
- Finding #14 is valid for `YouTubeConnector.collect()`. The older
  `YouTubeProcessor.process_urls()` path catches per URL, but the connector path
  loops `unique_urls` without a per-video try boundary.
- Finding #17 is valid with nuance: a bare `https://.../wiki/...` URL is allowed,
  but prose containing such a URL is falsely rejected because the scheme
  exemption only applies at the beginning of the whole string.
- Finding #23 impact depends on deployment timezone. On the validation host,
  local time was EDT (`-0400`), while naive timestamps were parsed as UTC.
- Finding #33 is maintainability risk, not a confirmed user-visible bug. Some
  `finally` structure exists, but usage recording remains spread across many
  call sites.

## Detailed Finding Matrix

| # | Status | Bead | Validation summary |
| --- | --- | --- | --- |
| 1 | Confirmed | `thoth-7v1.1` | Redaction-normalized cache keys collide across distinct sensitive inputs. |
| 2 | Confirmed | `thoth-7v1.2` | Finding persistence helpers have no production callers; capture state reads persisted findings only. |
| 3 | Confirmed with nuance | `thoth-7v1.4` | Checkpoint persists before returned payload delivery; capture queue can partially mitigate. |
| 4 | Confirmed | `thoth-7v1.5` | Compiled wiki page deletion does not check for other live event IDs. |
| 5 | Confirmed | `thoth-7v1.5` | Event retention metadata is stored, but eligibility requires policy rows that production does not create. |
| 6 | Confirmed | `thoth-7v1.5` | LLM cache context matching uses unescaped SQL LIKE patterns. |
| 7 | Confirmed | `thoth-7v1.6` | Cancelled bounded workers can leave already-marked rows in `processing`. |
| 8 | Confirmed | `thoth-7v1.6` | Imported markdown IDs are source prefix plus title slug only. |
| 9 | Confirmed | `thoth-7v1.3` | `source_trust_score=0` is treated as missing and defaults to full trust. |
| 10 | Confirmed | `thoth-7v1.3` | CJK override phrase embedded in surrounding CJK text does not match. |
| 11 | Confirmed | `thoth-7v1.2` | Wiki compiler allows open medium non-prompt findings that search/agent paths block. |
| 12 | Confirmed | `thoth-7v1.7` | API model defaults omitted capabilities to `[]`, bypassing artifact capability fallback. |
| 13 | Confirmed with nuance | `thoth-7v1.8` | Strict JSON parser rejects fenced output; single-chunk path falls back to raw transcript. |
| 14 | Confirmed with nuance | `thoth-7v1.8` | Connector path aborts on one video budget error; processor batch path catches per URL. |
| 15 | Confirmed | `thoth-7v1.9` | arXiv per-entry queue failure propagates out of discovery loops. |
| 16 | Confirmed | `thoth-7v1.9` | Pi skill config now hard-fails missing `artifact_types`. |
| 17 | Confirmed with nuance | `thoth-7v1.9` | Prose containing Wikipedia `/wiki/` URL is rejected; bare URL is allowed. |
| 18 | Confirmed | `thoth-7v1.10` | Wiki lint timestamp parser calls `.strip()` on PyYAML date objects. |
| 19 | Confirmed | `thoth-7v1.10` | `fakeyoutube.com` passes `endswith("youtube.com")` canonicalization. |
| 20 | Confirmed | `thoth-7v1.8` | `bool("false")` makes string false options truthy. |
| 21 | Confirmed | `thoth-7v1.10` | Hybrid hit serialization omits `resource`, `kind`, and `record_type`. |
| 22 | Confirmed | `thoth-7v1.10` | Evidence-free promoted facts are allowed by promotion gate but skipped by semantic wiki compiler. |
| 23 | Confirmed with deployment caveat | `thoth-7v1.10` | Producers emit naive local timestamps; search parses naive timestamps as UTC. |
| 24 | Confirmed | `thoth-7v1.7` | Review-policy artifact allowlist omits new `markdown` type. |
| 25 | Confirmed | `thoth-7v1.7` | Wiki updater type chain omits `MarkdownArtifact`. |
| 26 | Confirmed code-level | `thoth-7v1.11` | API query path constructs blocking `MetadataDB`/config services inside async endpoint. |
| 27 | Confirmed code-level | `thoth-7v1.11` | LLM usage recording ensures schema on every usage event. |
| 28 | Confirmed code-level | `thoth-7v1.11` | Capture-event search performs multiple event-store calls per event. |
| 29 | Confirmed code-level | `thoth-7v1.11` | Wiki search rereads and reparses every page per query. |
| 30 | Confirmed code-level | `thoth-7v1.11` | Retention inspect globs/parses pages per event and queries policies per target. |
| 31 | Confirmed code-level | `thoth-7v1.11` | Rejected semantic candidate equivalence scans rejected rows and fingerprints in Python. |
| 32 | Supported | `thoth-7v1.12` | Field-by-field frozen dataclass reconstruction risks dropped fields on future changes. |
| 33 | Partially supported | `thoth-7v1.12` | Usage recording is duplicated, though some `finally` structure exists. |
| 34 | Confirmed | `thoth-7v1.12` | `_now_iso` helpers diverge between local naive and UTC-Z. |
| 35 | Supported | `thoth-7v1.12` | Semantic and capture wiki write paths duplicate managed-page plumbing with different metadata coverage. |
| 36 | Supported | `thoth-7v1.12` | Agent surface handler map duplicates connector registry knowledge. |
| 37 | Confirmed | `thoth-7v1.12` | Native-ID candidate tables are duplicated and drifted. |
| 38 | Confirmed | `thoth-7v1.12` | Archivist schema upgrades bypass existing `_ensure_columns` helper. |
| 39 | Supported | `thoth-7v1.12` | Minor dead/redundant helper code is present. |
| 40 | Confirmed | `thoth-7v1.12` | JSON, slug, hash, and path helpers are duplicated across modules. |

## LLM Cache Isolation

Bead: `thoth-7v1.1`.

Affected code:
- `core/llm_cache.py`
- `core/sensitive_redaction.py`
- LLM cache tests.

Validation:

```text
cache_key_equal_email True
cache_get_bob {'summary': 'alice'}
```

The cache key is generated from redacted content, task type, and model. Different
inputs whose sensitive spans redact to the same placeholders share one key. The
fix must keep persisted cache metadata redacted while making identity collision
impractical. A raw-content cryptographic hash or HMAC is acceptable if raw
content is never stored or logged.

Success criteria:
- Distinct sensitive values no longer collide.
- Identical raw content still hits.
- Existing unsafe cache entries are versioned away, invalidated, or otherwise
  prevented from serving stale cross-document results.

## Capture Security Findings

Bead: `thoth-7v1.2`.

Affected code:
- `core/capture_event_store.py`
- `core/capture_lifecycle.py`
- `core/connector_capture.py`
- `core/capture_surface.py`
- `core/agent_context.py`
- `core/hybrid_search.py`
- `core/wiki_capture_compiler.py`

Validation:
- `upsert_prompt_security_findings()` and
  `upsert_security_findings_from_metadata()` exist but are only called by tests.
- Capture event detail and agent/search trust derive security state from
  persisted event-store findings.
- Wiki compilation currently blocks high/critical or prompt finding types, while
  search/agent paths treat any open finding as `needs_review`.

Implementation guidance:
- Persist findings during capture lifecycle after event/raw-ref IDs are known.
- Attach findings to both event and raw ref when metadata provenance supports it.
- Use stable fingerprints to avoid duplicate rows on repeated capture/upsert.
- Align wiki compiler with the same open-finding policy used by agent/search.

Success criteria:
- Prompt injection metadata from queued artifacts appears in capture event
  `security_findings`.
- Capture event API, agent surface, hybrid search, and wiki compiler agree on
  review-required state.

## Prompt Detection And Trust

Bead: `thoth-7v1.3`.

Affected code:
- `core/prompt_security.py`
- `core/hybrid_search.py`
- `core/agent_context.py`

Validation:

```text
cjk_threat_embedded []
cjk_threat_standalone ['multilingual_instruction_override']
trust_source_zero {'score': 1.0, 'reason': 'queue_status_pending', 'influence_sources': []}
capture_trust_zero {'score': 1.0, 'reason': 'capture_security_allowed', 'influence_sources': []}
```

The multilingual pattern begins with `\b`, which does not work as intended for
embedded CJK text. Trust parsing uses truthiness fallback, so explicit numeric
zero is treated as absent.

Success criteria:
- Embedded Chinese/Japanese override phrases match.
- Explicit zero trust score remains zero and affects filtering.
- Invalid trust values fail closed or produce a documented low-trust state.

## X Bookmark Checkpointing

Bead: `thoth-7v1.4`.

Affected code:
- `core/x_api_bookmark_sync.py`
- X API sync/backfill tests.

Validation:
- New IDs are added to `seen_ids` and persisted into
  `seen_bookmark_ids` before `sync_x_api_bookmarks()` returns payloads.
- If a later page or downstream queue write raises, earlier returned-payload
  delivery may not happen, but the checkpoint has already advanced.

Success criteria:
- A fetch failure after page 1 does not permanently skip page 1 for consumers
  that did not receive or durably queue it.
- Capture-event and returned-payload modes have explicit checkpoint boundaries.

## Retention Expiry

Bead: `thoth-7v1.5`.

Affected code:
- `core/retention_service.py`
- `core/capture_lifecycle.py`
- `core/capture_event_store.py`
- `core/metadata_db.py`
- retention API/CLI tests.

Validation:
- `_compiled_wiki_targets()` selects any page whose frontmatter contains the
  expiring event ID, regardless of other live event IDs.
- `_eligibility()` returns `missing retention policy` when no policy row exists.
- `CaptureLifecycleService` stores event retention metadata but does not create
  `RetentionPolicy` rows.
- `list_llm_cache_entries_for_contexts()` binds `LIKE '%{context}%'` without
  escaping wildcard characters.

Success criteria:
- Operator retention declarations are enforceable or explicitly rejected at
  capture time.
- Shared compiled pages are preserved or rewritten safely.
- Context matching is exact or wildcard-escaped.

## Ingestion And Imported Markdown Data Loss

Bead: `thoth-7v1.6`.

Affected code:
- `core/bounded_workers.py`
- `core/ingestion_runtime.py`
- `core/metadata_db.py`
- `collectors/imported_markdown_connector.py`

Validation:

```text
imported-markdown-journal
imported-markdown-journal
```

Two different files titled `Journal` produce the same default artifact ID.
Separately, bounded worker exceptions cancel sibling tasks. If a sibling has
already marked its row `processing`, `CancelledError` bypasses the
`except Exception` failure handler, and pending selection never sees it again.

Success criteria:
- No queue row remains permanently stuck in `processing` due to sibling
  cancellation.
- Imported markdown default IDs include enough path/hash identity to avoid
  title-only collisions.

## Capture Ingest And Markdown Routing

Bead: `thoth-7v1.7`.

Affected code:
- `thoth_api.py`
- `core/capture_lifecycle.py`
- `core/artifact_review_policy.py`
- `core/wiki_updater.py`
- markdown ingestion tests.

Validation:
- API capabilities default to `[]`, while lifecycle fallback runs only for
  `capabilities is None`.
- Imported markdown queues `artifact_type='markdown'`.
- Structural review allowlist omits `markdown`.
- `WikiUpdater._artifact_type_for_artifact()` omits `MarkdownArtifact`.

Success criteria:
- Omitted API capabilities preserve artifact-native capabilities.
- Markdown queue rows are processable.
- Markdown artifacts are represented as markdown in wiki/provenance surfaces.

## Transcript And YouTube Failures

Bead: `thoth-7v1.8`.

Affected code:
- `core/llm_validation.py`
- `processors/transcript_llm_processor.py`
- `processors/youtube_processor.py`
- `collectors/youtube_connector.py`
- `core/agent_surface.py`

Validation:

```text
code_fence_parse_error chunk was not valid JSON: Expecting value
```

Strict JSON parsing rejects common fenced output. Single-chunk validation errors
return `None`, and the YouTube file writer then falls back to raw transcript.
`ConnectorBudgetError` aborts `YouTubeConnector.collect()` because that path
lacks per-video exception handling. Agent connector options use `bool()` for
string values.

Success criteria:
- Transcript parser is safely lenient or fallback is redacted consistently.
- One over-budget YouTube video does not abort later URLs.
- String boolean options use `_optional_bool`.

## Collector Tolerance And Config Compatibility

Bead: `thoth-7v1.9`.

Affected code:
- `collectors/arxiv_collector.py`
- `collectors/pi_skill_connector.py`
- `collectors/skill_output_connector.py`

Validation:
- arXiv loops let queue/lifecycle failures propagate for a single entry.
- Pi skills now require `artifact_types`.
- `_looks_like_direct_wiki_path()` rejects prose containing a Wikipedia URL,
  though a bare URL is allowed.

Success criteria:
- Per-paper arXiv failures are reported without discarding successful papers.
- Legacy Pi skill config behavior is migrated or isolated.
- Direct wiki writes remain blocked, but ordinary web citations are accepted.

## Wiki Search And Canonical Correctness

Bead: `thoth-7v1.10`.

Affected code:
- `core/wiki_lint.py`
- `core/canonical_identity.py`
- `core/hybrid_search.py`
- `core/agent_surface.py`
- `core/semantic_memory.py`
- `core/semantic_wiki_compiler.py`
- timestamp-producing modules.

Validation:

```text
wiki_lint_date_error AttributeError 'datetime.date' object has no attribute 'strip'
fakeyoutube https://www.youtube.com/watch?v=X
naive_parse_tz 2026-05-01T12:00:00+00:00
```

The wiki query compatibility finding is also supported: hybrid hits contain
`source_type`, `source_id`, and provenance, but not `resource`, `kind`, or
`record_type`.

Success criteria:
- Date frontmatter linting does not crash.
- YouTube canonicalization uses exact host or dot-boundary matching.
- Timestamp producers and parsers agree on timezone semantics.
- Query API response compatibility is restored or versioned.
- Evidence-free promotion is either surfaced with explicit provenance or
  rejected before promotion.

## Performance Hot Paths

Bead: `thoth-7v1.11`.

Affected code:
- `thoth_api.py`
- `core/llm_usage.py`
- `core/hybrid_search.py`
- `core/retention_service.py`
- `core/semantic_memory.py`

Validation is code-level; no profiling was run during the validation pass.

Confirmed patterns:
- Async API endpoints construct blocking `MetadataDB`/config/service objects per
  request.
- LLM usage calls schema DDL/index creation for every usage event.
- Capture-event search performs multiple store calls per event.
- Wiki search rereads every page per query.
- Retention inspect parses all wiki pages per event and queries policies per
  target.
- Semantic memory rejected-candidate checks scan and fingerprint in Python.

Success criteria:
- Query/file-read counts are bounded or demonstrably reduced.
- Schema setup is migration/startup/once-only guarded.
- Correctness tests from prerequisite beads still pass.

## Consolidation Follow Up

Bead: `thoth-7v1.12`.

Affected code:
- `core/semantic_memory.py`
- timestamp helper modules
- wiki compiler modules
- `core/agent_surface.py`
- `core/artifact_review_policy.py`
- `core/capture_lifecycle.py`
- `core/metadata_db.py`
- duplicated JSON/slug/hash/path helper modules.

This bead should be handled after functional fixes. The goal is reducing future
drift, not broad style churn.

Supported consolidation targets:
- Replace manual frozen-dataclass reconstruction with `dataclasses.replace`.
- Centralize UTC timestamp generation.
- Share managed wiki page write plumbing or document intentional differences.
- Move connector executable adapters into registry/manifest metadata.
- Declare native ID keys once per artifact type.
- Use `_ensure_columns()` consistently.
- Consolidate duplicated JSON, slug, hash, and path helpers.

Success criteria:
- Refactors are behavior-preserving.
- Existing provenance, wiki frontmatter, and ingestion tests continue to pass.
- Tests cover drift-prone shared primitives such as markdown native IDs and
  connector dispatch.
