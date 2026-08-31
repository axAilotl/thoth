# Code review — branch `exocortext-update-1` vs `main`

- **Date:** 2026-07-01
- **Scope:** `git diff main...HEAD` — 142 files, ~42.5k insertions / ~1.3k deletions (~31k insertions in non-test source)
- **Method:** 8 parallel finder angles (2× line-by-line diff scan, removed-behavior audit, cross-file tracer, reuse, simplification, efficiency, altitude). **Adversarial verification was NOT completed** (stopped for budget) — treat every finding as a candidate to spot-check before fixing. Duplicated discovery across independent angles is noted where it happened (a strong signal).
- No CLAUDE.md files exist in the repo or user scope, so there are no convention findings by construction.

---

## Tier 1 — Likely-severe correctness bugs (data loss, security, wrong results)

### 1. LLM cache serves one document's results for another after redaction-normalized keys
**File:** `core/llm_cache.py:41`
**Found independently by two angles** (line-scan + removed-behavior audit — the strongest signal in this review).
Cache keys are now computed from redaction-normalized content instead of raw content. Two different inputs whose sensitive spans redact to the same placeholder (`alice@gmail.com` / `bob@yahoo.com` → `[[REDACTED_EMAIL_1]]`; two different phone numbers → `[[REDACTED_PHONE_1]]`) collide on the same key, and `get()` returns the *other* input's cached tags/summary/formatted transcript. Cross-content result leak that permanently attaches the wrong summary/tags to a wiki entry.

### 2. Prompt-security findings are never persisted to the capture event store
**File:** `core/capture_event_store.py:830`
`upsert_prompt_security_findings` / `upsert_security_findings_from_metadata` appear to have no production callers (tests only). `GET /api/capture/events/{id}` and `AgentSurfaceService.get_capture_event` derive `security_state` solely from event-store findings (`core/agent_context.py:200-212`, `core/capture_surface.py:606`), which stay empty — so an event whose queued artifact was flagged for prompt injection still reads back `security_state.status='allowed'` with trust 1.0, and an agent treats injected content as fully trusted.

### 3. X bookmark sync checkpoints pages before delivering them — permanent bookmark loss on error
**File:** `core/x_api_bookmark_sync.py:611`
Per-page checkpoint persists fetched tweet IDs into `seen_bookmark_ids` before the payloads are returned to the caller (payloads only return after the whole loop). If a later page raises (disk full, capture DB error), the caller never receives page 1's bookmarks, but the next run sees their IDs in `seen_ids`, sets `stopped_at_known_id`, and those bookmarks are never queued — silently and permanently.

### 4. Retention expiry deletes shared compiled wiki pages still backed by live events
**File:** `core/retention_service.py:463`
`_compiled_wiki_targets` marks a compiled page for deletion whenever the expiring event's ID appears in the page's `thoth_event_ids` frontmatter, with no check that the page cites other, non-expired events. `expire(event_id=A, delete_distilled=True)` unlinks a rollup page compiled from events A and B, destroying content that legitimately cites B.

### 5. Retention intents are unenforceable — expiry is a silent no-op
**File:** `core/retention_service.py:735`
Eligibility only consults the `retention_policies` table, and `CaptureEventStore.upsert_retention_policy` appears never to be called from production code, so no policy rows ever exist. Operator-declared retention on capture events (e.g. markdown frontmatter `retention: {action: delete, delete_after: ...}`, stored via `core/capture_lifecycle.py:512`) always yields `eligible=false, reason='missing retention policy'` — the data is never deleted.

### 6. SQL LIKE wildcards unescaped — retention deletes LLM cache rows belonging to other artifacts
**File:** `core/metadata_db.py:5164`
`list_llm_cache_entries_for_contexts` interpolates context identifiers into `LIKE '%{context}%'` without escaping `_`/`%`. Identifiers routinely contain underscores (`tweet_1234`), and `_` matches any character, so `tweetX1234:summary` also matches. `CaptureRetentionService._llm_cache_targets` then lists — and `expire()` deletes — cache entries for unrelated artifacts.

### 7. Cancelled ingestion workers leave queue rows stuck in `processing` forever
**File:** `core/ingestion_runtime.py:247`
With `concurrent_workers > 1`, one entry raising makes `map_bounded`'s gather cancel sibling workers after they've called `mark_ingestion_processing`. `CancelledError` is a `BaseException`, so `except Exception` never runs `mark_ingestion_failed`; the rows stay `processing`, and `get_pending_ingestions` (status `pending` only) never re-selects them — those artifacts are silently dropped from all future polls.

### 8. Imported-markdown artifact IDs collide on title — second file silently overwrites the first
**File:** `collectors/imported_markdown_connector.py:414`
`_artifact_id` is built from source-name prefix + title slug only. Two files both titled "Journal" in different directories map to the same `imported-markdown-journal` ID; the queue upsert (`ON CONFLICT DO UPDATE`) replaces the first, both report `queued=True`, only one is ingested.

### 9. Explicit trust score of 0 is treated as missing → full trust 1.0
**File:** `core/hybrid_search.py:1066` (also `_capture_trust` at ~1093)
`payload.get("source_trust_score") or payload.get("trust_score")` — falsy-zero bug. A payload carrying `{"source_trust_score": 0}` (explicitly untrusted) falls through to the 1.0 default; a `min_trust_score=0.5` filter then passes the untrusted artifact and reports trust 1.0.

### 10. CJK prompt-injection phrases bypass the multilingual override pattern
**File:** `core/prompt_security.py:111`
The pattern's single leading `\b` applies to all alternatives including Chinese/Japanese ones, and there is no word boundary between adjacent CJK characters, so e.g. 请忽略所有以前的指令 embedded in running CJK text never matches (finder reported verifying with `re.search`). Chinese/Japanese "ignore all previous instructions" injections skip sanitization, quarantine, and the untrusted-content warning.

---

## Tier 2 — Correctness bugs (wrong behavior, crashes, regressions)

### 11. Quarantined content leaks through compiled wiki pages
**File:** `core/wiki_capture_compiler.py:208`
The compiler only treats critical/high-severity or `prompt_injection` findings as review-blocking, while `hybrid_search.py:976-982` and `agent_context.py:206-210` treat ANY open finding as `needs_review`. An event with an open medium-severity finding is excluded from search and `get_capture_event` raises "requires security review" — yet the same content is published into a searchable compiled wiki rollup page.

### 12. API ingest silently wipes artifact capabilities
**File:** `thoth_api.py:765`
`CaptureIngestRequest.capabilities` uses `default_factory=list`, so an ingest that omits the field passes `[]` (not `None`) into `CaptureLifecycleService.capture`, which only falls back to the artifact's natural capabilities on `None` (`core/capture_lifecycle.py:265`). Result: `capabilities_json='[]'` persisted, artifact's default capabilities overwritten with an empty tuple — diverging from the CLI path, which passes `None` and gets the fallback.

### 13. Strict JSON parsing regression fails previously recoverable transcript chunks
**File:** `processors/transcript_llm_processor.py:304`
The lenient `_extract_json_object` (code-fence stripping, trailing-comma repair) and raw-response fallback were deleted for strict `parse_llm_json_response`. LLM output wrapped in ` ```json ` fences, or with a trailing comma / non-English tags / empty summary, now raises `LLMOutputValidationError` → chunk returns None, a failure record blocks retries for `retry_interval_hours`, and the raw **unredacted** transcript is written instead of formatted output.

### 14. One over-budget video aborts the entire YouTube connector run
**File:** `processors/youtube_processor.py:479`
`ConnectorBudgetError` is now explicitly re-raised out of `process_video`, and `YouTubeConnector.collect()`'s per-URL loop has no handler for it. On main it was logged and skipped; now one oversized transcript aborts the run and all remaining URLs are never processed.

### 15. One transient DB error aborts the entire arXiv discovery run
**File:** `collectors/arxiv_collector.py:190`
Per-paper failure tolerance was removed: the new `capture_queue.queue_artifact` path lets `CaptureLifecycleError` propagate. A transient SQLite "database is locked" on entry 3 of 50 makes `discover_papers` raise and report nothing, where main queued and returned the other 49.

### 16. Previously valid Pi-skill configs now fail to load at all
**File:** `collectors/pi_skill_connector.py:342`
The fallback defaulting a skill's `artifact_types` to all supported types was replaced with a hard `ValueError` (same for other newly required fields). An existing `sources.pi_skills` entry that omits `artifact_types` — valid on main — now breaks every pi_skills operation, including plan/status for unrelated skills.

### 17. Legitimate Wikipedia citations abort skill-output collection
**File:** `collectors/skill_output_connector.py:520` (helper reused in `collectors/pi_skill_connector.py:726`)
`_looks_like_direct_wiki_path`'s bare `'"/wiki/" in lowered'` substring test rejects content strings containing e.g. `https://en.wikipedia.org/wiki/Python` (the http/https exemption only applies when the whole string starts with the scheme). The `ValueError` aborts the entire `collect()` run, and retries deterministically fail on the same citation.

### 18. Wiki lint crashes on unquoted YAML dates in frontmatter
**File:** `core/wiki_lint.py:43`
`_parse_timestamp` calls `.strip()` on frontmatter values, but PyYAML parses unquoted `updated_at: 2026-05-01` into `datetime.date`, which has no `.strip()` → `AttributeError` aborts the entire lint run instead of reporting one page.

### 19. `fakeyoutube.com` merges into real YouTube canonical identities
**File:** `core/canonical_identity.py:576` (same pattern in `_normalize_url` ~597)
`host.endswith("youtube.com")` without a dot boundary: `https://fakeyoutube.com/watch?v=X` is assigned `youtube_video_id X` and its URL rewritten to `https://www.youtube.com/watch?v=X`, merging into the canonical entity of the real video — cross-domain identity collision polluting dedup and wiki linkage.

### 20. String `'false'` option values are truthy in YouTube connector options
**File:** `core/agent_surface.py:986` (also `no_resume` at ~956/990)
`archive_video`/`no_resume` are parsed with plain `bool()` instead of the module's `_optional_bool`. `run_connector('youtube', options={'archive_video': 'false'})` (string values from JSON/CLI/MCP callers) archives the video the user disabled; `'no_resume': 'false'` disables checkpoint resume the user asked to keep.

### 21. Wiki query hits dropped `resource` / `kind` / `record_type` fields
**File:** `core/agent_surface.py:748`
Main's `query_wiki` returned per-hit `resource` (canonical source URL from frontmatter), `kind`, and `record_type`; `_serialize_hybrid_hit` and the hybrid-search provenance dict never emit them and `HybridSearchHit` has no such attributes. Consumers keyed on those fields get KeyError/None and can no longer resolve the canonical source link or distinguish page kinds.

### 22. Evidence-free promoted facts silently vanish from the compiled wiki
**File:** `core/semantic_wiki_compiler.py:300`
The compiler skips candidates with no non-quarantined evidence (`if not safe_evidence: continue`), but the promotion gate explicitly allows evidence-free promotion via `explicit_confirmation` / `trusted_structured_input` (`core/semantic_memory.py:783-789`). An operator-confirmed, promoted fact appears on no wiki page and no digest, with no warning.

### 23. Naive local timestamps interpreted as UTC skew time filters and sorting
**File:** `core/hybrid_search.py:775`
`_parse_datetime` coerces naive timestamps to UTC, but producers write naive *local* time (`metadata_db` ingestion `created_at`, `semantic_memory.py:61`, `capture_lifecycle.py:741`). On a UTC-5 host every such timestamp is read 5 hours early: `--time-after` filters wrongly exclude/include items, and mixed naive-local vs `Z`-suffixed hits sort in the wrong order.

### 24. New `markdown` artifact type isn't in the review-policy allowlist → every import lands in needs_review
**File:** `core/artifact_review_policy.py:25`
`SUPPORTED_INGESTION_ARTIFACT_TYPES` is a hardcoded six-item frozenset consulted on every queue upsert (`core/metadata_db.py:4041`). The branch's own new imported_markdown connector enqueues `artifact_type='markdown'` (`collectors/imported_markdown_connector.py:256`), which isn't in the set, so every imported document is routed to `needs_review` as `unsupported_artifact_type` — and `tests/test_golden_connector_fixtures.py:423` bakes that fallout in as expected. (Altitude fix: derive the set from connector manifests, which already declare `artifact_types`.)

### 25. `wiki_updater`'s isinstance chain omits the new MarkdownArtifact
**File:** `core/wiki_updater.py:603`
`_artifact_type_for_artifact` maps artifact classes to type strings via an isinstance chain that doesn't include `MarkdownArtifact` (`core/artifacts/markdown.py`), so imported markdown falls through to generic `'artifact'` while `ingestion_runtime.dispatch_artifact` handles it explicitly. Same dispatch chain exists in `core/ingestion_runtime.py:387-400` and `core/canonical_identity.py:253+` — declare `artifact_type` on the artifact classes once instead.

---

## Tier 3 — Efficiency

### 26. Per-request `MetadataDB` construction runs full DDL inside the event loop
**File:** `thoth_api.py:2041` (also ~1192, ~1201)
Async endpoints construct a fresh `MetadataDB` per request; `__init__` synchronously runs the entire `_create_tables` DDL plus `Config()`/settings disk reads. Because the endpoints are `async def`, this blocks the whole FastAPI event loop, stalling all concurrent requests. Fix: construct once at startup (`app.state`), or make blocking endpoints plain `def`.

### 27. `record_llm_usage` runs CREATE TABLE + 4 CREATE INDEX on every LLM call
**File:** `core/llm_usage.py:108`
`ensure_llm_usage_schema` executes on every generate/embed call with a fresh SQLite connection, then a second connection for the INSERT. During bulk ingestion this repeats identical DDL thousands of times and serializes on the write lock. Fix: once-per-process guard + reuse one connection.

### 28. Hybrid search N+1: ~4+ queries per capture event per search request
**File:** `core/hybrid_search.py:507`
`_search_capture_events` loads every event unbounded, then per event calls `get_source`, `list_raw_refs`, `list_artifact_links`, `list_security_findings` (and more per raw_ref). 10k events ≈ 40k+ round-trips per `/api/query`. Fix: batched `IN` prefetches keyed by event_id + a source cache; push LIMIT/filtering into SQL.

### 29. Every search re-reads and re-parses every wiki page from disk
**File:** `core/hybrid_search.py:257`
`_search_wiki_pages` does O(pages) file opens + frontmatter parses per query. Fix: use the existing `archivist_corpus_fts` FTS5 index or cache parsed docs keyed by (path, mtime).

### 30. Retention inspect() is events × pages file reads plus per-target policy queries
**File:** `core/retention_service.py:457` (and `_policy_for` ~716)
`_compiled_wiki_targets` globs and parses all wiki frontmatter once per event; `_policy_for` issues one policy query per target. 1k events × 2k pages ≈ 2M file reads for one API call. Fix: build the event→pages index once per inspect(); prefetch policies with one `IN` query.

### 31. Rejected-candidate equivalence check is a full scan with Python fingerprinting on every write
**File:** `core/semantic_memory.py:909` (+ evidence N+1 at ~863)
Every add/update/transition SELECTs all rejected candidates of the type and recomputes fingerprints in Python, plus one evidence query per match. Fix: persist the (deterministic) fingerprint in an indexed column; batch evidence with `IN`.

---

## Tier 4 — Simplification / duplication / altitude

### 32. Manual field-by-field frozen-dataclass reconstruction (3 copies)
**File:** `core/semantic_memory.py:671`
`_candidate_with_timestamps`, `_evidence_with_timestamps`, `_candidate_with_promotion_audit` copy 14–19 fields by hand; any new field must be added to three blocks or is **silently dropped on update** (a real data-loss class). `dataclasses.replace(...)` does it in one line (already used in `core/capture_event_store.py:1128`).

### 33. Usage-recording blocks copy-pasted before every return/raise in llm_interface
**File:** `core/llm_interface.py:696`
~11 near-identical `_record_*_usage` call sites across `embed_texts`/`generate`; any new early-return path that forgets one silently loses telemetry. Restructure to record once at a single exit (try/finally).

### 34. Six duplicated `_now_iso` helpers with divergent semantics
**File:** `core/semantic_memory.py:61` (+ semantic_memory_review.py:29, capture_lifecycle.py:740, hybrid_search.py:619, semantic_wiki_compiler.py:70, wiki_capture_compiler.py:111)
Copies already diverge: some naive local `datetime.now()`, some UTC with `Z`. This divergence is the direct cause of finding #23. One canonical UTC helper, imported everywhere.

### 35. Semantic wiki compiler copy-pastes the managed-page write pipeline and drops provenance
**File:** `core/semantic_wiki_compiler.py:363`
`_update_page` duplicates `CaptureWikiCompiler._update_capture_page` (`core/wiki_capture_compiler.py:660`) — spec build, frontmatter merge, created_at preservation, atomic write, stale-page pruning — and the semantic copy drops `aliases`, `change_provenance`, `input_hash`/`input_manifest`, so semantic pages silently lack the provenance/lint guarantees other managed pages get. Extract one shared "write managed page from WikiPageSpec" in `core/wiki_contract.py`.

### 36. Connector dispatch via hand-maintained handlers dict duplicating registry aliasing
**File:** `core/agent_surface.py:568`
Every new connector must edit the private `handlers` dict or `run_connector` fails with "no executable adapter registered" even though the registry knows it; alias rows duplicate `ConnectorManifest.source_aliases` resolution (`core/connector_registry.py:63-72`). Carry the runner on the manifest instead.

### 37. Native-ID key table duplicated between review policy and capture lifecycle
**File:** `core/artifact_review_policy.py:251`
`_artifact_id_candidates_for_type` re-implements `_native_id_from_payload`/`keys_by_type` from `core/capture_lifecycle.py:791-807`; the copies have already drifted, so review policy and dedup can disagree on whether a payload "has a native id". Declare native-id fields once per artifact type.

### 38. Archivist schema upgrades bypass the branch's own `_ensure_columns` helper
**File:** `core/metadata_db.py:2154` (and ~2231)
Two inline PRAGMA + ALTER loops copy-paste what the new shared migration helper (`core/metadata_db.py:1664`) already does; hardening the helper won't reach these tables.

### 39. Dead code / redundant logic (minor)
- `core/semantic_memory.py:375` — `_validate_status_transition` call in `update_candidate` is dead (preceding check raises on any difference).
- `core/hybrid_search.py:1016` — `_OPEN_FINDING_STATUSES` membership test is redundant given disjoint sets; constant at line 44 effectively dead.
- `core/retention_service.py:1220` + `core/capture_event_store.py:88` — two hand-rolled `_is_relative_to` helpers; Python ≥3.9 has `Path.is_relative_to`.
- `core/agent_surface.py:408` — `retry/reject/mark_reviewed` artifact-review methods are three copy-pasted 18-line bodies differing in one method name.

### 40. Duplicated utility helpers across new modules (consolidate once)
- `_json_object`/`_json_payload` (4 copies): `core/agent_context.py:334`, `core/agent_surface.py:1240`, `core/artifact_review_policy.py:191`, `core/metadata_db.py:174` (+ near-copy `core/hybrid_search.py:867`).
- `_json_safe` recursive serializer (3 divergent copies): `core/capture_lifecycle.py:847`, `core/capture_surface.py:655`, `core/wiki_change_provenance.py:520` — divergence can skew stable-ID hashing (`_stable_json`).
- `_safe_slug` (4th copy added): `collectors/imported_markdown_connector.py:519` vs personal_transcript/pi_skill/skill_output connectors.
- `_slug_component` byte-identical in both new wiki compilers (`core/wiki_capture_compiler.py:369`, `core/semantic_wiki_compiler.py:153`) — belongs next to `normalize_wiki_slug` in `core/wiki_contract.py`.
- `_sha256_file` (5th variant added): `core/capture_event_store.py:70`, `collectors/imported_markdown_connector.py:526` vs existing copies in `core/archivist_retrieval/inventory.py:755`, `collectors/web_clipper_collector.py:467`, `core/translation_companion.py:476`.

---

## Suggested order of attack
1. **#1 (cache collision), #2 (findings never persisted), #10 (CJK bypass)** — trust/security integrity of the whole pipeline.
2. **#3, #4, #5, #6, #7, #8** — silent data loss paths.
3. **#24, #12, #13** — the new imported-markdown and transcript flows are visibly degraded today.
4. **#26–#31** — efficiency, before data volume grows.
5. Tier 4 consolidation opportunistically, alongside touching those files.
