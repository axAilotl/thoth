# Managed wiki proposals

Cissa supplies `ManagedWikiContext` with account/archive/job/lease identity,
author, granted topic IDs, UTC source cutoff, account root, and byte budgets
(`max_source_bytes`, `max_total_source_bytes`). `source_manifest` maps normalized
Thoth source paths to Cissa SourceRefs: account/archive, stable `source_id`,
`revision`, `content_hash`, local snapshot `file_sha256`, `observed_at`, and URI.
Only granted, unchanged snapshots at/before the cutoff enter retrieval. Scoped
inventory does not prune other source records. It filters before file parsing or
model embedding. Sources are verified again after generation.

`expected_pages` maps each selected output path to `{revision, content_hash}`;
both are null for a new page. Human revisions that do not match the expected
hash produce a blocked result. Imported `06ChatGPT` sources are never output
targets. Cissa's writer remains responsible for selected-vault path policy,
managed sections, area INDEX rules, current source authorization, and atomic
revision comparison when applying content.

Construct a `ManagedModelGateway(identity=context.identity(), tasks=routes,
invoke=authenticated_cissa_callback)`. Every route specifies a model and
`max_input_characters`; generation routes also specify `max_tokens`. No provider
token, dotenv lookup, Pi invocation or provider SDK is used by this gateway.
The callback enforces Cissa's actual token/cost budget and current job authority.
The configured excerpt length and default standalone generation settings live
under `llm.prompts.archivist` in settings; explicit model settings take priority.

Call the existing `run_archivist_topics` with explicit config, project root,
layout, database, `managed_context` and `llm_interface=gateway`. Results may be
`proposed`, `blocked`, `failed`, `skipped` or `dry_run`. Topic failures do not hide
successful sibling proposals. Each proposal contains identity, `proposal_id`,
relative path, author, expected revision/hash, proposed content/hash, cutoff,
source manifest and existing compiler provenance metadata.

Proposal generation creates no wiki scaffold, page, index or log and does not
mark the topic successfully compiled. Cissa applies the proposal, then calls
`acknowledge_wiki_proposal(context=..., layout=..., db=..., proposal_id=...,
receipt=...)`. The protected receipt includes the same identity, proposal ID,
`status: applied`, new `revision` and exact `content_hash`. Only a matching
receipt with still-authorized sources advances the existing dirty-check state.
Conflict/rejection leaves the proposal retryable and the success baseline
untouched. Thoth never uses this acknowledgement to write vault files.
