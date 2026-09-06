# Saved prompt index

Enable `sources.corpus_index.prompt_index_enabled` to discover saved prompts
during the existing explicit-root corpus scan. Successful corpus runs through
the API, CLI connector surface, or scheduler then publish `wiki/pages/Prompt Index.md`.
The main wiki index links to it. Direct collector calls only update metadata;
the runtime owns guarded wiki publication, as it does for other derived pages.

This is a source-linked list, not a new prompt storage location. Tweets,
threads, clippings and other indexed documents remain unchanged. Each matched
source identity has one visible link, with duplicate capture associations kept
in `prompt_index_records` in the database. No prompt bodies or per-prompt
sidecar notes are written. Generic tweet titles get a short source-text label.

Detection is model-agnostic and deterministic: explicit prompt labels/blocks
become **likely prompts**, while sharing invitations and prompt-library links
become **resources and references**. Ordinary discussion of prompt engineering
is not enough. These are candidate classifications, not guarantees; no model
is called to discover or summarize them, and saved prompt instructions are
never executed. Existing corpus embedding settings continue unchanged.

Only current, allowed vault inventory is considered. Restricted/sensitive/secret
and pending/blocked security records are excluded; policy exclusions are counted
in the run report. Generated tweet summaries are not used as prompt evidence.
Image-only prompts require future OCR; a reference to ALT text does not mean
the prompt itself has been extracted. Previously unindexed source roots must
be explicitly added to the corpus allowlist to participate.

The index refreshes with the corpus schedule and drops entries that no longer
match that scan. Unchanged content is not rewritten. Existing unowned indexes
and manually edited generated indexes block replacement; inspect
`automation_state["prompt_index:publication"]` for the result. The usual guarded
publication history remains in the database. Source discovery metadata is
rebuildable from the corpus and is not synced as extra Obsidian files.
