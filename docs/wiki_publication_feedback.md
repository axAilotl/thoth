# Wiki publication, edits, and archivist feedback

Topic Markdown is a reading surface: title, compact identity/type/update
frontmatter, synthesis, readable source citations, and optional human feedback.
Full source manifests, influence records and input/change provenance live in
`wiki_publication_metadata` in the configured THOTH database, not sidecar notes.
The database and its backups belong outside the Obsidian vault.

## Human edits are preserved

The publisher records its exact output as a baseline in `wiki_publications` and
retains observed/adopted/generated text in `wiki_publication_revisions`.
Before research and immediately before publishing, it checks the current page.

* A new page can be generated normally.
* An unchanged generated page can be regenerated normally.
* A page changed only by explicit feedback callouts can be regenerated, retaining
  those callouts verbatim (they are moved together to the end of the new page).
* Other edits block publication, even with `force=True`. Their observed text is
  retained in the database. THOTH does not guess which prose is expendable.
* A missing previously generated page is not silently resurrected.
* A pre-existing page with no recorded baseline is **unowned** and cannot be
  automatically replaced. Frontmatter claiming THOTH ownership is not proof.

Publication uses an outside-vault per-page lock to serialize THOTH writers, a
fresh hash check immediately before atomic replacement, and unique temporary
files cleaned after failure. External editors/sync clients do not participate in
THOTH's lock, so this is not a cross-application filesystem transaction. Keep
Obsidian revision/sync backups too. A crash after a file replacement but before
the database commits fails closed on the next pass, rather than assuming that a
mismatched file is safe to replace.

## Leave a research request

```markdown
> [!thoth-feedback]
> Explain the streaming latency tradeoffs more deeply.
> Include the ASR papers I added recently, where relevant.
```

Only top-level callouts in owned wiki pages are recognized. Fenced examples,
nested quotations, frontmatter, and imported source documents are not scanned
for human instructions. Feedback is added to retrieval query text and writing
context without expanding configured roots, filters, security rules, or tool
permissions. A local wiki edit is treated as an annotation from someone with
write access to that vault; it is **not authenticated human identity**. Do not
give untrusted automation write access to that surface. Model output cannot
create actionable feedback callouts.

`wiki_feedback` retains the exact block, request text, active flag, status and
the revision in which it was included. New/changed feedback is `pending` and
triggers another compilation even if sources are unchanged. Successful
publication sets `included`: this means submitted to research and writing, not
that THOTH has verified fulfillment. `addressed` and `needs_clarification` are
explicit operator/agent statuses. Removed blocks become inactive but remain in
history. Unchanged active annotations remain available on later regenerations.
Topic descriptions and configured prompt files remain the standing instructions.

## Reconcile changes missed while offline

The compiler checks immediately on each run. The registered `wiki_reconcile`
connector also observes known publications and configured topic paths, persisting
feedback and reporting conflicts without generating anything or editing pages.
Enable it through the existing connector scheduler:

```json
{
  "sources": {
    "wiki_reconcile": {
      "enabled": true,
      "schedule": {
        "enabled": true,
        "interval_seconds": 120,
        "run_on_startup": true
      }
    }
  }
}
```

The existing connector execution API can run `wiki_reconcile` on demand. Its
last report is `automation_state["wiki_reconcile:last_result"]`. This observes
only tracked/configured wiki publications, not all Obsidian files.

## Operator inspection and baseline adoption

From the configured THOTH runtime:

```bash
python -m core.wiki_publication_cli inspect pages/topic-asr.md
python -m core.wiki_publication_cli adopt pages/topic-asr.md --expected-hash HASH_FROM_INSPECTION
python -m core.wiki_publication_cli feedback-status pages/topic-asr.md --feedback-id ID --status addressed
```

Adoption is an explicit migration operation, **not a conflict override**. Verify
the page contains generated-only prose and archive its original version before
adopting it. Adoption preserves every byte and requires the inspected SHA-256;
it refuses changed files or an already established baseline. Do not adopt a
human-written or mixed-ownership page just to make a blocked run proceed.
For metadata-preserving migrations use
`WikiPublicationStore.adopt_baseline(path, expected_hash=..., metadata=...)`.
The compiler uses `metadata_for(path)` for previous input provenance and
`publish(path, generated_content, snapshot=..., metadata=...)` to store the new
metadata alongside its exact published revision.

Back up the complete THOTH SQLite database (using SQLite's backup API when
online) together with the original source vault. Metadata-only CCF export and
restore must include these publication/feedback records; Markdown alone is no
longer a complete backup of THOTH's working state.
