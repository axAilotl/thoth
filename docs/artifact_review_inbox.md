# Artifact review inbox

Open **Settings → Review** (`/settings#review`). The old `/review` URL redirects
to this tab. The inbox lists ingestion rows
in `needs_review`, `blocked`, or `failed`, with separate views for rejected and
previously reviewed rows. This is an operator exception queue, not a reading list.
The **Decision history** view retains approved/retried items even after processing.

Each item shows its source path, source checksum, attempts, scanner pattern IDs,
reason, and decision history. Source bodies are not rendered as HTML or sent to a
model by this page. Flagged research can contain quoted adversarial prompts or PDF
formatting characters; a match is not proof that the whole document is malicious.

- **Approve flagged content** explicitly grants the existing ingestion security
  override for the displayed queue revision and requeues it. Requires name,
  reason and acknowledgement. Other extraction, privacy and source checks remain.
- **Retry processing** requeues a non-security failure. It cannot silently grant
  a security override.
- **Reject processing** stops processing and retains the source and audit trail.

No bulk approval is provided. Stale or competing decisions fail with HTTP 409.
Approval of local Web Clipper documents also verifies the captured source hash
and allowlisted path. Changed source files require recapture/reconciliation;
the inbox does not overwrite originals to resolve that discrepancy.

Classification-routing cases remain visible but use the existing classification
CLI for approval/correction/rejection. PDFs still undergoing automatic retry are
not in the review queue until they reach `failed`.

## Obsidian links in containers

When the `.obsidian` directory is visible, the inbox finds the containing vault.
If only the content directory is mounted, configure these optional values in
`control.json` to generate links that work on the user's desktop:

```json
{
  "review_ui": {
    "obsidian_vault_name": "_vault_v",
    "obsidian_content_prefix": "knowledge_vault"
  }
}
```

The prefix must be relative to the Obsidian vault. No Obsidian credentials or
plugin configuration need to be exposed to the container.

## API and access boundary

`GET /api/review?status=active&limit=100&offset=0` returns a bounded page and
`has_more`. `POST /api/review/decision` accepts `artifact_id`, `revision`, `action`,
`actor`, `reason`, and `security_acknowledged`. Mutations require
`X-Thoth-Review: 1`; cross-origin browser requests are rejected. The actor is a
self-reported audit name, **not an authenticated identity**.

Like the existing settings console, this is for a trusted network or an
authenticated reverse proxy. CSRF checks do not substitute for authentication;
do not expose this service publicly without an access-control boundary.
