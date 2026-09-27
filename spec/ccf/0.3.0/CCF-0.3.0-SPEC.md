# Continuity Core Format 0.3.0

**Status:** draft
**Date:** 2026-09-24

CCF is a small, portable format for a person's own information: what exists,
where it lives, where it came from, when it changed, and how it relates to
everything else. It lets data move between devices and applications, and out
to somewhere else entirely, without losing those relationships.

CCF describes data; it does not have to hold it. A note can stay in an Obsidian
vault, a page in Notion, audio on a phone, a paper on a website. CCF records
point at those things, fingerprint them, and link them together. Applications
that want to keep the content themselves can, and CCF points there too.

0.3.0 is a new format. It shares nothing with 0.1.x or 0.2.x and is not
compatible with them.

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative.

## 1. Objects

There are three kinds of object.

- **Record** — a thing: a person, place, meeting, transcript, summary, note,
  document, task, paper, bookmark.
- **Link** — a typed relationship from one object to another.
- **Blob** — a pointer to bytes: audio, an image, a PDF, a file.

Every object is one JSON object. The JSON Schemas in `schemas/` are normative
for field names, types, and required fields. Their `$id` values are
identifiers, not download locations; validators SHOULD load all five schemas
locally.

### 1.1 Common fields

| Field | Required | Meaning |
|---|---|---|
| `id` | yes | UUIDv7, lowercase, hyphenated. See §2. |
| `kind` | yes | `record`, `link`, or `blob`. |
| `created` | yes | When this object was created. |
| `updated` | yes | When this object last changed. |
| `rev` | yes | Revision counter. Starts at `1`; increases by at least 1 on every change. |
| `source` | yes | Where the object came from. See §1.5. |
| `by` | no | Who or what produced it: `user`, `app:<name>`, or `model:<name>`. |
| `status` | no | `inferred`, `accepted`, or `rejected`. Absent means `accepted`. |
| `deleted` | no | Timestamp. Present means the object is deleted (a tombstone). |
| `ext` | no | Object for application-specific fields. |

`status` separates what a machine guessed from what a person confirmed. An
`inferred` object is a suggestion. Applications MUST NOT treat an `inferred`
object as if a person had confirmed it.

### 1.2 Record

| Field | Required | Meaning |
|---|---|---|
| `type` | yes | What the thing is. See §4. |
| `title` | no | Human-readable name. |
| `start` | no | When the thing happened or begins (events, meetings, recordings). |
| `end` | no | When it ends. |
| `loc` | no | Where the content lives. See §1.6. |
| `hash` | no | Content hash. See §3. |
| `data` | no | Small structured content for this type. |

```json
{"id":"01926f3a-0c1e-7b2a-8f10-5a1d2c3e4f50","kind":"record","type":"transcript",
 "title":"Weekly sync","created":"2026-09-20T16:02:11.000Z",
 "updated":"2026-09-20T16:02:11.000Z","rev":1,
 "source":{"app":"cissa","device":"pixel-8","native_id":"recording/8812"},
 "start":"2026-09-20T15:00:00.000Z","end":"2026-09-20T15:48:30.000Z",
 "loc":"vault:meetings/2026-09-20-weekly-sync.md","hash":"sha256:…"}
```

### 1.3 Link

| Field | Required | Meaning |
|---|---|---|
| `from` | yes | ID of the source object. |
| `rel` | yes | Relationship name. See §4. |
| `to` | yes | ID of the target object. |
| `data` | no | Detail about the relationship, such as a quoted span or timestamp range. |

Links are directional. `A derived_from B` means A was produced from B.

```json
{"id":"01926f3a-0d00-7c11-9a22-6b3c4d5e6f70","kind":"link",
 "from":"01926f3a-0c9f-7d00-8e11-1a2b3c4d5e6f","rel":"derived_from",
 "to":"01926f3a-0c1e-7b2a-8f10-5a1d2c3e4f50",
 "created":"2026-09-20T16:05:00.000Z","updated":"2026-09-20T16:05:00.000Z","rev":1,
 "source":{"app":"cissa"},"by":"model:qwen3-4b","status":"inferred"}
```

A Link endpoint MAY name an object that is not present locally. A missing
endpoint is not an error.

### 1.4 Blob

| Field | Required | Meaning |
|---|---|---|
| `loc` | yes | Where the bytes live. |
| `hash` | yes | SHA-256 of the bytes. See §3. |
| `media_type` | yes | IANA media type, for example `audio/ogg`. |
| `size` | yes | Byte length. |
| `title` | no | Human-readable name. |
| `data` | no | Small structured metadata, such as duration or page count. |

A Record is attached to a Blob with a Link, normally `rel: "has_file"`.

### 1.5 Source

`source` says where the object came from.

| Field | Required | Meaning |
|---|---|---|
| `app` | yes | The application that created the object, such as `cissa` or `thoth`. |
| `device` | no | The device or installation that created it. |
| `native_id` | no | The item's identifier in the originating system. |
| `uri` | no | Where the original item can be found, such as a URL. |

`source` is set when the object is created and does not change.

### 1.6 Locations

`loc` is a URI string that says where content lives. Any scheme is allowed.
Common forms:

- `vault:<path>` — a path relative to the user's notes root, such as an
  Obsidian vault;
- `file:<path>` — a local file;
- `https://…` — a web resource;
- an application scheme such as `notion:<page-id>` or `x-cissa:<id>`.

An application that cannot resolve a `loc` MUST keep it unchanged.

## 2. Identifiers and times

**IDs.** Every object ID is a UUIDv7 (RFC 9562) in lowercase hyphenated form.
The device or application that creates the object generates its ID. An ID MUST
NOT be changed or reassigned, including on sync, import, and export. Because
IDs are global, objects created on different devices never need renumbering.

**Timestamps.** All timestamps are UTC in exactly this form:
`YYYY-MM-DDTHH:MM:SS.sssZ` (three fractional digits, `Z` suffix).

**Numbers.** Every number anywhere in a CCF object MUST be written as a plain
integer (no fraction or exponent, so `1.0` and `1e3` are invalid) between
-(2^53 - 1) and 2^53 - 1. Represent other values as strings, for example
`"0.93"` or `"36.1627,-86.7816"`. This keeps hashing identical across languages.

## 3. Hashes

Hashes let you check that the thing an object points to or carries is still
what was there when it was recorded. Hashes are written as `sha256:` followed by
64 lowercase hexadecimal digits.

**Blob.** `hash` is the SHA-256 of the raw bytes.

**Record with `loc`.** `hash` is the SHA-256 of the raw bytes of the content at
`loc` when the Record was last updated.

**Record without `loc`.** `hash` is the SHA-256 of the canonical JSON (§3.1) of
`data`. A Record without `loc` or `data` has no `hash`.

**Link.** Links have no hash.

A hash mismatch means the content has changed since it was recorded. It is
information, not an error: an application SHOULD surface it and MUST NOT stop
processing because of it.

### 3.1 Canonical JSON

Canonical JSON is the UTF-8 encoding of a JSON value written with:

- no whitespace outside strings;
- object members sorted by key, comparing keys as sequences of UTF-16 code
  units;
- integers in plain decimal, with no leading zeros, no plus sign, and no
  fraction or exponent;
- strings escaped as `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, and every other
  character below U+0020 as `\u00xx` with lowercase hexadecimal; every other
  character is written as itself, unescaped.

Unpaired surrogates and duplicate keys are invalid. For integer-only JSON this is
the same output as RFC 8785 (JCS), so any JCS library produces it.

`vectors/hashes.json` contains test cases. Implementations MUST reproduce every
vector.

## 4. Types and relationships

`type` and `rel` are lowercase strings. Applications SHOULD use these core names
when they fit:

**Record types:** `person`, `org`, `place`, `event`, `meeting`, `conversation`,
`recording`, `transcript`, `summary`, `note`, `document`, `task`, `message`,
`bookmark`, `paper`, `topic`, `fact`.

**Relationships:**

| `rel` | Meaning |
|---|---|
| `derived_from` | `from` was produced from `to` (a summary from a transcript). |
| `has_file` | `from` has the Blob `to` as its content or attachment. |
| `part_of` | `from` is a part of `to` (a segment of a recording, a page of a document). |
| `mentions` | `from` refers to `to`. |
| `about` | `from` is mainly about `to`. |
| `participant` | `to` took part in the event or conversation `from`. |
| `same_as` | `from` and `to` are the same real-world thing. |
| `related` | A general association. |

Application-specific names use a dotted prefix, such as `thoth.bookmark` or
`cissa.speaker_turn`. Unknown types and relationships MUST be preserved and
MUST NOT be rejected.

Generated content SHOULD carry a `derived_from` Link to every source it was
produced from, so a summary can always be traced back to its transcript and a
fact to the documents it came from.

## 5. Changes, deletion, and sync

**Changes.** Changing an object replaces it in place with a new version: `rev`
increases and `updated` is set to the time of the change. `id`, `kind`,
`created`, and `source` MUST NOT change. CCF does not require history; an
application MAY keep old versions.

**Deletion.** Deleting an object sets `deleted` to the deletion time and
increases `rev`. The tombstone keeps `id`, `kind`, `created`, `updated`, `rev`,
`source`, `deleted`, and, for Records, `type`. All other fields SHOULD be
removed. Tombstones sync like any other change so every device learns about the
deletion. Deleting the underlying content at `loc` is up to the application.

**Merging.** When two copies of the same `id` meet, the winner is the copy with:

1. the higher `rev`; then, if equal,
2. the later `updated`; then, if equal,
3. the greater `source.device`, compared as UTF-8 bytes, with a missing
   device ordering first; then, if equal,
4. the greater canonical JSON (§3.1) of the whole object, compared as bytes.

Every implementation that applies this rule to the same inputs ends with the
same winner. When the copies differ and have the same `rev`, the two devices
edited the object independently. The application SHOULD keep the losing copy
and show the conflict to the person. A conflict MUST NOT block syncing or
processing of other objects.

`rev` detects a conflict only when both copies have the same `rev`. If one
device edits an object twice while another edits it once, the first device's
copy wins without a conflict being flagged. An application that needs to detect
every concurrent edit must keep its own per-device history, which CCF does not
require.

**Sync.** Devices exchange objects, not operations. For each peer, a device
tracks which of its objects that peer has not yet acknowledged, for example with
a local change counter or a dirty flag, and sends those objects. It MUST NOT use
`updated` to decide what to send: device clocks drift and can move backwards,
and objects stamped before a timestamp cursor would never sync. `updated` is
only for the merge rule and for display. The receiver applies the merge rule to
each object it receives. The transport, API, and scheduling are up to the
application.
Objects created while two devices were disconnected simply appear on the other
side when they reconnect; nothing is renumbered.

## 6. Export

An export is a directory, or a ZIP of that directory:

```text
manifest.json
records.jsonl
links.jsonl
blobs.jsonl
files/            optional
```

- Each `.jsonl` file holds one object per line, UTF-8, of the matching kind. An
  empty kind MAY be an empty file or omitted.
- `files/` holds copies of Blob bytes and of Record `loc` content, each named by
  its hash's 64 hex digits with no extension. An importer finds a copy by the
  object's `hash`, whatever its original `loc` was.
- `manifest.json` follows `schemas/manifest.schema.json`:

```json
{"format":"ccf-export","version":"0.3.0","exported_at":"2026-09-24T12:00:00.000Z",
 "source":{"app":"thoth","device":"homelab"},
 "counts":{"records":4,"links":3,"blobs":1,"files":1}}
```

Importing an export means applying the merge rule (§5) to every object in it.
An export keeps every ID, so all Links survive the move.

## 7. Handling problems

CCF never stops the machinery. If an object fails schema validation, fails a
hash check, conflicts, or refers to something missing, an implementation MUST:

- keep processing every other object;
- retain the problem object unchanged, or report exactly which object was
  skipped and why;
- MUST NOT reject a whole sync, export, import, or pipeline because of
  individual objects.

Unknown fields inside `ext`, `data`, and `source` MUST be preserved. Unknown
top-level fields SHOULD be preserved.

## 8. External files

CCF puts as little as possible into files it does not own. The only field CCF
places in an external file, such as Obsidian frontmatter, is the object ID:

```yaml
ccf_id: 01926f3a-0c1e-7b2a-8f10-5a1d2c3e4f50
```

Everything else lives in the application's own CCF store. An application MUST
NOT require other CCF fields in external files.

## 9. Storage

CCF does not prescribe storage. A store is correct if it can produce the
objects in this specification, apply the merge rule, and track what each peer
has acknowledged. A single SQLite or Postgres table per kind, keyed by `id`,
plus a local change counter per row, is enough.

## 10. Security (informative)

CCF has no per-object encryption, signatures, or keys. Protect CCF data the
same way as the content it describes: disk or database encryption at rest,
encrypted export archives (for example with `age`), authenticated APIs for
sync, and database row-level security for multi-tenant deployments. Content
hashes detect changes; they are not a tamper-proof audit trail.

## 11. Conformance

An implementation conforms if it:

1. produces objects that validate against `schemas/`;
2. reproduces every vector in `vectors/hashes.json`;
3. applies the merge rule in §5 and reproduces every vector in
   `vectors/merge.json`;
4. reads and writes the export layout in §6;
5. follows the problem handling in §7.

`tools/validate.py` checks an export directory and the vectors.
