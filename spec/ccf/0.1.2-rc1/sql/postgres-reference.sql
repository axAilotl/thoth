-- CCF 0.1.2-rc1 — PostgreSQL reference operational envelope
-- Canonical semantic kinds remain Record, Link, and Blob. Additional tables are
-- custody, admission, journal, sync, and rebuildable projection machinery.

CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- Optional: CREATE EXTENSION IF NOT EXISTS vector;
-- Optional: CREATE EXTENSION IF NOT EXISTS age;

CREATE DOMAIN ccf_digest AS text
  CHECK (VALUE ~ '^sha256:[0-9a-f]{64}$');

CREATE DOMAIN ccf_timestamp AS timestamptz;

CREATE TYPE ccf_object_kind AS ENUM ('record', 'link', 'blob');
CREATE TYPE ccf_compartment_kind AS ENUM ('structural', 'semantic', 'blob_content');
CREATE TYPE ccf_compartment_state AS ENUM ('plaintext', 'encrypted', 'withheld', 'erased');
CREATE TYPE ccf_availability_state AS ENUM ('available', 'withheld', 'erased', 'external');
CREATE TYPE ccf_admission_status AS ENUM ('admitted', 'existing', 'quarantined', 'rejected');
CREATE TYPE ccf_batch_status AS ENUM ('queued', 'verifying', 'terminal', 'conflict');
CREATE TYPE ccf_batch_disposition AS ENUM ('accepted', 'partially_accepted', 'content_rejected', 'quarantined');

CREATE TABLE ccf_archive (
    archive_id                  text PRIMARY KEY,
    epoch_id                    text NOT NULL,
    genesis_commit_hash         ccf_digest NOT NULL,
    hash_profile                text NOT NULL CHECK (hash_profile = 'ccf-jcs-sha256-v2'),
    signature_profile           text NOT NULL CHECK (signature_profile = 'ed25519-jcs-v1'),
    semantic_catalog_root       ccf_digest NOT NULL,
    created_at                  ccf_timestamp NOT NULL,
    CHECK (archive_id ~ '^urn:ccf:archive:'),
    CHECK (epoch_id ~ '^urn:ccf:lineage:')
);

CREATE TABLE ccf_record (
    id                          text PRIMARY KEY,
    spec                        text NOT NULL CHECK (spec = 'ccf/0.1.2-rc1'),
    hash_profile                text NOT NULL CHECK (hash_profile = 'ccf-jcs-sha256-v2'),
    structural_commitment       ccf_digest NOT NULL,
    semantic_commitment         ccf_digest,
    object_hash                 ccf_digest NOT NULL UNIQUE,
    CHECK (id ~ '^urn:ccf:record:')
);

CREATE TABLE ccf_link (
    id                          text PRIMARY KEY,
    spec                        text NOT NULL CHECK (spec = 'ccf/0.1.2-rc1'),
    hash_profile                text NOT NULL CHECK (hash_profile = 'ccf-jcs-sha256-v2'),
    structural_commitment       ccf_digest NOT NULL,
    semantic_commitment         ccf_digest,
    object_hash                 ccf_digest NOT NULL UNIQUE,
    CHECK (id ~ '^urn:ccf:link:')
);

CREATE TABLE ccf_blob (
    id                          text PRIMARY KEY,
    spec                        text NOT NULL CHECK (spec = 'ccf/0.1.2-rc1'),
    hash_profile                text NOT NULL CHECK (hash_profile = 'ccf-jcs-sha256-v2'),
    structural_commitment       ccf_digest NOT NULL,
    semantic_commitment         ccf_digest,
    object_hash                 ccf_digest NOT NULL UNIQUE,
    CHECK (id ~ '^urn:ccf:blob:')
);

CREATE TABLE ccf_compartment (
    object_kind                 ccf_object_kind NOT NULL,
    object_id                   text NOT NULL,
    compartment                ccf_compartment_kind NOT NULL,
    state                       ccf_compartment_state NOT NULL,
    availability                ccf_availability_state NOT NULL,
    format                      text,
    salt                        bytea,
    plaintext_json              jsonb,
    ciphertext                  bytea,
    ciphertext_digest           ccf_digest,
    encryption_profile          text,
    key_ref                     text,
    storage_ref                 text,
    erased_by_record_id         text,
    source_custody_proof        text,
    unavailability_lineage_id   text,
    updated_at                  ccf_timestamp NOT NULL,
    PRIMARY KEY (object_kind, object_id, compartment),
    CHECK (
      (state = 'plaintext' AND plaintext_json IS NOT NULL AND ciphertext IS NULL)
      OR (state = 'encrypted' AND ciphertext IS NOT NULL AND plaintext_json IS NULL)
      OR (state IN ('withheld', 'erased') AND plaintext_json IS NULL AND ciphertext IS NULL)
    ),
    CHECK (state <> 'erased' OR salt IS NULL)
    ,CHECK (
      (availability = 'available' AND state IN ('plaintext', 'encrypted'))
      OR (availability = 'withheld' AND state = 'withheld')
      OR (availability = 'erased' AND state = 'erased')
      OR (availability = 'external' AND state = 'withheld')
    )
);

CREATE INDEX ccf_compartment_state_idx
    ON ccf_compartment (state, object_kind, compartment);

CREATE TABLE ccf_blob_content (
    blob_id                     text PRIMARY KEY REFERENCES ccf_blob(id),
    state                       ccf_compartment_state NOT NULL,
    availability                ccf_availability_state NOT NULL,
    byte_length                 numeric(20,0),
    plaintext_bytes             bytea,
    ciphertext                  bytea,
    ciphertext_digest           ccf_digest,
    storage_ref                 text,
    encryption_profile          text,
    key_ref                     text,
    content_salt                bytea,
    erased_by_record_id         text,
    source_custody_proof        text,
    unavailability_lineage_id   text,
    updated_at                  ccf_timestamp NOT NULL,
    CHECK (
      (state = 'plaintext' AND plaintext_bytes IS NOT NULL AND ciphertext IS NULL)
      OR (state = 'encrypted' AND ciphertext IS NOT NULL AND plaintext_bytes IS NULL)
      OR (state IN ('withheld', 'erased') AND plaintext_bytes IS NULL AND ciphertext IS NULL)
    ),
    CHECK (state <> 'erased' OR content_salt IS NULL)
    ,CHECK (
      (availability = 'available' AND state IN ('plaintext', 'encrypted'))
      OR (availability = 'withheld' AND state = 'withheld')
      OR (availability = 'erased' AND state = 'erased')
      OR (availability = 'external' AND state = 'withheld')
    )
);

CREATE TABLE ccf_admission (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    commit_sequence             numeric(20,0) NOT NULL,
    commit_position             integer NOT NULL CHECK (commit_position >= 0),
    object_kind                 ccf_object_kind NOT NULL,
    object_id                   text NOT NULL,
    object_hash                 ccf_digest NOT NULL,
    admitted_at                 ccf_timestamp NOT NULL,
    PRIMARY KEY (archive_id, commit_sequence, commit_position),
    UNIQUE (archive_id, object_kind, object_id),
    UNIQUE (archive_id, commit_sequence, object_kind, object_id)
);

CREATE INDEX ccf_admission_object_idx
    ON ccf_admission (archive_id, object_kind, object_id);

CREATE TABLE ccf_origin_index (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    source_id                   text NOT NULL,
    native_id                   text NOT NULL,
    revision                    text NOT NULL,
    submission_hash             ccf_digest NOT NULL,
    object_kind                 ccf_object_kind NOT NULL,
    object_id                   text NOT NULL,
    lifecycle                   text NOT NULL DEFAULT 'active',
    PRIMARY KEY (archive_id, source_id, native_id, revision, object_kind)
);

CREATE TABLE ccf_producer_batch (
    batch_id                    text PRIMARY KEY,
    producer_id                 text NOT NULL,
    producer_sequence           numeric(20,0) NOT NULL,
    previous_batch_hash         ccf_digest,
    credential_id               text NOT NULL,
    created_at                  ccf_timestamp NOT NULL,
    semantic_catalog_root       ccf_digest NOT NULL,
    batch_hash                  ccf_digest NOT NULL UNIQUE,
    signature_profile           text NOT NULL,
    signature                   bytea NOT NULL,
    canonical_batch_json        jsonb NOT NULL,
    status                      ccf_batch_status NOT NULL,
    disposition                 ccf_batch_disposition,
    disposition_reason          text,
    received_at                 ccf_timestamp NOT NULL,
    committed_sequence          numeric(20,0),
    result_json                 jsonb,
    CHECK ((status = 'terminal') = (disposition IS NOT NULL)),
    UNIQUE (producer_id, producer_sequence)
);

CREATE TABLE ccf_producer_head (
    producer_id                 text PRIMARY KEY,
    producer_sequence           numeric(20,0) NOT NULL,
    batch_hash                  ccf_digest NOT NULL,
    credential_id               text NOT NULL,
    updated_at                  ccf_timestamp NOT NULL
);

CREATE TABLE ccf_commit_journal (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    sequence                    numeric(20,0) NOT NULL,
    commit_record_id            text NOT NULL REFERENCES ccf_record(id),
    parent_commit_hash          ccf_digest,
    commit_hash                 ccf_digest NOT NULL UNIQUE,
    batch_merkle_root           ccf_digest NOT NULL,
    member_count                numeric(20,0) NOT NULL,
    signer_key_id               text NOT NULL,
    semantic_catalog_root       ccf_digest NOT NULL,
    committed_at                ccf_timestamp NOT NULL,
    PRIMARY KEY (archive_id, sequence),
    UNIQUE (archive_id, commit_record_id)
);

CREATE TABLE ccf_commit_member (
    archive_id                  text NOT NULL,
    commit_sequence             numeric(20,0) NOT NULL,
    commit_position             integer NOT NULL,
    object_kind                 ccf_object_kind NOT NULL,
    object_id                   text NOT NULL,
    object_hash                 ccf_digest NOT NULL,
    admitted_at                 ccf_timestamp NOT NULL,
    leaf_hash                   ccf_digest NOT NULL,
    PRIMARY KEY (archive_id, commit_sequence, commit_position),
    FOREIGN KEY (archive_id, commit_sequence)
      REFERENCES ccf_commit_journal(archive_id, sequence)
);

CREATE TABLE ccf_archive_head (
    archive_id                  text PRIMARY KEY REFERENCES ccf_archive(archive_id),
    sequence                    numeric(20,0) NOT NULL,
    commit_record_id            text NOT NULL,
    commit_hash                 ccf_digest NOT NULL,
    semantic_catalog_root       ccf_digest NOT NULL,
    signer_key_id               text NOT NULL,
    updated_at                  ccf_timestamp NOT NULL
);

CREATE TABLE ccf_lineage_head (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    lineage_id                  text NOT NULL,
    head_record_id              text NOT NULL REFERENCES ccf_record(id),
    head_record_hash            ccf_digest NOT NULL,
    head_commit_sequence        numeric(20,0) NOT NULL,
    state                       text NOT NULL,
    valid_from                  ccf_timestamp NOT NULL,
    expires_at                  ccf_timestamp,
    PRIMARY KEY (archive_id, lineage_id)
);

CREATE TABLE ccf_generation_fence (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    namespace                   text NOT NULL,
    subject_key                 text NOT NULL,
    generation                  numeric(20,0) NOT NULL,
    changed_at_sequence         numeric(20,0) NOT NULL,
    direction                   text NOT NULL CHECK (direction IN ('tighten', 'widen', 'unknown')),
    cause_object_id             text NOT NULL,
    PRIMARY KEY (archive_id, namespace, subject_key)
);

CREATE TABLE ccf_projection_invalidation (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    projection_name             text NOT NULL,
    target_key                  text,
    cause_object_kind           ccf_object_kind,
    cause_object_id             text,
    cause_commit_sequence       numeric(20,0) NOT NULL,
    priority                    integer NOT NULL DEFAULT 0,
    status                      text NOT NULL CHECK (status IN ('queued','running','done','failed')),
    created_at                  ccf_timestamp NOT NULL,
    resolved_at                 ccf_timestamp,
    last_error                  text
);

CREATE INDEX ccf_projection_invalidation_work_idx
    ON ccf_projection_invalidation (projection_name, status, priority DESC, cause_commit_sequence);

CREATE TABLE projection_active_link (
    link_id                     text PRIMARY KEY REFERENCES ccf_link(id),
    type                        text,
    from_id                     text,
    to_id                       text,
    state                       text NOT NULL,
    selector_available          boolean NOT NULL,
    effective_through_sequence  numeric(20,0) NOT NULL,
    generation                  numeric(20,0) NOT NULL
);

CREATE TABLE projection_derivation_closure (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    ancestor_id                 text NOT NULL,
    descendant_id               text NOT NULL,
    minimum_depth               integer NOT NULL CHECK (minimum_depth > 0),
    active_path_count           numeric(20,0) NOT NULL CHECK (active_path_count > 0),
    computed_through_sequence   numeric(20,0) NOT NULL,
    generation                  numeric(20,0) NOT NULL,
    PRIMARY KEY (archive_id, generation, ancestor_id, descendant_id)
);

CREATE INDEX projection_derivation_desc_idx
    ON projection_derivation_closure (archive_id, generation, descendant_id, ancestor_id);

CREATE TABLE projection_entity_cluster (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    member_id                   text NOT NULL,
    cluster_id                  text NOT NULL,
    canonical_person_id         text,
    generation                  numeric(20,0) NOT NULL,
    computed_through_sequence   numeric(20,0) NOT NULL,
    PRIMARY KEY (archive_id, generation, member_id)
);

CREATE TABLE projection_policy_closure (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    object_id                   text NOT NULL,
    closure_hash                ccf_digest,
    closure_json                jsonb,
    status                      text NOT NULL CHECK (status IN ('clean','dirty','resolving','failed')),
    computed_through_sequence   numeric(20,0),
    dependency_generations      jsonb NOT NULL,
    latest_affecting_sequence   numeric(20,0) NOT NULL,
    last_error                  text,
    PRIMARY KEY (archive_id, object_id)
);

CREATE TABLE projection_authorization_cache (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    object_set_hash             ccf_digest NOT NULL,
    decision_context_hash       ccf_digest NOT NULL,
    policy_closure_hash         ccf_digest NOT NULL,
    decision_json               jsonb NOT NULL,
    evaluated_at_sequence       numeric(20,0) NOT NULL,
    generation_vector           jsonb NOT NULL,
    valid_until                 ccf_timestamp,
    PRIMARY KEY (archive_id, object_set_hash, decision_context_hash, policy_closure_hash)
);

-- Rebuildable acceleration only. Each row must resolve to a chain-covered
-- lineage.suppression_set Record and its governed Blob. Deleting this table
-- never removes canonical suppression authority.
CREATE TABLE projection_suppression_lookup (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    suppression_profile         text NOT NULL,
    keyed_commitment            bytea NOT NULL,
    suppression_set_record_id   text NOT NULL REFERENCES ccf_record(id),
    suppression_blob_id         text NOT NULL REFERENCES ccf_blob(id),
    erasure_receipt_id          text NOT NULL REFERENCES ccf_record(id),
    source_commit_sequence      numeric(20,0) NOT NULL,
    generation                  numeric(20,0) NOT NULL,
    PRIMARY KEY (archive_id, suppression_profile, keyed_commitment)
);

CREATE TABLE projection_checkpoint (
    archive_id                  text NOT NULL REFERENCES ccf_archive(archive_id),
    projection_name             text NOT NULL,
    generation                  numeric(20,0) NOT NULL,
    through_commit_sequence     numeric(20,0) NOT NULL,
    dependency_generations      jsonb NOT NULL,
    snapshot_digest             ccf_digest NOT NULL,
    storage_ref                 text NOT NULL,
    encryption_profile          text,
    key_ref                     text,
    created_at                  ccf_timestamp NOT NULL,
    PRIMARY KEY (archive_id, projection_name, generation)
);

-- Optional projections.
CREATE TABLE projection_full_text (
    object_id                   text PRIMARY KEY,
    document                    tsvector NOT NULL,
    source_commit_sequence      numeric(20,0) NOT NULL,
    generation                  numeric(20,0) NOT NULL
);

-- Multi-schema-safe pgvector fixture. Extension discovery uses pg_extension's
-- namespace instead of assuming public or the current search_path.
DO $ccf_vector$
DECLARE
  vector_schema name;
BEGIN
  SELECT n.nspname INTO vector_schema
    FROM pg_extension AS e
    JOIN pg_namespace AS n ON n.oid = e.extnamespace
   WHERE e.extname = 'vector';
  IF vector_schema IS NOT NULL THEN
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS projection_embedding (' ||
      'object_id text NOT NULL, model_id text NOT NULL, ' ||
      'embedding %I.vector(1536) NOT NULL, ' ||
      'source_commit_sequence numeric(20,0) NOT NULL, ' ||
      'generation numeric(20,0) NOT NULL, ' ||
      'PRIMARY KEY (object_id, model_id))',
      vector_schema
    );
  END IF;
END
$ccf_vector$;

-- Admission transaction requirements:
-- 1. lock ccf_archive_head FOR UPDATE;
-- 2. verify producer and origin uniqueness;
-- 3. validate lineage predecessor and cycle constraints;
-- 4. insert portable headers and compartments;
-- 5. advance generation fences before stale authorization can be served;
-- 6. insert admissions, members, commit Record, and journal row;
-- 7. update ccf_archive_head;
-- 8. commit, then acknowledge the producer.
-- A cryptographically valid batch receives a durable terminal disposition even
-- when every content item is rejected; its hash remains eligible as a producer
-- predecessor. A missing exact producer predecessor remains queued and retryable.
-- Invalid signatures, credentials, hashes, resource bounds, or outer envelopes
-- never advance producer-chain state.
-- Chain verification must prove a bijection between ccf_commit_member and
-- ccf_admission for every verified range, matching sequence, position, kind,
-- object ID, object hash, and admission time. Missing, duplicate, or mutated
-- admission coordinates fail verification. The commit Record self-exclusion is
-- the only special case.
