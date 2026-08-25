import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  blobContentCommitment,
  canonicalDigest,
  canonicalize,
  compartmentCommitment,
  objectHash,
  semanticCatalogRoot,
  submissionHash,
} from '../../0.1.2/tools/ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2');
const VECTORS = path.join(BASE, 'vectors');
const EXAMPLE = path.join(BASE, 'examples', 'personal-archive');
let checks = 0;

function check(condition, label) {
  checks += 1;
  if (!condition) throw new Error(`FAIL: ${label}`);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function readNdjson(file) {
  return fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map(JSON.parse);
}

const canonicalization = readJson(path.join(VECTORS, 'canonicalization.json'));
for (const vector of canonicalization.cases) {
  check(canonicalize(vector.value) === vector.expected, `canonical ${vector.name}`);
  check(
    canonicalDigest('ccf:canonicalization-vector:v1', vector.value) === vector.digest,
    `canonical digest ${vector.name}`,
  );
}
check(canonicalize(1e-7) === '1e-7', 'JCS exponent formatting regression');
check(
  canonicalize({ '\ue000': 1, '😀': 2 }) === '{"😀":2,"":1}',
  'JCS UTF-16 property ordering regression',
);

const objects = readJson(path.join(VECTORS, 'object-hashes.json'));
for (const [kind, vector] of Object.entries(objects)) {
  check(vector.header.spec === 'ccf/0.1.2', `${kind} portable format remains 0.1.2`);
  check(
    compartmentCommitment(kind, 'structural', vector.structural) === vector.expected_structural_commitment,
    `${kind} structural commitment`,
  );
  check(
    compartmentCommitment(kind, 'semantic', vector.semantic) === vector.expected_semantic_commitment,
    `${kind} semantic commitment`,
  );
  check(objectHash(vector.header) === vector.expected_object_hash, `${kind} object hash`);
}
check(
  blobContentCommitment(
    objects.blob.semantic.content.content_salt,
    fs.readFileSync(path.join(EXAMPLE, 'segment-1842.wav')),
  ) === objects.blob.expected_content_commitment,
  'Blob content commitment',
);

const batch = readJson(path.join(VECTORS, 'producer-batch.json')).batch;
const submissionVectors = readJson(path.join(VECTORS, 'submission-hashes.json'));
const submissions = [...batch.records, ...batch.links, ...batch.blobs];
for (const vector of [
  ...submissionVectors.records,
  ...submissionVectors.links,
  ...submissionVectors.blobs,
]) {
  const submission = submissions.find((item) => item.id === vector.id);
  check(Boolean(submission), `submission present ${vector.id}`);
  check(submissionHash(submission) === vector.expected_submission_hash, `submission hash ${vector.id}`);
}

const capsule = path.join(ROOT, 'examples', 'capsule');
const operationVectors = readJson(path.join(ROOT, 'vectors', 'canonical-store-operations.json'));
const operationById = new Map(operationVectors.cases.map((entry) => [entry.id, entry]));
const capsuleManifest = readJson(path.join(capsule, 'manifest.json'));
const capsuleSubmissions = [
  ...readNdjson(path.join(capsule, 'submissions', 'records.ndjson')),
  ...readNdjson(path.join(capsule, 'submissions', 'links.ndjson')),
];
const capsuleById = new Map(capsuleSubmissions.map((submission) => [submission.id, submission]));
const baseSchemaCatalog = readJson(path.join(BASE, 'schemas', 'catalog.json'));
const schemaDigestById = new Map(
  baseSchemaCatalog.schemas.map((entry) => [entry.id, entry.digest]),
);
const typeEntries = readJson(path.join(BASE, 'registries', 'types.registry.json')).entries;
const linkEntries = readJson(path.join(BASE, 'registries', 'links.registry.json')).entries;
const typeEntryByKey = new Map(typeEntries.map((entry) => [`${entry.name}\0${entry.version}`, entry]));
const linkEntryByKey = new Map(linkEntries.map((entry) => [`${entry.name}\0${entry.version}`, entry]));

function fixtureSalt(submission, compartment) {
  return crypto.createHash('sha256')
    .update(`ccf-0.2.0-capsule-uplift-vector\0${submission.id}\0${compartment}`)
    .digest()
    .toString('base64url');
}

function canonicalizeCapsuleSubmission(submission) {
  const kind = submission.submission_kind;
  const registryEntry = kind === 'record'
    ? typeEntryByKey.get(`${submission.type}\0${submission.type_version}`)
    : linkEntryByKey.get(`${submission.type}\0${submission.type_version}`);
  check(Boolean(registryEntry), `Capsule registry entry ${submission.id}`);
  const semanticSchemaId = kind === 'record'
    ? registryEntry.semantic_schema_id
    : 'urn:ccf:schema:0.1.2:objects.link-semantic-content';
  const structuralContent = {
    type: submission.type,
    type_version: submission.type_version,
    type_visibility: submission.type_visibility,
    schema_digest: schemaDigestById.get(semanticSchemaId),
    registry_entry_digest: canonicalDigest('ccf:registry-entry:v1', registryEntry),
    retention_profile: submission.retention_profile_hint,
    structural_payload: {},
    extensions: {},
  };
  if (kind === 'record' && submission.lineage) structuralContent.lineage = submission.lineage;
  if (kind === 'link' && registryEntry.endpoints_location === 'structural') {
    structuralContent.from_id = submission.from_id;
    structuralContent.to_id = submission.to_id;
  }
  const semanticContent = {
    recorded_by: submission.recorded_by,
    recorded_at: submission.recorded_at,
    ...(submission.occurred_at ? { occurred_at: submission.occurred_at } : {}),
    ...(submission.origin ? {
      origin: { ...submission.origin, submission_hash: submissionHash(submission) },
    } : {}),
    claimed: submission.claims,
    ...(submission.claims.authority ? { authority: submission.claims.authority } : {}),
    ...(kind === 'link' && registryEntry.endpoints_location === 'semantic' ? {
      endpoints: { from_id: submission.from_id, to_id: submission.to_id },
    } : {}),
    ...(submission.selector ? { selector: submission.selector } : {}),
    payload: submission.payload,
    extensions: submission.extensions,
  };
  const structural = {
    format: `ccf.${kind}-structural/0.1.2`,
    salt: fixtureSalt(submission, 'structural'),
    content: structuralContent,
  };
  const semantic = {
    format: `ccf.${kind}-semantic/0.1.2`,
    salt: fixtureSalt(submission, 'semantic'),
    content: semanticContent,
  };
  const header = {
    spec: 'ccf/0.1.2',
    object_kind: kind,
    id: submission.id,
    hash_profile: 'ccf-jcs-sha256-v2',
    structural_commitment: compartmentCommitment(kind, 'structural', structural),
    semantic_commitment: compartmentCommitment(kind, 'semantic', semantic),
  };
  header.object_hash = objectHash(header);
  return { submission_hash: submissionHash(submission), header, structural, semantic };
}

const canonicalCapsuleObjects = new Map(
  capsuleSubmissions.map((submission) => [submission.id, canonicalizeCapsuleSubmission(submission)]),
);
const completedUpliftVector = operationById.get('completed-capsule-uplift');
const completedUpliftById = new Map(
  completedUpliftVector.objects.map((entry) => [entry.id, entry]),
);
check(
  completedUpliftVector.objects.length === completedUpliftById.size,
  'completed Capsule uplift vector has no duplicate objects',
);
for (const submission of capsuleSubmissions) {
  const object = canonicalCapsuleObjects.get(submission.id);
  const expected = completedUpliftById.get(submission.id);
  check(Boolean(expected), `completed Capsule uplift vector ${submission.id}`);
  check(object.header.id === submission.id, `completed Capsule uplift preserves ID ${submission.id}`);
  check(objectHash(object.header) === object.header.object_hash, `completed Capsule object hash ${submission.id}`);
  check(
    compartmentCommitment(submission.submission_kind, 'structural', object.structural)
      === object.header.structural_commitment
      && compartmentCommitment(submission.submission_kind, 'semantic', object.semantic)
        === object.header.semantic_commitment,
    `completed Capsule commitments ${submission.id}`,
  );
  check(
    expected.structural_salt === object.structural.salt
      && expected.semantic_salt === object.semantic.salt
      && expected.submission_hash === object.submission_hash
      && expected.object_hash === object.header.object_hash
      && expected.structural_commitment === object.header.structural_commitment
      && expected.semantic_commitment === object.header.semantic_commitment,
    `completed Capsule uplift expected outputs ${submission.id}`,
  );
  check(
    JSON.stringify(object.semantic.content.payload) === JSON.stringify(submission.payload)
      && JSON.stringify(object.semantic.content.extensions) === JSON.stringify(submission.extensions),
    `completed Capsule semantic assertion ${submission.id}`,
  );
  if (submission.origin) {
    check(
      object.semantic.content.origin.submission_hash === submissionHash(submission),
      `completed Capsule origin evidence ${submission.id}`,
    );
  }
  if (submission.submission_kind === 'link') {
    check(
      object.structural.content.from_id === submission.from_id
        && object.structural.content.to_id === submission.to_id,
      `completed Capsule Link endpoints ${submission.id}`,
    );
  }
}
check(
  completedUpliftById.size === canonicalCapsuleObjects.size,
  'completed Capsule uplift vector exactly covers the Capsule',
);
const completedUpliftReceipt = readJson(
  path.join(capsule, 'completed-uplift-receipt.json'),
);
check(
  completedUpliftReceipt.source_pack_id === capsuleManifest.pack_id
    && completedUpliftReceipt.source_level === completedUpliftVector.source_level
    && completedUpliftReceipt.source_level === capsuleManifest.level
    && completedUpliftReceipt.destination_level === completedUpliftVector.destination_level
    && completedUpliftReceipt.destination_archive_id
      === completedUpliftVector.destination_archive_id
    && completedUpliftReceipt.status === 'accepted'
    && completedUpliftReceipt.conflicts.length === 0,
  'completed Capsule receipt top-level source and destination binding',
);
const completedReceiptById = new Map(
  completedUpliftReceipt.objects.map((entry) => [entry.source_id, entry]),
);
check(
  completedUpliftReceipt.objects.length === canonicalCapsuleObjects.size
    && completedReceiptById.size === canonicalCapsuleObjects.size,
  'completed Capsule receipt covers every object exactly once',
);
for (const [id, object] of canonicalCapsuleObjects) {
  const receiptEntry = completedReceiptById.get(id);
  const submission = capsuleById.get(id);
  const expectedResolution = {
    profile: completedUpliftVector.resolution_profile,
    retention_profile: object.structural.content.retention_profile,
  };
  check(
    receiptEntry?.object_kind === submission.submission_kind
      && receiptEntry.canonical_id === id
      && receiptEntry.source_submission_hash === object.submission_hash
      && receiptEntry.object_hash === object.header.object_hash
      && receiptEntry.disposition === 'admitted'
      && receiptEntry.producer_authentication === 'absent'
      && receiptEntry.producer_proof === null
      && JSON.stringify(receiptEntry.archive_resolution) === JSON.stringify(expectedResolution),
    `completed Capsule receipt ${id}`,
  );
}
const uplift = readJson(path.join(capsule, 'uplift-receipt.json'));
for (const entry of uplift.objects) {
  const submission = capsuleById.get(entry.source_id);
  check(Boolean(submission), `uplift source present ${entry.source_id}`);
  check(entry.canonical_id === entry.source_id, `uplift stable ID ${entry.source_id}`);
  check(
    entry.source_submission_hash === submissionHash(submission),
    `uplift JCS source hash ${entry.source_id}`,
  );
  check(
    entry.disposition === 'pending' && entry.object_hash === null,
    `pending uplift makes no object-hash claim ${entry.source_id}`,
  );
}

const admittedHeaders = [
  ...readNdjson(path.join(BASE, 'examples', 'mindpack', 'objects', 'records.ndjson')),
  ...readNdjson(path.join(BASE, 'examples', 'mindpack', 'objects', 'links.ndjson')),
  ...readNdjson(path.join(BASE, 'examples', 'mindpack', 'objects', 'blobs.ndjson')),
];
const admittedById = new Map(admittedHeaders.map((header) => [header.id, header]));
for (const submission of submissions) {
  const header = admittedById.get(submission.id);
  check(Boolean(header), `completed uplift preserves ID ${submission.id}`);
  check(objectHash(header) === header.object_hash, `completed uplift object hash ${submission.id}`);
}

function originKey(submission) {
  if (!submission.origin) return null;
  return [
    submission.origin.source_id,
    submission.origin.native_id,
    submission.origin.revision,
    submission.submission_kind,
  ].join('\0');
}
function stageSubmission(store, submission) {
  const digest = submissionHash(submission);
  const key = originKey(submission);
  if (key !== null) {
    const previousOrigin = store.origins.get(key);
    if (previousOrigin !== undefined) {
      return previousOrigin === digest ? 'existing' : 'origin_revision_conflict';
    }
  }
  const previousObject = store.objects.get(submission.id);
  if (previousObject !== undefined) {
    return previousObject.submission_hash === digest ? 'existing' : 'object_id_conflict';
  }
  store.objects.set(submission.id, canonicalizeCapsuleSubmission(submission));
  if (key !== null) store.origins.set(key, digest);
  return 'admitted';
}
function admitAtomically(store, batchSubmissions) {
  const staged = {
    objects: new Map(store.objects),
    origins: new Map(store.origins),
  };
  const dispositions = [];
  for (const submission of batchSubmissions) {
    const disposition = stageSubmission(staged, submission);
    dispositions.push(disposition);
    if (disposition.endsWith('_conflict')) return { accepted: false, dispositions };
  }
  store.objects = staged.objects;
  store.origins = staged.origins;
  return { accepted: true, dispositions };
}

const canonicalStore = { objects: new Map(), origins: new Map() };
const duplicateVector = operationById.get('duplicate-capsule');
const firstImport = admitAtomically(canonicalStore, capsuleSubmissions);
check(firstImport.accepted, 'first Capsule import commits atomically');
check(
  firstImport.dispositions.every(
    (disposition) => disposition === duplicateVector.first_disposition,
  ),
  'first Capsule import admits every stable ID',
);
const secondImport = admitAtomically(canonicalStore, capsuleSubmissions);
check(secondImport.accepted, 'duplicate Capsule import commits as a no-op');
check(
  secondImport.dispositions.every(
    (disposition) => disposition === duplicateVector.second_disposition,
  ),
  'duplicate Capsule import is idempotent for Records and Links with or without origins',
);
check(
  canonicalStore.objects.size === duplicateVector.expected_object_count
    && canonicalStore.origins.size === duplicateVector.expected_origin_count,
  'duplicate Capsule vector final store cardinality',
);

const conflictVector = operationById.get('same-origin-revision-conflict');
const originSubmission = capsuleSubmissions.find((submission) => submission.origin);
const changedSubmission = structuredClone(originSubmission);
changedSubmission.payload[Object.keys(changedSubmission.payload)[0]] = 'changed';
check(
  stageSubmission(
    { objects: new Map(canonicalStore.objects), origins: new Map(canonicalStore.origins) },
    changedSubmission,
  ) === conflictVector.expected,
  'same origin revision with changed JCS submission conflicts',
);

const newSubmission = structuredClone(originSubmission);
const atomicVector = operationById.get('atomic-conflict-rollback');
newSubmission.id = atomicVector.new_id;
newSubmission.origin.native_id = atomicVector.new_native_id;
const beforeAtomicObjects = new Map(canonicalStore.objects);
const beforeAtomicOrigins = new Map(canonicalStore.origins);
const failedAtomic = admitAtomically(canonicalStore, [newSubmission, changedSubmission]);
check(!failedAtomic.accepted, 'conflicting batch rejected');
check(
  JSON.stringify([...canonicalStore.objects]) === JSON.stringify([...beforeAtomicObjects])
    && JSON.stringify([...canonicalStore.origins]) === JSON.stringify([...beforeAtomicOrigins]),
  'conflicting batch leaves object storage and origin index unchanged',
);
const atomicOutcome = !failedAtomic.accepted
  && JSON.stringify([...canonicalStore.objects]) === JSON.stringify([...beforeAtomicObjects])
  && JSON.stringify([...canonicalStore.origins]) === JSON.stringify([...beforeAtomicOrigins])
  ? 'rejected_without_object_or_origin_writes'
  : 'unexpected_state_change';
check(atomicOutcome === atomicVector.expected, 'atomic rollback operation vector');

const unavailable = readJson(path.join(VECTORS, 'foreign-unavailability.json'));
const canonicalAvailabilityStates = new Set([
  'available',
  ...capsuleManifest.dependencies.map((entry) => entry.availability),
]);
check(
  JSON.stringify([...canonicalAvailabilityStates].sort())
    === JSON.stringify(['available', 'erased', 'external', 'withheld']),
  'available, erased, external, and withheld remain distinct',
);
const availabilityStates = new Set(unavailable.input.compartments.map((entry) => entry.availability));
check(availabilityStates.has('erased') && availabilityStates.has('withheld'), 'exact unavailable states');
for (let index = 0; index < unavailable.input.compartments.length; index += 1) {
  const source = unavailable.input.compartments[index];
  const destination = unavailable.expected_destination_compartments[index];
  check(destination.availability === source.availability, `availability preserved ${source.object_id}`);
  check(destination.commitment === source.commitment, `commitment preserved ${source.object_id}`);
  check(destination.plaintext === null, `unavailable plaintext not manufactured ${source.object_id}`);
}

const draftCatalog = readJson(path.join(ROOT, 'semantic-catalog.json'));
const { root: draftRoot, ...draftCatalogWithoutRoot } = draftCatalog;
check(semanticCatalogRoot(draftCatalogWithoutRoot) === draftRoot, 'draft semantic catalog root');
check(
  draftCatalog.portable_object_formats.length === 1
    && draftCatalog.portable_object_formats[0] === 'ccf/0.1.2',
  'draft preserves the 0.1.2 portable object format',
);

console.log(`CCF Canonical Store inherited vectors pass: ${checks} checks.`);
if (process.env.CCF_PRINT_CAPSULE_UPLIFT === '1') {
  console.log(JSON.stringify([...canonicalCapsuleObjects].map(([id, object]) => ({
    id,
    submission_hash: object.submission_hash,
    object_hash: object.header.object_hash,
    structural_commitment: object.header.structural_commitment,
    semantic_commitment: object.header.semantic_commitment,
  })), null, 2));
}
