import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import {
  compartmentCommitment,
  blobContentCommitment,
  objectHash,
  producerBatchHash,
  producerBatchSigningDigest,
  merkleRoot,
  commitSigningDigest,
  semanticCatalogRoot,
  canonicalDigest,
  canonicalize,
  submissionHash,
} from './ccf-jcs.mjs';
import {
  compareManifest,
  deriveManifestGroundTruth,
  actualStreams,
} from './mindpack-manifest.mjs';
import { verifyExampleSuppressionFixtures } from './verify-suppression-fixture.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const MP = path.join(ROOT, 'examples', 'mindpack');
const manifest = JSON.parse(fs.readFileSync(path.join(MP, 'manifest.json')));
let checks = 0;

function check(condition, label) {
  checks += 1;
  if (!condition) throw new Error(label);
}
function readJson(file) { return JSON.parse(fs.readFileSync(file, 'utf8')); }
function readNdjson(file) {
  const text = fs.readFileSync(file, 'utf8').trim();
  return text ? text.split('\n').filter(Boolean).map(JSON.parse) : [];
}
function uuidOf(id) { return id.slice(id.lastIndexOf(':') + 1); }
function compartmentPath(header, compartment) {
  return path.join(MP, 'compartments', `${header.object_kind}s`, `${uuidOf(header.id)}.${compartment}.json`);
}
function compartmentPathAt(root, header, compartment) {
  return path.join(root, 'compartments', `${header.object_kind}s`, `${uuidOf(header.id)}.${compartment}.json`);
}

const records = readNdjson(path.join(MP, 'objects', 'records.ndjson'));
const links = readNdjson(path.join(MP, 'objects', 'links.ndjson'));
const blobs = readNdjson(path.join(MP, 'objects', 'blobs.ndjson'));
const allHeaders = [...records, ...links, ...blobs];
const byId = new Map(allHeaders.map((header) => [header.id, header]));
check(byId.size === allHeaders.length, 'unique object IDs');

const structuralById = new Map();
const semanticById = new Map();
for (const header of allHeaders) {
  check(header.spec === 'ccf/0.1.2', `object spec: ${header.id}`);
  check(header.hash_profile === 'ccf-jcs-sha256-v2', `object hash profile: ${header.id}`);

  const structuralFile = compartmentPath(header, 'structural');
  if (fs.existsSync(structuralFile)) {
    const structural = readJson(structuralFile);
    structuralById.set(header.id, structural);
    check(
      compartmentCommitment(header.object_kind, 'structural', structural) === header.structural_commitment,
      `structural commitment: ${header.id}`,
    );
  }

  const semanticFile = compartmentPath(header, 'semantic');
  if (header.semantic_commitment === null) {
    check(!fs.existsSync(semanticFile), `unexpected semantic compartment: ${header.id}`);
  } else if (fs.existsSync(semanticFile)) {
    const semantic = readJson(semanticFile);
    semanticById.set(header.id, semantic);
    check(
      compartmentCommitment(header.object_kind, 'semantic', semantic) === header.semantic_commitment,
      `semantic commitment: ${header.id}`,
    );
  }
  check(objectHash(header) === header.object_hash, `object hash: ${header.id}`);
}

for (const header of blobs) {
  const structural = structuralById.get(header.id)?.content;
  const semantic = semanticById.get(header.id)?.content;
  const bytesPath = path.join(MP, 'blob-data', `${uuidOf(header.id)}.bin`);
  if (fs.existsSync(bytesPath)) {
    check(Boolean(structural), `Blob bytes require structural compartment: ${header.id}`);
    check(Boolean(semantic), `Blob bytes require semantic compartment: ${header.id}`);
    const bytes = fs.readFileSync(bytesPath);
    check(String(bytes.length) === structural.byte_length, `Blob byte length: ${header.id}`);
    check(
      blobContentCommitment(semantic.content_salt, bytes) === structural.content_commitment,
      `Blob content commitment: ${header.id}`,
    );
  }
}

const members = readNdjson(path.join(MP, 'integrity', 'members.ndjson'));
for (const member of members) {
  const header = byId.get(member.object_id);
  check(Boolean(header), `missing member object: ${member.object_id}`);
  check(header.object_kind === member.object_kind, `member kind mismatch: ${member.object_id}`);
  check(header.object_hash === member.object_hash, `member hash mismatch: ${member.object_id}`);
}

const commitSummaries = readNdjson(path.join(MP, 'integrity', 'commits.ndjson'))
  .sort((a, b) => (BigInt(a.sequence) < BigInt(b.sequence) ? -1 : BigInt(a.sequence) > BigInt(b.sequence) ? 1 : 0));
const commitSequences = new Set(commitSummaries.map((summary) => summary.sequence));
const memberObjectIds = new Set();
for (const member of members) {
  check(commitSequences.has(member.commit_sequence), `orphan member sequence: ${member.object_id}`);
  check(!memberObjectIds.has(member.object_id), `duplicate member object: ${member.object_id}`);
  memberObjectIds.add(member.object_id);
}
const commitRecordIds = new Set(commitSummaries.map((summary) => summary.record_id));
for (const recordId of commitRecordIds) {
  check(!memberObjectIds.has(recordId), `chain commit is also a member: ${recordId}`);
}
const catalog = readJson(path.join(MP, 'semantic-catalog.json'));
const catalogInput = structuredClone(catalog);
delete catalogInput.root;
check(semanticCatalogRoot(catalogInput) === catalog.root, 'semantic catalog self-hash');
const catalogPaths = new Set();
for (const [field, prefix, domain] of [
  ['schemas', 'schemas/', 'ccf:schema-artifact:v1'],
  ['registries', 'registries/', 'ccf:registry-artifact:v1'],
]) {
  for (const entry of catalog[field]) {
    check(entry.path.startsWith(prefix), `catalog ${field} path: ${entry.path}`);
    check(!catalogPaths.has(entry.path), `catalog duplicate path: ${entry.path}`);
    catalogPaths.add(entry.path);
    const artifactPath = path.join(MP, entry.path);
    check(fs.existsSync(artifactPath), `catalog artifact present: ${entry.path}`);
    check(
      canonicalDigest(domain, readJson(artifactPath)) === entry.digest,
      `catalog artifact digest: ${entry.path}`,
    );
  }
}
const packagedCatalogPaths = actualStreams(MP)
  .map((entry) => entry.path)
  .filter((entryPath) => entryPath.endsWith('.json')
    && entryPath !== 'schemas/catalog.json'
    && (entryPath.startsWith('schemas/') || entryPath.startsWith('registries/')));
check(
  packagedCatalogPaths.length === catalogPaths.size
    && packagedCatalogPaths.every((entryPath) => catalogPaths.has(entryPath)),
  'catalog artifact membership',
);
check(
  JSON.stringify(readJson(path.join(MP, 'schemas', 'catalog.json')))
    === JSON.stringify({ format: 'ccf.schema-catalog/0.1.2', schemas: catalog.schemas }),
  'schema catalog matches semantic catalog',
);
let parent = null;
let activePublicKey = null;
let chainIdentity = null;
for (const summary of commitSummaries) {
  const header = byId.get(summary.record_id);
  check(Boolean(header), `missing commit Record: ${summary.record_id}`);
  check(header.object_hash === summary.commit_hash, `commit hash: ${summary.sequence}`);
  const structuralEnvelope = structuralById.get(summary.record_id);
  const content = structuredClone(structuralEnvelope.content);
  check(content.type === 'integrity.commit', `commit type: ${summary.sequence}`);
  const payload = content.structural_payload;
  check(payload.sequence === summary.sequence, `commit sequence: ${summary.sequence}`);
  check(payload.parent_commit_hash === parent, `parent hash: ${summary.sequence}`);
  check(payload.parent_commit_hash === summary.parent_commit_hash, `summary parent: ${summary.sequence}`);
  check(payload.batch_merkle_root === summary.merkle_root, `summary Merkle root: ${summary.sequence}`);
  check(payload.semantic_catalog_root === catalog.root, `catalog root: ${summary.sequence}`);
  check(payload.hash_profile === 'ccf-jcs-sha256-v2', `commit hash profile: ${summary.sequence}`);
  const identity = {
    archive_id: payload.archive_id,
    epoch_id: payload.epoch_id,
    semantic_catalog_root: payload.semantic_catalog_root,
    active_profiles: payload.active_profiles,
    hash_profile: payload.hash_profile,
    signature_profile: payload.signature_profile,
    signer_key_id: payload.signer_key_id,
  };
  if (chainIdentity === null) chainIdentity = identity;
  else check(JSON.stringify(identity) === JSON.stringify(chainIdentity), `chain identity: ${summary.sequence}`);

  const sequenceMembers = members.filter((member) => member.commit_sequence === summary.sequence);
  check(merkleRoot(sequenceMembers) === payload.batch_merkle_root, `Merkle root: ${summary.sequence}`);
  check(String(sequenceMembers.length) === payload.member_count, `member count: ${summary.sequence}`);

  if (summary.sequence === '0') {
    activePublicKey = crypto.createPublicKey({
      key: { kty: 'OKP', crv: 'Ed25519', x: payload.signer_public_key },
      format: 'jwk',
    });
  }
  check(Boolean(activePublicKey), `missing signer key: ${summary.sequence}`);
  const signature = payload.signature;
  delete payload.signature;
  const headerForSigning = structuredClone(header);
  delete headerForSigning.structural_commitment;
  delete headerForSigning.object_hash;
  const signingDigest = commitSigningDigest(headerForSigning, content);
  check(
    crypto.verify(null, signingDigest, activePublicKey, Buffer.from(signature, 'base64url')),
    `commit signature: ${summary.sequence}`,
  );
  parent = header.object_hash;
}

const journalCoveredIds = new Set([
  ...members.map((member) => member.object_id),
  ...commitSummaries.map((summary) => summary.record_id),
]);
for (const header of allHeaders) {
  check(journalCoveredIds.has(header.id), `object outside journal: ${header.id}`);
  const content = structuralById.get(header.id)?.content;
  const payload = content?.structural_payload;
  if (content?.type !== 'integrity.commit'
      || !payload?.archive_id
      || payload.archive_id === chainIdentity.archive_id) continue;
  const foreignContent = structuredClone(content);
  const signature = foreignContent.structural_payload.signature;
  delete foreignContent.structural_payload.signature;
  const signingHeader = structuredClone(header);
  delete signingHeader.structural_commitment;
  delete signingHeader.object_hash;
  const publicKey = crypto.createPublicKey({
    key: { kty: 'OKP', crv: 'Ed25519', x: payload.signer_public_key },
    format: 'jwk',
  });
  check(
    crypto.verify(
      null,
      commitSigningDigest(signingHeader, foreignContent),
      publicKey,
      Buffer.from(signature, 'base64url'),
    ),
    `foreign commit signature: ${header.id}`,
  );
}

const archiveClaim = readJson(path.join(MP, 'archive.json'));
const genesisPayload = structuralById.get(commitSummaries[0].record_id).content.structural_payload;
const derivedArchive = {
  format: 'ccf.archive-row/0.1.2',
  archive_id: chainIdentity.archive_id,
  epoch_id: chainIdentity.epoch_id,
  genesis_commit_hash: commitSummaries[0].commit_hash,
  hash_profile: chainIdentity.hash_profile,
  signature_profile: chainIdentity.signature_profile,
  semantic_catalog_root: chainIdentity.semantic_catalog_root,
  active_profiles: chainIdentity.active_profiles,
  signer_key_id: chainIdentity.signer_key_id,
  created_at: genesisPayload.committed_at,
};
const { erasure_domain_id: ignoredOperationalDomain, ...archiveIdentityClaim } = archiveClaim;
check(/^urn:ccf:lineage:/.test(ignoredOperationalDomain), 'archive operational erasure domain shape');
check(canonicalize(archiveIdentityClaim) === canonicalize(derivedArchive), 'archive row derived from signed material');

const coordinates = new Map(members.map((member) => [member.object_id, member]));
const derivedLineageHeads = new Map();
for (const header of records) {
  const lineage = structuralById.get(header.id)?.content?.lineage;
  if (!lineage) continue;
  const member = coordinates.get(header.id);
  check(Boolean(member), `lineage admission coordinate: ${header.id}`);
  const current = derivedLineageHeads.get(lineage.lineage_id);
  check(
    (current === undefined && lineage.previous_head_id === null)
      || current?.head_record_id === lineage.previous_head_id,
    `lineage compare-and-swap: ${lineage.lineage_id}`,
  );
  derivedLineageHeads.set(lineage.lineage_id, {
    lineage_id: lineage.lineage_id,
    head_record_id: header.id,
    head_record_hash: header.object_hash,
    head_commit_sequence: member.commit_sequence,
    state: lineage.transition,
    valid_from: lineage.valid_from,
    expires_at: lineage.expires_at,
  });
}
const lineageClaims = readNdjson(path.join(MP, 'lineage-heads.ndjson'));
check(
  canonicalize(lineageClaims) === canonicalize([...derivedLineageHeads.values()].sort((a, b) => a.lineage_id.localeCompare(b.lineage_id))),
  'lineage heads derived from canonical objects',
);

const derivedOrigins = [];
for (const header of [...records, ...links, ...blobs]) {
  const origin = semanticById.get(header.id)?.content?.origin;
  if (!origin) continue;
  derivedOrigins.push({
    source_id: origin.source_id,
    native_id: origin.native_id,
    revision: origin.revision,
    submission_hash: origin.submission_hash,
    object_kind: header.object_kind,
    object_id: header.id,
    lifecycle: 'active',
  });
}
derivedOrigins.sort((a, b) => `${a.source_id}\0${a.native_id}\0${a.revision}\0${a.object_kind}`.localeCompare(`${b.source_id}\0${b.native_id}\0${b.revision}\0${b.object_kind}`));
check(
  canonicalize(readNdjson(path.join(MP, 'origin-index.ndjson'))) === canonicalize(derivedOrigins),
  'origin index derived from canonical objects',
);

const groundTruth = deriveManifestGroundTruth({
  root: MP,
  records,
  links,
  blobs,
  structuralById,
  semanticById,
  members,
  commitSummaries,
  chain: {
    genesis_commit_hash: commitSummaries[0].commit_hash,
    head_commit_hash: commitSummaries.at(-1).commit_hash,
    head_sequence: commitSummaries.at(-1).sequence,
    ...chainIdentity,
  },
  catalogRoot: catalog.root,
});
compareManifest(manifest, groundTruth, { operation: 'restore' });
check(true, 'unsigned manifest matches independently derived ground truth');

checks += verifyExampleSuppressionFixtures({
  root: MP,
  records,
  links,
  blobs,
  structuralById,
  semanticById,
  availability: groundTruth.availability,
});

function availabilityEntry(claim, compartment = 'semantic') {
  return claim.compartment_availability.find((entry) => entry.compartment === compartment);
}

function fixtureContents(root) {
  const fixtureRecords = readNdjson(path.join(root, 'objects', 'records.ndjson'));
  const fixtureLinks = readNdjson(path.join(root, 'objects', 'links.ndjson'));
  const fixtureBlobs = readNdjson(path.join(root, 'objects', 'blobs.ndjson'));
  const fixtureStructural = new Map();
  const fixtureSemantic = new Map();
  for (const header of [...fixtureRecords, ...fixtureLinks, ...fixtureBlobs]) {
    const structuralFile = compartmentPathAt(root, header, 'structural');
    const semanticFile = compartmentPathAt(root, header, 'semantic');
    if (fs.existsSync(structuralFile)) fixtureStructural.set(header.id, readJson(structuralFile));
    if (fs.existsSync(semanticFile)) fixtureSemantic.set(header.id, readJson(semanticFile));
  }
  return {
    records: fixtureRecords,
    links: fixtureLinks,
    blobs: fixtureBlobs,
    structuralById: fixtureStructural,
    semanticById: fixtureSemantic,
    members: readNdjson(path.join(root, 'integrity', 'members.ndjson')),
    commitSummaries: readNdjson(path.join(root, 'integrity', 'commits.ndjson')),
  };
}

function fixtureTruth(root) {
  const contents = fixtureContents(root);
  return deriveManifestGroundTruth({
    root,
    ...contents,
    chain: {
      genesis_commit_hash: contents.commitSummaries[0].commit_hash,
      head_commit_hash: contents.commitSummaries.at(-1).commit_hash,
      head_sequence: contents.commitSummaries.at(-1).sequence,
      ...chainIdentity,
    },
    catalogRoot: catalog.root,
  });
}

function applyDerivedClaims(claim, truth) {
  claim.counts = structuredClone(truth.counts);
  claim.streams = structuredClone(truth.streams);
  claim.external_dependencies = [...truth.external_dependencies.values()]
    .map((entry) => structuredClone(entry));
  claim.withheld = [...truth.withheld].sort();
  claim.erased = [...truth.erased].sort();
  claim.compartment_availability = [...truth.availability.values()]
    .map((entry) => structuredClone(entry));
  claim.foreign_custody_proofs = [...truth.foreign_custody_proofs].sort();
  claim.custody = structuredClone(truth.custody);
}

function tamperFixture(name) {
  const claim = structuredClone(manifest);
  const temporaryBase = fs.mkdtempSync(path.join(os.tmpdir(), 'ccf-manifest-fixture-'));
  const temporaryPack = path.join(temporaryBase, 'mindpack');
  fs.cpSync(MP, temporaryPack, { recursive: true });
  let operation = 'restore';
  if (name === 'erased-example') {
    const contents = fixtureContents(temporaryPack);
    const receiptIds = new Set(
      contents.records
        .filter((header) => contents.structuralById.get(header.id)?.content?.type === 'lineage.erasure_receipt')
        .map((header) => header.id),
    );
    const cover = contents.links.find((header) => {
      const content = contents.structuralById.get(header.id)?.content;
      return content?.type === 'ccf.covers' && receiptIds.has(content.from_id);
    });
    check(Boolean(cover), 'erased fixture has signed receipt coverage');
    const targetId = contents.structuralById.get(cover.id).content.to_id;
    const target = contents.records.find((header) => header.id === targetId);
    check(Boolean(target), 'erased fixture target is present');
    const semanticPath = compartmentPathAt(temporaryPack, target, 'semantic');
    if (fs.existsSync(semanticPath)) fs.unlinkSync(semanticPath);
  } else if (name === 'external-dependency-example') {
    const contents = fixtureContents(temporaryPack);
    const source = contents.records.find(
      (header) => contents.structuralById.get(header.id)?.content?.type === 'core.source',
    );
    check(Boolean(source), 'external fixture source is present');
    const remaining = contents.records.filter((header) => header.id !== source.id);
    fs.writeFileSync(
      path.join(temporaryPack, 'objects', 'records.ndjson'),
      `${remaining.map((header) => JSON.stringify(header)).join('\n')}\n`,
    );
    fs.unlinkSync(compartmentPathAt(temporaryPack, source, 'structural'));
    fs.unlinkSync(compartmentPathAt(temporaryPack, source, 'semantic'));
    operation = 'foreign_merge';
  }
  const truth = fixtureTruth(temporaryPack);
  applyDerivedClaims(claim, truth);
  compareManifest(claim, truth, { operation });
  return { claim, truth, operation, temporaryBase };
}

function applyTamper(vector, claim, truth) {
  const digest = `sha256:${'00'.repeat(32)}`;
  const entry = availabilityEntry(claim);
  switch (vector.mutation) {
    case 'count-low': claim.counts.records = String(Number(claim.counts.records) - 1); break;
    case 'count-high': claim.counts.records = String(Number(claim.counts.records) + 1); break;
    case 'remove-stream': claim.streams.pop(); break;
    case 'add-container-member': {
      const temporaryBase = fs.mkdtempSync(path.join(os.tmpdir(), 'ccf-manifest-vector-'));
      const temporaryPack = path.join(temporaryBase, 'mindpack');
      try {
        fs.cpSync(MP, temporaryPack, { recursive: true });
        fs.writeFileSync(path.join(temporaryPack, 'unlisted.bin'), 'x');
        truth.streams = actualStreams(temporaryPack);
      } finally {
        fs.rmSync(temporaryBase, { recursive: true, force: true });
      }
      break;
    }
    case 'available-to-erased':
      Object.assign(entry, {availability:'erased',source_custody_proof:'commit:0:0',unavailability_lineage_id:records[0].id});
      claim.erased.push(entry.object_id);
      break;
    case 'erased-to-available': {
      const erasedEntry = claim.compartment_availability.find(
        (candidate) => candidate.availability === 'erased',
      );
      Object.assign(erasedEntry, {availability:'available',source_custody_proof:null,unavailability_lineage_id:null});
      claim.erased = claim.erased.filter((objectId) => objectId !== erasedEntry.object_id);
      break;
    }
    case 'remove-dependency': claim.external_dependencies = []; break;
    case 'add-dependency': claim.external_dependencies.push({object_id:'urn:ccf:record:00000000-0000-4000-8000-000000000098',reason:'fabricated'}); break;
    case 'change-genesis': claim.genesis_commit_hash = digest; break;
    case 'change-head': claim.head_commit_hash = digest; break;
    case 'change-archive-id': claim.archive_id = 'urn:ccf:archive:00000000-0000-4000-8000-000000000097'; break;
    case 'change-catalog-root': claim.semantic_catalog_root = digest; break;
    case 'complete-to-partial': claim.custody = {completeness:'partial',restore_capable:false}; break;
    case 'restore-to-foreign-merge': claim.mode = 'foreign_merge'; break;
    case 'duplicate-availability': claim.compartment_availability.push(structuredClone(entry)); break;
    case 'contradict-availability':
      claim.compartment_availability.push({...structuredClone(entry),availability:'erased',source_custody_proof:'commit:0:0',unavailability_lineage_id:records[0].id});
      break;
    case 'add-optional-stream': claim.streams.push({path:'blob-data/absent.bin',digest,byte_length:'0',required:false}); break;
    case 'change-dependency-metadata': claim.external_dependencies[0].reason = 'attacker-selected'; break;
    case 'add-custody-proof': claim.foreign_custody_proofs.push(`urn:ccf:archive:00000000-0000-4000-8000-000000000096:${records[0].object_hash}`); break;
    default: throw new Error(`unknown manifest tamper mutation: ${vector.mutation}`);
  }
}

const tamperVectors = readJson(path.join(ROOT, 'vectors', 'mindpack-manifest-tamper.json'));
for (const vector of tamperVectors.cases) {
  const { claim, truth, operation, temporaryBase } = tamperFixture(vector.fixture);
  try {
    applyTamper(vector, claim, truth);
    let rejected = false;
    try {
      compareManifest(claim, truth, { operation });
    } catch {
      rejected = true;
    }
    check(rejected, `manifest tamper rejected: ${vector.id}`);
  } finally {
    fs.rmSync(temporaryBase, { recursive: true, force: true });
  }
}

const batchFiles = fs.readdirSync(path.join(MP, 'producer-batches')).filter((name) => name.endsWith('.json'));
const credentialHistories = new Map();
for (const header of records) {
  const content = structuralById.get(header.id)?.content;
  if (content?.type !== 'core.device_credential') continue;
  const lineage = content.lineage;
  const coordinate = coordinates.get(header.id);
  check(Boolean(lineage) && Boolean(coordinate), `credential lineage coordinate: ${header.id}`);
  const history = credentialHistories.get(lineage.lineage_id) ?? [];
  history.push({ header, payload: content.structural_payload, lineage, coordinate });
  credentialHistories.set(lineage.lineage_id, history);
}
const credentialVersions = new Map();
const allowedCredentialTransitions = new Map([
  [null, new Set(['issue'])],
  ['issue', new Set(['rotate', 'revoke'])],
  ['rotate', new Set(['rotate', 'revoke'])],
]);
for (const [lineageId, history] of credentialHistories) {
  history.sort((a, b) => {
    const sequence = Number(a.coordinate.commit_sequence) - Number(b.coordinate.commit_sequence);
    return sequence || Number(a.coordinate.commit_position) - Number(b.coordinate.commit_position);
  });
  let previousId = null;
  let previousState = null;
  let previousTime = null;
  history.forEach((version, index) => {
    const effectiveAt = Date.parse(version.lineage.valid_from);
    check(version.lineage.previous_head_id === previousId, `credential predecessor: ${lineageId}`);
    check(allowedCredentialTransitions.get(previousState)?.has(version.lineage.transition), `credential transition: ${lineageId}`);
    check(Number.isFinite(effectiveAt) && (previousTime === null || effectiveAt > previousTime), `credential effective time: ${lineageId}`);
    version.effectiveAt = effectiveAt;
    version.successorAt = index + 1 < history.length ? Date.parse(history[index + 1].lineage.valid_from) : null;
    const versions = credentialVersions.get(version.payload.credential_id) ?? [];
    versions.push(version);
    credentialVersions.set(version.payload.credential_id, versions);
    previousId = version.header.id;
    previousState = version.lineage.transition;
    previousTime = effectiveAt;
  });
}
function credentialsAt(credentialId, time) {
  return (credentialVersions.get(credentialId) ?? []).filter((version) => {
    if (version.lineage.transition === 'revoke') return false;
    const payloadStart = Date.parse(version.payload.valid_from);
    const payloadEnd = version.payload.expires_at === null ? null : Date.parse(version.payload.expires_at);
    const lineageEnd = version.lineage.expires_at === null ? null : Date.parse(version.lineage.expires_at);
    const ends = [payloadEnd, lineageEnd, version.successorAt].filter((value) => value !== null);
    return time >= Math.max(payloadStart, version.effectiveAt)
      && (ends.length === 0 || time < Math.min(...ends));
  });
}
for (const name of batchFiles) {
  const batch = readJson(path.join(MP, 'producer-batches', name));
  check(producerBatchHash(batch) === batch.batch_hash, `producer batch hash: ${name}`);
  const batchTime = Date.parse(batch.created_at);
  check(Number.isFinite(batchTime), `producer batch timestamp: ${name}`);
  const activeCredentials = credentialsAt(batch.credential_id, batchTime);
  check(activeCredentials.length === 1, `producer credential version at signed time: ${name}`);
  const credential = activeCredentials[0].payload;
  check(credential.subject_id === batch.producer_id, `producer credential subject: ${name}`);
  check(credential.scopes.includes('capture'), `producer credential scope: ${name}`);
  const publicKey = crypto.createPublicKey({
    key: { kty: 'OKP', crv: 'Ed25519', x: credential.signing_key.public_key },
    format: 'jwk',
  });
  check(
    crypto.verify(null, producerBatchSigningDigest(batch.batch_hash), publicKey, Buffer.from(batch.signature, 'base64url')),
    `producer batch signature: ${name}`,
  );
  for (const kind of ['records', 'links', 'blobs']) {
    for (const submission of batch[kind]) {
      const evidence = semanticById.get(submission.id)?.content?.producer_evidence;
      if (!evidence) {
        const state = groundTruth.availability.get(`${submission.id}\0semantic`);
        check(
          state?.availability === 'erased',
          `producer evidence absent without verified erasure: ${submission.id}`,
        );
        continue;
      }
      check(true, `producer evidence: ${submission.id}`);
      check(evidence.batch_id === batch.batch_id, `producer evidence batch: ${submission.id}`);
      check(evidence.credential_id === batch.credential_id, `producer evidence credential: ${submission.id}`);
      check(evidence.producer_sequence === batch.producer_sequence, `producer evidence sequence: ${submission.id}`);
      check(evidence.submission_hash === submissionHash(submission), `producer evidence submission hash: ${submission.id}`);
    }
  }
}
const revokedFixture = [...credentialHistories.values()].find((history) => history.at(-1).lineage.transition === 'revoke');
check(Boolean(revokedFixture), 'issue-to-revoke credential fixture present');
const revokedAt = Date.parse(revokedFixture.at(-1).lineage.valid_from);
check(credentialsAt(revokedFixture[0].payload.credential_id, revokedAt).length === 0, 'revoked credential rejected at successor time');
const producerHeads = readNdjson(path.join(MP, 'producer-heads.ndjson'));
const batches = batchFiles.map((name) => readJson(path.join(MP, 'producer-batches', name)));
const derivedProducerHeads = [];
for (const producerId of [...new Set(batches.map((batch) => batch.producer_id))].sort()) {
  const chain = batches.filter((batch) => batch.producer_id === producerId)
    .sort((a, b) => Number(a.producer_sequence) - Number(b.producer_sequence));
  let previous = null;
  chain.forEach((batch, index) => {
    check(Number(batch.producer_sequence) === index + 1, `producer sequence: ${producerId}`);
    check(batch.previous_batch_hash === previous, `producer previous hash: ${producerId}`);
    previous = batch.batch_hash;
  });
  const latest = chain.at(-1);
  derivedProducerHeads.push({
    producer_id: producerId,
    producer_sequence: latest.producer_sequence,
    batch_hash: latest.batch_hash,
    credential_id: latest.credential_id,
    updated_at: latest.created_at,
  });
}
check(canonicalize(producerHeads) === canonicalize(derivedProducerHeads), 'producer heads derived from signed batches');

console.log(`Example CCF 0.1.2 mindpack verified: ${checks} checks passed.`);
