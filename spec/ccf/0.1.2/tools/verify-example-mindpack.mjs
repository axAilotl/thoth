import fs from 'node:fs';
import path from 'node:path';
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
} from './ccf-jcs.mjs';
import {
  compareManifest,
  deriveManifestGroundTruth,
} from './mindpack-manifest.mjs';

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
const catalog = readJson(path.join(MP, 'semantic-catalog.json'));
const catalogInput = structuredClone(catalog);
delete catalogInput.root;
check(semanticCatalogRoot(catalogInput) === catalog.root, 'semantic catalog self-hash');
let parent = null;
let activePublicKey = null;
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
  },
  catalogRoot: catalog.root,
});
compareManifest(manifest, groundTruth, { operation: 'restore' });
check(true, 'unsigned manifest matches independently derived ground truth');

function cloneTruth(truth) {
  return {
    ...structuredClone({
      counts: truth.counts,
      streams: truth.streams,
      custody: truth.custody,
      genesis_commit_hash: truth.genesis_commit_hash,
      head_commit_hash: truth.head_commit_hash,
      head_sequence: truth.head_sequence,
      semantic_catalog_root: truth.semantic_catalog_root,
      hash_profile: truth.hash_profile,
    }),
    external_dependencies: new Set(truth.external_dependencies),
    withheld: new Set(truth.withheld),
    erased: new Set(truth.erased),
    availability: new Map([...truth.availability].map(([key, value]) => [key, structuredClone(value)])),
    foreign_custody_proofs: new Set(truth.foreign_custody_proofs),
  };
}

function availabilityEntry(claim, compartment = 'semantic') {
  return claim.compartment_availability.find((entry) => entry.compartment === compartment);
}

function tamperFixture(name) {
  const claim = structuredClone(manifest);
  const truth = cloneTruth(groundTruth);
  let operation = 'restore';
  if (name === 'erased-example') {
    const entry = availabilityEntry(claim);
    const key = `${entry.object_id}\0${entry.compartment}`;
    const lineageId = records[0].id;
    Object.assign(entry, {
      availability: 'erased',
      source_custody_proof: 'commit:0:0',
      unavailability_lineage_id: lineageId,
    });
    Object.assign(truth.availability.get(key), structuredClone(entry));
    claim.erased.push(entry.object_id);
    truth.erased.add(entry.object_id);
  } else if (name === 'external-dependency-example') {
    const objectId = 'urn:ccf:record:00000000-0000-4000-8000-000000000099';
    claim.external_dependencies.push({ object_id: objectId, reason: 'fixture dependency' });
    truth.external_dependencies.add(objectId);
    claim.custody = { completeness: 'partial', restore_capable: false };
    truth.custody = structuredClone(claim.custody);
    operation = 'foreign_merge';
  }
  compareManifest(claim, truth, { operation });
  return { claim, truth, operation };
}

function applyTamper(vector, claim, truth) {
  const digest = `sha256:${'00'.repeat(32)}`;
  const entry = availabilityEntry(claim);
  switch (vector.mutation) {
    case 'count-low': claim.counts.records = String(Number(claim.counts.records) - 1); break;
    case 'count-high': claim.counts.records = String(Number(claim.counts.records) + 1); break;
    case 'remove-stream': claim.streams.pop(); break;
    case 'add-container-member': truth.streams.push({path:'unlisted.bin',digest,byte_length:'1',required:true}); break;
    case 'available-to-erased':
      Object.assign(entry, {availability:'erased',source_custody_proof:'commit:0:0',unavailability_lineage_id:records[0].id});
      claim.erased.push(entry.object_id);
      break;
    case 'erased-to-available':
      Object.assign(entry, {availability:'available',source_custody_proof:null,unavailability_lineage_id:null});
      claim.erased = claim.erased.filter((objectId) => objectId !== entry.object_id);
      break;
    case 'remove-dependency': claim.external_dependencies = []; break;
    case 'add-dependency': claim.external_dependencies.push({object_id:'urn:ccf:record:00000000-0000-4000-8000-000000000098',reason:'fabricated'}); break;
    case 'change-genesis': claim.genesis_commit_hash = digest; break;
    case 'change-head': claim.head_commit_hash = digest; break;
    case 'complete-to-partial': claim.custody = {completeness:'partial',restore_capable:false}; break;
    case 'restore-to-foreign-merge': claim.mode = 'foreign_merge'; break;
    case 'duplicate-availability': claim.compartment_availability.push(structuredClone(entry)); break;
    case 'contradict-availability':
      claim.compartment_availability.push({...structuredClone(entry),availability:'erased',source_custody_proof:'commit:0:0',unavailability_lineage_id:records[0].id});
      break;
    default: throw new Error(`unknown manifest tamper mutation: ${vector.mutation}`);
  }
}

const tamperVectors = readJson(path.join(ROOT, 'vectors', 'mindpack-manifest-tamper.json'));
for (const vector of tamperVectors.cases) {
  const { claim, truth, operation } = tamperFixture(vector.fixture);
  applyTamper(vector, claim, truth);
  let rejected = false;
  try {
    compareManifest(claim, truth, { operation });
  } catch {
    rejected = true;
  }
  check(rejected, `manifest tamper rejected: ${vector.id}`);
}

const batchFiles = fs.readdirSync(path.join(MP, 'producer-batches')).filter((name) => name.endsWith('.json'));
for (const name of batchFiles) {
  const batch = readJson(path.join(MP, 'producer-batches', name));
  check(producerBatchHash(batch) === batch.batch_hash, `producer batch hash: ${name}`);
  const credentialRecord = records.find((header) => {
    const structural = structuralById.get(header.id)?.content;
    return structural?.type === 'core.device_credential' && structural.structural_payload?.credential_id === batch.credential_id;
  });
  check(Boolean(credentialRecord), `producer credential: ${name}`);
  const credential = structuralById.get(credentialRecord.id).content.structural_payload;
  const publicKey = crypto.createPublicKey({
    key: { kty: 'OKP', crv: 'Ed25519', x: credential.signing_key.public_key },
    format: 'jwk',
  });
  check(
    crypto.verify(null, producerBatchSigningDigest(batch.batch_hash), publicKey, Buffer.from(batch.signature, 'base64url')),
    `producer batch signature: ${name}`,
  );
}

console.log(`Example CCF 0.1.2 mindpack verified: ${checks} checks passed.`);
