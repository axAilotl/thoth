import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  digestString,
  compartmentCommitment,
  blobContentCommitment,
  objectHash,
  producerBatchHash,
  producerBatchSigningDigest,
  merkleRoot,
  commitSigningDigest,
  semanticCatalogRoot,
} from './ccf-jcs.mjs';

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

check(manifest.format === 'ccf.mindpack/0.1.2-rc1', 'manifest format');
check(manifest.hash_profile === 'ccf-jcs-sha256-v2', 'manifest hash profile');

for (const stream of manifest.streams) {
  const file = path.join(MP, stream.path);
  check(fs.existsSync(file), `manifest stream missing: ${stream.path}`);
  const bytes = fs.readFileSync(file);
  check(String(bytes.length) === stream.byte_length, `length mismatch: ${stream.path}`);
  check(digestString(bytes) === stream.digest, `digest mismatch: ${stream.path}`);
}

const records = readNdjson(path.join(MP, 'objects', 'records.ndjson'));
const links = readNdjson(path.join(MP, 'objects', 'links.ndjson'));
const blobs = readNdjson(path.join(MP, 'objects', 'blobs.ndjson'));
const allHeaders = [...records, ...links, ...blobs];
const byId = new Map(allHeaders.map((header) => [header.id, header]));
check(String(records.length) === manifest.counts.records, 'record count');
check(String(links.length) === manifest.counts.links, 'link count');
check(String(blobs.length) === manifest.counts.blobs, 'blob count');
check(byId.size === allHeaders.length, 'unique object IDs');
const availabilityByCompartment = new Map(
  manifest.compartment_availability.map((entry) => [
    `${entry.object_kind}:${entry.object_id}:${entry.compartment}`,
    entry,
  ]),
);

const structuralById = new Map();
const semanticById = new Map();
for (const header of allHeaders) {
  check(header.spec === 'ccf/0.1.2-rc1', `object spec: ${header.id}`);
  check(header.hash_profile === 'ccf-jcs-sha256-v2', `object hash profile: ${header.id}`);

  const structuralFile = compartmentPath(header, 'structural');
  const structuralAvailability = availabilityByCompartment.get(
    `${header.object_kind}:${header.id}:structural`,
  );
  check(Boolean(structuralAvailability), `structural availability: ${header.id}`);
  check(structuralAvailability.commitment === header.structural_commitment, `structural availability commitment: ${header.id}`);
  check(fs.existsSync(structuralFile), `missing structural compartment: ${header.id}`);
  const structural = readJson(structuralFile);
  structuralById.set(header.id, structural);
  check(
    compartmentCommitment(header.object_kind, 'structural', structural) === header.structural_commitment,
    `structural commitment: ${header.id}`,
  );

  const semanticFile = compartmentPath(header, 'semantic');
  if (header.semantic_commitment === null) {
    check(!fs.existsSync(semanticFile), `unexpected semantic compartment: ${header.id}`);
  } else {
    const semanticAvailability = availabilityByCompartment.get(
      `${header.object_kind}:${header.id}:semantic`,
    );
    check(Boolean(semanticAvailability), `semantic availability: ${header.id}`);
    check(semanticAvailability.commitment === header.semantic_commitment, `semantic availability commitment: ${header.id}`);
    check(fs.existsSync(semanticFile), `missing semantic compartment: ${header.id}`);
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
  const structural = structuralById.get(header.id).content;
  const semantic = semanticById.get(header.id).content;
  const bytesPath = path.join(MP, 'blob-data', `${uuidOf(header.id)}.bin`);
  const contentAvailability = availabilityByCompartment.get(`blob:${header.id}:blob_content`);
  check(Boolean(contentAvailability), `Blob content availability: ${header.id}`);
  check(contentAvailability.commitment === structural.content_commitment, `Blob availability commitment: ${header.id}`);
  if (contentAvailability.availability === 'available') {
    check(fs.existsSync(bytesPath), `available Blob bytes missing: ${header.id}`);
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
check(String(commitSummaries.length) === manifest.counts.commits, 'commit count');
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
  check(payload.semantic_catalog_root === manifest.semantic_catalog_root, `catalog root: ${summary.sequence}`);
  check(payload.hash_profile === manifest.hash_profile, `commit hash profile: ${summary.sequence}`);

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
check(commitSummaries[0].commit_hash === manifest.genesis_commit_hash, 'manifest genesis');
check(commitSummaries.at(-1).commit_hash === manifest.head_commit_hash, 'manifest head');
check(commitSummaries.at(-1).sequence === manifest.head_sequence, 'manifest head sequence');

const catalog = readJson(path.join(MP, 'semantic-catalog.json'));
const catalogInput = structuredClone(catalog);
delete catalogInput.root;
check(semanticCatalogRoot(catalogInput) === catalog.root, 'semantic catalog self-hash');
check(catalog.root === manifest.semantic_catalog_root, 'manifest semantic catalog root');

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

console.log(`Example CCF 0.1.2-rc1 mindpack verified: ${checks} checks passed.`);
