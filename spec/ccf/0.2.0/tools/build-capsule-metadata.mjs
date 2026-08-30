import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { submissionHash } from '../../0.1.2/tools/ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CAPSULE = path.join(ROOT, 'examples', 'capsule');
const BASE = path.resolve(ROOT, '..', '0.1.2');
const manifestPath = path.join(CAPSULE, 'manifest.json');
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));

for (const stream of manifest.streams) {
  const content = fs.readFileSync(path.join(CAPSULE, stream.path));
  stream.digest = `sha256:${crypto.createHash('sha256').update(content).digest('hex')}`;
  stream.byte_length = String(content.byteLength);
}

fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

const downgradePath = path.join(CAPSULE, 'downgrade-receipt.json');
const downgrade = JSON.parse(fs.readFileSync(downgradePath, 'utf8'));
const sourceMindpack = path.join(BASE, 'examples', 'mindpack');
const sourceManifest = JSON.parse(fs.readFileSync(path.join(sourceMindpack, 'manifest.json'), 'utf8'));
const downgradeSourceRoot = path.join(CAPSULE, 'downgrade-source');
const downgradeExportRoot = path.join(CAPSULE, 'downgrade-export');
fs.rmSync(downgradeSourceRoot, { recursive: true, force: true });
fs.rmSync(downgradeExportRoot, { recursive: true, force: true });
fs.mkdirSync(downgradeSourceRoot, { recursive: true });
fs.mkdirSync(downgradeExportRoot, { recursive: true });
const selectedBatchPath = 'producer-batches/98d352bf-7abb-4fdf-824c-3c93c4e55901.json';
const sourceFiles = [
  ['other', 'objects/records.ndjson'],
  ['other', 'objects/links.ndjson'],
  ['other', 'objects/blobs.ndjson'],
  ['journal_proof', 'integrity/commits.ndjson'],
  ['journal_proof', 'integrity/members.ndjson'],
  ['other', 'origin-index.ndjson'],
  ['other', selectedBatchPath],
];
const selectedBatch = JSON.parse(fs.readFileSync(path.join(sourceMindpack, selectedBatchPath), 'utf8'));
const selectedExportSubmissions = selectedBatch.records.filter(
  (submission) => submission.type === 'core.session',
);
for (const submission of selectedExportSubmissions) {
  const uuid = submission.id.slice(submission.id.lastIndexOf(':') + 1);
  for (const compartment of ['structural', 'semantic']) {
    sourceFiles.push(['compartment', `compartments/records/${uuid}.${compartment}.json`]);
  }
}
const selectedCommits = fs.readFileSync(path.join(sourceMindpack, 'integrity', 'commits.ndjson'), 'utf8')
  .trim().split('\n').filter(Boolean).map(JSON.parse);
for (const commit of selectedCommits) {
  const uuid = commit.record_id.slice(commit.record_id.lastIndexOf(':') + 1);
  sourceFiles.push(['compartment', `compartments/records/${uuid}.structural.json`]);
}
for (const [, relativePath] of sourceFiles) {
  const destination = path.join(downgradeSourceRoot, relativePath);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.copyFileSync(
    path.join(sourceMindpack, relativePath),
    destination,
  );
}
const genesisUuid = selectedCommits[0].record_id.slice(
  selectedCommits[0].record_id.lastIndexOf(':') + 1,
);
const genesisEnvelope = JSON.parse(fs.readFileSync(
  path.join(sourceMindpack, 'compartments', 'records', `${genesisUuid}.structural.json`),
  'utf8',
));
const genesisPayload = genesisEnvelope.content.structural_payload;
const sourceIdentityPath = 'source-identity.json';
const sourceIdentity = {
  format: 'ccf.verified-source-identity/0.2.0',
  archive_id: sourceManifest.archive_id,
  epoch_id: sourceManifest.epoch_id,
  genesis_commit_hash: sourceManifest.genesis_commit_hash,
  head_commit_hash: sourceManifest.head_commit_hash,
  head_sequence: sourceManifest.head_sequence,
  semantic_catalog_root: sourceManifest.semantic_catalog_root,
  trusted_genesis_signer_key_id: genesisPayload.signer_key_id,
  trusted_genesis_signer_public_key: genesisPayload.signer_public_key,
};
fs.writeFileSync(
  path.join(downgradeSourceRoot, sourceIdentityPath),
  `${JSON.stringify(sourceIdentity, null, 2)}\n`,
);
const exchangePath = 'submissions/records.ndjson';
const exchangeDestination = path.join(downgradeExportRoot, exchangePath);
fs.mkdirSync(path.dirname(exchangeDestination), { recursive: true });
fs.writeFileSync(
  exchangeDestination,
  `${selectedExportSubmissions.map((submission) => JSON.stringify(submission)).join('\n')}\n`,
);
const exchangeBytes = fs.readFileSync(exchangeDestination);

function inventoryEntry(category, subject) {
  const content = fs.readFileSync(path.join(CAPSULE, subject));
  return {
    category,
    subject,
    digest: `sha256:${crypto.createHash('sha256').update(content).digest('hex')}`,
  };
}

const sourceSubjects = sourceFiles.map(([category, relativePath]) => (
  [category, `downgrade-source/${relativePath}`]
));
sourceSubjects.push(['journal_proof', `downgrade-source/${sourceIdentityPath}`]);
const sourceInventory = sourceSubjects.map(([category, subject]) => (
  inventoryEntry(category, subject)
));
const assertionInventory = selectedExportSubmissions.map((submission) => ({
  category: 'submission',
  subject: `submission:${submission.id}`,
  digest: submissionHash(submission),
}));
sourceInventory.push(...assertionInventory);
const exportInventory = structuredClone(assertionInventory);
fs.writeFileSync(
  path.join(CAPSULE, downgrade.source_inventory.path),
  `${JSON.stringify(sourceInventory, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(CAPSULE, downgrade.export_inventory.path),
  `${JSON.stringify(exportInventory, null, 2)}\n`,
);
for (const inventory of [downgrade.source_inventory, downgrade.export_inventory]) {
  const content = fs.readFileSync(path.join(CAPSULE, inventory.path));
  inventory.digest = `sha256:${crypto.createHash('sha256').update(content).digest('hex')}`;
}
downgrade.omissions = sourceInventory
  .filter((source) => !exportInventory.some(
    (exported) => exported.category === source.category && exported.subject === source.subject,
  ))
  .map(({ category, subject }) => ({
    category,
    subject,
    reason: 'The Exchange export retains only the selected source assertions and omits producer-batch, canonical-object, and archive-history material.',
  }));
downgrade.preserved_opaque = [];
for (const item of downgrade.preserved_opaque) {
  const content = fs.readFileSync(path.join(CAPSULE, item.path));
  item.digest = `sha256:${crypto.createHash('sha256').update(content).digest('hex')}`;
}
const downgradeExportManifest = {
  format: 'ccf.capsule/0.2.0',
  pack_id: downgrade.export_pack_id,
  created_at: downgrade.created_at,
  level: 'ccf-exchange-v1',
  capabilities: [],
  root_record_id: selectedExportSubmissions[0].id,
  membership_link_types: ['ccf.part_of'],
  custody: {
    completeness: 'partial',
    losslessness: downgrade.losslessness,
    omissions: structuredClone(downgrade.omissions),
  },
  catalog_dependencies: structuredClone(manifest.catalog_dependencies),
  streams: [{
    path: exchangePath,
    media_type: 'application/x-ndjson',
    content_role: 'submissions',
    handling: 'activate',
    activation_requirements: { minimum_level: 'ccf-exchange-v1', capabilities: [] },
    digest: `sha256:${crypto.createHash('sha256').update(exchangeBytes).digest('hex')}`,
    byte_length: String(exchangeBytes.byteLength),
    required: true,
  }],
  dependencies: [{
    object_id: selectedExportSubmissions[0].origin.source_id,
    availability: 'external',
    reason: 'The source-device Record is outside this scoped downgrade export.',
    locator: null,
    source_custody_proof: `receipt:${downgrade.receipt_id}`,
    unavailability_lineage_id: null,
  }],
  proofs: [],
  extensions: { 'org.example.transfer_label': 'Selected verified source assertion' },
};
fs.writeFileSync(
  path.join(downgradeExportRoot, 'manifest.json'),
  `${JSON.stringify(downgradeExportManifest, null, 2)}\n`,
);
fs.writeFileSync(downgradePath, `${JSON.stringify(downgrade, null, 2)}\n`);

console.log(
  `capsule metadata (${manifest.streams.length} streams, 2 downgrade inventories, ${downgrade.preserved_opaque.length} opaque exports)`,
);
