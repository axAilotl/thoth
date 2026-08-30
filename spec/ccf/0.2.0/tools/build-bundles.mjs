import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2');
const OUTPUT = path.join(ROOT, 'bundles');

function walk(directory) {
  const files = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...walk(absolute));
    else files.push(absolute);
  }
  return files;
}

function digest(file) {
  return `sha256:${crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')}`;
}

function artifact(sourcePackage, sourceRoot, relativePath) {
  return {
    source_package: sourcePackage,
    path: relativePath,
    digest: digest(path.join(sourceRoot, relativePath)),
  };
}

const draftArtifacts = [
  'CCF-0.2.0-DRAFT.md',
  'README.md',
  'semantic-catalog.json',
  'examples/capsule/manifest.json',
  'examples/capsule/submissions/links.ndjson',
  'examples/capsule/submissions/records.ndjson',
  'examples/capsule/opaque/governance-material.ndjson',
  ...walk(path.join(ROOT, 'schemas')).map((file) => path.relative(ROOT, file).replaceAll(path.sep, '/')),
  ...walk(path.join(ROOT, 'registries')).map((file) => path.relative(ROOT, file).replaceAll(path.sep, '/')),
].sort().map((relativePath) => artifact('ccf-0.2.0', ROOT, relativePath));

const baseCatalog = JSON.parse(fs.readFileSync(path.join(BASE, 'schemas', 'catalog.json'), 'utf8'));
const baseSchemaPathById = new Map(baseCatalog.schemas.map((entry) => [entry.id, entry.path]));
const baseTypes = JSON.parse(fs.readFileSync(path.join(BASE, 'registries', 'types.registry.json'), 'utf8')).entries;
const requirements = JSON.parse(
  fs.readFileSync(path.join(ROOT, 'registries', 'semantic-requirements.registry.json'), 'utf8'),
).entries;
const requirementByRecord = new Map(
  requirements
    .filter((entry) => entry.resource_kind === 'record_type')
    .map((entry) => [`${entry.name}\0${entry.version}`, entry]),
);

function recordSchemaPaths(predicate) {
  const paths = new Set();
  for (const entry of baseTypes) {
    const requirement = requirementByRecord.get(`${entry.name}\0${entry.version}`);
    if (!predicate(requirement)) continue;
    for (const schemaId of [entry.semantic_schema_id, entry.structural_schema_id]) {
      if (schemaId !== null) paths.add(baseSchemaPathById.get(schemaId));
    }
  }
  return [...paths].sort();
}

const exchangeBasePaths = [
  'schemas/common/defs.schema.json',
  'schemas/submissions/blob-submission.schema.json',
  'schemas/submissions/link-submission.schema.json',
  'schemas/submissions/record-submission.schema.json',
  ...recordSchemaPaths(
    (requirement) => requirement.minimum_level === 'ccf-exchange-v1'
      && requirement.semantic_pack === null
      && requirement.required_capabilities.length === 0,
  ),
  ...walk(path.join(BASE, 'registries'))
    .filter((file) => file.endsWith('.json'))
    .map((file) => path.relative(BASE, file).replaceAll(path.sep, '/')),
];

const canonicalBasePaths = [
  'schemas/common/compartment-envelope.schema.json',
  ...walk(path.join(BASE, 'schemas', 'objects'))
    .filter((file) => file.endsWith('.json') && !file.includes(`${path.sep}structural${path.sep}integrity-`))
    .filter((file) => !file.includes(`${path.sep}structural${path.sep}`))
    .filter((file) => !file.endsWith('commit-member.schema.json'))
    .filter((file) => !file.endsWith('mindpack-manifest.schema.json'))
    .map((file) => path.relative(BASE, file).replaceAll(path.sep, '/')),
  'schemas/operational/admission.schema.json',
  'schemas/operational/batch-result.schema.json',
  'schemas/operational/body-storage.schema.json',
];

const verifiedBasePaths = [
  'schemas/objects/commit-member.schema.json',
  'schemas/objects/mindpack-manifest.schema.json',
  'schemas/objects/structural/integrity-commit.schema.json',
  'schemas/payloads/integrity/commit.schema.json',
  'schemas/payloads/integrity/catalog_transition.schema.json',
  'schemas/objects/structural/integrity-catalog-transition.schema.json',
];

function baseArtifacts(paths) {
  return [...new Set(paths)].sort().map((relativePath) => artifact('ccf-0.1.2', BASE, relativePath));
}

const bundleDefinitions = [
  {
    filename: 'exchange.bundle.json',
    id: 'ccf-exchange-bundle-v1',
    kind: 'level',
    provides: 'ccf-exchange-v1',
    depends_on: [],
    artifacts: [...draftArtifacts, ...baseArtifacts(exchangeBasePaths)],
  },
  {
    filename: 'canonical-store.bundle.json',
    id: 'ccf-canonical-store-bundle-v1',
    kind: 'level',
    provides: 'ccf-canonical-store-v1',
    depends_on: ['ccf-exchange-bundle-v1'],
    artifacts: baseArtifacts(canonicalBasePaths),
  },
  {
    filename: 'verified-archive.bundle.json',
    id: 'ccf-verified-archive-bundle-v1',
    kind: 'level',
    provides: 'ccf-verified-archive-v1',
    depends_on: ['ccf-canonical-store-bundle-v1'],
    artifacts: baseArtifacts(verifiedBasePaths),
  },
  {
    filename: 'governed-archive.bundle.json',
    id: 'ccf-governed-archive-bundle-v1',
    kind: 'level',
    provides: 'ccf-governed-archive-v1',
    depends_on: ['ccf-verified-archive-bundle-v1'],
    artifacts: baseArtifacts([
      ...walk(path.join(BASE, 'schemas')).filter((file) => file.endsWith('.json')).map((file) => path.relative(BASE, file).replaceAll(path.sep, '/')),
      ...walk(path.join(BASE, 'registries')).filter((file) => file.endsWith('.json')).map((file) => path.relative(BASE, file).replaceAll(path.sep, '/')),
    ]),
  },
  {
    filename: 'signed-producer-sync.bundle.json',
    id: 'ccf-signed-producer-sync-bundle-v1',
    kind: 'capability',
    provides: 'ccf-signed-producer-sync-v1',
    depends_on: ['ccf-exchange-bundle-v1'],
    artifacts: baseArtifacts([
      'schemas/common/compartment-envelope.schema.json',
      'schemas/objects/record-header.schema.json',
      'schemas/objects/record-structural-content.schema.json',
      'schemas/objects/record-structural.schema.json',
      'schemas/sync/producer-batch.schema.json',
      'schemas/sync/delta-pack-manifest.schema.json',
      'schemas/sync/sync-head.schema.json',
      'schemas/security/device-credential.schema.json',
      'schemas/objects/structural/core-device-credential.schema.json',
      'schemas/payloads/sync/producer_batch_receipt.schema.json',
    ]),
  },
  ...[
    ['continuity', 'ccf-continuity-pack-v1'],
    ['work', 'ccf-work-pack-v1'],
    ['agent', 'ccf-agent-pack-v1'],
  ].map(([namespace, packId]) => ({
    filename: `${namespace}.bundle.json`,
    id: `ccf-${namespace}-pack-bundle-v1`,
    kind: 'semantic_pack',
    provides: packId,
    depends_on: ['ccf-exchange-bundle-v1'],
    artifacts: baseArtifacts(recordSchemaPaths((requirement) => requirement.semantic_pack === packId)),
  })),
];

fs.mkdirSync(OUTPUT, { recursive: true });
for (const definition of bundleDefinitions) {
  const { filename, ...manifest } = definition;
  fs.writeFileSync(
    path.join(OUTPUT, filename),
    `${JSON.stringify({ format: 'ccf.bundle-manifest/0.2.0', status: 'working-draft', ...manifest }, null, 2)}\n`,
  );
}
console.log(`distribution bundles: ${bundleDefinitions.length}`);
