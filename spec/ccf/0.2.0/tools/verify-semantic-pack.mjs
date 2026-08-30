import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2');
const requested = process.argv[2];
const readJson = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));
const packs = readJson(path.join(ROOT, 'registries', 'semantic-packs.registry.json')).entries;
const pack = packs.find((entry) => entry.id === requested);
if (!pack) throw new Error(`unknown semantic pack: ${requested}`);

const requirements = readJson(
  path.join(ROOT, 'registries', 'semantic-requirements.registry.json'),
).entries.filter((entry) => entry.semantic_pack === requested);
if (requirements.length === 0) throw new Error(`semantic pack has no registered resources: ${requested}`);
if (requirements.some((entry) => entry.minimum_level !== 'ccf-exchange-v1')) {
  throw new Error(`semantic pack is not Exchange-activatable: ${requested}`);
}

const bundle = fs.readdirSync(path.join(ROOT, 'bundles'))
  .filter((name) => name.endsWith('.bundle.json'))
  .map((name) => readJson(path.join(ROOT, 'bundles', name)))
  .find((candidate) => candidate.kind === 'semantic_pack' && candidate.provides === requested);
if (!bundle) throw new Error(`semantic pack has no distribution bundle: ${requested}`);

const baseCatalog = readJson(path.join(BASE, 'schemas', 'catalog.json'));
const schemaPathById = new Map(baseCatalog.schemas.map((entry) => [entry.id, entry.path]));
const registryByKind = {
  record_type: 'types.registry.json',
  link_type: 'links.registry.json',
  blob_type: 'blobs.registry.json',
};
const expectedSchemas = new Set();
for (const requirement of requirements) {
  const registryName = registryByKind[requirement.resource_kind];
  if (!registryName) continue;
  const registryEntry = readJson(path.join(BASE, 'registries', registryName)).entries.find(
    (entry) => entry.name === requirement.name && entry.version === requirement.version,
  );
  if (!registryEntry) throw new Error(`missing base registry entry: ${requirement.name}`);
  const schemaPath = schemaPathById.get(registryEntry.semantic_schema_id);
  if (!schemaPath) throw new Error(`missing semantic schema: ${registryEntry.semantic_schema_id}`);
  expectedSchemas.add(schemaPath);
}

const expectedArtifacts = new Set(
  [...expectedSchemas].map((schemaPath) => `ccf-0.1.2\0${schemaPath}`),
);
const sameSet = (left, right) => (
  left.size === right.size && [...left].every((value) => right.has(value))
);
const bundledArtifacts = new Set(
  bundle.artifacts.map((artifact) => `${artifact.source_package}\0${artifact.path}`),
);
if (!sameSet(bundledArtifacts, expectedArtifacts)) {
  const missing = [...expectedArtifacts].filter((expected) => !bundledArtifacts.has(expected));
  const extra = [...bundledArtifacts].filter((candidate) => !expectedArtifacts.has(candidate));
  throw new Error(`bundle artifact boundary mismatch; missing=${missing.join(',')} extra=${extra.join(',')}`);
}
const contaminatedArtifacts = new Set([
  ...bundledArtifacts,
  'ccf-0.2.0\0tools/foreign-pack-hook.mjs',
]);
if (sameSet(contaminatedArtifacts, expectedArtifacts)) {
  throw new Error('semantic-pack exact-boundary negative check is ineffective');
}
for (const artifact of bundle.artifacts) {
  const source = artifact.source_package === 'ccf-0.1.2' ? BASE : ROOT;
  if (!fs.existsSync(path.join(source, artifact.path))) {
    throw new Error(`bundle artifact is absent: ${artifact.path}`);
  }
}

console.log(
  `${requested} semantic pack passes: ${requirements.length} resources, ${expectedSchemas.size} payload schemas.`,
);
