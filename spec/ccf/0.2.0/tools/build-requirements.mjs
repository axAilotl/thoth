import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2', 'registries');

const profileCapabilities = new Map([
  ['ccf-local-sync-0.1.2', 'ccf-signed-producer-sync-v1'],
  ['ccf-archive-encryption-derived-v1', 'ccf-archive-encryption-derived-v1'],
  ['ccf-object-erasure-v1', 'ccf-object-erasure-v1'],
  ['ccf-witnessed-integrity-v1', 'ccf-witnessed-integrity-v1'],
  ['ccf-succession-v1', 'ccf-succession-v1'],
]);

const profileSemanticPacks = new Map([
  ['ccf-continuity-pack-0.1.2', 'ccf-continuity-pack-v1'],
  ['ccf-work-pack-0.1.2', 'ccf-work-pack-v1'],
  ['ccf-agent-pack-0.1.2', 'ccf-agent-pack-v1'],
]);

function readRegistry(filename) {
  return JSON.parse(fs.readFileSync(path.join(BASE, filename), 'utf8')).entries;
}

function recordMinimumLevel(entry) {
  if (entry.name === 'integrity.commit' || entry.name === 'integrity.catalog_transition') {
    return 'ccf-verified-archive-v1';
  }
  if (entry.profile === 'ccf-witnessed-integrity-v1' || entry.profile === 'ccf-succession-v1') {
    return 'ccf-verified-archive-v1';
  }
  if (entry.profile === 'ccf-archive-encryption-derived-v1') {
    return 'ccf-canonical-store-v1';
  }
  if (
    entry.name.startsWith('governance.')
    || entry.name.startsWith('lineage.')
    || entry.name === 'semantic.entity_resolution'
  ) {
    return 'ccf-governed-archive-v1';
  }
  return 'ccf-exchange-v1';
}

function recordStateEffectsLevel(entry) {
  if (entry.lineage_mode !== 'compare_and_swap') return null;
  if (entry.name === 'core.device_credential') return 'ccf-exchange-v1';
  if (entry.profile === 'ccf-archive-encryption-derived-v1') return 'ccf-canonical-store-v1';
  if (
    entry.name === 'integrity.catalog_transition'
    || entry.profile === 'ccf-succession-v1'
  ) {
    return 'ccf-verified-archive-v1';
  }
  return 'ccf-governed-archive-v1';
}

function linkMinimumLevel(entry) {
  const governedLinks = new Set([
    'ccf.authorized_by',
    'ccf.covers',
    'ccf.destroys_key',
    'ccf.governed_by',
    'ccf.invalidates',
    'ccf.tombstones',
  ]);
  return governedLinks.has(entry.name) ? 'ccf-governed-archive-v1' : 'ccf-exchange-v1';
}

function blobMinimumLevel(entry) {
  return entry.name === 'blob.suppression_set'
    ? 'ccf-governed-archive-v1'
    : 'ccf-exchange-v1';
}

function predicateRequirements(entry) {
  if (entry.name.startsWith('ccf.work.') || entry.name === 'ccf.source.selected_for_project') {
    return { minimumLevel: 'ccf-exchange-v1', semanticPack: 'ccf-work-pack-v1' };
  }
  if (entry.name === 'ccf.preference.targets') {
    return { minimumLevel: 'ccf-exchange-v1', semanticPack: 'ccf-continuity-pack-v1' };
  }
  return { minimumLevel: 'ccf-exchange-v1', semanticPack: null };
}

function requirement(
  resourceKind,
  entry,
  minimumLevel,
  profile = null,
  semanticPack = null,
  stateEffectsLevel = null,
) {
  const capability = profileCapabilities.get(profile);
  return {
    resource_kind: resourceKind,
    name: entry.name,
    version: entry.version,
    minimum_level: minimumLevel,
    state_effects_level: stateEffectsLevel,
    required_capabilities: capability === undefined ? [] : [capability],
    semantic_pack: semanticPack ?? profileSemanticPacks.get(profile) ?? null,
    below_minimum_behavior: 'preserve_inert_or_refuse',
  };
}

const entries = [
  ...readRegistry('types.registry.json').map((entry) =>
    requirement(
      'record_type',
      entry,
      recordMinimumLevel(entry),
      entry.profile,
      null,
      recordStateEffectsLevel(entry),
    )),
  ...readRegistry('links.registry.json').map((entry) =>
    requirement('link_type', entry, linkMinimumLevel(entry), entry.profile)),
  ...readRegistry('blobs.registry.json').map((entry) =>
    requirement('blob_type', entry, blobMinimumLevel(entry), entry.profile)),
  ...readRegistry('predicates.registry.json').map((entry) => {
    const { minimumLevel, semanticPack } = predicateRequirements(entry);
    return requirement('predicate', entry, minimumLevel, null, semanticPack);
  }),
].sort((left, right) =>
  `${left.resource_kind}\0${left.name}\0${left.version}`.localeCompare(
    `${right.resource_kind}\0${right.name}\0${right.version}`,
  ));

const registry = {
  registry: 'ccf.semantic-requirements/0.2.0',
  base_registry_set: 'ccf/0.1.2',
  entries,
};

fs.writeFileSync(
  path.join(ROOT, 'registries', 'semantic-requirements.registry.json'),
  `${JSON.stringify(registry, null, 2)}\n`,
);
console.log(`semantic requirements: ${entries.length} inherited resources`);
