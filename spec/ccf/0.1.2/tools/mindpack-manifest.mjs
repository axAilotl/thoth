import fs from 'node:fs';
import path from 'node:path';
import { digestString } from './ccf-jcs.mjs';

function fail(label) {
  throw new Error(`manifest mismatch: ${label}`);
}

function equalJson(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function sorted(values) {
  return [...values].sort();
}

export function actualStreams(root) {
  function walk(directory) {
    const files = [];
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isDirectory()) files.push(...walk(absolute));
      else files.push(absolute);
    }
    return files;
  }
  return walk(root)
    .map((absolute) => {
      const relative = path.relative(root, absolute).replaceAll(path.sep, '/');
      const bytes = fs.readFileSync(absolute);
      return {
        path: relative,
        digest: digestString(bytes),
        byte_length: String(bytes.length),
        required: !relative.startsWith('blob-data/'),
      };
    })
    .filter((entry) => entry.path !== 'manifest.json')
    .sort((a, b) => a.path.localeCompare(b.path));
}

function referencedObjectIds(values) {
  const references = new Set();
  function visit(value) {
    if (typeof value === 'string') {
      if (/^urn:ccf:(record|link|blob):/.test(value)) references.add(value);
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    if (value !== null && typeof value === 'object') {
      for (const item of Object.values(value)) visit(item);
    }
  }
  for (const value of values) visit(value);
  return references;
}

function receiptCoverage(headers, structuralById) {
  const receipts = new Set(
    headers
      .filter((header) => structuralById.get(header.id)?.content?.type === 'lineage.erasure_receipt')
      .map((header) => header.id),
  );
  const covered = new Map();
  for (const header of headers.filter((candidate) => candidate.object_kind === 'link')) {
    const content = structuralById.get(header.id)?.content;
    if (content?.type === 'ccf.covers' && receipts.has(content.from_id)) {
      if (!covered.has(content.to_id) || content.from_id < covered.get(content.to_id)) {
        covered.set(content.to_id, content.from_id);
      }
    }
  }
  return covered;
}

export function deriveManifestGroundTruth({
  root,
  records,
  links,
  blobs,
  structuralById,
  semanticById,
  members,
  commitSummaries,
  chain,
  catalogRoot,
}) {
  const headers = [...records, ...links, ...blobs];
  const includedIds = new Set(headers.map((header) => header.id));
  const coordinates = new Map(
    members.map((member) => [
      member.object_id,
      `${member.commit_sequence}:${member.commit_position}`,
    ]),
  );
  const covered = receiptCoverage(headers, structuralById);
  const availability = new Map();
  const withheld = new Set();
  const erased = new Set();
  for (const header of headers) {
    const structural = structuralById.get(header.id)?.content;
    const expected = ['structural'];
    if (header.semantic_commitment !== null) expected.push('semantic');
    if (header.object_kind === 'blob') expected.push('blob_content');
    const states = [];
    for (const compartment of expected) {
      const present = compartment === 'structural'
        ? structuralById.has(header.id)
        : compartment === 'semantic'
          ? semanticById.has(header.id)
          : fs.existsSync(path.join(root, 'blob-data', `${header.id.slice(header.id.lastIndexOf(':') + 1)}.bin`));
      const state = present ? 'available' : covered.has(header.id) ? 'erased' : 'withheld';
      states.push(state);
      const coordinate = coordinates.get(header.id);
      const commitment = compartment === 'structural'
        ? header.structural_commitment
        : compartment === 'semantic'
          ? header.semantic_commitment
          : structural?.content_commitment;
      const retentionProfile = structural?.retention_profile
        ?? (compartment === 'structural' && state === 'erased' ? 'erasable' : null);
      availability.set(`${header.id}\0${compartment}`, {
        object_kind: header.object_kind,
        object_id: header.id,
        compartment,
        availability: state,
        commitment,
        retention_profile: retentionProfile,
        source_custody_proof: state === 'available' ? null : coordinate ? `commit:${coordinate}` : null,
        unavailability_lineage_id: state === 'erased' ? covered.get(header.id) : null,
      });
    }
    if (states.includes('erased')) erased.add(header.id);
    else if (states.includes('withheld')) withheld.add(header.id);
  }

  const references = referencedObjectIds([
    ...structuralById.values(),
    ...semanticById.values(),
  ]);
  for (const objectId of includedIds) references.delete(objectId);
  for (const objectId of withheld) references.delete(objectId);
  for (const objectId of erased) references.delete(objectId);
  const missingMemberIds = new Set(
    members.map((member) => member.object_id).filter((objectId) => !includedIds.has(objectId)),
  );
  const custodyComplete = references.size === 0
    && missingMemberIds.size === 0
    && withheld.size === 0;
  const externalDependencies = new Map(
    [...references].map((objectId) => [
      objectId,
      { object_id: objectId, reason: 'unresolved_reference' },
    ]),
  );
  const foreignCustodyProofs = new Set();
  for (const header of headers) {
    const content = structuralById.get(header.id)?.content;
    const sourceArchiveId = content?.structural_payload?.archive_id;
    if (content?.type === 'integrity.commit'
        && sourceArchiveId
        && sourceArchiveId !== chain.archive_id) {
      foreignCustodyProofs.add(`${sourceArchiveId}:${header.object_hash}`);
    }
  }
  return {
    counts: {
      records: String(records.length),
      links: String(links.length),
      blobs: String(blobs.length),
      commits: String(commitSummaries.length),
    },
    streams: actualStreams(root),
    external_dependencies: externalDependencies,
    withheld,
    erased,
    availability,
    custody: {
      completeness: custodyComplete ? 'complete' : 'partial',
      restore_capable: custodyComplete,
    },
    genesis_commit_hash: chain.genesis_commit_hash,
    head_commit_hash: chain.head_commit_hash,
    head_sequence: chain.head_sequence,
    semantic_catalog_root: catalogRoot,
    hash_profile: chain.hash_profile,
    archive_id: chain.archive_id,
    epoch_id: chain.epoch_id,
    profiles: chain.active_profiles,
    foreign_custody_proofs: foreignCustodyProofs,
  };
}

export function compareManifest(manifest, truth, { operation = 'restore' } = {}) {
  for (const field of ['records', 'links', 'blobs', 'commits']) {
    if (manifest.counts[field] !== truth.counts[field]) fail(`counts.${field}`);
  }

  const claimedStreams = new Map();
  for (const entry of manifest.streams) {
    if (claimedStreams.has(entry.path)) fail(`duplicate stream ${entry.path}`);
    claimedStreams.set(entry.path, entry);
  }
  if (claimedStreams.size !== truth.streams.length) fail('stream membership');
  for (const actual of truth.streams) {
    if (!equalJson(claimedStreams.get(actual.path), actual)) fail(`stream ${actual.path}`);
  }

  for (const field of [
    'archive_id',
    'epoch_id',
    'genesis_commit_hash',
    'head_commit_hash',
    'head_sequence',
    'semantic_catalog_root',
    'hash_profile',
    'profiles',
  ]) {
    const matches = field === 'profiles'
      ? equalJson(manifest[field], truth[field])
      : manifest[field] === truth[field];
    if (!matches) fail(field);
  }
  const allowedModes = operation === 'restore'
    ? new Set(['restore', 'replica'])
    : operation === 'foreign_merge'
      ? new Set(['restore', 'replica', 'foreign_merge'])
      : new Set(['restore', 'replica']);
  if (!allowedModes.has(manifest.mode)) fail('mode');
  if (!equalJson(manifest.custody, truth.custody)) fail('custody');
  if (operation === 'restore' && !truth.custody.restore_capable) fail('restore capability');

  if (!equalJson(sorted(manifest.withheld), sorted(truth.withheld))) fail('withheld');
  if (!equalJson(sorted(manifest.erased), sorted(truth.erased))) fail('erased');
  const claimedAvailability = new Map();
  for (const entry of manifest.compartment_availability) {
    const key = `${entry.object_id}\0${entry.compartment}`;
    if (claimedAvailability.has(key)) fail(`duplicate availability ${key}`);
    claimedAvailability.set(key, entry);
  }
  if (claimedAvailability.size !== truth.availability.size) fail('availability membership');
  for (const [key, actual] of truth.availability) {
    if (!equalJson(claimedAvailability.get(key), actual)) fail(`availability ${key}`);
  }

  const claimedDependencies = new Map();
  for (const entry of manifest.external_dependencies) {
    if (claimedDependencies.has(entry.object_id)) fail('duplicate external dependency');
    claimedDependencies.set(entry.object_id, entry);
  }
  if (claimedDependencies.size !== truth.external_dependencies.size) fail('external dependencies');
  for (const [objectId, actual] of truth.external_dependencies) {
    if (!equalJson(claimedDependencies.get(objectId), actual)) fail(`external dependency ${objectId}`);
  }
  if (!equalJson(sorted(manifest.foreign_custody_proofs), sorted(truth.foreign_custody_proofs))) fail('foreign custody proofs');
  if (Object.keys(manifest.extensions).length !== 0) fail('unknown extensions');
}
