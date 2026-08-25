import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  compartmentCommitment,
  commitSigningDigest,
  merkleRoot,
  objectHash,
  semanticCatalogRoot,
  submissionHash,
} from '../../0.1.2/tools/ccf-jcs.mjs';
import { actualStreams } from '../../0.1.2/tools/mindpack-manifest.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2');
const VECTORS = path.join(BASE, 'vectors');
const MINDPACK = path.join(BASE, 'examples', 'mindpack');
const operationVectors = JSON.parse(
  fs.readFileSync(path.join(ROOT, 'vectors', 'verified-archive-operations.json'), 'utf8'),
);
if (path.resolve(ROOT, operationVectors.source_fixture) !== MINDPACK) {
  throw new Error('verified operation vector points at an unexpected source fixture');
}
let checks = 0;

function check(condition, label) {
  checks += 1;
  if (!condition) throw new Error(`FAIL: ${label}`);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function readNdjson(file) {
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
}

const merkleVectors = readJson(path.join(VECTORS, 'merkle.json'));
check(merkleRoot([]) === merkleVectors.empty_expected, 'empty Merkle root');
for (const name of ['commit1', 'commit2']) {
  check(
    merkleRoot(merkleVectors[name].members) === merkleVectors[name].expected_root,
    `${name} Merkle root`,
  );
}

const publicKey = fs.readFileSync(path.join(VECTORS, 'archive-ed25519-public.pem'));
const commitVectors = readJson(path.join(VECTORS, 'commit-signing.json'));
for (const [name, vector] of Object.entries(commitVectors)) {
  const digest = commitSigningDigest(
    vector.signing_header,
    vector.structural_content_without_signature,
  );
  check(
    `sha256:${digest.toString('hex')}` === vector.expected_signing_digest,
    `${name} signing digest`,
  );
  check(
    crypto.verify(null, digest, publicKey, Buffer.from(vector.signature, 'base64url')),
    `${name} archive signature`,
  );
  check(objectHash(vector.header) === vector.expected_commit_hash, `${name} commit hash`);

  const tamperedSignature = Buffer.from(vector.signature, 'base64url');
  tamperedSignature[0] ^= 1;
  check(
    !crypto.verify(null, digest, publicKey, tamperedSignature),
    `${name} signature tamper rejected`,
  );
}

const baseCatalog = readJson(path.join(BASE, 'semantic-catalog.json'));
const { root: baseRoot, ...baseCatalogWithoutRoot } = baseCatalog;
check(semanticCatalogRoot(baseCatalogWithoutRoot) === baseRoot, 'base semantic catalog root');

const manifest = readJson(path.join(MINDPACK, 'manifest.json'));
const actualStreamMap = new Map(actualStreams(MINDPACK).map((entry) => [entry.path, entry]));
const manifestStreamMap = new Map(manifest.streams.map((entry) => [entry.path, entry]));
check(
  actualStreamMap.size === manifestStreamMap.size
    && [...actualStreamMap].every(
      ([streamPath, entry]) => JSON.stringify(entry) === JSON.stringify(manifestStreamMap.get(streamPath)),
    ),
  'mindpack stream inventory and digests',
);
const tamperedStreams = new Map(manifestStreamMap);
const firstStream = tamperedStreams.values().next().value;
tamperedStreams.set(firstStream.path, { ...firstStream, byte_length: String(Number(firstStream.byte_length) + 1) });
check(
  ![...actualStreamMap].every(
    ([streamPath, entry]) => JSON.stringify(entry) === JSON.stringify(tamperedStreams.get(streamPath)),
  ),
  'mindpack stream tamper rejected',
);
const commits = readNdjson(path.join(MINDPACK, 'integrity', 'commits.ndjson'));
const members = readNdjson(path.join(MINDPACK, 'integrity', 'members.ndjson'));
const headers = [
  ...readNdjson(path.join(MINDPACK, 'objects', 'records.ndjson')),
  ...readNdjson(path.join(MINDPACK, 'objects', 'links.ndjson')),
  ...readNdjson(path.join(MINDPACK, 'objects', 'blobs.ndjson')),
];
const headerById = new Map(headers.map((header) => [header.id, header]));
const genesisUuid = commits[0].record_id.slice(commits[0].record_id.lastIndexOf(':') + 1);
const genesisEnvelope = readJson(
  path.join(MINDPACK, 'compartments', 'records', `${genesisUuid}.structural.json`),
);
const genesisPayload = genesisEnvelope.content.structural_payload;
check(
  crypto.createPublicKey(publicKey).export({ format: 'jwk' }).x === genesisPayload.signer_public_key,
  'trusted archive key matches pinned genesis signer',
);
check(commits.length === Number(manifest.counts.commits), 'mindpack commit count');
check(commits[0].commit_hash === manifest.genesis_commit_hash, 'mindpack genesis pin');
check(commits.at(-1).commit_hash === manifest.head_commit_hash, 'mindpack head pin');
check(commits.at(-1).sequence === manifest.head_sequence, 'mindpack head sequence');
check(manifest.semantic_catalog_root === baseRoot, 'mindpack catalog pin');

function journalIsBound(
  candidateCommits,
  candidateMembers,
  {
    sourceRoot = MINDPACK,
    sourceHeaders = headers,
    sourceHeaderById = headerById,
    sourceIdentity = manifest,
    sourceCommitEnvelopes = null,
  } = {},
) {
  try {
    const trustedGenesisUuid = candidateCommits[0].record_id.slice(
      candidateCommits[0].record_id.lastIndexOf(':') + 1,
    );
    const trustedGenesisEnvelope = sourceCommitEnvelopes?.get(candidateCommits[0].record_id)
      ?? readJson(path.join(
        sourceRoot,
        'compartments',
        'records',
        `${trustedGenesisUuid}.structural.json`,
      ));
    const trustedGenesisPayload = trustedGenesisEnvelope.content.structural_payload;
    if (
      candidateCommits[0].commit_hash !== sourceIdentity.genesis_commit_hash
      || candidateCommits.at(-1).commit_hash !== sourceIdentity.head_commit_hash
      || candidateCommits.at(-1).sequence !== sourceIdentity.head_sequence
      || (sourceIdentity.trusted_genesis_signer_key_id !== undefined
        && trustedGenesisPayload.signer_key_id !== sourceIdentity.trusted_genesis_signer_key_id)
      || (sourceIdentity.trusted_genesis_signer_public_key !== undefined
        && trustedGenesisPayload.signer_public_key !== sourceIdentity.trusted_genesis_signer_public_key)
    ) return false;
    const commitRecordIds = new Set(candidateCommits.map((commit) => commit.record_id));
    const expectedMemberIds = new Set(
      sourceHeaders.filter((header) => !commitRecordIds.has(header.id)).map((header) => header.id),
    );
    const memberIds = new Set(candidateMembers.map((member) => member.object_id));
    if (
      memberIds.size !== candidateMembers.length
      || memberIds.size !== expectedMemberIds.size
      || [...memberIds].some((id) => !expectedMemberIds.has(id))
      || candidateMembers.some(
        (member) => !candidateCommits.some((commit) => commit.sequence === member.commit_sequence)
          || sourceHeaderById.get(member.object_id)?.object_hash !== member.object_hash,
      )
    ) return false;
    return candidateCommits.every((commit, index) => {
      const expectedSequence = String(index);
      const expectedParent = index === 0 ? null : candidateCommits[index - 1].commit_hash;
      const commitMembers = candidateMembers.filter(
        (member) => member.commit_sequence === commit.sequence,
      );
      const header = sourceHeaderById.get(commit.record_id);
      if (!header || header.object_hash !== commit.commit_hash || objectHash(header) !== commit.commit_hash) {
        return false;
      }
      const uuid = commit.record_id.slice(commit.record_id.lastIndexOf(':') + 1);
      const envelope = sourceCommitEnvelopes?.get(commit.record_id)
        ?? readJson(path.join(
          sourceRoot,
          'compartments',
          'records',
          `${uuid}.structural.json`,
        ));
      if (compartmentCommitment('record', 'structural', envelope) !== header.structural_commitment) {
        return false;
      }
      const content = envelope.content;
      const payload = content.structural_payload;
      if (
        content.type !== 'integrity.commit'
        || payload.sequence !== commit.sequence
        || payload.parent_commit_hash !== commit.parent_commit_hash
        || payload.batch_merkle_root !== commit.merkle_root
        || payload.member_count !== String(commitMembers.length)
        || payload.archive_id !== sourceIdentity.archive_id
        || payload.epoch_id !== sourceIdentity.epoch_id
        || payload.semantic_catalog_root !== sourceIdentity.semantic_catalog_root
        || payload.signer_key_id !== trustedGenesisPayload.signer_key_id
        || payload.signer_public_key !== trustedGenesisPayload.signer_public_key
        || commit.sequence !== expectedSequence
        || commit.parent_commit_hash !== expectedParent
        || merkleRoot(commitMembers) !== commit.merkle_root
      ) return false;
      const signingHeader = structuredClone(header);
      delete signingHeader.object_hash;
      delete signingHeader.structural_commitment;
      const unsignedContent = structuredClone(content);
      delete unsignedContent.structural_payload.signature;
      const signingKey = crypto.createPublicKey({
        key: { kty: 'OKP', crv: 'Ed25519', x: trustedGenesisPayload.signer_public_key },
        format: 'jwk',
      });
      return crypto.verify(
        null,
        commitSigningDigest(signingHeader, unsignedContent),
        signingKey,
        Buffer.from(payload.signature, 'base64url'),
      );
    });
  } catch {
    return false;
  }
}
check(journalIsBound(commits, members), 'restore journal rows bind to signed commit objects');
const tamperedCommits = structuredClone(commits);
tamperedCommits[1].commit_hash = `sha256:${'d'.repeat(64)}`;
tamperedCommits[2].parent_commit_hash = tamperedCommits[1].commit_hash;
check(!journalIsBound(tamperedCommits, members), 'signed middle-commit substitution rejected');
const extraMembers = structuredClone(members);
extraMembers.push({ ...extraMembers[0], commit_sequence: '99' });
check(!journalIsBound(commits, extraMembers), 'extra or duplicate journal membership rejected');

const tamperedMembers = structuredClone(members);
tamperedMembers[0].object_hash = `sha256:${'0'.repeat(64)}`;
const tamperedSequence = members[0].commit_sequence;
const firstCommitMembers = members.filter(
  (member) => member.commit_sequence === tamperedSequence,
);
const tamperedFirstCommitMembers = tamperedMembers.filter(
  (member) => member.commit_sequence === tamperedSequence,
);
check(
  merkleRoot(firstCommitMembers) !== merkleRoot(tamperedFirstCommitMembers),
  'membership tamper rejected',
);

function restoreMindpack() {
  const objects = new Map();
  for (const header of headers) {
    check(objectHash(header) === header.object_hash, `restore object hash ${header.id}`);
    objects.set(header.id, structuredClone(header));
  }
  const admissions = new Map();
  for (const member of members) {
    const header = objects.get(member.object_id);
    check(header?.object_hash === member.object_hash, `restore member object ${member.object_id}`);
    admissions.set(member.object_id, {
      commit_sequence: member.commit_sequence,
      commit_position: member.commit_position,
      admitted_at: member.admitted_at,
    });
  }
  for (const commit of commits) {
    const uuid = commit.record_id.slice(commit.record_id.lastIndexOf(':') + 1);
    const envelope = readJson(
      path.join(MINDPACK, 'compartments', 'records', `${uuid}.structural.json`),
    );
    admissions.set(commit.record_id, {
      commit_sequence: commit.sequence,
      commit_position: null,
      admitted_at: envelope.content.structural_payload.committed_at,
    });
  }
  return {
    archive_id: genesisPayload.archive_id,
    epoch_id: genesisPayload.epoch_id,
    genesis_commit_hash: commits[0].commit_hash,
    head_commit_hash: commits.at(-1).commit_hash,
    head_sequence: commits.at(-1).sequence,
    objects,
    admissions,
  };
}

const restored = restoreMindpack();
check(
  ['archive_id', 'epoch_id', 'genesis_commit_hash', 'head_commit_hash', 'head_sequence']
    .every((field) => restored[field] === operationVectors.restore[field]),
  'restore preserves archive identity, epoch, genesis, and head',
);
check(
  restored.objects.size === operationVectors.restore.object_count
    && restored.admissions.size === operationVectors.restore.admission_count,
  'restore operation vector object and admission counts',
);
for (const member of members) {
  const coordinate = restored.admissions.get(member.object_id);
  check(
    coordinate.commit_sequence === member.commit_sequence
      && coordinate.commit_position === member.commit_position
      && coordinate.admitted_at === member.admitted_at,
    `restore admission coordinate ${member.object_id}`,
  );
}

const downgradeSource = path.join(ROOT, 'examples', 'capsule', 'downgrade-source');
const downgradeExport = path.join(ROOT, 'examples', 'capsule', 'downgrade-export');
const downgradeReceipt = readJson(path.join(ROOT, 'examples', 'capsule', 'downgrade-receipt.json'));
const downgradeExportManifest = readJson(path.join(downgradeExport, 'manifest.json'));
const downgradeSourceIdentity = readJson(path.join(downgradeSource, 'source-identity.json'));
check(
  JSON.stringify(downgradeSourceIdentity) === JSON.stringify(operationVectors.source_identity),
  'downgrade source identity matches the trusted operation vector',
);
check(
  downgradeExportManifest.pack_id === downgradeReceipt.export_pack_id
    && downgradeExportManifest.level === downgradeReceipt.target_level
    && downgradeExportManifest.custody.losslessness === downgradeReceipt.losslessness
    && JSON.stringify(downgradeExportManifest.custody.omissions)
      === JSON.stringify(downgradeReceipt.omissions),
  'downgrade receipt binds the target Capsule and its loss declaration',
);
const downgradeHeaders = [
  ...readNdjson(path.join(downgradeSource, 'objects', 'records.ndjson')),
  ...readNdjson(path.join(downgradeSource, 'objects', 'links.ndjson')),
  ...readNdjson(path.join(downgradeSource, 'objects', 'blobs.ndjson')),
];
const downgradeHeaderById = new Map(downgradeHeaders.map((header) => [header.id, header]));
const downgradeBatchFiles = fs.readdirSync(path.join(downgradeSource, 'producer-batches'));
check(downgradeBatchFiles.length === 1, 'downgrade source selects one producer batch');
const downgradeBatch = readJson(
  path.join(downgradeSource, 'producer-batches', downgradeBatchFiles[0]),
);
const downgradeBatchById = new Map(
  [...downgradeBatch.records, ...downgradeBatch.links, ...downgradeBatch.blobs]
    .map((submission) => [submission.id, submission]),
);
const downgradeSubmissionStreams = downgradeExportManifest.streams.filter(
  (stream) => stream.content_role === 'submissions',
);
check(downgradeSubmissionStreams.length === 1, 'downgrade target has one submission stream');
const downgradeExportSubmissions = readNdjson(
  path.join(downgradeExport, downgradeSubmissionStreams[0].path),
);
check(
  downgradeExportSubmissions.length === 1
    && downgradeExportSubmissions.every(
      (submission) => JSON.stringify(downgradeBatchById.get(submission.id)) === JSON.stringify(submission),
    ),
  'downgrade exports exact selected source assertions',
);
const downgradeExportInventory = readJson(
  path.join(ROOT, 'examples', 'capsule', 'downgrade-export-inventory.json'),
);
check(
  downgradeExportInventory.length === downgradeExportSubmissions.length
    && downgradeExportSubmissions.every((submission) => downgradeExportInventory.some(
      (entry) => entry.category === 'submission'
        && entry.subject === `submission:${submission.id}`
        && entry.digest === submissionHash(submission),
    )),
  'downgrade logical inventory binds selected submission hashes',
);
for (const submission of downgradeExportSubmissions) {
    const header = downgradeHeaderById.get(submission.id);
    check(Boolean(header) && objectHash(header) === header.object_hash, `downgrade object ${submission.id}`);
    const uuid = submission.id.slice(submission.id.lastIndexOf(':') + 1);
    const kind = submission.submission_kind;
    const structural = readJson(
      path.join(downgradeSource, 'compartments', `${kind}s`, `${uuid}.structural.json`),
    );
    check(
      compartmentCommitment(kind, 'structural', structural) === header.structural_commitment,
      `downgrade structural commitment ${submission.id}`,
    );
    if (header.semantic_commitment !== null) {
      const semantic = readJson(
        path.join(downgradeSource, 'compartments', `${kind}s`, `${uuid}.semantic.json`),
      );
      check(
        compartmentCommitment(kind, 'semantic', semantic) === header.semantic_commitment,
        `downgrade semantic commitment ${submission.id}`,
      );
      const evidence = semantic.content.producer_evidence;
      check(
        evidence?.batch_id === downgradeBatch.batch_id
          && evidence.credential_id === downgradeBatch.credential_id
          && evidence.producer_sequence === downgradeBatch.producer_sequence
          && evidence.submission_hash === submissionHash(submission),
        `downgrade submission evidence ${submission.id}`,
      );
    }
}
const downgradeCommits = readNdjson(path.join(downgradeSource, 'integrity', 'commits.ndjson'));
const downgradeMembers = readNdjson(path.join(downgradeSource, 'integrity', 'members.ndjson'));
check(
  journalIsBound(downgradeCommits, downgradeMembers, {
      sourceRoot: downgradeSource,
      sourceHeaders: downgradeHeaders,
      sourceHeaderById: downgradeHeaderById,
      sourceIdentity: downgradeSourceIdentity,
    }),
  'downgrade source independently verifies its authenticated archive prefix',
);

const foreignUnavailable = readJson(path.join(VECTORS, 'foreign-unavailability.json'));
const foreignVector = operationVectors.foreign_merge;
const destinationArchive = foreignVector.destination_archive_id;
const destinationEpoch = foreignVector.destination_epoch_id;
const destinationPrivateKey = fs.readFileSync(
  path.join(VECTORS, 'TEST-ONLY-archive-ed25519-private.pem'),
);
const destinationPublicKey = fs.readFileSync(path.join(VECTORS, 'archive-ed25519-public.pem'));

function createDestinationCommit({ sequence, parentCommitHash, commitMembers, recordId, committedAt }) {
  const template = commitVectors.commit1;
  const signingHeader = structuredClone(template.signing_header);
  signingHeader.id = recordId;
  const structural = structuredClone(template.structural_content_without_signature);
  Object.assign(structural.structural_payload, {
    archive_id: destinationArchive,
    epoch_id: destinationEpoch,
    sequence,
    parent_commit_hash: parentCommitHash,
    batch_merkle_root: merkleRoot(commitMembers),
    member_count: String(commitMembers.length),
    committed_at: committedAt,
  });
  const signature = crypto.sign(
    null,
    commitSigningDigest(signingHeader, structural),
    destinationPrivateKey,
  ).toString('base64url');
  const envelope = {
    format: 'ccf.record-structural/0.1.2',
    salt: Buffer.alloc(32, Number(sequence) + 48).toString('base64url'),
    content: structuredClone(structural),
  };
  envelope.content.structural_payload.signature = signature;
  const header = {
    ...signingHeader,
    structural_commitment: compartmentCommitment('record', 'structural', envelope),
  };
  header.object_hash = objectHash(header);
  return {
    header,
    envelope,
    row: {
      sequence,
      record_id: recordId,
      commit_hash: header.object_hash,
      parent_commit_hash: parentCommitHash,
      merkle_root: structural.structural_payload.batch_merkle_root,
    },
  };
}

function destinationCommitIsValid(commit, commitMembers) {
  const payload = commit.envelope.content.structural_payload;
  const unsignedContent = structuredClone(commit.envelope.content);
  delete unsignedContent.structural_payload.signature;
  const signingHeader = structuredClone(commit.header);
  delete signingHeader.object_hash;
  delete signingHeader.structural_commitment;
  return objectHash(commit.header) === commit.header.object_hash
    && compartmentCommitment('record', 'structural', commit.envelope)
      === commit.header.structural_commitment
    && commit.row.commit_hash === commit.header.object_hash
    && commit.row.parent_commit_hash === payload.parent_commit_hash
    && commit.row.merkle_root === merkleRoot(commitMembers)
    && crypto.verify(
      null,
      commitSigningDigest(signingHeader, unsignedContent),
      destinationPublicKey,
      Buffer.from(payload.signature, 'base64url'),
    );
}

function foreignMerge(sourceHeaders) {
  const objects = new Map(
    sourceHeaders.map((header) => [header.id, structuredClone(header)]),
  );
  const destinationMembers = sourceHeaders.map((header, commitPosition) => ({
    commit_sequence: foreignVector.commit_sequence,
    commit_position: commitPosition,
    object_kind: header.object_kind,
    object_id: header.id,
    object_hash: header.object_hash,
    admitted_at: foreignVector.admitted_at,
  }));
  const genesis = createDestinationCommit({
    sequence: '0',
    parentCommitHash: null,
    commitMembers: [],
    recordId: 'urn:ccf:record:dddddddd-dddd-4ddd-8ddd-dddddddddddd',
    committedAt: foreignVector.genesis_committed_at,
  });
  const importedCommit = createDestinationCommit({
    sequence: foreignVector.commit_sequence,
    parentCommitHash: genesis.header.object_hash,
    commitMembers: destinationMembers,
    recordId: 'urn:ccf:record:cccccccc-cccc-4ccc-8ccc-cccccccccccc',
    committedAt: foreignVector.admitted_at,
  });
  return {
    archive_id: destinationArchive,
    epoch_id: destinationEpoch,
    objects,
    members: destinationMembers,
    genesis,
    commit: importedCommit,
    head_commit_hash: importedCommit.header.object_hash,
    source_custody_proof: {
      archive_id: manifest.archive_id,
      epoch_id: manifest.epoch_id,
      genesis_commit_hash: manifest.genesis_commit_hash,
      head_commit_hash: manifest.head_commit_hash,
      head_sequence: manifest.head_sequence,
      semantic_catalog_root: manifest.semantic_catalog_root,
      commits: structuredClone(commits),
      members: structuredClone(members),
      headers: structuredClone(sourceHeaders),
      commit_envelopes: new Map(commits.map((commit) => {
        const uuid = commit.record_id.slice(commit.record_id.lastIndexOf(':') + 1);
        return [commit.record_id, readJson(
          path.join(MINDPACK, 'compartments', 'records', `${uuid}.structural.json`),
        )];
      })),
    },
  };
}

const foreign = foreignMerge(headers);
check(foreign.archive_id !== manifest.archive_id, 'foreign merge uses destination archive identity');
check(foreign.epoch_id !== manifest.epoch_id, 'foreign merge uses destination epoch');
check(
  foreign.objects.size === foreignVector.object_count,
  'foreign merge operation vector object count',
);
for (const header of headers) {
  const imported = foreign.objects.get(header.id);
  check(
    JSON.stringify(imported) === JSON.stringify(header) && imported.object_hash === header.object_hash,
    `foreign merge preserves portable object ${header.id}`,
  );
  check(
    foreign.source_custody_proof.members.some(
      (member) => member.object_id === header.id && member.object_hash === header.object_hash,
    ) || foreign.source_custody_proof.commits.some(
      (commit) => commit.record_id === header.id && commit.commit_hash === header.object_hash,
    ),
    `foreign merge source custody proof ${header.id}`,
  );
}
check(
  merkleRoot(foreign.members) === foreign.commit.row.merkle_root
    && foreign.commit.row.merkle_root === foreignVector.expected_merkle_root,
  'foreign merge destination Merkle membership vector',
);
for (const positionVector of [foreignVector.first_position, foreignVector.last_position]) {
  const member = foreign.members[positionVector.commit_position];
  check(
    member.object_id === positionVector.object_id
      && member.commit_position === positionVector.commit_position,
    `foreign merge destination coordinate ${positionVector.object_id}`,
  );
}
check(
  destinationCommitIsValid(foreign.genesis, []),
  'foreign merge destination genesis is a valid signed commit object',
);
check(
  destinationCommitIsValid(foreign.commit, foreign.members)
    && foreign.commit.row.parent_commit_hash === foreign.genesis.header.object_hash
    && foreign.commit.row.sequence === '1',
  'foreign merge destination commit extends its signed genesis',
);
check(
  foreign.genesis.header.object_hash === foreignVector.expected_genesis_commit_hash
    && foreign.head_commit_hash === foreignVector.expected_head_commit_hash
    && foreign.head_commit_hash === foreign.commit.header.object_hash,
  'foreign merge destination genesis and head hashes match the operation vector',
);
check(
  journalIsBound(
    foreign.source_custody_proof.commits,
    foreign.source_custody_proof.members,
    {
      sourceRoot: path.join(ROOT, 'does-not-exist'),
      sourceHeaders: foreign.source_custody_proof.headers,
      sourceHeaderById: new Map(
        foreign.source_custody_proof.headers.map((header) => [header.id, header]),
      ),
      sourceIdentity: foreign.source_custody_proof,
      sourceCommitEnvelopes: foreign.source_custody_proof.commit_envelopes,
    },
  ),
  'foreign merge retains verifiable source journal evidence',
);

const foreignMergedAvailability = foreignUnavailable.input.compartments.map((entry) => ({
  ...entry,
  plaintext: null,
}));
for (let index = 0; index < foreignMergedAvailability.length; index += 1) {
  const source = foreignUnavailable.input.compartments[index];
  const merged = foreignMergedAvailability[index];
  check(merged.object_id === source.object_id, `foreign merge stable ID ${source.object_id}`);
  check(merged.commitment === source.commitment, `foreign merge commitment ${source.object_id}`);
  check(
    merged.source_custody_proof === source.source_custody_proof,
    `foreign merge custody proof ${source.object_id}`,
  );
  check(merged.plaintext === null, `foreign merge does not manufacture plaintext ${source.object_id}`);
}

for (const member of members) {
  check(headerById.get(member.object_id)?.object_hash === member.object_hash, `member ${member.object_id}`);
}

console.log(
  `CCF Verified Archive inherited history checks pass: ${checks} checks. `
    + `Destination genesis=${foreign.genesis.header.object_hash} head=${foreign.head_commit_hash}.`,
);
