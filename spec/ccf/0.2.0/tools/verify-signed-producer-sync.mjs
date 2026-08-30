import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  compartmentCommitment,
  objectHash,
  producerBatchHash,
  producerBatchSigningDigest,
  submissionHash,
} from '../../0.1.2/tools/ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2');
const VECTORS = path.join(BASE, 'vectors');
const vector = JSON.parse(fs.readFileSync(path.join(VECTORS, 'producer-batch.json'), 'utf8'));
const publicKey = fs.readFileSync(path.join(VECTORS, 'device-ed25519-public.pem'));
const privateKey = fs.readFileSync(path.join(VECTORS, 'TEST-ONLY-device-ed25519-private.pem'));
const { batch } = vector;
const credentialTrustAnchor = JSON.parse(fs.readFileSync(
  path.join(ROOT, 'vectors', 'signed-producer-sync-trust.json'),
  'utf8',
));
let checks = 0;
const UINT64_MAX = 18446744073709551615n;

function check(condition, label) {
  checks += 1;
  if (!condition) throw new Error(`FAIL: ${label}`);
}

function isCanonicalUint64(value, { nonzero = false } = {}) {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value)) return false;
  if (value.length > 20 || (value.length === 20 && value > UINT64_MAX.toString())) return false;
  return !nonzero || value !== '0';
}

check(producerBatchHash(batch) === vector.expected_batch_hash, 'producer batch hash');
check(
  crypto.verify(
    null,
    producerBatchSigningDigest(batch.batch_hash),
    publicKey,
    Buffer.from(batch.signature, 'base64url'),
  ),
  'producer batch signature',
);
const changedBatch = structuredClone(batch);
changedBatch.records[0].payload = { ...changedBatch.records[0].payload, tampered: true };
check(producerBatchHash(changedBatch) !== batch.batch_hash, 'content tamper changes batch hash');
const changedSignature = Buffer.from(batch.signature, 'base64url');
changedSignature[0] ^= 1;
check(
  !crypto.verify(
    null,
    producerBatchSigningDigest(batch.batch_hash),
    publicKey,
    changedSignature,
  ),
  'signature tamper rejected',
);
check(batch.producer_sequence === '1' && batch.previous_batch_hash === null, 'genesis producer chain');

const credentialDirectory = path.join(BASE, 'examples', 'mindpack', 'compartments', 'records');
const credentialFiles = fs.readdirSync(credentialDirectory)
  .filter((name) => name.endsWith('.structural.json'));
const recordHeaders = fs.readFileSync(
  path.join(BASE, 'examples', 'mindpack', 'objects', 'records.ndjson'),
  'utf8',
).trim().split('\n').filter(Boolean).map(JSON.parse);
const recordHeaderById = new Map(recordHeaders.map((header) => [header.id, header]));
const credentialState = credentialFiles
  .map((name) => {
    const recordId = `urn:ccf:record:${name.slice(0, -'.structural.json'.length)}`;
    return {
      record_id: recordId,
      header: recordHeaderById.get(recordId),
      envelope: JSON.parse(fs.readFileSync(path.join(credentialDirectory, name), 'utf8')),
    };
  })
  .filter(
    (record) => record.envelope.content.type === 'core.device_credential'
      && record.envelope.content.structural_payload.credential_id === batch.credential_id,
  );
const credential = credentialState.find(
  (record) => record.record_id === credentialTrustAnchor.issue_record_id,
)?.envelope.content.structural_payload;
check(Boolean(credential), 'retained producer credential');
check(credentialState.length === 2, 'retained canonical credential lineage');
check(credential.subject_id === batch.producer_id, 'credential binds producer');
check(credential.scopes.includes('sync'), 'credential grants sync scope');
check(
  crypto.createPublicKey(publicKey).export({ format: 'jwk' }).x
    === credential.signing_key.public_key,
  'credential binds signing public key',
);
const createdAt = Date.parse(batch.created_at);
check(
  createdAt >= Date.parse(credential.valid_from)
    && (credential.expires_at === null || createdAt < Date.parse(credential.expires_at)),
  'credential active at signed batch time',
);

function verifyEnvelope(candidate) {
  const activeCredential = trustedCredentialAt(
    credentialState,
    credentialTrustAnchor,
    candidate.created_at,
  );
  if (!activeCredential) return false;
  try {
    const activeKey = crypto.createPublicKey({
      key: {
        kty: 'OKP',
        crv: 'Ed25519',
        x: activeCredential.signing_key.public_key,
      },
      format: 'jwk',
    });
    return candidate.credential_id === activeCredential.credential_id
      && candidate.producer_id === activeCredential.subject_id
      && activeCredential.signing_key.profile === 'ed25519'
      && activeCredential.scopes.includes('sync')
      && producerBatchHash(candidate) === candidate.batch_hash
      && crypto.verify(
        null,
        producerBatchSigningDigest(candidate.batch_hash),
        activeKey,
        Buffer.from(candidate.signature, 'base64url'),
      );
  } catch {
    return false;
  }
}

function signedSuccessor(previous, sequence, id, timestamp) {
  const candidate = structuredClone(previous);
  candidate.batch_id = id;
  candidate.producer_sequence = String(sequence);
  candidate.previous_batch_hash = previous.batch_hash;
  candidate.created_at = timestamp;
  candidate.batch_hash = producerBatchHash(candidate);
  candidate.signature = crypto.sign(
    null,
    producerBatchSigningDigest(candidate.batch_hash),
    privateKey,
  ).toString('base64url');
  return candidate;
}

function resign(candidate) {
  candidate.batch_hash = producerBatchHash(candidate);
  candidate.signature = crypto.sign(
    null,
    producerBatchSigningDigest(candidate.batch_hash),
    privateKey,
  ).toString('base64url');
  return candidate;
}

function ingest(state, candidate) {
  if (!verifyEnvelope(candidate)) return 'invalid_envelope';
  if (!isCanonicalUint64(candidate.producer_sequence, { nonzero: true })) return 'invalid_sequence';
  const sequence = BigInt(candidate.producer_sequence);
  if ((sequence === 1n) !== (candidate.previous_batch_hash === null)) {
    return 'invalid_genesis_shape';
  }
  const key = `${candidate.producer_id}\0${sequence}`;
  const previous = state.bySequence.get(key);
  if (previous !== undefined) {
    return previous === candidate.batch_hash ? 'existing' : 'producer_sequence_conflict';
  }
  if (sequence > 1n) {
    const predecessor = state.bySequence.get(`${candidate.producer_id}\0${sequence - 1n}`);
    if (predecessor !== candidate.previous_batch_hash) return 'predecessor_missing';
  }
  state.bySequence.set(key, candidate.batch_hash);
  return 'accepted';
}

const retryState = { bySequence: new Map() };
const zeroSequence = resign({ ...structuredClone(batch), producer_sequence: '0' });
check(ingest(retryState, zeroSequence) === 'invalid_sequence', 'signed sequence zero rejected');
const leadingZeroSequence = resign({ ...structuredClone(batch), producer_sequence: '01' });
check(ingest(retryState, leadingZeroSequence) === 'invalid_sequence', 'signed leading-zero sequence rejected');
const malformedSequence = resign({ ...structuredClone(batch), producer_sequence: '1e0' });
check(ingest(retryState, malformedSequence) === 'invalid_sequence', 'signed malformed sequence rejected without throwing');
const numericSequence = resign({ ...structuredClone(batch), producer_sequence: 1 });
check(ingest(retryState, numericSequence) === 'invalid_sequence', 'signed numeric sequence rejected');
const overflowSequence = resign({
  ...structuredClone(batch),
  producer_sequence: '18446744073709551616',
});
check(ingest(retryState, overflowSequence) === 'invalid_sequence', 'signed uint64 overflow rejected');
const oversizedSequence = resign({ ...structuredClone(batch), producer_sequence: '9'.repeat(4301) });
check(ingest(retryState, oversizedSequence) === 'invalid_sequence', 'signed oversized decimal sequence rejected without parsing');
const linkedGenesis = resign({
  ...structuredClone(batch),
  previous_batch_hash: `sha256:${'f'.repeat(64)}`,
});
check(ingest(retryState, linkedGenesis) === 'invalid_genesis_shape', 'signed non-null genesis parent rejected');
check(ingest(retryState, batch) === 'accepted', 'first authenticated batch accepted');
check(ingest(retryState, batch) === 'existing', 'stable retry is idempotent');
const conflictingRetry = structuredClone(batch);
conflictingRetry.records[0].payload = { ...conflictingRetry.records[0].payload, changed: true };
conflictingRetry.batch_hash = producerBatchHash(conflictingRetry);
conflictingRetry.signature = crypto.sign(
  null,
  producerBatchSigningDigest(conflictingRetry.batch_hash),
  privateKey,
).toString('base64url');
check(ingest(retryState, conflictingRetry) === 'producer_sequence_conflict', 'changed retry conflicts');

const successor = signedSuccessor(
  batch,
  2,
  'urn:ccf:batch:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  '2026-08-11T21:42:20.400Z',
);
check(verifyEnvelope(successor), 'signed predecessor-linked successor');
const resumeState = { bySequence: new Map() };
check(ingest(resumeState, successor) === 'predecessor_missing', 'early successor remains pending');
check(ingest(resumeState, batch) === 'accepted', 'missing predecessor accepted');
check(ingest(resumeState, successor) === 'accepted', 'pending signed successor resumes');

const third = signedSuccessor(
  successor,
  3,
  'urn:ccf:batch:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
  '2026-08-11T21:42:20.500Z',
);
const delta = [batch, successor, third];
const after = (sequence) => delta.filter(
  (candidate) => BigInt(candidate.producer_sequence) > BigInt(sequence),
);
check(after(1).map((candidate) => candidate.producer_sequence).join(',') === '2,3', 'delta resumes after cursor');
const firstDeltaResponse = JSON.stringify(after(1));
check(
  JSON.stringify(after(1)) === firstDeltaResponse,
  'repeated delta request is byte-stable',
);
check(after(3).length === 0, 'acknowledged delta head has no replay gap');

const unsafeSequence = 9007199254740992n;
const unsafePredecessor = signedSuccessor(
  third,
  unsafeSequence,
  'urn:ccf:batch:dddddddd-dddd-4ddd-8ddd-dddddddddddd',
  '2026-08-11T21:42:20.600Z',
);
const unsafeSuccessor = signedSuccessor(
  unsafePredecessor,
  unsafeSequence + 1n,
  'urn:ccf:batch:eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
  '2026-08-11T21:42:20.700Z',
);
const unsafeState = { bySequence: new Map([
  [`${batch.producer_id}\0${unsafeSequence - 1n}`, third.batch_hash],
]) };
check(ingest(unsafeState, unsafePredecessor) === 'accepted', 'unsafe integer predecessor accepted exactly');
check(ingest(unsafeState, unsafeSuccessor) === 'accepted', 'adjacent unsafe sequence remains distinct');
check(
  unsafeState.bySequence.get(`${batch.producer_id}\0${unsafeSequence}`) === unsafePredecessor.batch_hash
    && unsafeState.bySequence.get(`${batch.producer_id}\0${unsafeSequence + 1n}`) === unsafeSuccessor.batch_hash,
  'unsafe decimal-string sequence keys do not alias',
);

function trustedCredentialAt(retainedCredentialState, trustAnchor, timestamp) {
  try {
    if (
      trustAnchor.format !== 'ccf.signed-producer-sync-trust/0.2.0'
      || trustAnchor.credential_id === undefined
      || trustAnchor.lineage_id === undefined
      || !Array.isArray(trustAnchor.lineage_records)
    ) return null;
    const stateById = new Map(
      retainedCredentialState.map((record) => [record.record_id, record]),
    );
    if (stateById.size !== retainedCredentialState.length) return null;
    const trustedHashById = new Map(
      trustAnchor.lineage_records.map((record) => [record.record_id, record.object_hash]),
    );
    if (
      trustedHashById.size !== trustAnchor.lineage_records.length
      || trustedHashById.size !== retainedCredentialState.length
      || trustAnchor.lineage_records[0]?.record_id !== trustAnchor.issue_record_id
      || trustAnchor.lineage_records.at(-1)?.record_id !== trustAnchor.current_head_record_id
    ) return null;
    const issue = stateById.get(trustAnchor.issue_record_id);
    const currentHead = stateById.get(trustAnchor.current_head_record_id);
    if (
      !issue
      || !currentHead
      || issue.header.object_hash !== trustAnchor.issue_object_hash
      || currentHead.header.object_hash !== trustAnchor.current_head_object_hash
    ) return null;
    for (const record of retainedCredentialState) {
      const { header, envelope } = record;
      const content = envelope.content;
      const payload = content.structural_payload;
      if (
        !header
        || header.id !== record.record_id
        || header.object_kind !== 'record'
        || header.semantic_commitment !== null
        || objectHash(header) !== header.object_hash
        || trustedHashById.get(record.record_id) !== header.object_hash
        || compartmentCommitment('record', 'structural', envelope) !== header.structural_commitment
        || content.type !== 'core.device_credential'
        || payload.credential_id !== trustAnchor.credential_id
        || payload.issuer_key_id !== trustAnchor.issuer_key_id
        || content.lineage.lineage_id !== trustAnchor.lineage_id
      ) return null;
    }
    if (
      issue.envelope.content.lineage.transition !== 'issue'
      || issue.envelope.content.lineage.previous_head_id !== null
    ) return null;
    const chain = [issue];
    const visited = new Set([issue.record_id]);
    const allowedTransitions = new Map([
      ['issue', new Set(['rotate', 'revoke'])],
      ['rotate', new Set(['rotate', 'revoke'])],
      ['revoke', new Set()],
    ]);
    while (chain.at(-1).record_id !== currentHead.record_id) {
      const successors = retainedCredentialState.filter(
        (record) => record.envelope.content.lineage.previous_head_id === chain.at(-1).record_id,
      );
      if (successors.length !== 1 || visited.has(successors[0].record_id)) return null;
      const previousTransition = chain.at(-1).envelope.content.lineage.transition;
      const successorTransition = successors[0].envelope.content.lineage.transition;
      if (!allowedTransitions.get(previousTransition)?.has(successorTransition)) return null;
      const previousTime = Date.parse(chain.at(-1).envelope.content.lineage.valid_from);
      const successorTime = Date.parse(successors[0].envelope.content.lineage.valid_from);
      if (!Number.isFinite(successorTime) || successorTime < previousTime) return null;
      chain.push(successors[0]);
      visited.add(successors[0].record_id);
    }
    if (visited.size !== retainedCredentialState.length) return null;
    if (
      chain.map((record) => record.record_id).join('\0')
      !== trustAnchor.lineage_records.map((record) => record.record_id).join('\0')
    ) return null;
    const at = Date.parse(timestamp);
    if (!Number.isFinite(at)) return null;
    const active = chain.filter(
      (record) => Date.parse(record.envelope.content.lineage.valid_from) <= at,
    ).at(-1);
    if (!active || active.envelope.content.lineage.transition === 'revoke') return null;
    const activeLineageExpiry = active.envelope.content.lineage.expires_at;
    if (activeLineageExpiry !== null && at >= Date.parse(activeLineageExpiry)) return null;
    return active.envelope.content.structural_payload;
  } catch {
    return null;
  }
}

function verifyProducerProof(entry, retainedBatch, retainedCredentialState, trustAnchor) {
  const proof = entry.producer_proof;
  const retainedCredential = trustedCredentialAt(
    retainedCredentialState,
    trustAnchor,
    retainedBatch.created_at,
  );
  if (!proof || !retainedCredential) return false;
  const retainedSubmissions = [
    ...retainedBatch.records,
    ...retainedBatch.links,
    ...retainedBatch.blobs,
  ];
  const retainedSubmission = retainedSubmissions.find((item) => item.id === entry.source_id);
  const batchTime = Date.parse(retainedBatch.created_at);
  let credentialKey;
  try {
    credentialKey = crypto.createPublicKey({
      key: {
        kty: 'OKP',
        crv: 'Ed25519',
        x: retainedCredential.signing_key.public_key,
      },
      format: 'jwk',
    });
  } catch {
    return false;
  }
  return entry.producer_authentication === 'verified'
    && proof.profile === 'ccf-signed-producer-sync-v1'
    && proof.credential_id === retainedCredential.credential_id
    && proof.batch_id === retainedBatch.batch_id
    && proof.proof_digest === retainedBatch.batch_hash
    && retainedBatch.credential_id === retainedCredential.credential_id
    && retainedBatch.producer_id === retainedCredential.subject_id
    && isCanonicalUint64(retainedBatch.producer_sequence, { nonzero: true })
    && retainedCredential.signing_key.profile === 'ed25519'
    && retainedCredential.scopes.includes('sync')
    && Number.isFinite(batchTime)
    && batchTime >= Date.parse(retainedCredential.valid_from)
    && (retainedCredential.expires_at === null
      || batchTime < Date.parse(retainedCredential.expires_at))
    && Boolean(retainedSubmission)
    && submissionHash(retainedSubmission) === entry.source_submission_hash
    && producerBatchHash(retainedBatch) === retainedBatch.batch_hash
    && crypto.verify(
      null,
      producerBatchSigningDigest(retainedBatch.batch_hash),
      credentialKey,
      Buffer.from(retainedBatch.signature, 'base64url'),
    );
}

const provedSubmission = batch.records[0];
const verifiedEntry = {
  source_id: provedSubmission.id,
  source_submission_hash: submissionHash(provedSubmission),
  producer_authentication: 'verified',
  producer_proof: {
    profile: 'ccf-signed-producer-sync-v1',
    credential_id: credential.credential_id,
    batch_id: batch.batch_id,
    proof_digest: batch.batch_hash,
  },
};
check(
  verifyProducerProof(verifiedEntry, batch, credentialState, credentialTrustAnchor),
  'verified receipt proof resolves against trusted canonical credential state',
);
const forgedEntry = structuredClone(verifiedEntry);
forgedEntry.producer_proof.proof_digest = `sha256:${'0'.repeat(64)}`;
check(
  !verifyProducerProof(forgedEntry, batch, credentialState, credentialTrustAnchor),
  'forged receipt proof rejected',
);
const overflowProofBatch = resign({
  ...structuredClone(batch),
  producer_sequence: '18446744073709551616',
});
const overflowProofEntry = structuredClone(verifiedEntry);
overflowProofEntry.producer_proof.proof_digest = overflowProofBatch.batch_hash;
check(
  !verifyProducerProof(
    overflowProofEntry,
    overflowProofBatch,
    credentialState,
    credentialTrustAnchor,
  ),
  'receipt proof rejects a signed non-uint64 producer sequence',
);

function recanonicalizeCredentialState(state) {
  for (const record of state) {
    record.header.structural_commitment = compartmentCommitment(
      'record',
      'structural',
      record.envelope,
    );
    record.header.object_hash = objectHash(record.header);
  }
  return state;
}

function repinCredentialState(state, anchor) {
  const result = structuredClone(anchor);
  const stateById = new Map(state.map((record) => [record.record_id, record]));
  const ordered = [stateById.get(result.issue_record_id)];
  while (ordered.at(-1).record_id !== result.current_head_record_id) {
    const successors = state.filter(
      (record) => record.envelope.content.lineage.previous_head_id === ordered.at(-1).record_id,
    );
    if (successors.length !== 1) throw new Error('test credential state is not a single lineage');
    ordered.push(successors[0]);
  }
  result.issue_object_hash = ordered[0].header.object_hash;
  result.current_head_object_hash = ordered.at(-1).header.object_hash;
  result.lineage_records = ordered.map((record) => ({
    record_id: record.record_id,
    object_hash: record.header.object_hash,
  }));
  return result;
}

const wrongKeyState = structuredClone(credentialState);
for (const record of wrongKeyState) {
  record.envelope.content.structural_payload.signing_key.public_key = crypto.createPublicKey(
  fs.readFileSync(path.join(VECTORS, 'archive-ed25519-public.pem')),
  ).export({ format: 'jwk' }).x;
}
recanonicalizeCredentialState(wrongKeyState);
check(
  !verifyProducerProof(
    verifiedEntry,
    batch,
    wrongKeyState,
    repinCredentialState(wrongKeyState, credentialTrustAnchor),
  ),
  'batch signed by a key other than the trusted credential key is rejected',
);

const unscopedState = structuredClone(credentialState);
for (const record of unscopedState) {
  const payload = record.envelope.content.structural_payload;
  payload.scopes = payload.scopes.filter((scope) => scope !== 'sync');
}
recanonicalizeCredentialState(unscopedState);
check(
  !verifyProducerProof(
    verifiedEntry,
    batch,
    unscopedState,
    repinCredentialState(unscopedState, credentialTrustAnchor),
  ),
  'credential without sync scope rejected',
);

const expiredState = structuredClone(credentialState);
for (const record of expiredState) {
  record.envelope.content.structural_payload.expires_at = batch.created_at;
}
recanonicalizeCredentialState(expiredState);
check(
  !verifyProducerProof(
    verifiedEntry,
    batch,
    expiredState,
    repinCredentialState(expiredState, credentialTrustAnchor),
  ),
  'expired credential proof rejected',
);

check(
  !verifyProducerProof(
    verifiedEntry,
    batch,
    credentialState.filter((record) => record.record_id !== credentialTrustAnchor.current_head_record_id),
    credentialTrustAnchor,
  ),
  'credential state cannot omit the trusted current head',
);

const postRevocationBatch = resign({
  ...structuredClone(batch),
  created_at: '2026-08-11T21:42:20.900Z',
});
const postRevocationEntry = structuredClone(verifiedEntry);
postRevocationEntry.producer_proof.proof_digest = postRevocationBatch.batch_hash;
check(
  !verifyEnvelope(postRevocationBatch),
  'batch ingestion rejects a credential revoked at signed creation time',
);
check(
  !verifyProducerProof(
    postRevocationEntry,
    postRevocationBatch,
    credentialState,
    credentialTrustAnchor,
  ),
  'canonically revoked credential rejected at batch creation time',
);

const invalidReactivationState = structuredClone(credentialState);
const invalidReactivation = structuredClone(invalidReactivationState.find(
  (record) => record.record_id === credentialTrustAnchor.current_head_record_id,
));
invalidReactivation.record_id = 'urn:ccf:record:99999999-9999-4999-8999-999999999999';
invalidReactivation.header.id = invalidReactivation.record_id;
invalidReactivation.envelope.content.lineage.previous_head_id = credentialTrustAnchor.current_head_record_id;
invalidReactivation.envelope.content.lineage.transition = 'rotate';
invalidReactivation.envelope.content.lineage.valid_from = '2026-08-11T21:42:21.000Z';
invalidReactivationState.push(invalidReactivation);
recanonicalizeCredentialState(invalidReactivationState);
const invalidReactivationAnchorInput = structuredClone(credentialTrustAnchor);
invalidReactivationAnchorInput.current_head_record_id = invalidReactivation.record_id;
const invalidReactivationAnchor = repinCredentialState(
  invalidReactivationState,
  invalidReactivationAnchorInput,
);
const invalidReactivationBatch = resign({
  ...structuredClone(batch),
  created_at: '2026-08-11T21:42:21.100Z',
});
const invalidReactivationEntry = structuredClone(verifiedEntry);
invalidReactivationEntry.producer_proof.proof_digest = invalidReactivationBatch.batch_hash;
check(
  !verifyProducerProof(
    invalidReactivationEntry,
    invalidReactivationBatch,
    invalidReactivationState,
    invalidReactivationAnchor,
  ),
  'terminal revoke cannot be followed by a credential reactivation',
);

const rotatedState = structuredClone(credentialState);
const rotatedRevoke = rotatedState.find(
  (record) => record.record_id === credentialTrustAnchor.current_head_record_id,
);
const rotateRecord = structuredClone(rotatedRevoke);
rotateRecord.record_id = 'urn:ccf:record:88888888-8888-4888-8888-888888888888';
rotateRecord.header.id = rotateRecord.record_id;
rotateRecord.envelope.content.lineage.previous_head_id = credentialTrustAnchor.issue_record_id;
rotateRecord.envelope.content.lineage.transition = 'rotate';
rotateRecord.envelope.content.lineage.valid_from = '2026-08-11T21:42:20.500Z';
rotatedRevoke.envelope.content.lineage.previous_head_id = rotateRecord.record_id;
rotatedState.push(rotateRecord);
recanonicalizeCredentialState(rotatedState);
const rotatedAnchor = repinCredentialState(rotatedState, credentialTrustAnchor);
const rotationBatch = resign({
  ...structuredClone(batch),
  created_at: '2026-08-11T21:42:20.600Z',
});
const rotationEntry = structuredClone(verifiedEntry);
rotationEntry.producer_proof.proof_digest = rotationBatch.batch_hash;
check(
  verifyProducerProof(rotationEntry, rotationBatch, rotatedState, rotatedAnchor),
  'trusted issue-to-rotate-to-revoke lineage verifies during the rotation interval',
);

const rotationAttackerKeys = crypto.generateKeyPairSync('ed25519');
const mutatedRotationState = structuredClone(rotatedState);
const mutatedRotation = mutatedRotationState.find(
  (record) => record.record_id === rotateRecord.record_id,
);
mutatedRotation.envelope.content.structural_payload.signing_key.public_key
  = rotationAttackerKeys.publicKey.export({ format: 'jwk' }).x;
recanonicalizeCredentialState(mutatedRotationState);
const mutatedRotationBatch = structuredClone(rotationBatch);
mutatedRotationBatch.signature = crypto.sign(
  null,
  producerBatchSigningDigest(mutatedRotationBatch.batch_hash),
  rotationAttackerKeys.privateKey,
).toString('base64url');
check(
  !verifyProducerProof(
    rotationEntry,
    mutatedRotationBatch,
    mutatedRotationState,
    rotatedAnchor,
  ),
  'mutated intermediate rotation is rejected by the ordered lineage trust commitment',
);

const attackerKeys = crypto.generateKeyPairSync('ed25519');
const selfMintedState = structuredClone(credentialState);
const attackerPublicKey = attackerKeys.publicKey.export({ format: 'jwk' }).x;
for (const record of selfMintedState) {
  record.envelope.content.structural_payload.signing_key.public_key = attackerPublicKey;
}
recanonicalizeCredentialState(selfMintedState);
const selfMintedBatch = structuredClone(batch);
selfMintedBatch.signature = crypto.sign(
  null,
  producerBatchSigningDigest(selfMintedBatch.batch_hash),
  attackerKeys.privateKey,
).toString('base64url');
check(
  !verifyProducerProof(
    verifiedEntry,
    selfMintedBatch,
    selfMintedState,
    credentialTrustAnchor,
  ),
  'self-minted credential and matching batch key rejected by trust anchor',
);

console.log(`Signed Producer Sync capability vectors pass: ${checks} checks.`);
