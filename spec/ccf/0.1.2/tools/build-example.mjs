import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  canonicalize,
  canonicalDigest,
  compartmentCommitment,
  objectHash,
  submissionHash,
  producerBatchHash,
  producerBatchSigningDigest,
  blobContentCommitment,
  merkleRoot,
  commitSigningDigest,
  digestString,
} from './ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const EX = path.join(ROOT, 'examples', 'thoth-capture');
const MP = path.join(ROOT, 'examples', 'mindpack');
fs.rmSync(EX, { recursive: true, force: true });
fs.rmSync(MP, { recursive: true, force: true });
for (const p of [EX, path.join(MP, 'objects'), path.join(MP, 'compartments', 'records'), path.join(MP, 'compartments', 'links'), path.join(MP, 'compartments', 'blobs'), path.join(MP, 'blob-data'), path.join(MP, 'integrity'), path.join(MP, 'producer-batches')]) fs.mkdirSync(p, { recursive: true });
fs.cpSync(path.join(ROOT, 'schemas'), path.join(MP, 'schemas'), { recursive: true });
fs.cpSync(path.join(ROOT, 'registries'), path.join(MP, 'registries'), { recursive: true });
fs.copyFileSync(path.join(ROOT, 'semantic-catalog.json'), path.join(MP, 'semantic-catalog.json'));

function uuid4(label) {
  const b = crypto.createHash('sha256').update(`ccf-0.1.2:${label}`).digest().subarray(0, 16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
function urn(kind, label) { return `urn:ccf:${kind}:${uuid4(label)}`; }
function salt(n) { return Buffer.alloc(32, n).toString('base64url'); }
function writeJson(file, value) { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n'); }
function stem(id) { return id.slice(id.lastIndexOf(':') + 1); }
function rawEd25519PublicKey(pem) {
  const der = crypto.createPublicKey(pem).export({ type: 'spki', format: 'der' });
  return der.subarray(der.length - 32).toString('base64url');
}

const catalog = JSON.parse(fs.readFileSync(path.join(ROOT, 'semantic-catalog.json'), 'utf8'));
const schemaDigest = new Map(catalog.schemas.map((e) => [e.id, e.digest]));
const types = JSON.parse(fs.readFileSync(path.join(ROOT, 'registries', 'types.registry.json'), 'utf8'));
const linksRegistry = JSON.parse(fs.readFileSync(path.join(ROOT, 'registries', 'links.registry.json'), 'utf8'));
const blobsRegistry = JSON.parse(fs.readFileSync(path.join(ROOT, 'registries', 'blobs.registry.json'), 'utf8'));
const typeEntry = new Map(types.entries.map((e) => [`${e.name}@${e.version}`, e]));
const linkEntry = new Map(linksRegistry.entries.map((e) => [`${e.name}@${e.version}`, e]));
const blobEntry = blobsRegistry.entries[0];
function entryDigest(entry) { return canonicalDigest('ccf:registry-entry:v1', entry); }

const ids = {
  archive: urn('archive', 'archive'), epoch: urn('lineage', 'epoch'), policyLineage: urn('lineage', 'policy'), runLineage: urn('lineage', 'run'), credentialLineage: urn('lineage', 'credential'),
  person: urn('record', 'person'), runtime: urn('record', 'runtime'), credential: urn('record', 'credential-record'), source: urn('record', 'source'), policy: urn('record', 'policy'), session: urn('record', 'session'), artifact: urn('record', 'artifact'), run: urn('record', 'run'), utterance: urn('record', 'utterance'), candidate: urn('record', 'candidate'), review: urn('record', 'review'), accepted: urn('record', 'accepted'), genesis: urn('record', 'genesis'), commit1: urn('record', 'commit1'), commit2: urn('record', 'commit2'),
  blob: urn('blob', 'audio'), hasBlob: urn('link', 'hasBlob'), capturedIn: urn('link', 'capturedIn'), derivedFrom: urn('link', 'derivedFrom'), generatedBy: urn('link', 'generatedBy'), evidenceFor: urn('link', 'evidenceFor'), supersedes: urn('link', 'supersedes'), covers: urn('link', 'covers'),
  archiveKey: urn('key', 'archive-signing'), deviceKey: urn('key', 'device-signing'), credentialId: urn('credential', 'device-credential'), batch: urn('batch', 'producer-batch'), pack: urn('pack', 'mindpack'), erasureDomain: urn('lineage', 'erasure-domain')
};

const archivePriv = fs.readFileSync(path.join(ROOT, 'vectors', 'TEST-ONLY-archive-ed25519-private.pem'));
const archivePubPem = fs.readFileSync(path.join(ROOT, 'vectors', 'archive-ed25519-public.pem'));
const archivePub = rawEd25519PublicKey(archivePubPem);
const devicePriv = fs.readFileSync(path.join(ROOT, 'vectors', 'TEST-ONLY-device-ed25519-private.pem'));
const devicePubPem = fs.readFileSync(path.join(ROOT, 'vectors', 'device-ed25519-public.pem'));
const devicePub = rawEd25519PublicKey(devicePubPem);

const records = [];
const links = [];
const blobs = [];

function compartment(kind, comp, n, content) {
  return { format: `ccf.${kind}-${comp}/0.1.2`, salt: salt(n), content };
}
function header(kind, id, structural, semantic = null) {
  const h = {
    spec: 'ccf/0.1.2', object_kind: kind, id, hash_profile: 'ccf-jcs-sha256-v2',
    structural_commitment: compartmentCommitment(kind, 'structural', structural),
    semantic_commitment: semantic ? compartmentCommitment(kind, 'semantic', semantic) : null,
  };
  h.object_hash = objectHash(h);
  return h;
}
function recordStructural(type, retention, payload = {}, lineage = undefined, visibility = 'clear') {
  const e = typeEntry.get(`${type}@1`); if (!e) throw new Error(`missing type ${type}`);
  const out = { type: visibility === 'sealed' ? 'sealed.record' : type, type_version: 1, type_visibility: visibility, schema_digest: schemaDigest.get(e.semantic_schema_id), registry_entry_digest: entryDigest(e), retention_profile: retention, structural_payload: payload, extensions: {} };
  if (lineage) out.lineage = lineage;
  return out;
}
function linkStructural(type, fromId, toId, retention = 'structural_retention_required', payload = {}, visibility = 'clear') {
  const e = linkEntry.get(`${type}@1`); if (!e) throw new Error(`missing Link type ${type}`);
  const out = { type: visibility === 'sealed' ? 'sealed.link' : type, type_version: 1, type_visibility: visibility, schema_digest: schemaDigest.get('urn:ccf:schema:0.1.2:objects.link-semantic-content'), registry_entry_digest: entryDigest(e), retention_profile: retention, structural_payload: payload, extensions: {} };
  if (e.endpoints_location === 'structural') { out.from_id = fromId; out.to_id = toId; }
  return out;
}
function blobStructural(mediaType, byteLength, contentCommitment) {
  return { type: 'blob.manifest', type_version: 1, type_visibility: 'clear', schema_digest: schemaDigest.get(blobEntry.semantic_schema_id), registry_entry_digest: entryDigest(blobEntry), retention_profile: 'payload_erasable', media_type: mediaType, byte_length: String(byteLength), content_commitment: contentCommitment, content_profile: 'ccf-blob-content-v2', availability_class: 'controlled', erasure_domain_id: ids.erasureDomain, structural_payload: {}, extensions: {} };
}
function addRecord(id, type, n, semanticContent, { retention, structuralPayload = {}, lineage = undefined, visibility = 'clear', semantic = true } = {}) {
  const e = typeEntry.get(`${type}@1`); const rp = retention ?? e.retention_profile;
  const structural = compartment('record', 'structural', n, recordStructural(type, rp, structuralPayload, lineage, visibility));
  const sem = semantic ? compartment('record', 'semantic', n + 80, semanticContent) : null;
  const h = header('record', id, structural, sem);
  records.push({ header: h, structural, semantic: sem }); return records.at(-1);
}
function addLink(id, type, fromId, toId, n, semanticContent) {
  const e = linkEntry.get(`${type}@1`);
  const structural = compartment('link', 'structural', n, linkStructural(type, fromId, toId, e.retention_profile));
  const semContent = { ...semanticContent };
  if (e.endpoints_location === 'semantic') semContent.endpoints = { from_id: fromId, to_id: toId };
  const semantic = compartment('link', 'semantic', n + 80, semContent);
  const h = header('link', id, structural, semantic);
  links.push({ header: h, structural, semantic }); return links.at(-1);
}
function addBlob(id, n, structuralContent, semanticContent) {
  const structural = compartment('blob', 'structural', n, structuralContent);
  const semantic = compartment('blob', 'semantic', n + 80, semanticContent);
  const h = header('blob', id, structural, semantic);
  blobs.push({ header: h, structural, semantic }); return blobs.at(-1);
}

function privacy(classes = [], subjects = []) { return { data_subjects: subjects, data_classes: classes, consent_refs: [], legal_basis_refs: [], subject_coverage: subjects.length ? 'complete' : 'unknown' }; }
function claims(personId = ids.person, perspectiveId = ids.person, classes = [], subjects = []) { return { person_id: personId, perspective_id: perspectiveId, privacy: privacy(classes, subjects), authority: { basis: 'runtime_import', asserted_by: ids.runtime, accepted_by: null }, policy_hint: ids.policyLineage, extensions: {} }; }
function rootPolicyRef() { return { lineage_id: ids.policyLineage, head_id_at_write: null, policy_object_hash: null, evaluator_profile: 'ccf-deny-overrides-v1', semantic_catalog_root: catalog.root }; }
let policyRef;

const policy = addRecord(ids.policy, 'governance.policy', 1, {
  person_id: ids.person, recorded_by: ids.runtime, recorded_at: '2026-08-11T21:40:00.000Z', privacy: privacy(['identity_data'], []), policy_ref: rootPolicyRef(), authority: { basis: 'explicit_authorization', asserted_by: ids.person, accepted_by: ids.person },
  payload: { profile: 'ccf.policy/0.1.2', evaluator_profile: 'ccf-deny-overrides-v1', combining_algorithm: 'deny_overrides_v1', default_effect: 'deny', rules: [{ rule_id: 'owner-local', effect: 'allow', operations: ['read','search','derive'], purposes: ['personal_knowledge','agent_context'], recipients: [ids.person, ids.runtime], destinations: ['local','same_archive'], data_classes: [], conditions: [], obligations: [], valid_from: '2026-08-11T21:40:00.000Z', expires_at: null }], provenance_requirement: 'lineage_only', retention: { minimum_until: null, maximum_until: null, on_expiry: 'review' }, extensions: {} }, extensions: {}
}, { lineage: { lineage_id: ids.policyLineage, previous_head_id: null, transition: 'create', valid_from: '2026-08-11T21:40:00.000Z', expires_at: null } });
policyRef = { lineage_id: ids.policyLineage, head_id_at_write: ids.policy, policy_object_hash: policy.header.object_hash, evaluator_profile: 'ccf-deny-overrides-v1', semantic_catalog_root: catalog.root };

addRecord(ids.person, 'core.person', 2, { person_id: ids.person, perspective_id: ids.person, recorded_by: ids.runtime, recorded_at: '2026-08-11T21:40:00.000Z', privacy: privacy(['identity_data'], [{ person_id: ids.person, role: 'archive_principal', identity_state_at_write: 'verified' }]), policy_ref: policyRef, authority: { basis: 'first_person_statement', asserted_by: ids.person, accepted_by: ids.person }, payload: { kind: 'human', display_name: 'Example Person', aliases: [], identity_anchors: [], extensions: {} }, extensions: {} });
addRecord(ids.runtime, 'core.runtime', 3, { person_id: ids.person, recorded_by: ids.runtime, recorded_at: '2026-08-11T21:40:00.000Z', policy_ref: policyRef, authority: { basis: 'runtime_import', asserted_by: ids.runtime, accepted_by: null }, payload: { kind: 'backend', name: 'Thoth CCF adapter', version: '0.1.2-example', instance_id: 'thoth-local', capabilities: ['capture','transcribe','extract','sync'], operator_id: ids.person, extensions: {} }, extensions: {} });
addRecord(ids.credential, 'core.device_credential', 4, null, { semantic: false, structuralPayload: { credential_id: ids.credentialId, subject_id: ids.runtime, issuer_key_id: ids.archiveKey, signing_key: { profile: 'ed25519', public_key: devicePub, key_id: ids.deviceKey }, encryption_key: null, scopes: ['capture','sync','derive'], valid_from: '2026-08-11T21:40:00.000Z', expires_at: null, offline_grace_until: null, extensions: {} }, lineage: { lineage_id: ids.credentialLineage, previous_head_id: null, transition: 'issue', valid_from: '2026-08-11T21:40:00.000Z', expires_at: null } });

function makeCommit(id, seq, parentHash, members, time) {
  const merkle = merkleRoot(members);
  const e = typeEntry.get('integrity.commit@1');
  const structuralContent = recordStructural('integrity.commit', 'epoch_lifetime_required', {
    archive_id: ids.archive, epoch_id: ids.epoch, sequence: String(seq), parent_commit_hash: parentHash, batch_merkle_root: merkle, member_count: String(members.length), hash_profile: 'ccf-jcs-sha256-v2', signature_profile: 'ed25519-jcs-v1', signer_key_id: ids.archiveKey, signer_public_key: archivePub, semantic_catalog_root: catalog.root, active_profiles: ['ccf-core-0.1.2','ccf-local-sync-0.1.2','ccf-continuity-pack-0.1.2'], committed_at: time
  });
  const headerForSigning = { spec: 'ccf/0.1.2', object_kind: 'record', id, hash_profile: 'ccf-jcs-sha256-v2', semantic_commitment: null };
  const signing = commitSigningDigest(headerForSigning, structuralContent);
  structuralContent.structural_payload.signature = crypto.sign(null, signing, archivePriv).toString('base64url');
  const structural = compartment('record', 'structural', 40 + seq, structuralContent);
  const h = header('record', id, structural, null);
  records.push({ header: h, structural, semantic: null });
  return { header: h, structural, members, sequence: String(seq), commit_hash: h.object_hash, merkle_root: merkle };
}

const emptyMembers = [];
const genesis = makeCommit(ids.genesis, 0, null, emptyMembers, '2026-08-11T21:40:00.000Z');
const bootstrapObjects = records.filter((r) => ![ids.genesis].includes(r.header.id));
const members1 = bootstrapObjects.map((o, i) => ({ commit_sequence: '1', commit_position: i, admitted_at: '2026-08-11T21:40:01.000Z', object_kind: 'record', object_id: o.header.id, object_hash: o.header.object_hash }));
const commit1 = makeCommit(ids.commit1, 1, genesis.commit_hash, members1, '2026-08-11T21:40:01.000Z');

// Producer submissions.
const subject = [{ person_id: ids.person, role: 'speaker', identity_state_at_write: 'verified' }];
const sourceSubmission = { submission_kind: 'record', id: ids.source, type: 'core.source', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:41:47.900Z', claims: claims(ids.person, ids.person, ['identity_data'], []), payload: { kind: 'wearable_audio', name: 'Maxc test source', connector: 'thoth.capture', native_identity: 'device:maxc-test', trust_class: 'authenticated', producer_key_id: ids.deviceKey, extensions: {} }, extensions: {} };
const sessionSubmission = { submission_kind: 'record', id: ids.session, type: 'core.session', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:41:48.000Z', occurred_at: { start: '2026-08-11T21:41:48.000Z', end: '2026-08-11T21:42:18.000Z', precision: 'millisecond', clock_uncertainty_ms: 12 }, origin: { source_id: ids.source, native_id: 'boot-8891/session-1', revision: '1' }, claims: claims(ids.person, ids.person, ['voice_recording'], subject), payload: { source_id: ids.source, native_id: 'boot-8891/session-1', channel: 'ambient', started_at: '2026-08-11T21:41:48.000Z', ended_at: '2026-08-11T21:42:18.000Z', participants: [ids.person], capture_mode: 'manual-test', extensions: {} }, extensions: {} };

function makeWav() { const sr=16000,samples=1600,dataBytes=samples*2,b=Buffer.alloc(44+dataBytes); b.write('RIFF',0); b.writeUInt32LE(36+dataBytes,4); b.write('WAVE',8); b.write('fmt ',12); b.writeUInt32LE(16,16); b.writeUInt16LE(1,20); b.writeUInt16LE(1,22); b.writeUInt32LE(sr,24); b.writeUInt32LE(sr*2,28); b.writeUInt16LE(2,32); b.writeUInt16LE(16,34); b.write('data',36); b.writeUInt32LE(dataBytes,40); return b; }
const wav = makeWav(); const contentSalt = salt(200); const contentCommit = blobContentCommitment(contentSalt, wav);
const blobSubmission = { submission_kind: 'blob', id: ids.blob, retention_profile_hint: 'payload_erasable', media_type: 'audio/wav', byte_length: String(wav.length), content_salt: contentSalt, content_commitment: contentCommit, content_profile: 'ccf-blob-content-v2', origin: { source_id: ids.source, native_id: 'boot-8891/segment-1842', revision: '1' }, claims: claims(ids.person, ids.person, ['voice_recording'], subject), extensions: {} };
const artifactSubmission = { submission_kind: 'record', id: ids.artifact, type: 'experience.artifact', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:18.331Z', occurred_at: { start: '2026-08-11T21:41:48.000Z', end: '2026-08-11T21:42:18.000Z', precision: 'millisecond', clock_uncertainty_ms: 12 }, origin: { source_id: ids.source, native_id: 'boot-8891/segment-1842', revision: '1' }, claims: claims(ids.person, ids.person, ['voice_recording'], subject), payload: { name: 'segment-1842.wav', media_type: 'audio/wav', description: 'Short test capture', external_uri: null, artifact_role: 'raw_capture', extensions: {} }, extensions: {} };
const runSubmission = { submission_kind: 'record', id: ids.run, type: 'process.run', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:19.000Z', claims: claims(), lineage: { lineage_id: ids.runLineage, previous_head_id: null, transition: 'succeed', valid_from: '2026-08-11T21:42:19.000Z', expires_at: null }, payload: { run_kind: 'transcription', framework: 'thoth', task: 'Transcribe captured audio', status: 'succeeded', configuration_ref: null, parent_run_id: null, started_at: '2026-08-11T21:42:18.400Z', terminal_at: '2026-08-11T21:42:19.000Z', extensions: {} }, extensions: {} };
const utteranceSubmission = { submission_kind: 'record', id: ids.utterance, type: 'experience.utterance', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:19.100Z', occurred_at: { start: '2026-08-11T21:41:48.000Z', end: '2026-08-11T21:42:18.000Z', precision: 'millisecond', clock_uncertainty_ms: 12 }, origin: { source_id: ids.source, native_id: 'boot-8891/segment-1842/utterance-1', revision: '1' }, claims: { ...claims(ids.person, ids.person, ['speech_content'], subject), authority: { basis: 'quoted_statement', asserted_by: ids.person, accepted_by: null } }, payload: { text: 'I want the schema open because adoption is still a win.', language: 'en', speaker_id: ids.person, sequence: '1', transcription: { engine: 'example-stt', engine_version: '1', mean_confidence: 0.96, language_detected: 'en' }, extensions: {} }, extensions: {} };
const candidateSubmission = { submission_kind: 'record', id: ids.candidate, type: 'continuity.preference', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:19.200Z', claims: { ...claims(ids.person, ids.person, ['derived_profile'], subject), authority: { basis: 'machine_inference', asserted_by: ids.runtime, accepted_by: null } }, payload: { target: { value: 'open adoption of the continuity schema', datatype: 'string' }, stance: 'prefer', strength: 0.9, context: { domain: 'software standards' }, rationale: 'Explicitly stated as a strategic preference.', extensions: {} }, extensions: {} };
const reviewSubmission = { submission_kind: 'record', id: ids.review, type: 'governance.review_decision', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:20.000Z', claims: { ...claims(ids.person, ids.person, ['derived_profile'], subject), authority: { basis: 'person_accepted', asserted_by: ids.person, accepted_by: ids.person } }, payload: { target_ids: [ids.candidate], decision: 'accept', reason: 'The extraction matches the stated intent.', reviewer_id: ids.person, evidence_refs: [ids.utterance], extensions: {} }, extensions: {} };
const acceptedSubmission = { submission_kind: 'record', id: ids.accepted, type: 'continuity.preference', type_version: 1, type_visibility: 'clear', retention_profile_hint: 'payload_erasable', recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:20.100Z', claims: { ...claims(ids.person, ids.person, ['derived_profile'], subject), authority: { basis: 'person_accepted', asserted_by: ids.person, accepted_by: ids.person } }, payload: candidateSubmission.payload, extensions: {} };

function linkSub(id, type, from_id, to_id, selector = {}, payload = {}) { return { submission_kind: 'link', id, type, type_version: 1, type_visibility: 'clear', retention_profile_hint: 'structural_retention_required', from_id, to_id, recorded_by: ids.runtime, recorded_at: '2026-08-11T21:42:20.200Z', claims: claims(ids.person, ids.person, [], []), selector, payload, extensions: {} }; }
const linkSubs = [
  linkSub(ids.hasBlob,'ccf.has_blob',ids.artifact,ids.blob), linkSub(ids.capturedIn,'ccf.captured_in',ids.artifact,ids.session),
  linkSub(ids.derivedFrom,'ccf.derived_from',ids.utterance,ids.artifact,{kind:'media_time',start_ms:0,end_ms:30000}),
  linkSub(ids.generatedBy,'ccf.generated_by',ids.utterance,ids.run), linkSub(ids.evidenceFor,'ccf.evidence_for',ids.utterance,ids.candidate,{kind:'text_span',start:0,end:57}),
  linkSub(ids.supersedes,'ccf.supersedes',ids.accepted,ids.candidate), linkSub(ids.covers,'ccf.covers',ids.review,ids.candidate)
];

const producerBatch = { format: 'ccf.producer-batch/0.1.2', batch_id: ids.batch, producer_id: ids.runtime, producer_sequence: '1', previous_batch_hash: null, credential_id: ids.credentialId, created_at: '2026-08-11T21:42:20.300Z', semantic_catalog_root: catalog.root, records: [sourceSubmission,sessionSubmission,artifactSubmission,runSubmission,utteranceSubmission,candidateSubmission,reviewSubmission,acceptedSubmission], links: linkSubs, blobs: [blobSubmission], blob_transfers: [{ blob_id: ids.blob, transfer_ref: 'segment-1842.wav', offset: '0', length: String(wav.length), complete: true }], extensions: {} };
producerBatch.signature_profile = 'ed25519-jcs-v1';
producerBatch.batch_hash = producerBatchHash(producerBatch);
producerBatch.signature = crypto.sign(null, producerBatchSigningDigest(producerBatch.batch_hash), devicePriv).toString('base64url');

const evidence = { batch_id: ids.batch, credential_id: ids.credentialId, producer_sequence: '1' };
function resolveRecord(sub, n) {
  const sh = submissionHash(sub); const origin = sub.origin ? { ...sub.origin, submission_hash: sh } : undefined;
  const c = sub.claims;
  const sem = { person_id: c.person_id, perspective_id: c.perspective_id, recorded_by: sub.recorded_by, recorded_at: sub.recorded_at, ...(sub.occurred_at ? { occurred_at: sub.occurred_at } : {}), ...(origin ? { origin } : {}), claimed: c, privacy: c.privacy, policy_ref: policyRef, authority: c.authority, producer_evidence: { ...evidence, submission_hash: sh }, payload: sub.payload, extensions: sub.extensions };
  if (sub.type === 'experience.utterance') sem.epistemic = { confidence: 0.98, method: 'source-grounded-transcript', calibration_profile: 'example-v1' };
  if (sub.type === 'continuity.preference' && sub.id === ids.candidate) sem.epistemic = { confidence: 0.83, method: 'preference-extractor-v1', calibration_profile: 'example-v1' };
  return addRecord(sub.id, sub.type, n, sem, { retention: sub.retention_profile_hint, lineage: sub.lineage, visibility: sub.type_visibility });
}
function resolveLink(sub, n) {
  const sh=submissionHash(sub); const c=sub.claims;
  return addLink(sub.id, sub.type, sub.from_id, sub.to_id, n, { recorded_by: sub.recorded_by, recorded_at: sub.recorded_at, claimed: c, privacy: c.privacy, policy_ref: policyRef, authority: c.authority, producer_evidence: { ...evidence, submission_hash: sh }, selector: sub.selector, payload: sub.payload, extensions: sub.extensions });
}
function resolveBlob(sub, n) {
  const sh=submissionHash(sub); const c=sub.claims;
  return addBlob(sub.id, n, blobStructural(sub.media_type, sub.byte_length, sub.content_commitment), { content_salt: sub.content_salt, filename: 'segment-1842.wav', origin: { ...sub.origin, submission_hash: sh }, privacy: c.privacy, policy_ref: policyRef, producer_evidence: { ...evidence, submission_hash: sh }, content_encryption_profile: 'none', content_key_ref: null, extensions: sub.extensions });
}

let n=10; for (const s of producerBatch.records) resolveRecord(s,n++); for (const s of producerBatch.links) resolveLink(s,n++); resolveBlob(blobSubmission,n++);
const captureObjects = [...records.filter((r) => ![ids.policy,ids.person,ids.runtime,ids.credential,ids.genesis,ids.commit1].includes(r.header.id)), ...links, ...blobs];
const members2 = captureObjects.map((o,i)=>({commit_sequence:'2',commit_position:i,admitted_at:'2026-08-11T21:42:21.000Z',object_kind:o.header.object_kind,object_id:o.header.id,object_hash:o.header.object_hash}));
const commit2 = makeCommit(ids.commit2,2,commit1.commit_hash,members2,'2026-08-11T21:42:21.000Z');

// Example directory.
for (const o of records) {
  const s=stem(o.header.id); writeJson(path.join(EX,`record-${s}.header.json`),o.header); writeJson(path.join(EX,`record-${s}.structural.json`),o.structural); if(o.semantic) writeJson(path.join(EX,`record-${s}.semantic.json`),o.semantic);
}
for (const o of links) { const s=stem(o.header.id); writeJson(path.join(EX,`link-${s}.header.json`),o.header); writeJson(path.join(EX,`link-${s}.structural.json`),o.structural); writeJson(path.join(EX,`link-${s}.semantic.json`),o.semantic); }
for (const o of blobs) { const s=stem(o.header.id); writeJson(path.join(EX,`blob-${s}.header.json`),o.header); writeJson(path.join(EX,`blob-${s}.structural.json`),o.structural); writeJson(path.join(EX,`blob-${s}.semantic.json`),o.semantic); }
fs.writeFileSync(path.join(EX,'segment-1842.wav'),wav); writeJson(path.join(EX,'producer-batch.json'),producerBatch); writeJson(path.join(EX,'commit-members-1.json'),members1); writeJson(path.join(EX,'commit-members-2.json'),members2); writeJson(path.join(EX,'ids.json'),ids);
fs.writeFileSync(path.join(EX,'README.md'),'# Thoth capture trace\n\nBootstrap genesis and identity, then admit one signed offline capture batch containing source, session, audio Blob, artifact, transcription run, utterance, candidate preference, human review, accepted preference, and provenance Links.\n');

// Mindpack object streams and compartments.
fs.writeFileSync(path.join(MP,'objects','records.ndjson'),records.map((o)=>canonicalize(o.header)).join('\n')+'\n');
fs.writeFileSync(path.join(MP,'objects','links.ndjson'),links.map((o)=>canonicalize(o.header)).join('\n')+'\n');
fs.writeFileSync(path.join(MP,'objects','blobs.ndjson'),blobs.map((o)=>canonicalize(o.header)).join('\n')+'\n');
for (const o of records) { const s=stem(o.header.id); writeJson(path.join(MP,'compartments','records',`${s}.structural.json`),o.structural); if(o.semantic) writeJson(path.join(MP,'compartments','records',`${s}.semantic.json`),o.semantic); }
for (const o of links) { const s=stem(o.header.id); writeJson(path.join(MP,'compartments','links',`${s}.structural.json`),o.structural); writeJson(path.join(MP,'compartments','links',`${s}.semantic.json`),o.semantic); }
for (const o of blobs) { const s=stem(o.header.id); writeJson(path.join(MP,'compartments','blobs',`${s}.structural.json`),o.structural); writeJson(path.join(MP,'compartments','blobs',`${s}.semantic.json`),o.semantic); }
fs.writeFileSync(path.join(MP,'blob-data',`${stem(ids.blob)}.bin`),wav);
fs.writeFileSync(path.join(MP,'integrity','commits.ndjson'),[genesis,commit1,commit2].map((c)=>canonicalize({sequence:c.sequence,record_id:c.header.id,commit_hash:c.commit_hash,parent_commit_hash:c.structural.content.structural_payload.parent_commit_hash,merkle_root:c.merkle_root})).join('\n')+'\n');
fs.writeFileSync(path.join(MP,'integrity','members.ndjson'),[...members1,...members2].map(canonicalize).join('\n')+'\n');
writeJson(path.join(MP,'producer-batches',`${stem(ids.batch)}.json`),producerBatch);
fs.writeFileSync(path.join(MP,'README.md'),'# Example CCF 0.1.2 mindpack\n\nSelf-contained restore example generated from `tools/build-example.mjs`.\n');

function walk(dir){const out=[];for(const e of fs.readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())out.push(...walk(p));else out.push(p);}return out;}
const streams=[]; for(const f of walk(MP).sort()){const rel=path.relative(MP,f).replaceAll(path.sep,'/'); if(rel==='manifest.json')continue; const b=fs.readFileSync(f); streams.push({path:rel,digest:digestString(b),byte_length:String(b.length),required:!rel.startsWith('blob-data/')});}
const compartmentAvailability=[];
for(const o of [...records,...links,...blobs]){
  const retentionProfile=o.structural.content.retention_profile;
  compartmentAvailability.push({object_kind:o.header.object_kind,object_id:o.header.id,compartment:'structural',availability:'available',commitment:o.header.structural_commitment,retention_profile:retentionProfile,source_custody_proof:null,unavailability_lineage_id:null});
  if(o.header.semantic_commitment!==null) compartmentAvailability.push({object_kind:o.header.object_kind,object_id:o.header.id,compartment:'semantic',availability:'available',commitment:o.header.semantic_commitment,retention_profile:retentionProfile,source_custody_proof:null,unavailability_lineage_id:null});
  if(o.header.object_kind==='blob') compartmentAvailability.push({object_kind:'blob',object_id:o.header.id,compartment:'blob_content',availability:'available',commitment:o.structural.content.content_commitment,retention_profile:retentionProfile,source_custody_proof:null,unavailability_lineage_id:null});
}
const manifest={format:'ccf.mindpack/0.1.2',mode:'restore',custody:{completeness:'complete',restore_capable:true},pack_id:ids.pack,archive_id:ids.archive,epoch_id:ids.epoch,created_at:'2026-08-11T21:42:22.000Z',genesis_commit_hash:genesis.commit_hash,head_commit_hash:commit2.commit_hash,head_sequence:'2',semantic_catalog_root:catalog.root,hash_profile:'ccf-jcs-sha256-v2',profiles:['ccf-core-0.1.2','ccf-local-sync-0.1.2','ccf-continuity-pack-0.1.2'],counts:{records:String(records.length),links:String(links.length),blobs:String(blobs.length),commits:'3'},streams,external_dependencies:[],withheld:[],erased:[],foreign_custody_proofs:[],compartment_availability:compartmentAvailability,extensions:{}};
writeJson(path.join(MP,'manifest.json'),manifest);
console.log(`built example: ${records.length} records, ${links.length} links, ${blobs.length} blob`);
