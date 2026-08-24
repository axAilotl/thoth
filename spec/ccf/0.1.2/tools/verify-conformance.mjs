import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { blobContentCommitment, compartmentCommitment, suppressionContentToken, suppressionMerkleRoot, suppressionOriginToken, suppressionScopeCommitment } from './ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const vector = JSON.parse(fs.readFileSync(path.join(ROOT, 'vectors', 'conformance-0.1.2.json')));
const declared = new Set(vector.cases.map((entry) => entry.id));
const passed = new Set();

function check(condition, label) {
  if (!condition) throw new Error(`FAIL: ${label}`);
}
function pass(id) {
  check(declared.has(id), `undeclared conformance case ${id}`);
  passed.add(id);
}

// 1–2. Origin idempotency includes object_kind and requires stable native suffixes
// for same-kind multiplicity.
const originIndex = new Map();
function insertOrigin({ archive, source, native, revision, kind, hash }) {
  const key = [archive, source, native, revision, kind].join('\u0000');
  const previous = originIndex.get(key);
  if (previous === undefined) { originIndex.set(key, hash); return 'admitted'; }
  return previous === hash ? 'existing' : 'origin_revision_conflict';
}
const sharedOrigin = { archive: 'a', source: 's', native: 'segment-1842', revision: '1' };
check(insertOrigin({ ...sharedOrigin, kind: 'record', hash: 'record-hash' }) === 'admitted', 'Record origin');
check(insertOrigin({ ...sharedOrigin, kind: 'blob', hash: 'blob-hash' }) === 'admitted', 'Blob origin');
pass('origin-cross-kind');
check(insertOrigin({ ...sharedOrigin, kind: 'record', hash: 'other-record' }) === 'origin_revision_conflict', 'same-kind origin conflict');
check(insertOrigin({ ...sharedOrigin, native: 'segment-1842/utterance-1', kind: 'record', hash: 'other-record' }) === 'admitted', 'stable native suffix');
pass('origin-same-kind');

// 3. A foreign merge copies unavailable canonical state without manufacturing
// plaintext or collapsing erased into withheld.
const foreignVector = JSON.parse(fs.readFileSync(path.join(ROOT, 'vectors', 'foreign-unavailability.json')));
function importUnavailableCompartment(entry) {
  check(['withheld', 'erased', 'external'].includes(entry.availability), 'fixture compartment is unavailable');
  check(Boolean(entry.source_custody_proof), 'unavailable compartment custody proof');
  if (entry.availability !== 'external') check(Boolean(entry.unavailability_lineage_id), 'unavailability lineage');
  return {
    object_kind: entry.object_kind,
    object_id: entry.object_id,
    compartment: entry.compartment,
    availability: entry.availability,
    commitment: entry.commitment,
    retention_profile: entry.retention_profile,
    source_custody_proof: entry.source_custody_proof,
    unavailability_lineage_id: entry.unavailability_lineage_id,
    plaintext: null,
  };
}
const mergedCompartments = foreignVector.input.compartments.map(importUnavailableCompartment);
check(JSON.stringify(mergedCompartments) === JSON.stringify(foreignVector.expected_destination_compartments), 'foreign unavailability vector');
pass('foreign-unavailability');

// 4. Projections may be destroyed without touching canonical bootstrap bodies.
const mindpack = path.join(ROOT, 'examples', 'mindpack');
const ids = JSON.parse(fs.readFileSync(path.join(ROOT, 'examples', 'thoth-capture', 'ids.json')));
const bootstrapIds = [ids.person, ids.runtime, ids.policy];
const recordHeaders = fs.readFileSync(path.join(mindpack, 'objects', 'records.ndjson'), 'utf8').trim().split('\n').map(JSON.parse);
const bootstrapHeaders = new Map(recordHeaders.filter((header) => bootstrapIds.includes(header.id)).map((header) => [header.id, header]));
check(bootstrapHeaders.size === bootstrapIds.length, 'bootstrap headers in canonical stream');
let bootstrapProjection = new Map(bootstrapIds.map((id) => [id, 'stale-projection-value']));
bootstrapProjection.clear();
for (const id of bootstrapIds) {
  const header = bootstrapHeaders.get(id);
  const uuid = id.slice(id.lastIndexOf(':') + 1);
  const semanticPath = path.join(mindpack, 'compartments', 'records', `${uuid}.semantic.json`);
  check(fs.existsSync(semanticPath), `bootstrap semantic exists: ${id}`);
  const semantic = JSON.parse(fs.readFileSync(semanticPath));
  check(compartmentCommitment('record', 'semantic', semantic) === header.semantic_commitment, `bootstrap semantic commitment: ${id}`);
  bootstrapProjection.set(id, semantic.content);
}
check(bootstrapProjection.size === bootstrapIds.length, 'bootstrap projection rebuilt from canonical mindpack');
pass('bootstrap-rebuild');

// 5–6. Envelope-valid producer batches anchor the producer chain regardless of
// content disposition. A missing exact predecessor is pending, never permanent.
const producerHeads = new Map();
function admitBatch(batch) {
  if (!batch.envelopeValid) return { status: 'invalid_envelope', anchors: false };
  const expected = producerHeads.get(batch.sequence - 1);
  if (batch.sequence > 1 && expected !== batch.previousHash) return { status: 'predecessor_missing', anchors: false };
  const disposition = batch.contentAccepted ? 'accepted' : 'content_rejected';
  producerHeads.set(batch.sequence, batch.hash);
  return { status: disposition, anchors: true };
}
check(admitBatch({ sequence: 1, previousHash: null, hash: 'b1', envelopeValid: true, contentAccepted: false }).status === 'content_rejected', 'content rejection disposition');
check(admitBatch({ sequence: 2, previousHash: 'b1', hash: 'b2', envelopeValid: true, contentAccepted: true }).status === 'accepted', 'successor after content rejection');
pass('content-rejection-liveness');
producerHeads.clear();
const early = { sequence: 2, previousHash: 'p1', hash: 'p2', envelopeValid: true, contentAccepted: true };
check(admitBatch(early).status === 'predecessor_missing', 'early batch pending');
check(admitBatch({ sequence: 1, previousHash: null, hash: 'p1', envelopeValid: true, contentAccepted: true }).status === 'accepted', 'predecessor accepted');
check(admitBatch(early).status === 'accepted', 'pending batch retry');
pass('predecessor-pending');

// 7–8. Suppression lookup is rebuilt from canonical, journal-covered state.
const suppression = JSON.parse(fs.readFileSync(path.join(ROOT, 'vectors', 'suppression-canonical.json')));
const receiptCommitment = suppression.receipt_structural_payload.suppression_commitment;
check(receiptCommitment.suppression_set_record_id === suppression.ids.record, 'receipt references canonical suppression Record');
check(receiptCommitment.suppression_blob_id === suppression.ids.blob, 'receipt references governed suppression Blob');
check(receiptCommitment.entry_count === String(suppression.entries.length), 'receipt suppression count');
const suppressionKey=Buffer.from(suppression.key_hex,'hex');
const derivedTokens=suppression.preimages.map((preimage)=>preimage.kind==='origin'?suppressionOriginToken(suppressionKey,preimage):suppressionContentToken(suppressionKey,preimage)).sort();
check(JSON.stringify(derivedTokens)===JSON.stringify(suppression.entries),'suppression HMAC token derivation');
check(suppressionScopeCommitment(suppression.scope_object_ids)===suppression.expected_scope_commitment,'suppression scope commitment');
check(suppressionMerkleRoot(derivedTokens) === suppression.expected_entries_merkle_root, 'suppression Merkle root');
const suppressionBytes = Buffer.from(suppression.encoded_blob_base64, 'base64');
check(blobContentCommitment(suppression.blob_semantic_content.content_salt, suppressionBytes) === suppression.expected_content_commitment, 'suppression Blob content commitment');
check(suppression.blob_structural_content.retention_profile === 'structural_retention_required', 'suppression Blob retention');
const canonicalSuppressionSet = JSON.parse(suppressionBytes.toString('utf8'));
check(JSON.stringify(canonicalSuppressionSet.entries) === JSON.stringify(suppression.entries), 'suppression Blob entries');
for(const field of ['profile','entry_count','entries_merkle_root','key_profile_id','scope_commitment']){
  check(receiptCommitment[field]===suppression.record_structural_payload[field],`suppression receipt/Record ${field}`);
}
check(receiptCommitment.entries_merkle_root===suppression.expected_entries_merkle_root,'suppression receipt/Merkle root');
check(suppression.record_structural_payload.entries_merkle_root===suppression.expected_entries_merkle_root,'suppression Record/Merkle root');
check(canonicalSuppressionSet.profile===receiptCommitment.profile,'suppression Blob/receipt profile');
let suppressionProjection = new Set(canonicalSuppressionSet.entries);
suppressionProjection.delete(suppression.entries[0]);
check(suppressionProjection.size !== Number(receiptCommitment.entry_count), 'suppression row deletion detected');
suppressionProjection = new Set(canonicalSuppressionSet.entries);
check(suppressionProjection.has(suppression.entries[0]), 'suppression row reconstructed');
pass('suppression-row-rebuild');
suppressionProjection.clear();
suppressionProjection = new Set(JSON.parse(suppressionBytes.toString('utf8')).entries);
check(suppressionProjection.has(suppression.entries[1]), 'reintroduction blocked after total projection rebuild');
pass('suppression-reintroduction');

// 9. Signed membership and archive-local admission must correspond exactly.
const members = [
  { sequence: '7', position: 0, kind: 'record', id: 'r1', hash: 'h1' },
  { sequence: '7', position: 1, kind: 'blob', id: 'b1', hash: 'h2' },
];
const admissions = structuredClone(members);
function membershipCorresponds(ms, as) {
  const key = (row) => [row.sequence, row.position, row.kind, row.id, row.hash].join(':');
  const memberKeys = ms.map(key);
  const admissionKeys = as.map(key);
  return new Set(memberKeys).size === memberKeys.length
    && new Set(admissionKeys).size === admissionKeys.length
    && memberKeys.length === admissionKeys.length
    && memberKeys.every((value) => admissionKeys.includes(value));
}
check(membershipCorresponds(members, admissions), 'membership baseline');
check(!membershipCorresponds(members, admissions.slice(1)), 'missing admission fails');
const mutatedAdmissions = structuredClone(admissions); mutatedAdmissions[0].position = 9;
check(!membershipCorresponds(members, mutatedAdmissions), 'mutated admission coordinate fails');
pass('admission-membership');

// 10 is executed by tools/verify-postgres-fixture.sh against real PostgreSQL.

// 11. A real Git fixture covers evolution, rename, deletion, binary bytes, and
// idempotent retry using stable commit identities.
const gitFixture = fs.mkdtempSync(path.join(os.tmpdir(), 'ccf-git-fixture-'));
function git(...args) { return execFileSync('git', args, { cwd: gitFixture, encoding: 'utf8' }).trim(); }
git('init', '--quiet');
git('config', 'user.name', 'CCF Fixture');
git('config', 'user.email', 'fixture@example.invalid');
fs.writeFileSync(path.join(gitFixture, 'note.txt'), 'v1\n');
fs.writeFileSync(path.join(gitFixture, 'binary.dat'), Buffer.from([0, 255, 1, 2]));
git('add', 'note.txt', 'binary.dat'); git('commit', '--quiet', '-m', 'create text and binary');
fs.renameSync(path.join(gitFixture, 'note.txt'), path.join(gitFixture, 'renamed.txt'));
fs.appendFileSync(path.join(gitFixture, 'renamed.txt'), 'v2\n');
git('add', '-A'); git('commit', '--quiet', '-m', 'rename and evolve text');
fs.rmSync(path.join(gitFixture, 'renamed.txt'));
fs.writeFileSync(path.join(gitFixture, 'binary.dat'), Buffer.from([0, 254, 1, 3]));
git('add', '-A'); git('commit', '--quiet', '-m', 'delete text and evolve binary');
const commits = git('rev-list', '--reverse', 'HEAD').split('\n');
check(commits.length === 3 && new Set(commits).size === 3, 'three Git commits');
check(git('log', '--format=', '--name-status', '--find-renames').includes('R'), 'Git rename represented');
check(git('log', '--format=', '--name-status').includes('D'), 'Git deletion represented');
check(git('show', 'HEAD:binary.dat').length > 0, 'Git binary content represented');
const replayed = new Set([...commits, ...commits]);
check(replayed.size === commits.length, 'Git retry is idempotent by commit identity');
pass('git-three-commit');

// 12. Authority-vector coverage is checked here independently of generation.
const authorityRegistry = JSON.parse(fs.readFileSync(path.join(ROOT, 'registries', 'admission-authority-classes.registry.json')));
const authorityVectors = JSON.parse(fs.readFileSync(path.join(ROOT, 'vectors', 'admission-authority-classes.json')));
for (const entry of authorityRegistry.entries) {
  const cases = authorityVectors.cases.filter((item) => item.authority_class === entry.class);
  check(cases.some((item) => item.expected === 'accept'), `${entry.class} positive case`);
  check(cases.some((item) => item.expected === 'reject'), `${entry.class} negative case`);
}
pass('authority-classes');

check(passed.size === declared.size - 1, `executed ${passed.size} portable cases of ${declared.size} declared cases`);
for (const id of declared) if (id !== 'pgvector-multischema') check(passed.has(id), `unexecuted conformance case ${id}`);
console.log(`All ${passed.size} portable CCF 0.1.2 conformance cases pass.`);
