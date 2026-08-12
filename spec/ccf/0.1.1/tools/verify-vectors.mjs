import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  canonicalize, canonicalDigest, compartmentCommitment, objectHash, submissionHash,
  producerBatchHash, producerBatchSigningDigest, merkleRoot, commitSigningDigest,
  blobContentCommitment, semanticCatalogRoot,
} from './ccf-jcs.mjs';

const ROOT=path.resolve(path.dirname(new URL(import.meta.url).pathname),'..');
const V=path.join(ROOT,'vectors'); const E=path.join(ROOT,'examples','thoth-capture');
let checks=0;
function ok(cond,label){checks+=1;if(!cond)throw new Error(`FAIL: ${label}`);}
function read(p){return JSON.parse(fs.readFileSync(p,'utf8'));}

const canon=read(path.join(V,'canonicalization.json'));
for(const c of canon.cases){ok(canonicalize(c.value)===c.expected,`canonical ${c.name}`);ok(canonicalDigest('ccf:canonicalization-vector:v1',c.value)===c.digest,`canonical digest ${c.name}`);}
for(const [value,label] of [[-0,'negative zero'],[NaN,'NaN'],[Infinity,'Infinity']]){let threw=false;try{canonicalize(value);}catch{threw=true;}ok(threw,`reject ${label}`);}
let threw=false;try{canonicalize('\ud800');}catch{threw=true;}ok(threw,'reject unpaired surrogate');

const objects=read(path.join(V,'object-hashes.json'));
for(const [kind,o] of Object.entries(objects)){
 ok(compartmentCommitment(kind,'structural',o.structural)===o.expected_structural_commitment,`${kind} structural commitment`);
 ok(compartmentCommitment(kind,'semantic',o.semantic)===o.expected_semantic_commitment,`${kind} semantic commitment`);
 ok(objectHash(o.header)===o.expected_object_hash,`${kind} object hash`);
}
ok(blobContentCommitment(objects.blob.semantic.content.content_salt,fs.readFileSync(path.join(E,'segment-1842.wav')))===objects.blob.expected_content_commitment,'Blob content commitment');

const batchVector=read(path.join(V,'producer-batch.json'));const batch=batchVector.batch;
ok(producerBatchHash(batch)===batchVector.expected_batch_hash,'producer batch hash');
ok(crypto.verify(null,producerBatchSigningDigest(batch.batch_hash),fs.readFileSync(path.join(V,'device-ed25519-public.pem')),Buffer.from(batch.signature,'base64url')),'producer batch signature');

const subs=read(path.join(V,'submission-hashes.json'));
const allSubs=[...batch.records,...batch.links,...batch.blobs];
for(const v of [...subs.records,...subs.links,...subs.blobs]){const s=allSubs.find((x)=>x.id===v.id);ok(!!s,`submission present ${v.id}`);ok(submissionHash(s)===v.expected_submission_hash,`submission hash ${v.id}`);}

const merkle=read(path.join(V,'merkle.json'));ok(merkleRoot([])===merkle.empty_expected,'empty Merkle');for(const name of ['commit1','commit2'])ok(merkleRoot(merkle[name].members)===merkle[name].expected_root,`${name} Merkle`);

const commits=read(path.join(V,'commit-signing.json'));for(const [name,c] of Object.entries(commits)){const digest=commitSigningDigest(c.signing_header,c.structural_content_without_signature);ok(`sha256:${digest.toString('hex')}`===c.expected_signing_digest,`${name} signing digest`);ok(crypto.verify(null,digest,fs.readFileSync(path.join(V,'archive-ed25519-public.pem')),Buffer.from(c.signature,'base64url')),`${name} signature`);ok(objectHash(c.header)===c.expected_commit_hash,`${name} commit hash`);}

const ordering=read(path.join(V,'ordering.json')).ordered;const sorted=[...ordering].sort((a,b)=>{const sa=BigInt(a.commit_sequence),sb=BigInt(b.commit_sequence);if(sa<sb)return-1;if(sa>sb)return 1;return a.commit_position-b.commit_position;});ok(JSON.stringify(sorted)===JSON.stringify(ordering),'numeric ordering');

const sc=read(path.join(ROOT,'semantic-catalog.json'));const {root,...withoutRoot}=sc;ok(semanticCatalogRoot(withoutRoot)===root,'semantic catalog root');
console.log(`All ${checks} vector checks pass.`);
