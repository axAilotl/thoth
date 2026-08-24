import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  canonicalize, canonicalDigest, compartmentCommitment, objectHash, submissionHash,
  producerBatchHash, producerBatchSigningDigest, merkleRoot, commitSigningDigest,
  blobContentCommitment,
} from './ccf-jcs.mjs';

const ROOT=path.resolve(path.dirname(new URL(import.meta.url).pathname),'..');
const V=path.join(ROOT,'vectors'); const E=path.join(ROOT,'examples','thoth-capture');
function read(name){return JSON.parse(fs.readFileSync(path.join(E,name),'utf8'));}
function write(name,value){fs.writeFileSync(path.join(V,name),JSON.stringify(value,null,2)+'\n');}
const ids=read('ids.json'); const stem=(id)=>id.slice(id.lastIndexOf(':')+1);

const canonCases=[
 {name:'object-key-order',value:{z:1,a:'x'},expected:'{"a":"x","z":1}'},
 {name:'number-100',value:100,expected:'100'},
 {name:'number-small',value:0.000001,expected:'0.000001'},
 {name:'combining-form',value:'e\u0301',expected:'"é"'},
 {name:'precomposed-form',value:'é',expected:'"é"'},
 {name:'literal-null-codepoint',value:'a\u0000b',expected:'"a\\u0000b"'},
 {name:'timestamp-is-opaque-canonical-string',value:'2026-08-11T21:42:18.331Z',expected:'"2026-08-11T21:42:18.331Z"'},
];
for(const c of canonCases)c.digest=canonicalDigest('ccf:canonicalization-vector:v1',c.value);
write('canonicalization.json',{profile:'ccf-jcs-sha256-v2',cases:canonCases,rejections:['negative-zero','NaN','Infinity','unpaired-surrogate','duplicate-key-before-parse','noncanonical-timestamp-by-schema','unsafe-integer-by-schema']});

const sampleRecord=ids.utterance,sampleLink=ids.derivedFrom,sampleBlob=ids.blob;
function obj(kind,id){const s=stem(id);return {header:read(`${kind}-${s}.header.json`),structural:read(`${kind}-${s}.structural.json`),semantic:read(`${kind}-${s}.semantic.json`)};}
const objects={record:obj('record',sampleRecord),link:obj('link',sampleLink),blob:obj('blob',sampleBlob)};
const objectVectors={};
for(const [kind,o] of Object.entries(objects))objectVectors[kind]={...o,expected_structural_commitment:compartmentCommitment(kind,'structural',o.structural),expected_semantic_commitment:compartmentCommitment(kind,'semantic',o.semantic),expected_object_hash:objectHash(o.header)};
objectVectors.blob.expected_content_commitment=blobContentCommitment(objects.blob.semantic.content.content_salt,fs.readFileSync(path.join(E,'segment-1842.wav')));
write('object-hashes.json',objectVectors);

const batch=read('producer-batch.json');
write('producer-batch.json',{batch,expected_batch_hash:producerBatchHash(batch),expected_signature_valid:crypto.verify(null,producerBatchSigningDigest(batch.batch_hash),fs.readFileSync(path.join(V,'device-ed25519-public.pem')),Buffer.from(batch.signature,'base64url'))});

const members1=read('commit-members-1.json'),members2=read('commit-members-2.json');
write('merkle.json',{empty_expected:merkleRoot([]),commit1:{members:members1,expected_root:merkleRoot(members1)},commit2:{members:members2,expected_root:merkleRoot(members2)}});

function commitVector(id){const s=stem(id);const header=read(`record-${s}.header.json`);const structural=read(`record-${s}.structural.json`);const content=structuredClone(structural.content);const sig=content.structural_payload.signature;delete content.structural_payload.signature;const signingHeader={spec:header.spec,object_kind:header.object_kind,id:header.id,hash_profile:header.hash_profile,semantic_commitment:header.semantic_commitment};const digest=commitSigningDigest(signingHeader,content);return{header,structural,signing_header:signingHeader,structural_content_without_signature:content,expected_signing_digest:`sha256:${digest.toString('hex')}`,signature:sig,expected_signature_valid:crypto.verify(null,digest,fs.readFileSync(path.join(V,'archive-ed25519-public.pem')),Buffer.from(sig,'base64url')),expected_commit_hash:objectHash(header)};}
write('commit-signing.json',{genesis:commitVector(ids.genesis),commit1:commitVector(ids.commit1),commit2:commitVector(ids.commit2)});

write('ordering.json',{rule:'compare commit_sequence as unsigned integer, then commit_position as integer',ordered:[{commit_sequence:'9',commit_position:99},{commit_sequence:'10',commit_position:0},{commit_sequence:'9999',commit_position:4},{commit_sequence:'10000',commit_position:0}],lexicographic_is_invalid:true});

// Submission hashes are extracted from the signed producer batch.
write('submission-hashes.json',{records:batch.records.map((s)=>({id:s.id,expected_submission_hash:submissionHash(s)})),links:batch.links.map((s)=>({id:s.id,expected_submission_hash:submissionHash(s)})),blobs:batch.blobs.map((s)=>({id:s.id,expected_submission_hash:submissionHash(s)}))});
console.log('vectors built');
