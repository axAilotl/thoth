import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import {
  canonicalize, canonicalDigest, compartmentCommitment, objectHash, submissionHash,
  producerBatchHash, producerBatchSigningDigest, merkleRoot, commitSigningDigest,
  blobContentCommitment,
  suppressionContentDigest,
  suppressionContentToken,
  suppressionOriginToken,
  suppressionMerkleRoot,
  suppressionScopeCommitment,
} from './ccf-jcs.mjs';

const ROOT=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const V=path.join(ROOT,'vectors'); const E=path.join(ROOT,'examples','personal-archive');
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

const authorityRegistry=JSON.parse(fs.readFileSync(path.join(ROOT,'registries','admission-authority-classes.registry.json'),'utf8'));
function positiveAuthorityFixture(entry){
  const fixture={admitted_by_archive:false,lineage_state_machine_passed:true,claim:null};
  if(entry.evaluation_mode==='archive_admission_only')fixture.admitted_by_archive=true;
  if(entry.claim_required||entry.evaluation_mode==='archive_or_basis')fixture.claim={basis:entry.acceptable_authority_bases[0]??'runtime_import',asserted_by:'urn:ccf:record:00000000-0000-4000-8000-000000000001',accepted_by:null};
  return fixture;
}
function negativeAuthorityFixture(entry){
  if(entry.evaluation_mode==='structural_state_machine')return{admitted_by_archive:false,lineage_state_machine_passed:false,claim:null};
  if(entry.evaluation_mode==='archive_admission_only')return{admitted_by_archive:false,lineage_state_machine_passed:true,claim:null};
  if(entry.evaluation_mode==='archive_or_basis')return{admitted_by_archive:false,lineage_state_machine_passed:true,claim:{basis:'runtime_import',asserted_by:'urn:ccf:record:00000000-0000-4000-8000-000000000001',accepted_by:null}};
  if(entry.evaluation_mode==='any_pinned_basis')return{admitted_by_archive:false,lineage_state_machine_passed:true,claim:null};
  const allBases=['runtime_import','direct_observation','deterministic_derivation','machine_inference','quoted_statement','third_party_statement','first_person_statement','explicit_authorization','person_accepted'];
  const rejectedBasis=allBases.find((basis)=>!entry.acceptable_authority_bases.includes(basis))??'machine_inference';
  return{admitted_by_archive:false,lineage_state_machine_passed:true,claim:{basis:rejectedBasis,asserted_by:'urn:ccf:record:00000000-0000-4000-8000-000000000001',accepted_by:null}};
}
write('admission-authority-classes.json',{
  evaluator_profile:authorityRegistry.evaluator_profile,
  cases:authorityRegistry.entries.flatMap((entry)=>[
    {name:`${entry.class}:positive`,authority_class:entry.class,fixture:positiveAuthorityFixture(entry),expected:'accept'},
    {name:`${entry.class}:negative`,authority_class:entry.class,fixture:negativeAuthorityFixture(entry),expected:'reject'},
    ...(entry.archive_admission_basis_override==='any_pinned_basis'?[{
      name:`${entry.class}:archive-positive`,authority_class:entry.class,
      fixture:{admitted_by_archive:true,lineage_state_machine_passed:true,claim:{basis:'runtime_import',asserted_by:'urn:ccf:record:00000000-0000-4000-8000-000000000001',accepted_by:null}},expected:'accept',
    }]:[]),
  ]),
});

write('conformance-0.1.2.json',{
  format:'ccf.conformance/0.1.2',
  cases:[
    {id:'origin-cross-kind',requirement:'Record and Blob sharing one origin tuple succeed because object_kind differs',expected:'accept_both'},
    {id:'origin-same-kind',requirement:'Same-kind origin collision conflicts unless native IDs differ',expected:'conflict_then_suffix_accepts'},
    {id:'foreign-unavailability',requirement:'Erased and withheld compartments survive foreign merge',expected:'preserve_exact_state'},
    {id:'bootstrap-rebuild',requirement:'Bootstrap objects retain semantic compartments through reload and projection destruction',expected:'canonical_compartments_unchanged'},
    {id:'content-rejection-liveness',requirement:'A cryptographically valid content-rejected batch anchors successors',expected:'successor_eligible'},
    {id:'predecessor-pending',requirement:'An early batch remains pending and succeeds after its exact predecessor appears',expected:'queued_then_accepted'},
    {id:'suppression-row-rebuild',requirement:'Deleting a suppression projection row is detected and reconstructed from canonical state',expected:'detected_and_restored'},
    {id:'suppression-reintroduction',requirement:'Erased content remains blocked after total projection destruction',expected:'blocked_after_rebuild'},
    {id:'admission-membership',requirement:'Deleting or mutating admission state causes chain verification to fail',expected:'fail_closed'},
    {id:'pgvector-multischema',requirement:'PostgreSQL detects pgvector without public or search_path assumptions',expected:'extension_namespace_qualified'},
    {id:'git-three-commit',requirement:'Three commits cover evolution, rename, delete, binary content, and retry',expected:'stable_replay'},
    {id:'authority-classes',requirement:'Every authority class has positive and negative admission vectors',expected:'complete_matrix'},
  ],
});

const foreignUnavailableInput={
  mode:'foreign_merge',
  source_archive_id:'urn:ccf:archive:00000000-0000-4000-8000-000000000010',
  compartments:[
    {object_kind:'record',object_id:'urn:ccf:record:00000000-0000-4000-8000-000000000011',compartment:'semantic',availability:'erased',commitment:canonicalDigest('ccf:foreign-fixture:v1','erased'),retention_profile:'payload_erasable',source_custody_proof:'source-commit:7:2',unavailability_lineage_id:'urn:ccf:record:00000000-0000-4000-8000-000000000012'},
    {object_kind:'blob',object_id:'urn:ccf:blob:00000000-0000-4000-8000-000000000013',compartment:'blob_content',availability:'withheld',commitment:canonicalDigest('ccf:foreign-fixture:v1','withheld'),retention_profile:'payload_erasable',source_custody_proof:'source-commit:8:1',unavailability_lineage_id:'urn:ccf:record:00000000-0000-4000-8000-000000000014'},
  ],
};
write('foreign-unavailability.json',{
  input:foreignUnavailableInput,
  expected_destination_compartments:foreignUnavailableInput.compartments.map((entry)=>({...entry,plaintext:null})),
});

const suppressionIds={
  record:'urn:ccf:record:00000000-0000-4000-8000-000000000021',
  blob:'urn:ccf:blob:00000000-0000-4000-8000-000000000022',
  receipt:'urn:ccf:record:00000000-0000-4000-8000-000000000023',
  decision:'urn:ccf:record:00000000-0000-4000-8000-000000000024',
};
const suppressionProfile='ccf-hmac-sha256-suppression-v1';
const suppressionKey=Buffer.alloc(32,202);
const suppressionContentFixture={content_class:'record-semantic',canonical_plaintext:{language:'en',text:'erased fixture content'}};
const suppressionContentDigestValue=suppressionContentDigest(suppressionContentFixture.canonical_plaintext);
const suppressionPreimages=[
  {format:'ccf.suppression-preimage/1',kind:'origin',source_id:'urn:ccf:record:00000000-0000-4000-8000-000000000031',native_id:'segment-1842',revision:'1',object_kind:'record'},
  {format:'ccf.suppression-preimage/1',kind:'content',content_class:suppressionContentFixture.content_class,content_digest:suppressionContentDigestValue},
];
const suppressionEntries=suppressionPreimages.map((preimage)=>preimage.kind==='origin'?suppressionOriginToken(suppressionKey,preimage):suppressionContentToken(suppressionKey,preimage)).sort();
const suppressionRoot=suppressionMerkleRoot(suppressionEntries);
const suppressionSalt=Buffer.alloc(32,201).toString('base64url');
const suppressionBytes=Buffer.from(canonicalize({profile:suppressionProfile,entries:suppressionEntries}),'utf8');
const suppressionContentCommitment=blobContentCommitment(suppressionSalt,suppressionBytes);
const suppressionCatalog=JSON.parse(fs.readFileSync(path.join(ROOT,'semantic-catalog.json'),'utf8'));
const suppressionSchemaDigest=new Map(suppressionCatalog.schemas.map((entry)=>[entry.id,entry.digest]));
const suppressionBlobRegistry=JSON.parse(fs.readFileSync(path.join(ROOT,'registries','blobs.registry.json'),'utf8'));
const suppressionBlobEntry=suppressionBlobRegistry.entries.find((entry)=>entry.name==='blob.suppression_set');
const scopeObjectIds=[suppressionIds.record];
const expectedScopeCommitment=suppressionScopeCommitment(scopeObjectIds);
const suppressionRecordPayload={profile:suppressionProfile,suppression_blob_id:suppressionIds.blob,entry_count:String(suppressionEntries.length),entries_merkle_root:suppressionRoot,key_profile_id:'fixture-key-profile-v1',scope_commitment:expectedScopeCommitment,erasure_receipt_id:suppressionIds.receipt};
const suppressionReceiptPayload={decision_id:suppressionIds.decision,profile:'storage_verified',verified_at:'2026-08-12T12:00:00.000Z',target_count:'1',destroyed_key_count:'0',status:'verified',membership_link_type:'ccf.covers',suppression_commitment:{profile:suppressionRecordPayload.profile,suppression_set_record_id:suppressionIds.record,suppression_blob_id:suppressionIds.blob,entry_count:suppressionRecordPayload.entry_count,entries_merkle_root:suppressionRoot,key_profile_id:suppressionRecordPayload.key_profile_id,scope_commitment:suppressionRecordPayload.scope_commitment}};
const suppressionBlobStructural={type:'blob.suppression_set',type_version:1,type_visibility:'clear',schema_digest:suppressionSchemaDigest.get(suppressionBlobEntry.semantic_schema_id),registry_entry_digest:canonicalDigest('ccf:registry-entry:v1',suppressionBlobEntry),retention_profile:'structural_retention_required',media_type:'application/vnd.ccf.suppression-set+json',byte_length:String(suppressionBytes.length),content_commitment:suppressionContentCommitment,content_profile:'ccf-blob-content-v2',availability_class:'controlled',erasure_domain_id:null,structural_payload:{sensitivity:'governed_sensitive_metadata'},extensions:{}};
const suppressionBlobSemantic={content_salt:suppressionSalt,filename:'suppression-set.json',content_encryption_profile:'none',content_key_ref:null,extensions:{}};
const rejectedSuppressionPreimages=[
  {name:'unknown-kind',value:{format:'ccf.suppression-preimage/1',kind:'other'}},
  {name:'unknown-field',value:{...suppressionPreimages[0],unexpected:true}},
  {name:'missing-origin-field',value:{format:'ccf.suppression-preimage/1',kind:'origin',source_id:suppressionPreimages[0].source_id,native_id:'segment-1842',revision:'1'}},
  {name:'malformed-source-urn',value:{...suppressionPreimages[0],source_id:'urn:ccf:record:------------------------------------'}},
  {name:'invalid-content-class',value:{format:'ccf.suppression-preimage/1',kind:'content',content_class:'unknown',content_digest:suppressionContentDigestValue}},
  {name:'invalid-content-digest',value:{format:'ccf.suppression-preimage/1',kind:'content',content_class:'record-semantic',content_digest:'sha256:not-a-digest'}},
];
write('suppression-canonical.json',{ids:suppressionIds,content_fixture:{...suppressionContentFixture,expected_content_digest:suppressionContentDigestValue},preimages:suppressionPreimages,rejected_preimages:rejectedSuppressionPreimages,key_hex:suppressionKey.toString('hex'),entries:suppressionEntries,scope_object_ids:scopeObjectIds,expected_scope_commitment:expectedScopeCommitment,record_structural_payload:suppressionRecordPayload,blob_structural_content:suppressionBlobStructural,blob_semantic_content:suppressionBlobSemantic,receipt_structural_payload:suppressionReceiptPayload,encoded_blob_base64:suppressionBytes.toString('base64'),expected_content_commitment:suppressionContentCommitment,expected_entries_merkle_root:suppressionRoot});

write('mindpack-manifest-tamper.json',{
  format:'ccf.manifest-tamper-vectors/0.1.2',
  rule:'Every unsigned-manifest mismatch rejects before destination mutation',
  cases:[
    {id:'forged-low-count',fixture:'complete-example',mutation:'count-low',expected:'reject'},
    {id:'forged-high-count',fixture:'complete-example',mutation:'count-high',expected:'reject'},
    {id:'removed-stream-entry',fixture:'complete-example',mutation:'remove-stream',expected:'reject'},
    {id:'extra-unlisted-stream',fixture:'complete-example',mutation:'add-container-member',expected:'reject'},
    {id:'available-changed-to-erased',fixture:'complete-example',mutation:'available-to-erased',expected:'reject'},
    {id:'erased-changed-to-available',fixture:'erased-example',mutation:'erased-to-available',expected:'reject'},
    {id:'missing-dependency',fixture:'external-dependency-example',mutation:'remove-dependency',expected:'reject'},
    {id:'fabricated-dependency',fixture:'complete-example',mutation:'add-dependency',expected:'reject'},
    {id:'changed-genesis',fixture:'complete-example',mutation:'change-genesis',expected:'reject'},
    {id:'changed-head',fixture:'complete-example',mutation:'change-head',expected:'reject'},
    {id:'changed-archive-id',fixture:'complete-example',mutation:'change-archive-id',expected:'reject'},
    {id:'changed-catalog-root',fixture:'complete-example',mutation:'change-catalog-root',expected:'reject'},
    {id:'complete-changed-to-partial',fixture:'complete-example',mutation:'complete-to-partial',expected:'reject'},
    {id:'restore-changed-to-foreign-merge',fixture:'complete-example',mutation:'restore-to-foreign-merge',expected:'reject'},
    {id:'duplicate-availability-row',fixture:'complete-example',mutation:'duplicate-availability',expected:'reject'},
    {id:'contradictory-availability-row',fixture:'complete-example',mutation:'contradict-availability',expected:'reject'},
    {id:'absent-optional-stream',fixture:'complete-example',mutation:'add-optional-stream',expected:'reject'},
    {id:'changed-dependency-metadata',fixture:'external-dependency-example',mutation:'change-dependency-metadata',expected:'reject'},
    {id:'fabricated-custody-proof',fixture:'complete-example',mutation:'add-custody-proof',expected:'reject'},
  ],
});
console.log('vectors built');
