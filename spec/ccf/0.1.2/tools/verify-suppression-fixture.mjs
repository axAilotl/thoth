import {
  blobContentCommitment,
  suppressionMerkleRoot,
  suppressionScopeCommitment,
} from './ccf-jcs.mjs';

function requireFixture(condition, message) {
  if (!condition) throw new Error(`suppression fixture: ${message}`);
}

function contentOf(structuralById, objectId) {
  return structuralById.get(objectId)?.content;
}

/** Cross-check canonical suppression state carried by an example mindpack. */
export function verifySuppressionFixture({
  records,
  links,
  blobs,
  structuralById,
  semanticById,
  blobBytesById,
  availability,
}) {
  const recordsById = new Map(records.map((header) => [header.id, header]));
  const blobsById = new Map(blobs.map((header) => [header.id, header]));
  const verifiedReceipts = records.filter((header) => {
    const content = contentOf(structuralById, header.id);
    return content?.type === 'lineage.erasure_receipt'
      && content.structural_payload?.status === 'verified';
  });
  requireFixture(verifiedReceipts.length > 0, 'no verified erasure receipt');

  for (const receiptHeader of verifiedReceipts) {
    const receiptContent = contentOf(structuralById, receiptHeader.id);
    const receipt = receiptContent.structural_payload;
    const commitment = receipt.suppression_commitment;
    requireFixture(commitment?.profile === 'ccf-hmac-sha256-suppression-v1', 'receipt profile');

    const setHeader = recordsById.get(commitment.suppression_set_record_id);
    const setContent = contentOf(structuralById, setHeader?.id);
    requireFixture(setContent?.type === 'lineage.suppression_set', 'suppression-set reference has wrong type');
    const set = setContent.structural_payload;
    requireFixture(set.erasure_receipt_id === receiptHeader.id, 'suppression set points to wrong receipt');
    for (const field of [
      'profile',
      'suppression_blob_id',
      'entry_count',
      'entries_merkle_root',
      'key_profile_id',
      'scope_commitment',
    ]) {
      requireFixture(set[field] === commitment[field], `receipt/set ${field} mismatch`);
    }

    const blobHeader = blobsById.get(commitment.suppression_blob_id);
    const blob = contentOf(structuralById, blobHeader?.id);
    requireFixture(blob?.type === 'blob.suppression_set', 'suppression Blob has wrong type');
    requireFixture(
      blob.media_type === 'application/vnd.ccf.suppression-set+json',
      'suppression Blob has ordinary media type',
    );
    requireFixture(
      blob.retention_profile === 'structural_retention_required'
        && blob.structural_payload?.sensitivity === 'governed_sensitive_metadata',
      'suppression Blob is not governed retained metadata',
    );
    const semantic = semanticById.get(blobHeader.id)?.content;
    const bytes = blobBytesById.get(blobHeader.id);
    requireFixture(Boolean(semantic) && Buffer.isBuffer(bytes), 'suppression Blob content is unavailable');
    requireFixture(String(bytes.length) === blob.byte_length, 'suppression Blob byte length mismatch');
    requireFixture(
      blobContentCommitment(semantic.content_salt, bytes) === blob.content_commitment,
      'suppression Blob content commitment mismatch',
    );
    let document;
    try {
      document = JSON.parse(bytes.toString('utf8'));
    } catch (error) {
      throw new Error(`suppression fixture: Blob is not canonical JSON: ${error.message}`);
    }
    requireFixture(
      document !== null
        && typeof document === 'object'
        && !Array.isArray(document)
        && Object.keys(document).sort().join(',') === 'entries,profile',
      'suppression Blob document shape',
    );
    requireFixture(document.profile === commitment.profile, 'suppression Blob profile mismatch');
    requireFixture(Array.isArray(document.entries), 'suppression Blob entries are not an array');
    requireFixture(String(document.entries.length) === commitment.entry_count, 'suppression token count mismatch');
    requireFixture(
      suppressionMerkleRoot(document.entries) === commitment.entries_merkle_root,
      'suppression token Merkle root mismatch',
    );

    const coveredTargets = links
      .filter((header) => {
        const content = contentOf(structuralById, header.id);
        return content?.type === receipt.membership_link_type
          && content.from_id === receiptHeader.id;
      })
      .map((header) => contentOf(structuralById, header.id).to_id)
      .sort();
    requireFixture(new Set(coveredTargets).size === coveredTargets.length, 'duplicate covered target');
    requireFixture(String(coveredTargets.length) === receipt.target_count, 'receipt target count mismatch');
    requireFixture(
      suppressionScopeCommitment(coveredTargets) === commitment.scope_commitment,
      'suppression scope commitment mismatch',
    );
    for (const targetId of coveredTargets) {
      const targetHeader = recordsById.get(targetId) ?? blobsById.get(targetId);
      requireFixture(Boolean(targetHeader), `covered target is absent: ${targetId}`);
      const erasableCompartments = [];
      if (targetHeader.semantic_commitment !== null) erasableCompartments.push('semantic');
      if (targetHeader.object_kind === 'blob') erasableCompartments.push('blob_content');
      requireFixture(
        erasableCompartments.some((compartment) => {
          const state = availability.get(`${targetId}\0${compartment}`);
          return state?.availability === 'erased'
            && state.unavailability_lineage_id === receiptHeader.id;
        }),
        `verified erasure contradicts availability for ${targetId}`,
      );
    }
  }
  return verifiedReceipts.length;
}
