import crypto from 'node:crypto';

function assertUnicodeScalarString(value) {
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      if (i + 1 >= value.length) throw new Error('unpaired high surrogate');
      const next = value.charCodeAt(i + 1);
      if (next < 0xdc00 || next > 0xdfff) throw new Error('unpaired high surrogate');
      i += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new Error('unpaired low surrogate');
    }
  }
}

export function canonicalize(value) {
  if (value === null) return 'null';
  const kind = typeof value;
  if (kind === 'boolean') return value ? 'true' : 'false';
  if (kind === 'number') {
    if (!Number.isFinite(value)) throw new Error('non-finite number');
    if (Object.is(value, -0)) throw new Error('negative zero');
    return JSON.stringify(value);
  }
  if (kind === 'string') {
    assertUnicodeScalarString(value);
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(',')}]`;
  if (kind === 'object') {
    const keys = Object.keys(value).sort();
    for (const key of keys) {
      assertUnicodeScalarString(key);
      if (value[key] === undefined) throw new Error(`undefined value at key ${key}`);
    }
    return `{${keys.map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(',')}}`;
  }
  throw new Error(`unsupported JSON value: ${kind}`);
}

export function sha256Bytes(bytes) {
  return crypto.createHash('sha256').update(bytes).digest();
}

export function digestString(bytes) {
  return `sha256:${sha256Bytes(bytes).toString('hex')}`;
}

export function parseDigest(digest) {
  if (!/^sha256:[0-9a-f]{64}$/.test(digest)) throw new Error(`invalid SHA-256 digest: ${digest}`);
  return Buffer.from(digest.slice(7), 'hex');
}

export function domainHashBytes(domain, ...parts) {
  const chunks = [Buffer.from(domain, 'utf8'), Buffer.from([0]), ...parts.map((part) => Buffer.isBuffer(part) ? part : Buffer.from(part))];
  return sha256Bytes(Buffer.concat(chunks));
}

export function domainDigest(domain, ...parts) {
  return `sha256:${domainHashBytes(domain, ...parts).toString('hex')}`;
}

function decodeB64Url(text) {
  return Buffer.from(text, 'base64url');
}

export function compartmentCommitment(objectKind, compartment, envelope) {
  const domain = `ccf:${objectKind}-${compartment}:v2`;
  if (!['record', 'link', 'blob'].includes(objectKind)) throw new Error(`unsupported object kind: ${objectKind}`);
  if (!['structural', 'semantic'].includes(compartment)) throw new Error(`unsupported compartment: ${compartment}`);
  const salt = decodeB64Url(envelope.salt);
  if (salt.length !== 32) throw new Error('compartment salt must be 32 bytes');
  return domainDigest(domain, salt, Buffer.from(canonicalize(envelope.content), 'utf8'));
}

export function blobContentCommitment(contentSalt, bytes) {
  const salt = decodeB64Url(contentSalt);
  if (salt.length !== 32) throw new Error('Blob content salt must be 32 bytes');
  return domainDigest('ccf:blob-content:v2', salt, bytes);
}

export function objectHash(header) {
  const kind = header.object_kind;
  if (!['record', 'link', 'blob'].includes(kind)) throw new Error(`unsupported object kind: ${kind}`);
  const input = structuredClone(header);
  delete input.object_hash;
  return domainDigest(`ccf:${kind}:v2`, Buffer.from(canonicalize(input), 'utf8'));
}

export function submissionHash(submission) {
  return domainDigest('ccf:submission:v2', Buffer.from(canonicalize(submission), 'utf8'));
}

export function producerBatchHash(batch) {
  const input = structuredClone(batch);
  delete input.batch_hash;
  delete input.signature;
  return domainDigest('ccf:producer-batch:v1', Buffer.from(canonicalize(input), 'utf8'));
}

export function producerBatchSigningDigest(batchHash) {
  return parseDigest(batchHash);
}

export function commitLeaf(member) {
  return domainHashBytes('ccf:commit-leaf:v2', Buffer.from(canonicalize(member), 'utf8'));
}

function largestPowerOfTwoLessThan(n) {
  let k = 1;
  while ((k << 1) < n) k <<= 1;
  return k;
}

export function merkleRootFromLeafBytes(leaves) {
  if (leaves.length === 0) return domainHashBytes('ccf:merkle-empty:v2');
  if (leaves.length === 1) return leaves[0];
  const split = largestPowerOfTwoLessThan(leaves.length);
  const left = merkleRootFromLeafBytes(leaves.slice(0, split));
  const right = merkleRootFromLeafBytes(leaves.slice(split));
  return domainHashBytes('ccf:merkle-node:v2', left, right);
}

export function merkleRoot(members) {
  const ordered = [...members].sort((a, b) => {
    const ap = Number(a.commit_position);
    const bp = Number(b.commit_position);
    return ap - bp;
  });
  const positions = new Set(ordered.map((item) => Number(item.commit_position)));
  if (positions.size !== ordered.length) throw new Error('duplicate commit position');
  for (let i = 0; i < ordered.length; i += 1) {
    if (Number(ordered[i].commit_position) !== i) throw new Error('commit positions must be contiguous from zero');
  }
  return `sha256:${merkleRootFromLeafBytes(ordered.map(commitLeaf)).toString('hex')}`;
}

export function commitSigningDigest(headerWithoutCommitments, structuralContentWithoutSignature) {
  const input = {
    header: headerWithoutCommitments,
    structural_content: structuralContentWithoutSignature,
  };
  return domainHashBytes('ccf:commit-sig:v2', Buffer.from(canonicalize(input), 'utf8'));
}

export function semanticCatalogRoot(catalogWithoutRoot) {
  return domainDigest('ccf:semantic-catalog:v1', Buffer.from(canonicalize(catalogWithoutRoot), 'utf8'));
}

export function canonicalDigest(domain, value) {
  return domainDigest(domain, Buffer.from(canonicalize(value), 'utf8'));
}
