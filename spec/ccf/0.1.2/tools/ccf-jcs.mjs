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

function checkSuppressionKey(key) {
  if (!Buffer.isBuffer(key) || key.length < 32) throw new Error('suppression key must contain at least 32 bytes');
}

export function suppressionOriginToken(key, canonicalPreimage) {
  checkSuppressionKey(key);
  if (canonicalPreimage === null || typeof canonicalPreimage !== 'object' || Array.isArray(canonicalPreimage)) throw new Error('suppression preimage must be an object');
  const required = ['format', 'kind', 'source_id', 'native_id', 'revision', 'object_kind'];
  if (canonicalPreimage.kind !== 'origin' || canonicalPreimage.format !== 'ccf.suppression-preimage/1') throw new Error('unknown suppression origin preimage format or kind');
  const keys = Object.keys(canonicalPreimage).sort();
  if (keys.length !== required.length || !required.every((name) => keys.includes(name))) throw new Error('suppression preimage has missing or unknown fields');
  if (!/^urn:ccf:record:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(canonicalPreimage.source_id)) throw new Error('invalid suppression origin source_id');
  if (typeof canonicalPreimage.native_id !== 'string' || canonicalPreimage.native_id.length < 1 || canonicalPreimage.native_id.length > 2048) throw new Error('invalid suppression origin native_id');
  if (typeof canonicalPreimage.revision !== 'string' || canonicalPreimage.revision.length < 1 || canonicalPreimage.revision.length > 256) throw new Error('invalid suppression origin revision');
  if (!['record', 'link', 'blob'].includes(canonicalPreimage.object_kind)) throw new Error('invalid suppression origin object_kind');
  const preimage = Buffer.from(canonicalize(canonicalPreimage), 'utf8');
  const message = Buffer.concat([Buffer.from('ccf:suppression-token:v1\0', 'utf8'), preimage]);
  return `hmac-sha256:${crypto.createHmac('sha256', key).update(message).digest('hex')}`;
}

export function suppressionContentDigest(value, { bytes = false } = {}) {
  const plaintext = bytes ? Buffer.from(value) : Buffer.from(canonicalize(value), 'utf8');
  return `sha256:${crypto.createHash('sha256').update(plaintext).digest('hex')}`;
}

export function suppressionContentToken(key, canonicalPreimage) {
  checkSuppressionKey(key);
  if (canonicalPreimage === null || typeof canonicalPreimage !== 'object' || Array.isArray(canonicalPreimage)) throw new Error('suppression preimage must be an object');
  const required = ['format', 'kind', 'content_class', 'content_digest'];
  const keys = Object.keys(canonicalPreimage).sort();
  if (canonicalPreimage.kind !== 'content' || canonicalPreimage.format !== 'ccf.suppression-preimage/1') throw new Error('unknown suppression content preimage format or kind');
  if (keys.length !== required.length || !required.every((name) => keys.includes(name))) throw new Error('suppression preimage has missing or unknown fields');
  if (!['record-semantic', 'link-semantic', 'blob-content'].includes(canonicalPreimage.content_class)) throw new Error('invalid suppression content class');
  if (!/^sha256:[0-9a-f]{64}$/.test(canonicalPreimage.content_digest)) throw new Error('invalid suppression content digest');
  const digest = Buffer.from(canonicalPreimage.content_digest.slice('sha256:'.length), 'hex');
  const message = Buffer.concat([
    Buffer.from('ccf:suppression-content:v1\0', 'utf8'),
    Buffer.from(canonicalPreimage.content_class, 'utf8'),
    Buffer.from([0]),
    digest,
  ]);
  return `hmac-sha256:${crypto.createHmac('sha256', key).update(message).digest('hex')}`;
}

export function suppressionScopeCommitment(scopeObjectIds) {
  if (!Array.isArray(scopeObjectIds) || !scopeObjectIds.every((id) => typeof id === 'string')) throw new Error('suppression scope object IDs must be an array of strings');
  return canonicalDigest('ccf:suppression-scope:v1', [...scopeObjectIds].sort());
}

export function suppressionMerkleRoot(tokens) {
  const ordered = [...new Set(tokens)].sort();
  if (ordered.length !== tokens.length) throw new Error('duplicate suppression token');
  const leaves = ordered.map((token) => {
    if (!/^hmac-sha256:[0-9a-f]{64}$/.test(token)) throw new Error(`invalid suppression token: ${token}`);
    return domainHashBytes('ccf:suppression-leaf:v1', Buffer.from(token, 'utf8'));
  });
  function root(nodes) {
    if (nodes.length === 0) return domainHashBytes('ccf:suppression-empty:v1');
    if (nodes.length === 1) return nodes[0];
    const split = largestPowerOfTwoLessThan(nodes.length);
    return domainHashBytes('ccf:suppression-node:v1', root(nodes.slice(0, split)), root(nodes.slice(split)));
  }
  return `sha256:${root(leaves).toString('hex')}`;
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
