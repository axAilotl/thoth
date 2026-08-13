import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const INVENTORY = 'PACKAGE-INVENTORY.md';
const CHECKSUMS = 'SHA256SUMS';

function walk(directory) {
  const files = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...walk(absolute));
    else files.push(path.relative(ROOT, absolute).replaceAll(path.sep, '/'));
  }
  return files;
}

const packageFiles = walk(ROOT)
  .filter((file) => file !== INVENTORY && file !== CHECKSUMS)
  .sort();
const topCounts = new Map();
for (const file of packageFiles) {
  const top = file.split('/')[0];
  topCounts.set(top, (topCounts.get(top) ?? 0) + 1);
}
const schemaCount = packageFiles.filter((file) => file.startsWith('schemas/') && file.endsWith('.schema.json')).length;
const registryCount = packageFiles.filter((file) => file.startsWith('registries/') && file.endsWith('.json')).length;
const markdownCount = packageFiles.filter((file) => file.endsWith('.md')).length;
const inventory = [
  '# CCF 0.1.2-rc1 package inventory',
  '',
  'Generated deterministically from the validated reference package.',
  '',
  `- Files before checksum manifest and inventory: **${packageFiles.length}**`,
  `- JSON Schemas: **${schemaCount}**`,
  `- Registry files: **${registryCount}**`,
  `- Markdown documents: **${markdownCount}**`,
  '',
  '## Top-level counts',
  '',
  ...[...topCounts].sort(([a], [b]) => a.localeCompare(b)).map(([name, count]) => `- \`${name}\`: ${count}`),
  '',
  '## Files',
  '',
  ...packageFiles.map((file) => `- \`${file}\``),
  '',
].join('\n');
fs.writeFileSync(path.join(ROOT, INVENTORY), inventory);

const checksumFiles = walk(ROOT).filter((file) => file !== CHECKSUMS).sort();
const checksums = checksumFiles.map((file) => {
  const digest = crypto.createHash('sha256').update(fs.readFileSync(path.join(ROOT, file))).digest('hex');
  return `${digest}  ./${file}`;
}).join('\n') + '\n';
fs.writeFileSync(path.join(ROOT, CHECKSUMS), checksums);
console.log(`package metadata: ${packageFiles.length} inventory files, ${checksumFiles.length} checksums`);
