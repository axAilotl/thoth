import fs from 'node:fs';
import path from 'node:path';
import { canonicalDigest, semanticCatalogRoot } from './ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');

function walk(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walk(p));
    else out.push(p);
  }
  return out;
}

const schemas = [];
for (const file of walk(path.join(ROOT, 'schemas')).filter((p) => p.endsWith('.json') && !p.endsWith('catalog.json')).sort()) {
  const obj = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (!obj.$id) continue;
  schemas.push({
    id: obj.$id,
    path: path.relative(ROOT, file).replaceAll(path.sep, '/'),
    digest: canonicalDigest('ccf:schema-artifact:v1', obj),
  });
}

const registries = [];
for (const file of walk(path.join(ROOT, 'registries')).filter((p) => p.endsWith('.json')).sort()) {
  const obj = JSON.parse(fs.readFileSync(file, 'utf8'));
  registries.push({
    name: obj.registry ?? path.basename(file),
    path: path.relative(ROOT, file).replaceAll(path.sep, '/'),
    digest: canonicalDigest('ccf:registry-artifact:v1', obj),
  });
}

const schemaCatalog = {
  format: 'ccf.schema-catalog/0.1.2',
  schemas,
};
fs.writeFileSync(path.join(ROOT, 'schemas', 'catalog.json'), JSON.stringify(schemaCatalog, null, 2) + '\n');

const semantic = {
  format: 'ccf.semantic-catalog/0.1.2',
  version: '0.1.2',
  schemas,
  registries,
};
semantic.root = semanticCatalogRoot(semantic);
fs.writeFileSync(path.join(ROOT, 'semantic-catalog.json'), JSON.stringify(semantic, null, 2) + '\n');
console.log(`semantic catalog ${semantic.root} (${schemas.length} schemas, ${registries.length} registries)`);
