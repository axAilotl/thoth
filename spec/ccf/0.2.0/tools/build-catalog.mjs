import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalDigest, semanticCatalogRoot } from '../../0.1.2/tools/ccf-jcs.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BASE = path.resolve(ROOT, '..', '0.1.2');

function walk(directory) {
  const files = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...walk(absolute));
    else files.push(absolute);
  }
  return files;
}

const schemas = walk(path.join(ROOT, 'schemas'))
  .filter((file) => file.endsWith('.json') && !file.endsWith('catalog.json'))
  .sort()
  .map((file) => {
    const value = JSON.parse(fs.readFileSync(file, 'utf8'));
    return {
      id: value.$id,
      path: path.relative(ROOT, file).replaceAll(path.sep, '/'),
      digest: canonicalDigest('ccf:schema-artifact:v1', value),
    };
  });

const registries = walk(path.join(ROOT, 'registries'))
  .filter((file) => file.endsWith('.json'))
  .sort()
  .map((file) => {
    const value = JSON.parse(fs.readFileSync(file, 'utf8'));
    return {
      name: value.registry,
      path: path.relative(ROOT, file).replaceAll(path.sep, '/'),
      digest: canonicalDigest('ccf:registry-artifact:v1', value),
    };
  });

const schemaCatalog = {
  format: 'ccf.schema-catalog/0.2.0',
  status: 'working-draft',
  schemas,
};
fs.writeFileSync(
  path.join(ROOT, 'schemas', 'catalog.json'),
  `${JSON.stringify(schemaCatalog, null, 2)}\n`,
);

const baseCatalog = JSON.parse(fs.readFileSync(path.join(BASE, 'semantic-catalog.json'), 'utf8'));
const semanticCatalog = {
  format: 'ccf.semantic-catalog/0.2.0',
  version: '0.2.0',
  status: 'working-draft',
  portable_object_formats: ['ccf/0.1.2'],
  base_catalogs: [{ version: baseCatalog.version, root: baseCatalog.root }],
  schemas,
  registries,
};
semanticCatalog.root = semanticCatalogRoot(semanticCatalog);
fs.writeFileSync(
  path.join(ROOT, 'semantic-catalog.json'),
  `${JSON.stringify(semanticCatalog, null, 2)}\n`,
);

console.log(
  `semantic catalog ${semanticCatalog.root} (${schemas.length} schemas, ${registries.length} registries)`,
);
