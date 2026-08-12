#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT=Path(__file__).resolve().parents[1]
schemas={}; registry=Registry()
for p in (ROOT/'schemas').rglob('*.json'):
    if p.name=='catalog.json': continue
    obj=json.loads(p.read_text())
    if '$id' in obj:
        if obj['$id'] in schemas: raise SystemExit(f'duplicate schema id {obj["$id"]}')
        schemas[obj['$id']]=obj; registry=registry.with_resource(obj['$id'],Resource.from_contents(obj))

def validate(schema_id, instance, label):
    if schema_id not in schemas: raise SystemExit(f'missing schema {schema_id} for {label}')
    v=Draft202012Validator(schemas[schema_id],registry=registry,format_checker=FormatChecker())
    errors=sorted(v.iter_errors(instance),key=lambda e:list(e.path))
    if errors:
        print(f'FAIL {label}: {len(errors)} error(s)')
        for e in errors[:30]: print('  ','/'.join(map(str,e.path)),e.message)
        raise SystemExit(1)
    print('OK  ',label)

# Registry validation.
registry_specs=[
 ('types.registry.json','urn:ccf:schema:0.1.1:registries.type-registry'),
 ('links.registry.json','urn:ccf:schema:0.1.1:registries.link-registry'),
 ('blobs.registry.json','urn:ccf:schema:0.1.1:registries.blob-registry'),
 ('predicates.registry.json','urn:ccf:schema:0.1.1:registries.predicate-registry'),
 ('state-machines.registry.json','urn:ccf:schema:0.1.1:registries.state-machine-registry'),
 ('profiles.registry.json','urn:ccf:schema:0.1.1:registries.profile-registry'),
]
loaded={}
for fname,sid in registry_specs:
    obj=json.loads((ROOT/'registries'/fname).read_text());loaded[fname]=obj;validate(sid,obj,fname)

profiles={e['name'] for e in loaded['profiles.registry.json']['entries']}
states={e['id'] for e in loaded['state-machines.registry.json']['entries']}
types={(e['name'],e['version']):e for e in loaded['types.registry.json']['entries']}
links={(e['name'],e['version']):e for e in loaded['links.registry.json']['entries']}
for e in types.values():
    if e['semantic_schema_id'] not in schemas: raise SystemExit(f'missing semantic schema for {e["name"]}: {e["semantic_schema_id"]}')
    if e['structural_schema_id'] is not None and e['structural_schema_id'] not in schemas: raise SystemExit(f'missing structural schema for {e["name"]}')
    if e['profile'] not in profiles: raise SystemExit(f'missing profile for {e["name"]}: {e["profile"]}')
    if e['lineage_mode']=='compare_and_swap' and e['state_machine_id'] not in states: raise SystemExit(f'missing state machine for {e["name"]}')
    if e['lineage_mode']=='none' and e['state_machine_id'] is not None: raise SystemExit(f'unexpected state machine for {e["name"]}')
for e in links.values():
    if e['profile'] not in profiles: raise SystemExit(f'missing profile for Link {e["name"]}')
print('OK   registry cross references')

ex=ROOT/'examples/thoth-capture'
headers={}
for kind in ['record','link','blob']:
    for hp in sorted(ex.glob(f'{kind}-*.header.json')):
        h=json.loads(hp.read_text()); headers[h['id']]=h
        validate(f'urn:ccf:schema:0.1.1:objects.{kind}-header',h,hp.name)
        sp=hp.with_name(hp.name.replace('.header.json','.structural.json'))
        s=json.loads(sp.read_text());validate(f'urn:ccf:schema:0.1.1:objects.{kind}-structural',s,sp.name)
        sem_p=hp.with_name(hp.name.replace('.header.json','.semantic.json'))
        sem=json.loads(sem_p.read_text()) if sem_p.exists() else None
        if sem is not None: validate(f'urn:ccf:schema:0.1.1:objects.{kind}-semantic',sem,sem_p.name)
        st=s['content']
        if kind=='record':
            actual=sem['content'].get('sealed_type',{}).get('type') if st['type_visibility']=='sealed' and sem else st['type']
            version=sem['content'].get('sealed_type',{}).get('type_version') if st['type_visibility']=='sealed' and sem else st['type_version']
            e=types[(actual,version)]
            if e['lineage_mode']=='compare_and_swap' and 'lineage' not in st: raise SystemExit(f'missing lineage for {h["id"]} {actual}')
            if e['structural_schema_id'] is not None: validate(e['structural_schema_id'],st['structural_payload'],sp.name+' structural payload')
            if sem is not None: validate(e['semantic_schema_id'],sem['content']['payload'],sem_p.name+' semantic payload')
            elif e['semantic_schema_id']!='urn:ccf:schema:0.1.1:payload.core.empty':
                raise SystemExit(f'non-empty semantic type lacks semantic compartment: {actual}')
        elif kind=='link':
            e=links[(st['type'],st['type_version'])]
            if e['endpoints_location']=='structural' and not {'from_id','to_id'} <= st.keys(): raise SystemExit(f'missing structural endpoints {h["id"]}')
            if e['endpoints_location']=='semantic' and (sem is None or 'endpoints' not in sem['content']): raise SystemExit(f'missing semantic endpoints {h["id"]}')
        else:
            validate('urn:ccf:schema:0.1.1:objects.blob-structural-content',st,sp.name+' content')

batch=json.loads((ex/'producer-batch.json').read_text());validate('urn:ccf:schema:0.1.1:sync.producer-batch',batch,'producer-batch.json')
# Same-batch references resolve to an existing or submitted ID.
submitted={s['id'] for s in batch['records']+batch['links']+batch['blobs']}
known=set(headers)
for l in batch['links']:
    for k in ['from_id','to_id']:
        if l[k] not in submitted|known: raise SystemExit(f'dangling same-batch ref {l[k]}')
print('OK   same-batch references')

manifest=json.loads((ROOT/'examples/mindpack/manifest.json').read_text());validate('urn:ccf:schema:0.1.1:objects.mindpack-manifest',manifest,'mindpack manifest')
cat=json.loads((ROOT/'schemas/catalog.json').read_text())
ids=[e['id'] for e in cat['schemas']]
if len(ids)!=len(set(ids)): raise SystemExit('duplicate catalog schema ID')
for e in cat['schemas']:
    if not (ROOT/e['path']).exists(): raise SystemExit(f'missing catalog path {e["path"]}')
print(f'OK   schema catalog ({len(ids)} schemas)')
print('\nAll CCF 0.1.1 package examples and registries validate.')
