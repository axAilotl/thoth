# CCF 0.2.0 Working Draft registries

The registries separate concerns previously combined in
`profiles.registry.json`:

- `levels.registry.json` defines the four cumulative guarantee levels;
- `roles.registry.json` defines implementation behavior classes;
- `capabilities.registry.json` defines optional operational and security claims;
- `semantic-packs.registry.json` defines optional domain vocabularies;
- `semantic-requirements.registry.json` assigns semantic and consequential-state
  levels plus capabilities to every inherited 0.1.2 type and predicate;
- `legacy-profile-mappings.registry.json` maps frozen 0.1.2 profiles without
  changing them;
- `compatibility-rules.registry.json` pins the six mandatory cross-level rules.

`semantic-requirements.registry.json` is generated deterministically from the
frozen 0.1.2 registries plus the mapping rules in
`tools/build-requirements.mjs`.

The complementary manifests in `../bundles/` define separate artifact
distributions for the four levels and three semantic packs.
