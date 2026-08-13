# CCF 0.1.2-rc1 JSON Schemas

Schemas use JSON Schema 2020-12 and immutable `urn:ccf:schema:0.1.2-rc1:*` identifiers.

The object wire shape is split into a portable header, a structural compartment, and an optional semantic compartment. Payload schemas validate `semantic.content.payload`; type-registry metadata determines retention, lineage, and visibility behavior.

`schemas/catalog.json` is generated after all files are written. Published schemas are never edited in place; a later incompatible form receives a new schema ID and profile.
