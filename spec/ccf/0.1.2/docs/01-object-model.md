# 1. Object model and invariants

## 1.1 Three portable object kinds

### Record

A typed thing: person, source, event, assertion, policy, decision, process run, receipt, or integrity operation.

### Link

An independently addressable typed relationship between two CCF objects. Attachments are Links. A bad relationship is corrected through an append-only disposition Record.

### Blob

A commitment and governed manifest for bytes. The bytes may be present, external, withheld, disclosed, published, or erased.

No fourth canonical object kind is introduced for attachments, batches, entity clusters, policy caches, or projections.

## 1.2 Four layers of a stored object

Each portable object participates in four layers:

1. **Portable header** — object kind, globally stable ID, cryptographic profile, compartment commitments, object hash.
2. **Structural compartment** — minimum type, lineage, endpoint, or integrity material needed by the declared retention profile.
3. **Semantic compartment** — person/perspective, source claims, archive resolution, privacy, policy, authority, selectors, and payload.
4. **Archive-local admission** — commit sequence, position, admission time, local storage, caches, and indexes.

Only the first three are portable object material. Admission metadata is authenticated by commit membership but excluded from the portable object hash.

## 1.3 Structural retention is type-declared

The type and Link registries declare one of four profiles:

- `erasable` — both compartments may be removed; the header and commitments may remain.
- `payload_erasable` — structural compartment remains; semantic compartment may be removed.
- `structural_retention_required` — type-specific structural data needed for lineage or replay remains.
- `epoch_lifetime_required` — required structural material remains for the archive epoch.

A retained compartment remains policy-governed and may be encrypted at rest. Retained does not mean public.

## 1.4 Canonical versus projection state

Canonical information consists of admitted Records, Links, Blobs, their available compartments, and journal history.

Disposable projections include:

- current-state views;
- entity clusters;
- derivation closure;
- effective-policy caches;
- full-text and vector indexes;
- graph caches;
- summaries and wiki pages unless preserved as generated artifacts;
- prompt/context selections;
- archive-head caches;
- invalidation queues and checkpoints.

> If a human decided it, it is a Record. If a machine can recompute it without losing human judgment, it may be a projection.

## 1.5 Foundational invariants

```text
Evidence is not belief.
Content is not instruction.
Inference is not acceptance.
Preference is not authority to act.
Consent is not delegated agency.
A Runtime is not the Person.
A model of a Person is not the Person.
A generated summary never replaces its source.
Every derived object links to exact evidence.
Every object has a governance binding while its governed content is usable.
Admission order is the state-precedence clock.
Valid time determines when admitted state applies.
Corrections append; they do not rewrite admitted history.
Erasure may remove protected content while preserving permitted commitments and lineage.
Unknown semantics survive round trips but activate no behavior.
Canonical data is Records, Links, and Blobs.
Everything optimized for one application is a projection.
```

## 1.6 N-ary decisions

Links remain binary. An n-ary operation is a Record with authoritative membership Links:

```text
ErasureDecision
  ├── ccf.covers → Record A
  ├── ccf.covers → Blob B
  ├── ccf.invalidates → Link C
  └── ccf.destroys_key → key receipt D
```

Payload lists are presentation aids only. Active membership Links define canonical membership.

## 1.7 Unknown and sealed semantics

An unknown type or predicate MUST be preserved but MUST NOT grant authority or behavior.

A sensitive object MAY use `sealed.record` or `sealed.link` in its structural compartment. The exact type and, for a sealed Link, endpoints live in the semantic compartment. If that compartment is erased, the exact semantics intentionally become unavailable. Sealed Links cannot participate in structural derivation or governance propagation after erasure.
