# 9. Governance and contextual authorization

## 9.1 Baseline evaluator

Every Archive implements `ccf-deny-overrides-v1`:

- root default is deny;
- applicable deny overrides allow;
- mandatory obligations accumulate;
- unknown required context yields deny or pending;
- destination overlays may tighten imported policy but may not silently widen it;
- legal bases may be explicit alternatives rather than all required simultaneously;
- an impossible mandatory obligation denies or remains pending.

Jurisdiction-specific modules are optional pinned inputs. CCF represents decisions and evidence; it does not certify legal sufficiency.

## 9.2 Policy closure

Applicable inputs are deterministically collected from:

- direct object policy;
- active `governed_by` Links;
- active derivation ancestors where policy propagates;
- current resolved data-subject identities;
- consent, restriction, objection, legal-basis, and hold lineages;
- archive governance;
- destination-local tightening overlays.

The exact predicates and evaluator version are pinned in the semantic catalog.

## 9.3 Decision context

There is no context-free “effective policy.” A decision includes:

```text
operation
purpose
requester
recipient
runtime
destination
jurisdiction
requested time
object set
archive head
```

The result includes allow/deny/pending, obligations, reason codes, closure hash, context hash, evaluator version, generation vector, and expiry.

## 9.4 Local reads versus consequential egress

CCF Core does not require a remote governance call for local owner access to a local replica. The local runtime evaluates against its synchronized head and cached policy inputs.

A deployment MUST require fresh authorization at the point of consequential external action such as:

- disclosure outside the archive control domain;
- publication;
- message sending;
- spending;
- destructive remote action;
- model training or adaptation using protected data.

## 9.5 Generation fences

A governance mutation atomically advances relevant generation fences in the same transaction as admission. Cached decisions record the generations used.

A cached decision is usable only when every required generation matches. Fine-grained dirty discovery may run asynchronously; the fence closes the unsafe window immediately.

Widening changes may remain conservatively denied while recomputing. Tightening or unknown-direction changes block stale allows.

## 9.6 Pending behavior

When dependencies are dirty, the API returns structured pending information with dirty sequence, dependency estimate, retry hint, and request ID. The implementation must prioritize requested objects and eventually return allow, deny, or a documented terminal error. An implementation that permanently returns pending or denies everything fails positive conformance tests.

## 9.7 Fenced external capabilities

For external egress, an authorization capability binds:

- operation and purpose;
- exact objects;
- requester, recipient, runtime, and destination;
- archive head and generation vector;
- availability state;
- short expiry and use count.

The key-unwrapping or egress boundary consumes it and rechecks generations. A second ordinary read alone is not full linearization.

## 9.8 Consequential receipts

Consequential disclosures and actions create canonical receipts. High-volume internal reads may use chained audit segments stored as governed Blobs with periodically committed roots, avoiding one global commit per prompt-context lookup.
