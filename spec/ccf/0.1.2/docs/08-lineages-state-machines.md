# 8. Lineages, current state, valid time, and graph correction

## 8.1 Total order

Current-state precedence uses numeric:

```text
(commit_sequence, commit_position)
```

Source times and wall clocks are evidence, not precedence authorities.

## 8.2 Compare-and-swap is universal for stateful lineages

Every registered stateful type declares a state machine and requires:

```text
lineage_id
previous_head_id
transition
valid_from
expires_at
```

The submitted predecessor must equal the current admitted head. This applies to policy, consent, legal basis determinations, grants, restrictions, entity adjudication, Link disposition, erasure decisions, credentials, keysets, catalog transitions, and succession.

Last-writer-wins is permitted only for a type explicitly registered as a non-authoritative observation stream.

## 8.3 Admission time versus valid time

For a query at archive head `H` and effective time `T`:

1. consider only transitions known by `H`;
2. follow valid state-machine transitions in admission order;
3. apply the latest admitted applicable transition whose valid interval contains `T`;
4. fail closed on invalid overlaps unless the registered state machine defines precedence.

Backdated transitions do not rewrite what was known at an earlier head. Historical evaluation asks, “what state effective at T was known at H?”

## 8.4 Link dispositions

Links are immutable; their current use is governed by `lineage.link_disposition` Records:

```text
retract
restore
supersede
invalidate_selector
tombstone
```

The target Link ID, action, predecessor, replacement ID, and terminal flag are structurally retained. Human and machine dispositions use the same compare-and-swap rule.

A physical or cryptographic erasure tombstone is terminal. A logical retraction may be restored if the state machine permits.

## 8.5 Entity resolution

An entity decision is a Record plus authoritative membership Links admitted atomically. The Record declares the operation; active `same_as` or `distinct_from` Links declare membership.

If the Record and Link set disagree, admission fails. Entity clusters remain projections.

A merge or split advances the entity-generation fence and invalidates dependent consent and policy decisions.

## 8.6 Derivation graph

Active `derived_from` Links form a DAG. New or restored edges are cycle-checked inside the serialized admission transaction. Other relation types may be cyclic.

Recursive CTEs are the correctness baseline. A closure table is a rebuildable acceleration projection.

## 8.7 Human decisions survive projection loss

Accepted candidates, entity decisions, quarantine releases, manual corrections, policy exceptions, fold decisions, and deletion approvals are Records. Destroying every projection must not erase them.
