"""Reference completeness for packs (spec section 2.5).

A reference is complete when its target is included in the pack, declared
as an external dependency, declared withheld, declared erased with a
resolvable receipt, or preserved as a foreign custody proof. An undeclared
dangling reference makes the pack incomplete; importers fail closed on
incomplete packs unless the caller explicitly requests a partial import.

Reference extraction mirrors the admission-time completeness rule
(``ccf.admission._validate_references``): Link endpoints, semantic
``origin.source_id``, ``recorded_by``, claimed ``person_id`` /
``perspective_id``, lineage ``previous_head_id``, and Link-disposition
payload targets.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ccf.sync.packio import PackObject


@dataclass
class CompletenessReport:
    """Classification of every reference made by objects in a pack."""

    included: list[str] = field(default_factory=list)
    external: list[str] = field(default_factory=list)
    withheld: list[str] = field(default_factory=list)
    erased: list[str] = field(default_factory=list)
    foreign_custody: list[str] = field(default_factory=list)
    dangling: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not self.dangling

    def to_dict(self) -> dict:
        return {
            "complete": self.complete,
            "included": list(self.included),
            "external": list(self.external),
            "withheld": list(self.withheld),
            "erased": list(self.erased),
            "foreign_custody": list(self.foreign_custody),
            "dangling": list(self.dangling),
        }


def object_references(obj: PackObject) -> set[str]:
    """All portable IDs ``obj`` references, per the admission rule set."""
    refs: set[str] = set()
    structural = (obj.structural or {}).get("content") or {}
    semantic = (obj.semantic or {}).get("content") or {}

    if obj.object_kind == "link":
        for key in ("from_id", "to_id"):
            if structural.get(key):
                refs.add(structural[key])
        endpoints = semantic.get("endpoints") or {}
        for key in ("from_id", "to_id"):
            if endpoints.get(key):
                refs.add(endpoints[key])
    if semantic.get("origin") and semantic["origin"].get("source_id"):
        refs.add(semantic["origin"]["source_id"])
    if semantic.get("recorded_by"):
        refs.add(semantic["recorded_by"])
    claimed = semantic.get("claimed") or {}
    for key in ("person_id", "perspective_id"):
        if claimed.get(key):
            refs.add(claimed[key])
        # Bootstrap-style semantic contents carry these at top level.
        if semantic.get(key):
            refs.add(semantic[key])
    lineage = structural.get("lineage") or {}
    if lineage.get("previous_head_id"):
        refs.add(lineage["previous_head_id"])
    if structural.get("type") == "lineage.link_disposition":
        payload = structural.get("structural_payload") or {}
        if payload.get("target_link_id"):
            refs.add(payload["target_link_id"])
        if payload.get("replacement_link_id"):
            refs.add(payload["replacement_link_id"])
    return refs


def classify_references(
    objects: dict[str, PackObject],
    *,
    external_ids: set[str] | None = None,
    withheld_ids: set[str] | None = None,
    erased_ids: set[str] | None = None,
    foreign_ids: set[str] | None = None,
    known_ids: set[str] | None = None,
) -> CompletenessReport:
    """Classify every reference made by the pack's objects.

    ``known_ids`` models targets already present in the importing archive
    (delta packs), which count as satisfied without being included.
    """
    external_ids = external_ids or set()
    withheld_ids = withheld_ids or set()
    erased_ids = erased_ids or set()
    foreign_ids = foreign_ids or set()
    known_ids = known_ids or set()

    report = CompletenessReport()
    buckets = {
        "included": set(),
        "external": set(),
        "withheld": set(),
        "erased": set(),
        "foreign": set(),
        "dangling": set(),
    }
    for obj in objects.values():
        for ref in object_references(obj):
            if ref in objects or ref in known_ids:
                buckets["included"].add(ref)
            elif ref in external_ids:
                buckets["external"].add(ref)
            elif ref in withheld_ids:
                buckets["withheld"].add(ref)
            elif ref in erased_ids:
                buckets["erased"].add(ref)
            elif ref in foreign_ids:
                buckets["foreign"].add(ref)
            else:
                buckets["dangling"].add(ref)
    report.included = sorted(buckets["included"])
    report.external = sorted(buckets["external"])
    report.withheld = sorted(buckets["withheld"])
    report.erased = sorted(buckets["erased"])
    report.foreign_custody = sorted(buckets["foreign"])
    report.dangling = sorted(buckets["dangling"])
    return report
