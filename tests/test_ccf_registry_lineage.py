"""Pure (no-DB) tests for pinned registries, lineage rules, and the DAG check."""

from __future__ import annotations

import pytest

from ccf.lineage import (
    LineageDeclarationError,
    add_edge,
    check_state_transition,
    creates_cycle,
    declare_lineage,
    remove_edge,
)
from ccf.registry import PinnedRegistries, RegistryError


@pytest.fixture(scope="module")
def registries(ccf_package_root):
    return PinnedRegistries.load(ccf_package_root)


def test_registry_lookups_fail_closed(registries):
    with pytest.raises(RegistryError):
        registries.type_entry("no.such.type")
    with pytest.raises(RegistryError):
        registries.link_entry("ccf.no_such_link")
    with pytest.raises(RegistryError):
        registries.state_machine("ccf.state.nope-v1")


def test_registry_entry_digest_matches_example(registries, ccf_examples_dir, load_ccf_json):
    """The pinned entry digest must equal the vendored example's structural binding."""
    structural = load_ccf_json(
        ccf_examples_dir / "record-8a28d4f2-8e94-4ab1-a0b4-08d6c6f1cc81.structural.json"
    )
    entry = registries.type_entry("core.source")
    assert (
        registries.entry_digest(entry)
        == structural["content"]["registry_entry_digest"]
    )


def test_declare_lineage_enforces_registry_mode(registries):
    stateful = registries.type_entry("process.run")
    with pytest.raises(LineageDeclarationError, match="requires a lineage"):
        declare_lineage({"type": "process.run"}, type_entry=stateful, registries=registries)

    stateless = registries.type_entry("core.person")
    with pytest.raises(LineageDeclarationError, match="not stateful"):
        declare_lineage(
            {"type": "core.person", "lineage": {"lineage_id": "x"}},
            type_entry=stateless,
            registries=registries,
        )
    assert declare_lineage({"type": "core.person"}, type_entry=stateless, registries=registries) is None


def test_state_machine_transitions(registries):
    machine = registries.state_machine("ccf.state.process-run-v1")
    assert check_state_transition(machine, current_state=None, transition="queue") is None
    assert check_state_transition(machine, current_state=None, transition="succeed") is None
    assert check_state_transition(machine, current_state="queue", transition="start") is None
    assert "not an initial" in check_state_transition(
        machine, current_state=None, transition="interrupt" + "x"
    )
    assert "terminal" in check_state_transition(
        machine, current_state="succeed", transition="start"
    )
    assert "not allowed" in check_state_transition(
        machine, current_state="queue", transition="succeed"
    )


def test_creates_cycle_pure():
    edges: dict[str, set[str]] = {}
    add_edge(edges, "a", "b")
    add_edge(edges, "b", "c")
    assert creates_cycle(edges, "c", "a")
    assert creates_cycle(edges, "a", "a")
    assert not creates_cycle(edges, "c", "d")
    remove_edge(edges, "b", "c")
    assert not creates_cycle(edges, "c", "a")
