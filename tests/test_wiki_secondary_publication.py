"""Every deterministic wiki writer must honor the same human-edit boundary."""

from copy import deepcopy

import pytest

from core.config import config
from core.path_layout import build_path_layout
from core.semantic_memory import SemanticMemoryCandidate, SemanticMemoryEvidence, SemanticMemoryStore
from core.semantic_wiki_compiler import SemanticMemoryWikiCompiler
from core.wiki_capture_compiler import CaptureWikiCompiler
from core.wiki_contract import WikiContract
from tests.test_wiki_updater import _capture_store_with_public_and_restricted_events, _configure_runtime_config


@pytest.fixture(params=("capture", "semantic"))
def secondary_writer(tmp_path, monkeypatch, request):
    monkeypatch.chdir(tmp_path)
    previous = deepcopy(config.data)
    _configure_runtime_config(tmp_path)
    try:
        layout = build_path_layout(config)
        layout.ensure_directories()
        contract = WikiContract(root=layout.wiki_root)
        if request.param == "capture":
            compiler = CaptureWikiCompiler(layout=layout, contract=contract)
            store = _capture_store_with_public_and_restricted_events(layout)
            slug = "capture-daily-2026-04-04"
            renderer = "_render_capture_page"
        else:
            compiler = SemanticMemoryWikiCompiler(layout=layout, contract=contract)
            store = SemanticMemoryStore(compiler.publications.db)
            store.add_candidate(
                SemanticMemoryCandidate(
                    candidate_id="candidate-ada", candidate_type="preference", status="confirmed",
                    text="Ada prefers morning writing.", entity_id="person:ada", entity_type="person",
                    entity_name="Ada",
                ),
                evidence=(SemanticMemoryEvidence(
                    candidate_id="candidate-ada", evidence_id="evidence-ada", source_path="notes/ada.md",
                    evidence_text="Ada asked to reserve mornings for writing.",
                ),),
            )
            slug = "person-ada"
            renderer = "_render_page"
        target = contract.pages_dir / f"{slug}.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        yield compiler, store, target, renderer
    finally:
        config.data = previous


def _result_for(results, page):
    return next(result for result in results if result.page_path == page)


def test_secondary_writer_cannot_adopt_existing_prose_implicitly(secondary_writer):
    compiler, store, page, _ = secondary_writer
    original = "# Existing article\n\nHuman prose must survive.\n"
    page.write_text(original)
    result = _result_for(compiler.compile(store), page)
    assert result.action == "blocked"
    assert page.read_text() == original
    assert compiler.publications.inspect(page).status == "unowned"


def test_secondary_writer_tracks_new_baseline_and_blocks_later_human_edits(secondary_writer):
    compiler, store, page, _ = secondary_writer
    assert _result_for(compiler.compile(store), page).action == "created"
    assert compiler.publications.metadata_for(page)["thoth_id"] == page.stem
    assert _result_for(compiler.compile(store), page).action == "updated"
    edited = page.read_text() + "\nAn important manual correction.\n"
    page.write_text(edited)
    assert _result_for(compiler.compile(store), page).action == "blocked"
    assert page.read_text() == edited


def test_secondary_writer_preserves_feedback_without_claiming_it_was_used(secondary_writer):
    compiler, store, page, _ = secondary_writer
    compiler.compile(store)
    raw = "> [!thoth-feedback]\n> Please research this in more depth.\n"
    page.write_text(page.read_text() + "\n" + raw)
    assert _result_for(compiler.compile(store), page).action == "updated"
    assert page.read_text().count(raw) == 1
    records = compiler.publications.feedback_records(page)
    assert records[0]["raw_text"] == raw
    assert records[0]["status"] == "pending"
    assert records[0]["included_revision"] is None


def test_secondary_writer_rechecks_page_after_render(secondary_writer, monkeypatch):
    compiler, store, page, renderer = secondary_writer
    compiler.compile(store)
    render = getattr(compiler, renderer)
    edited = page.read_text() + "\nEdited concurrently while rendering.\n"

    def edit_during_render(spec, group):
        if spec.slug == page.stem:
            page.write_text(edited)
        return render(spec, group)

    monkeypatch.setattr(compiler, renderer, edit_during_render)
    assert _result_for(compiler.compile(store), page).action == "blocked"
    assert page.read_text() == edited


def test_secondary_writer_does_not_resurrect_deleted_owned_page(secondary_writer):
    compiler, store, page, _ = secondary_writer
    compiler.compile(store)
    page.unlink()
    assert _result_for(compiler.compile(store), page).action == "blocked"
    assert not page.exists()


@pytest.mark.parametrize("edit", ["", "\nUser annotation.\n", "\n> [!thoth-feedback]\n> Keep this.\n"])
def test_stale_semantic_pages_are_reported_and_preserved(secondary_writer, edit):
    compiler, store, page, _ = secondary_writer
    if not isinstance(compiler, SemanticMemoryWikiCompiler):
        pytest.skip("Only semantic compilation has stale-page pruning")
    compiler.compile(store)
    original = page.read_text() + edit
    page.write_text(original)
    store.transition_candidate("candidate-ada", "rejected")
    assert _result_for(compiler.compile(store), page).action == "stale"
    assert page.read_text() == original


def test_unowned_stale_semantic_marker_is_not_deletion_authority(secondary_writer):
    compiler, store, page, _ = secondary_writer
    if not isinstance(compiler, SemanticMemoryWikiCompiler):
        pytest.skip("Only semantic compilation has stale-page pruning")
    store.transition_candidate("candidate-ada", "rejected")
    original = "---\nthoth_semantic_memory_page: true\n---\n# Human article\n"
    page.write_text(original)
    assert _result_for(compiler.compile(store), page).action == "stale"
    assert page.read_text() == original
