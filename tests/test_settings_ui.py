from pathlib import Path


def test_settings_ui_exposes_archivist_web_clipper_and_translation_controls():
    html = (Path(__file__).resolve().parents[1] / "static" / "settings.html").read_text(
        encoding="utf-8"
    )

    expected_snippets = [
        'id="task-translation-enabled"',
        'id="task-translation-provider"',
        'id="task-translation-model"',
        'id="task-archivist-enabled"',
        'id="task-archivist-provider"',
        'id="task-archivist-model"',
        'id="task-transcript-model"',
        'id="task-transcript-retry-hours"',
        'id="source-web-clipper-enabled"',
        'id="source-web-clipper-note-dirs"',
        'id="source-web-clipper-attachment-dirs"',
        'id="automation-archivist-enabled"',
        'id="automation-archivist-run-on-startup"',
        'id="automation-archivist-interval-hours"',
        'id="path-raw"',
        'id="path-library"',
        'id="path-wiki"',
        'id="path-system"',
        'id="path-archivist-topics"',
        'id="runtime-archivist-registry"',
        'data-tab="archivist"',
        'id="archivist-route-summary"',
        'id="archivist-registry-path"',
        'id="archivist-registry-editor"',
        'id="archivist-topic-count"',
        'id="archivist-topics-list"',
        'id="runtime-web-clipper-watch-dirs"',
        "const TASK_ROUTE_KEYS = ['tags', 'summary', 'alt_text', 'transcript', 'translation', 'archivist'];",
        "function renderProviderModels(name, provider)",
        "function updateTaskModelDropdown(task)",
        "async function loadArchivistRegistry()",
        "async function saveArchivistRegistry()",
        "async function runArchivistTopics(options = {})",
        "async function runArchivistTopic(topicId, options = {})",
        "async function queueArchivistForce(topicId)",
        "async function clearArchivistForce(topicId)",
        'href="/static/thoth.png"',
        'src="/static/thoth.png"',
        "'anthropic': 'ANTHROPIC_API'",
        "'openrouter': 'OPEN_ROUTER_API_KEY'",
    ]

    for snippet in expected_snippets:
        assert snippet in html
