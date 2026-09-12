from collections import Counter
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import shutil
import subprocess
from xml.etree.ElementTree import Element, SubElement

import pytest


class SettingsMarkup(HTMLParser):
    """Parse actual element ancestry, including markup rendered by the topic UI."""

    def __init__(self, html):
        super().__init__()
        self.root = Element("document")
        self.stack = [self.root]
        self.feed(html)
        assert self.stack == [self.root], "Unclosed settings elements"

    def handle_starttag(self, tag, attrs):
        element = SubElement(self.stack[-1], tag, dict(attrs))
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input",
                       "link", "meta", "param", "source", "track", "wbr"}:
            self.stack.append(element)

    def handle_endtag(self, tag):
        assert self.stack[-1].tag == tag, f"Mismatched settings closing tag: {tag}"
        self.stack.pop()

    def handle_data(self, data):
        self.stack[-1].text = (self.stack[-1].text or "") + data


def settings_html():
    return (Path(__file__).resolve().parents[1] / "static/settings.html").read_text()


def element_ids(element):
    return {node.attrib["id"] for node in element.iter() if "id" in node.attrib}


def disclosure_title(element):
    return " ".join("".join(element.find("summary").itertext()).split())


def test_x_sync_message_uses_backfill_count_and_distinguishes_processing():
    html = (Path(__file__).resolve().parents[1] / 'static/settings.html').read_text()
    function = html.split('async function runXApiSync()', 1)[1].split('async function loadArchivistRegistry()', 1)[0]
    assert 'result.backfill?.bookmarks_emitted' in function
    assert 'result.queued ?? 0' in function
    assert 'queued for background processing' in function
    assert 'result.backfill?.checkpoint?.pagination_token' in function
    assert 'result.bookmarks_emitted' not in function


def test_review_is_a_settings_tab_using_shared_components():
    root = Path(__file__).resolve().parents[1]
    html = (root / 'static/settings.html').read_text()
    assert 'data-tab="review"' in html
    assert 'id="review" class="tab-content"' in html
    assert 'href="/review"' not in html
    assert 'type="module" src="/static/review.js' in html
    assert 'href="/static/review.css?v=3"' in html
    assert 'src="/static/review.js?v=3"' in html
    panel = (root / 'static/review-panel.html').read_text()
    assert '<html' not in panel and '<body' not in panel
    assert 'class="card"' in panel
    assert 'class="btn btn-primary"' in panel
    css = (root / 'static/review.css').read_text()
    assert ':root' not in css
    assert 'var(--bg-card)' in css
    assert 'var(--border)' in css


def test_settings_ui_exposes_archivist_web_clipper_and_translation_controls():
    html = (Path(__file__).resolve().parents[1] / "static" / "settings.html").read_text(
        encoding="utf-8"
    )

    expected_snippets = [
        'id="task-translation-enabled"',
        'id="task-translation-provider"',
        'id="task-translation-model"',
        'id="task-embedding-enabled"',
        'id="task-embedding-provider"',
        'id="task-embedding-model"',
        'id="task-archivist-enabled"',
        'id="task-archivist-provider"',
        'id="task-archivist-model"',
        'id="task-transcript-model"',
        'id="task-transcript-retry-hours"',
        'id="source-web-clipper-enabled"',
        'id="source-web-clipper-note-dirs"',
        'id="source-web-clipper-attachment-dirs"',
        'id="source-pi-skills-enabled"',
        'id="source-pi-skills-default-provider"',
        'id="source-pi-skills-default-model"',
        'id="source-pi-skills-output-dir"',
        'id="source-pi-skills-run-skill"',
        'id="source-pi-skills-prompt"',
        'id="source-pi-skills-input-paths"',
        'id="overview-providers"',
        'id="overview-connectors"',
        'id="overview-skills"',
        'id="overview-okf"',
        'id="overview-what-happened"',
        'id="overview-what-stuck"',
        'id="overview-run-next"',
        'id="health-sources"',
        'id="health-events"',
        'id="health-queues"',
        'id="health-stale-pages"',
        'id="health-compiler-runs"',
        'id="health-stuck"',
        'id="health-lineage-pages"',
        'id="health-lineage-files"',
        'id="health-llm-cost"',
        'id="health-llm-calls"',
        'id="health-source-table"',
        'id="health-sessions-table"',
        'id="health-compiler-list"',
        'id="health-stuck-list"',
        'id="health-lineage-list"',
        'id="health-llm-expensive-runs"',
        'id="health-llm-source-totals"',
        'id="health-llm-task-totals"',
        'id="health-errors"',
        'id="automation-archivist-enabled"',
        'id="automation-archivist-run-on-startup"',
        'id="automation-archivist-interval-hours"',
        'id="path-wiki"',
        'id="path-system"',
        'id="path-archivist-topics"',
        'id="runtime-archivist-registry"',
        'id="runtime-archivist-corpus"',
        'id="runtime-connectors-summary"',
        'id="runtime-config-groups"',
        'data-tab="archivist"',
        'id="archivist-route-summary"',
        'id="archivist-registry-path"',
        'id="archivist-registry-editor"',
        'id="archivist-topic-count"',
        'id="archivist-corpus-summary"',
        'id="archivist-topics-list"',
        'id="runtime-web-clipper-watch-dirs"',
        'data-tab="stats">Overview</button>',
        'data-tab="pipeline">Sources &amp; Skills</button>',
        'data-tab="archivist">Wiki &amp; Archivist</button>',
        'data-tab="security">Security</button>',
        'data-tab="paths">Advanced</button>',
        '<h2 class="card-title">Advanced: Model Providers</h2>',
        '<h2 class="card-title">Advanced: Task Routing</h2>',
        'id="security-auth-summary"',
        'id="security-prompt-summary"',
        'id="security-side-effects"',
        'id="security-dashboard-stats"',
        'id="security-artifacts-total"',
        'id="security-findings-by-source"',
        'id="security-quarantine-list"',
        'id="security-redactions"',
        'id="security-strict-failures"',
        'id="security-okf-lint-button"',
        'id="security-security-lint-button"',
        'id="security-okf-lint-download"',
        'id="security-security-lint-download"',
        'id="security-lint-result"',
        "const TASK_ROUTE_KEYS = ['tags', 'summary', 'alt_text', 'transcript', 'translation', 'embedding', 'archivist'];",
        "function showTab(tabName)",
        "function renderProviderModels(name, provider)",
        "function updateTaskModelDropdown(task)",
        "function formatConnectorRuntimeSummary(group)",
        "function formatConnectorBudgetSummary(budgets)",
        "function formatSecurityRuntimeSummary(security)",
        "function renderSecurityDashboard(security)",
        "function renderAdminStatusDashboard(status)",
        "function renderHealthStuck(rows)",
        "function renderLineagePages(lineage)",
        "function renderLLMExpensiveRuns(rows)",
        "function renderLLMTotals(rows, key, emptyText)",
        "async function runSettingsLint(kind)",
        "function formatGroupedRuntimeSummary(groups)",
        "function runPiSkill(options = {})",
        "function updateOverviewRuntime(groups)",
        "function renderPiSkillOptions(piSkills)",
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


def test_settings_ui_bounds_long_status_tables_and_topics():
    root = Path(__file__).resolve().parents[1]
    html = (root / "static/settings.html").read_text(encoding="utf-8")

    assert '.security-table-wrap {' in html
    assert 'max-height: 22rem;' in html
    assert 'position: sticky;' in html
    assert '.archivist-topic-list {' in html
    assert 'max-height: 60vh;' in html
    assert 'overscroll-behavior: contain;' in html


def test_source_settings_have_one_copy_in_their_own_disclosure():
    root = SettingsMarkup(settings_html()).root
    ids = Counter(node.attrib["id"] for node in root.iter() if "id" in node.attrib)
    assert all(count == 1 for count in ids.values())
    pipeline = root.find(".//*[@id='pipeline']")
    groups = {
        disclosure_title(group): group
        for group in pipeline.findall(".//details[@name='source-settings']")
    }
    expected_prefixes = {
        "ArXiv Discovery": ("source-arxiv-",),
        "Vault Markdown Imports": ("source-web-clipper-",),
        "Pi Skills": ("source-pi-skills-",),
        "GitHub & HuggingFace": ("source-github-", "source-hf-", "social-github-", "social-hf-"),
        "X API Bookmarks": ("source-x-api-", "social-x-api-", "x-api-auth-"),
        "Automation & Schedules": ("automation-",),
    }
    assert groups.keys() == expected_prefixes.keys()
    for title, prefixes in expected_prefixes.items():
        group = groups[title]
        assert "open" not in group.attrib
        assert group[0].tag == "summary"
        assert group.find("./div[@class='settings-section-body']") is not None
        expected = {field_id for field_id in ids if field_id.startswith(prefixes)}
        assert expected
        assert element_ids(group) == expected
    assert {"source-hf-models", "source-hf-datasets", "source-hf-spaces"} <= element_ids(
        groups["GitHub & HuggingFace"]
    )
    assert len(pipeline.findall(".//button[@onclick='saveSources()']")) == 1
    assert all(not group.findall(".//button[@onclick='saveSources()']") for group in groups.values())


def test_skill_runner_defaults_fold_without_hiding_run_controls():
    root = SettingsMarkup(settings_html()).root
    skill_group = next(
        group for group in root.findall(".//details[@name='source-settings']")
        if disclosure_title(group) == "Pi Skills"
    )
    defaults = skill_group.find(".//details")
    assert disclosure_title(defaults) == "Runner Defaults & Output"
    assert "open" not in defaults.attrib
    assert element_ids(defaults) == {
        "source-pi-skills-default-provider", "source-pi-skills-default-model",
        "source-pi-skills-output-dir",
    }
    assert not defaults.findall(".//button")
    assert {button.attrib["onclick"] for button in skill_group.findall(".//button")} == {
        "runPiSkill({ dryRun: true })", "runPiSkill({ execute: true })",
    }


def test_advanced_groups_keep_controls_and_save_actions_accessible():
    root = SettingsMarkup(settings_html()).root
    expected = [
        ("providers", "Advanced: Model Providers", "saveProviders()", "providers-list"),
        ("tasks", "Advanced: Task Routing", "saveTasks()", "task-tags-enabled"),
        ("pipeline", "Pipeline Stages", "savePipeline()", "enable-llm"),
        ("paths", "File Paths", "savePaths()", "path-vault"),
    ]
    for tab_id, title, handler, control in expected:
        group = root.find(f".//*[@id='{tab_id}']/details")
        assert disclosure_title(group).startswith(title)
        assert "open" not in group.attrib
        assert control in element_ids(group)
        assert group.find(f"./div[@class='btn-group']/button[@onclick='{handler}']") is not None
        assert group.find(f".//div[@class='settings-section-body']//button[@onclick='{handler}']") is None
    diagnostics = root.findall(".//*[@id='paths']/details")[1]
    assert disclosure_title(diagnostics).startswith("Resolved Runtime Layout")
    assert "open" not in diagnostics.attrib
    assert len(element_ids(diagnostics)) == 8
    assert all("readonly" in field.attrib for field in diagnostics.iter() if "id" in field.attrib)
    assert "? ['providers', 'tasks', 'paths']" in settings_html()


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is required to exercise the topic renderer")
def test_rendered_topics_fold_all_filters_and_keep_actions_outside():
    html = settings_html()
    helpers = html[html.index("function formatOptionalDate("):html.index("function renderArchivistRegistry(")]
    escape = html[html.index("function escapeHtml("):html.index("function ensureProviderModels(")]
    topics = [
        {
            "id": "topic-1", "title": 'Security <papers> & "preprints"',
            "include_roots": ["papers/security"], "exclude_roots": ["drafts"],
            "source_types": ["pdf"], "include_tags": ["research"], "exclude_tags": ["draft"],
            "include_terms": ["attack"], "exclude_terms": ["spam"],
            "retrieval": {"source_type_weights": {"pdf": 2}},
            "allow_manual_force": True,
        },
        {"id": "topic-2", "title": "Automatic only", "allow_manual_force": False},
        {"id": "topic-3", "title": "State unavailable", "allow_manual_force": True,
         "state_error": "Unable to read topic state"},
    ]
    script = "const container = {}; const document = {getElementById: () => container};\n"
    script += escape + helpers
    script += f"\nrenderArchivistTopics({json.dumps(topics)}); process.stdout.write(container.innerHTML);"
    rendered = subprocess.run(
        ["node", "-e", script], check=True, capture_output=True, text=True,
    ).stdout
    root = SettingsMarkup(rendered).root
    cards = root.findall("article")
    assert len(cards) == 3
    for card, topic in zip(cards, topics):
        main = card.find("./div[@class='archivist-topic-main']")
        assert main.find("h3").text == topic["title"]
        details = main.find("details")
        assert "open" not in details.attrib
        assert disclosure_title(details) == "Filters & weights"
        panel = details.find("./div[@class='archivist-topic-detail-panel']")
        assert panel.attrib["tabindex"] == "0"
        assert panel.attrib["aria-label"] == f"Filters and weights for {topic['title']}"
        assert [label.text for label in panel.findall("./div[@class='archivist-section-label']")] == [
            "Include Roots", "Exclude Roots", "Source Types", "Include Tags", "Exclude Tags",
            "Include Terms", "Exclude Terms", "Retrieval Policy", "Source Type Weights",
        ]
        assert not details.findall(".//button")
        handlers = [f"runArchivistTopic('{topic['id']}')"]
        if topic["allow_manual_force"]:
            handlers += [f"queueArchivistForce('{topic['id']}')", f"clearArchivistForce('{topic['id']}')"]
        assert [button.attrib["onclick"] for button in card.findall("./div[@class='btn-group']/button")] == handlers
        state = main.find("./div[@class='archivist-state-box error-state']")
        if "state_error" in topic:
            assert state.text == topic["state_error"]
        else:
            assert "Force Queued" in "".join(main.find("./div[@class='archivist-state-box']").itertext())
    assert "pdf:2" in "".join(cards[0].itertext())


def test_topic_details_are_out_of_flow_and_scroll_within_the_card():
    html = settings_html()
    css = html.split("<style>", 1)[1].split("</style>", 1)[0]
    rules = dict(re.findall(r"(\.[\w-]+)\s*\{([^}]+)\}", css))
    assert "position: relative;" in rules[".archivist-topic-main"]
    panel = rules[".archivist-topic-detail-panel"]
    assert "position: absolute;" in panel
    assert "inset: 0 0 2.25rem;" in panel
    assert "overflow: auto;" in panel
    assert "overscroll-behavior: contain;" in panel
    settings_body = rules[".settings-section-body"]
    assert "max-height: min(32rem, 60vh);" in settings_body
    assert "overflow: auto;" in settings_body
