import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from collectors.arxiv_collector import ArXivCollector
from collectors.imported_markdown_connector import ImportedMarkdownConnector
from collectors.personal_transcript_connector import PersonalTranscriptConnector
from collectors.pi_skill_connector import PiSkillConnector
from collectors.social_collector import SocialCollector
from collectors.skill_output_connector import SkillOutputConnector
from collectors.web_clipper_collector import WebClipperCollector
from collectors.youtube_connector import YouTubeConnector
from core.connector_capture import ConnectorCaptureQueue
from core.artifacts import PaperArtifact
from core.config import Config
from core.ingestion_runtime import IngestionDispatchResult, KnowledgeArtifactRuntime
from core.metadata_db import MetadataDB
from core.path_layout import PathLayout, build_path_layout
from core.research_graph import ResearchGraphService
from core.wiki_io import read_document
from processors.youtube_processor import YouTubeVideo


class RecordingCaptureEventStore:
    def __init__(self, *, raw_roots):
        self.raw_roots = tuple(raw_roots)
        self.sources = {}
        self.sessions = {}
        self.events = {}
        self.raw_refs = {}
        self.artifact_links = {}

    def upsert_source(self, source):
        self.sources[source.source_id] = source
        return source

    def upsert_session(self, session):
        self.sessions[session.session_id] = session
        return session

    def upsert_event(self, event):
        self.events[event.event_id] = event
        return event

    def upsert_raw_ref(self, raw_ref):
        self.raw_refs[raw_ref.raw_ref_id] = raw_ref
        return raw_ref

    def upsert_artifact_link(self, link):
        self.artifact_links[link.artifact_link_id] = link
        return link


def _config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / ".thoth_system"))
    config.set("paths.cache_dir", "cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return config


def _layout_and_db(tmp_path: Path) -> tuple[Config, PathLayout, MetadataDB]:
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    return config, layout, MetadataDB(str(layout.database_path))


def _store(layout: PathLayout) -> RecordingCaptureEventStore:
    return RecordingCaptureEventStore(
        raw_roots=(layout.raw_root, layout.library_root, layout.vault_root)
    )


def _arxiv_feed_entry(arxiv_id: str):
    return SimpleNamespace(
        id=f"https://arxiv.org/abs/{arxiv_id}",
        link=f"https://arxiv.org/abs/{arxiv_id}",
        title="Capture Events for Research Connectors",
        authors=[SimpleNamespace(name="Ada")],
        summary="A paper about capture events.",
        links=[
            SimpleNamespace(
                href=f"https://arxiv.org/pdf/{arxiv_id}.pdf",
                type="application/pdf",
            )
        ],
        published="2026-04-01T00:00:00Z",
    )


def _paper(
    paper_id: str,
    *,
    title: str,
    references=None,
) -> PaperArtifact:
    return PaperArtifact(
        id=paper_id,
        source_type="arxiv",
        raw_content=json.dumps({"id": paper_id, "references": references or []}),
        title=title,
        authors=["Ada Lovelace"],
        arxiv_id=paper_id,
        pdf_url=f"https://arxiv.org/pdf/{paper_id}.pdf",
        references=references or [],
        source_provider="arxiv",
        ingested_at="2026-04-04T00:00:00",
    )


def test_web_clipper_connector_records_capture_event_links(tmp_path: Path):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("sources.web_clipper.note_dirs", ["Clippings"])
    config.set("sources.web_clipper.attachment_dirs", ["clipper-assets"])
    note_dir = layout.vault_root / "Clippings"
    asset_dir = layout.vault_root / "clipper-assets"
    note_dir.mkdir(parents=True)
    asset_dir.mkdir(parents=True)
    note_path = note_dir / "capture.md"
    note_path.write_text(
        "---\ntitle: Capture\nurl: https://example.test/capture\n---\n\n# Capture\n",
        encoding="utf-8",
    )
    event_store = _store(layout)

    collector = WebClipperCollector(
        config,
        layout=layout,
        db=db,
        capture_event_store=event_store,
    )
    records = collector.collect()

    assert records[0].artifact.id == "webclip:Clippings/capture.md"
    assert len(event_store.sources) == 1
    assert len(event_store.sessions) == 1
    assert len(event_store.events) == 1
    assert len(event_store.raw_refs) == 1
    assert len(event_store.artifact_links) == 1
    source = next(iter(event_store.sources.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    link = next(iter(event_store.artifact_links.values()))
    assert source.source_name == "web_clipper"
    assert next(iter(event_store.sessions.values())).session_type == "web_clipper_scan"
    assert event.native_event_id == "Clippings/capture.md"
    assert raw_ref.path == str(note_path.resolve())
    assert link.event_id == event.event_id
    assert link.artifact_id == "webclip:Clippings/capture.md"
    assert db.get_ingestion_entry("webclip:Clippings/capture.md") is not None


def test_arxiv_collector_records_capture_event_and_raw_feed_entry(
    monkeypatch,
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)

    monkeypatch.setattr(
        "collectors.arxiv_collector.feedparser.parse",
        lambda _url: SimpleNamespace(entries=[_arxiv_feed_entry("2604.00001")]),
    )
    collector = ArXivCollector(
        db=db,
        config=config,
        layout=layout,
        capture_event_store=event_store,
    )

    discovered = collector.discover_papers(["capture events"], max_results=1)

    assert discovered[0].id == "2604.00001"
    source = next(iter(event_store.sources.values()))
    session = next(iter(event_store.sessions.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    link = next(iter(event_store.artifact_links.values()))
    assert source.source_name == "arxiv"
    assert source.source_type == "arxiv"
    assert session.session_type == "arxiv_discovery"
    assert event.event_type == "arxiv_paper_discovered"
    assert event.native_event_id == "2604.00001"
    assert event.privacy == {"classification": "public"}
    assert Path(raw_ref.path).is_relative_to(layout.raw_root)
    assert link.artifact_id == "2604.00001"
    assert db.get_ingestion_entry("2604.00001") is not None


def test_social_collector_records_github_star_capture_event(
    monkeypatch,
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    monkeypatch.setenv("GITHUB_API", "token-123")

    collector = SocialCollector(
        db=db,
        config=config,
        layout=layout,
        capture_event_store=event_store,
    )

    def fake_get(_url, headers=None, params=None, timeout=None):
        return SimpleNamespace(
            status_code=200,
            text="[]",
            json=lambda: [
                {
                    "id": 123,
                    "full_name": "octo/repo",
                    "description": "Useful repo",
                    "stargazers_count": 42,
                    "language": "Python",
                    "topics": ["agents"],
                    "updated_at": "2026-04-03T00:00:00Z",
                    "html_url": "https://github.com/octo/repo",
                }
            ],
        )

    collector.session = SimpleNamespace(get=fake_get)

    discovered = collector.discover_github_stars(None, limit=1)

    assert discovered[0].id == "gh_123"
    source = next(iter(event_store.sources.values()))
    session = next(iter(event_store.sessions.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    assert source.source_name == "github"
    assert source.source_type == "github"
    assert session.session_type == "github_scan"
    assert event.event_type == "github_star"
    assert event.native_event_id == "octo/repo"
    assert event.privacy == {"classification": "personal"}
    assert Path(raw_ref.path).is_relative_to(layout.raw_root)
    assert db.get_ingestion_entry("gh_123") is not None


def test_duplicate_repository_connector_captures_share_canonical_wiki_page(
    monkeypatch,
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    queue = ConnectorCaptureQueue(
        config,
        layout=layout,
        db=db,
        capture_event_store=event_store,
    )

    with queue.lifecycle() as lifecycle:
        first = queue.queue_payload(
            lifecycle,
            artifact_type="repository",
            payload={
                "id": "gh_123",
                "source_type": "github",
                "repo_name": "octo/repo",
                "full_name": "octo/repo",
                "description": "Useful repo from stars.",
            },
            source={"source_name": "github", "source_type": "github"},
            event={
                "event_type": "github_star",
                "native_event_id": "octo/repo",
                "privacy": {"classification": "personal"},
            },
        )
        second = queue.queue_payload(
            lifecycle,
            artifact_type="repository",
            payload={
                "id": "manual-octo-repo",
                "source_type": "github",
                "repo_name": "octo/repo",
                "full_name": "octo/repo",
                "description": "Useful repo from manual import.",
            },
            source={"source_name": "manual_repo_import", "source_type": "github"},
            event={
                "event_type": "manual_repository_import",
                "native_event_id": "octo/repo",
                "privacy": {"classification": "personal"},
            },
        )

    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)

    async def fake_dispatch(artifact):
        return IngestionDispatchResult(
            artifact_id=artifact.id,
            artifact_type="repository",
            source=artifact.source_type,
            status="processed",
            processed_at="2026-04-04T00:00:00",
            details={"repo_name": artifact.repo_name},
        )

    monkeypatch.setattr(runtime, "dispatch_artifact", fake_dispatch)
    results = asyncio.run(runtime.process_pending_ingestions_once(limit=10))

    pages = sorted((layout.wiki_root / "pages").glob("repo-*.md"))
    document = read_document(pages[0])
    link_canonical_ids = {
        link.metadata.get("canonical_id")
        for link in event_store.artifact_links.values()
    }

    assert first.queue_artifact_id == "gh_123"
    assert second.queue_artifact_id == "manual-octo-repo"
    assert [result.status for result in results] == ["processed", "processed"]
    assert len(pages) == 1
    assert link_canonical_ids == {"repository:native_repo:github:octo:repo"}
    assert document.frontmatter["thoth_canonical_id"] == (
        "repository:native_repo:github:octo:repo"
    )
    assert document.frontmatter["thoth_artifact_id"] == "gh_123"
    assert document.frontmatter["thoth_artifact_ids"] == [
        "gh_123",
        "manual-octo-repo",
    ]
    assert len(document.frontmatter["thoth_event_ids"]) == 2


def test_social_collector_records_huggingface_like_capture_event(
    monkeypatch,
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("sources.huggingface.username", "example-user")
    event_store = _store(layout)

    fake_module = SimpleNamespace(
        list_liked_repos=lambda user, token=None: SimpleNamespace(
            models=["org/model-a"],
            datasets=[],
            spaces=[],
        ),
        repo_info=lambda repo_id, repo_type=None, token=None: SimpleNamespace(
            id=repo_id,
            description="Model description",
            likes=10,
            pipeline_tag="text-generation",
            library_name="transformers",
            tags=["model", "ai"],
            created_at=None,
        ),
    )
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)
    collector = SocialCollector(
        db=db,
        config=config,
        layout=layout,
        capture_event_store=event_store,
    )

    discovered = collector.discover_hf_likes(None, limit=1)

    assert discovered[0].id == "hf_model_org_model-a"
    source = next(iter(event_store.sources.values()))
    session = next(iter(event_store.sessions.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    assert source.source_name == "huggingface"
    assert source.source_type == "huggingface"
    assert session.session_type == "huggingface_scan"
    assert event.event_type == "huggingface_like"
    assert event.native_event_id == "org/model-a"
    assert event.privacy == {"classification": "public"}
    assert Path(raw_ref.path).is_relative_to(layout.raw_root)
    assert db.get_ingestion_entry("hf_model_org_model-a") is not None


def test_research_graph_missing_paper_queue_records_capture_event(tmp_path: Path):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    service = ResearchGraphService(
        db,
        config=config,
        layout=layout,
        capture_event_store=event_store,
    )
    shared_reference = {
        "title": "Shared Missing Paper",
        "arxiv_id": "2501.12345",
        "pdf_url": "https://arxiv.org/pdf/2501.12345.pdf",
    }

    service.record_paper_artifact(
        _paper("2401.00001", title="First Paper", references=[shared_reference]),
        queue_missing=False,
    )
    service.record_paper_artifact(
        _paper("2401.00002", title="Second Paper", references=[shared_reference]),
        queue_missing=False,
    )
    queued = service.queue_high_confidence_missing_papers(min_references=2)

    assert queued["queued"] == ["research_graph:arxiv:2501.12345"]
    source = next(iter(event_store.sources.values()))
    session = next(iter(event_store.sessions.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    assert source.source_name == "research_graph"
    assert source.source_type == "research_graph"
    assert session.session_type == "research_graph_missing_papers"
    assert event.event_type == "research_graph_missing_paper"
    assert event.native_event_id == "arxiv:2501.12345"
    assert event.privacy == {"classification": "public"}
    assert Path(raw_ref.path).is_relative_to(layout.raw_root)
    assert db.get_ingestion_entry("research_graph:arxiv:2501.12345") is not None


class FakeYouTubeProcessor:
    def __init__(self, transcripts_dir: Path):
        self.transcripts_dir = transcripts_dir
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)

    def extract_video_id(self, url: str) -> str | None:
        return url.rsplit("/", 1)[-1].split("?", 1)[0]

    def find_existing_transcript_files(self, video_id: str) -> list[Path]:
        return sorted(self.transcripts_dir.glob(f"youtube_{video_id}_*.md"))

    async def process_video(self, video_id: str, **_kwargs):
        transcript_path = self.transcripts_dir / f"youtube_{video_id}_Fixture.md"
        transcript_path.write_text("# Fixture\n\nTranscript text.\n", encoding="utf-8")
        return (
            YouTubeVideo(
                video_id=video_id,
                title="Fixture",
                description="Fixture video",
                published_at="2026-04-04T00:00:00Z",
                channel_id="channel-1",
                channel_title="Channel",
                transcript="Raw transcript",
                formatted_transcript="Transcript text.",
                transcript_summary="Summary",
            ),
            {},
        )


def test_youtube_connector_records_video_and_transcript_capture_events(tmp_path: Path):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    connector = YouTubeConnector(
        config,
        layout=layout,
        db=db,
        processor=FakeYouTubeProcessor(layout.vault_root / "transcripts"),
        capture_event_store=event_store,
    )

    result = asyncio.run(connector.collect(urls=["https://youtu.be/abc123"]))

    assert result.records[0].video_artifact_id == "yt_video_abc123"
    assert {event.event_type for event in event_store.events.values()} == {
        "youtube_transcript",
        "youtube_video",
    }
    assert len(event_store.sessions) == 1
    assert len(event_store.raw_refs) == 1
    assert len(event_store.artifact_links) == 2
    assert next(iter(event_store.raw_refs.values())).path == str(
        (layout.raw_root / "youtube" / "abc123.json").resolve()
    )
    assert db.get_ingestion_entry("yt_video_abc123") is not None
    assert db.get_ingestion_entry("yt_transcript_abc123") is not None


def test_skill_output_connector_records_raw_envelope_capture_event(tmp_path: Path):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    output_path = tmp_path / "skill-output.json"
    output_path.write_text(
        json.dumps(
            {
                "artifact_type": "transcript",
                "artifact_id": "skill-note",
                "source_name": "local-skill",
                "payload": {"title": "Skill Note", "raw_transcript": "Skill text"},
            }
        ),
        encoding="utf-8",
    )
    connector = SkillOutputConnector(
        config,
        layout=layout,
        db=db,
        capture_event_store=event_store,
    )

    result = asyncio.run(connector.collect(output_paths=[output_path]))

    assert result.records[0].artifact_id == "skill-note"
    source = next(iter(event_store.sources.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    link = next(iter(event_store.artifact_links.values()))
    assert source.source_name == "local-skill"
    assert source.source_type == "skill_output"
    assert event.event_type == "skill_output_artifact"
    assert event.native_event_id == "skill-note"
    assert event.privacy == {
        "classification": "operator_supplied",
        "privacy_class": "operator_supplied",
    }
    assert event.retention["retention_class"] == "skill_output"
    assert event.provenance["capture_run_id"]
    assert event.provenance["security_policy"] == "prompt_security_scan_on_queue"
    assert next(iter(event_store.sessions.values())).metadata["capture_run_id"]
    assert raw_ref.path == str(result.records[0].raw_output_path.resolve())
    assert link.artifact_id == "skill-note"


def test_pi_skill_outputs_record_pi_capture_provenance(
    monkeypatch,
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    config.set(
        "llm.providers.pi",
        {
            "enabled": True,
            "type": "pi",
            "command": "pi",
            "models": {"archivist_agent": {"id": "glm-5.2"}},
        },
    )
    config.set(
        "sources.pi_skills",
        {
            "enabled": True,
            "output_dir": str(tmp_path / "pi-output"),
            "default_provider": "pi",
            "default_model": "archivist_agent",
            "skills": [
                {
                    "id": "collect-notes",
                    "artifact_types": ["transcript"],
                    "inputs": ["operator_prompt"],
                    "outputs": ["skill_output_envelopes", "artifact_queue:transcript"],
                    "auth": ["llm.providers.pi"],
                    "safety_mode": "no_tools_json",
                    "queue_behavior": "queues_artifacts",
                    "allowed_side_effects": ["llm_api_call", "local_file_write"],
                    "source_name": "pi_skill:collect-notes",
                    "prompt": "Collect notes.",
                }
            ],
        },
    )

    class FakeLLMInterface:
        def __init__(self, _config):
            self.providers = {"pi": object()}

        async def generate(self, *_args, **_kwargs):
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "artifact_type": "transcript",
                        "artifact_id": "pi-note",
                        "payload": {"title": "Pi Note", "raw_transcript": "Pi text"},
                    }
                ),
                error=None,
            )

    monkeypatch.setattr("collectors.pi_skill_connector.LLMInterface", FakeLLMInterface)
    event_store = _store(layout)
    connector = PiSkillConnector(config, layout=layout, db=db)
    connector_output = SkillOutputConnector(
        config,
        layout=layout,
        db=db,
        capture_event_store=event_store,
        collector_name="pi_skill_connector",
    )
    monkeypatch.setattr(
        "collectors.pi_skill_connector.SkillOutputConnector",
        lambda *_args, **_kwargs: connector_output,
    )

    result = asyncio.run(connector.collect(skill_id="collect-notes"))

    assert result.skill_output["queued_count"] == 1
    source = next(iter(event_store.sources.values()))
    event = next(iter(event_store.events.values()))
    assert source.source_name == "pi_skill:collect-notes"
    assert source.source_type == "pi_skill"
    assert event.provenance["collector"] == "pi_skill_connector"
    assert db.get_ingestion_entry("pi-note") is not None


def test_personal_transcript_connector_records_omi_session_capture(
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    export_path = tmp_path / "omi-export.json"
    export_path.write_text(
        json.dumps(
            {
                "id": "session-1",
                "title": "Omi Session",
                "device_id": "omi-device",
                "started_at": "2026-04-04T10:00:00Z",
                "summary": "Session summary.",
                "segments": [{"speaker": "Ada", "text": "Transcript text."}],
            }
        ),
        encoding="utf-8",
    )
    connector = PersonalTranscriptConnector(
        config,
        layout=layout,
        db=db,
        capture_event_store=event_store,
    )

    result = asyncio.run(connector.collect(export_paths=[export_path]))

    assert result.records[0].artifact_id == "omi_transcript_session-1"
    source = next(iter(event_store.sources.values()))
    session = next(iter(event_store.sessions.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    link = next(iter(event_store.artifact_links.values()))
    assert source.source_name == "omi"
    assert source.source_type == "personal_transcript"
    assert session.native_session_id == "session-1"
    assert session.metadata["capture_run_id"]
    assert event.native_event_id == "session-1"
    assert event.privacy["classification"] == "personal"
    assert event.privacy["privacy_class"] == "personal"
    assert event.retention["retention_class"] == "personal_export"
    assert event.provenance["security_policy"] == "prompt_security_scan_on_queue"
    assert raw_ref.path == str(result.records[0].raw_export_path.resolve())
    assert link.artifact_id == "omi_transcript_session-1"


def test_imported_markdown_connector_records_capture_event_metadata(
    tmp_path: Path,
):
    config, layout, db = _layout_and_db(tmp_path)
    event_store = _store(layout)
    import_path = tmp_path / "manual-note.md"
    import_path.write_text(
        "\n".join(
            [
                "---",
                "title: Imported Markdown Note",
                "source: manual_import",
                'created: "2026-04-04T13:00:00Z"',
                "tags:",
                "  - imported-markdown",
                "---",
                "",
                "# Imported Markdown Note",
                "",
                "Capture this markdown note.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    connector = ImportedMarkdownConnector(
        config,
        layout=layout,
        db=db,
        capture_event_store=event_store,
    )

    result = asyncio.run(connector.collect(import_paths=[import_path]))

    assert result.records[0].artifact_id == "manual-imported-markdown-note"
    source = next(iter(event_store.sources.values()))
    session = next(iter(event_store.sessions.values()))
    event = next(iter(event_store.events.values()))
    raw_ref = next(iter(event_store.raw_refs.values()))
    link = next(iter(event_store.artifact_links.values()))
    assert source.source_name == "manual_import"
    assert source.source_type == "imported_markdown"
    assert session.session_type == "imported_markdown_import"
    assert session.metadata["capture_run_id"]
    assert event.event_type == "imported_markdown_note"
    assert event.native_event_id == str(import_path.resolve())
    assert event.privacy["classification"] == "personal"
    assert event.retention["retention_class"] == "imported_markdown"
    assert event.provenance["security_policy"] == "prompt_security_scan_on_queue"
    assert raw_ref.path == str(result.records[0].raw_markdown_path.resolve())
    assert Path(raw_ref.path).is_relative_to(layout.raw_root)
    assert link.artifact_id == "manual-imported-markdown-note"
    entry = db.get_ingestion_entry("manual-imported-markdown-note")
    assert entry is not None
    payload = json.loads(entry.payload_json)
    assert payload["normalized_metadata"]["capture_event_id"] == event.event_id
