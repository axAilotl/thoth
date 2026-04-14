"""
Tests for ArXiv collection and metadata parsing.
"""

from types import SimpleNamespace

from collectors.arxiv_collector import ArXivCollector
from processors.arxiv_processor_v2 import ArXivProcessorV2


class FakeDB:
    def __init__(self):
        self.entries = []
        self.existing = {}

    def get_ingestion_entry(self, artifact_id):
        return self.existing.get(artifact_id)

    def upsert_ingestion_entry(self, entry):
        self.entries.append(entry)
        return True


def make_feed_entry(arxiv_id, include_pdf_link=True):
    links = [SimpleNamespace(href=f"https://arxiv.org/abs/{arxiv_id}", type="text/html")]
    if include_pdf_link:
        links.append(
            SimpleNamespace(href=f"https://arxiv.org/pdf/{arxiv_id}.pdf", type="application/pdf")
        )

    return SimpleNamespace(
        id=f"https://arxiv.org/abs/{arxiv_id}",
        link=f"https://arxiv.org/abs/{arxiv_id}",
        title="A Useful\nPaper",
        authors=[SimpleNamespace(name="Alice"), SimpleNamespace(name="Bob")],
        summary="An abstract\nwith line breaks.",
        links=links,
        published="2026-04-01T00:00:00Z",
    )


def test_arxiv_api_discovery_uses_query_endpoint(monkeypatch):
    db = FakeDB()
    collector = ArXivCollector(db=db)
    called_urls = []

    def fake_parse(url):
        called_urls.append(url)
        return SimpleNamespace(entries=[make_feed_entry("2604.00001")])

    monkeypatch.setattr("collectors.arxiv_collector.feedparser.parse", fake_parse)

    discovered = collector.discover_papers(["agentic ai"], max_results=5)

    assert len(discovered) == 1
    assert called_urls == [
        "https://export.arxiv.org/api/query?search_query=all%3A%22agentic+ai%22&start=0&max_results=5&sortBy=submittedDate&sortOrder=descending"
    ]
    assert discovered[0].arxiv_id == "2604.00001"
    assert discovered[0].authors == ["Alice", "Bob"]
    assert db.entries[0].source == "arxiv"


def test_arxiv_rss_scan_uses_category_feed_and_derives_pdf(monkeypatch):
    db = FakeDB()
    collector = ArXivCollector(db=db)
    called_urls = []

    def fake_parse(url):
        called_urls.append(url)
        return SimpleNamespace(entries=[make_feed_entry("2604.00002", include_pdf_link=False)])

    monkeypatch.setattr("collectors.arxiv_collector.feedparser.parse", fake_parse)

    discovered = collector.scan_rss_feeds(["cs.AI", "cs.LG+stat.ML"], max_results=10)

    assert len(discovered) == 2
    assert called_urls == [
        "https://rss.arxiv.org/rss/cs.AI",
        "https://rss.arxiv.org/rss/cs.LG+stat.ML",
    ]
    assert discovered[0].pdf_url == "https://arxiv.org/pdf/2604.00002.pdf"
    assert db.entries[0].source == "arxiv_rss"


def test_arxiv_processor_parses_entry_metadata_not_feed_metadata(tmp_path):
    processor = ArXivProcessorV2(output_dir=str(tmp_path))

    class FakeResponse:
        text = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query: id_list=2604.00003</title>
  <entry>
    <id>http://arxiv.org/abs/2604.00003v1</id>
    <updated>2026-04-01T00:00:00Z</updated>
    <published>2026-04-01T00:00:00Z</published>
    <title>Entry Title</title>
    <summary>Entry Summary</summary>
    <author><name>Alice</name></author>
    <author><name>Bob</name></author>
    <category term="cs.AI" />
    <category term="cs.LG" />
    <arxiv:primary_category term="cs.AI" />
  </entry>
</feed>
"""

        def raise_for_status(self):
            return None

    processor.session = SimpleNamespace(get=lambda *args, **kwargs: FakeResponse())

    metadata = processor._fetch_arxiv_metadata("2604.00003")

    assert metadata == {
        "title": "Entry Title",
        "abstract": "Entry Summary",
        "authors": ["Alice", "Bob"],
        "categories": ["cs.AI", "cs.LG"],
    }


def test_arxiv_processor_renames_legacy_query_title_filename(tmp_path):
    processor = ArXivProcessorV2(output_dir=str(tmp_path))
    legacy_path = (
        tmp_path
        / "papers"
        / "2512.08296-arxiv-query-search-query-amp-id-list-2512-08296-amp-start-0-amp-max-results-10.pdf"
    )
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_path.write_bytes(b"%PDF-1.4 legacy")

    processor._fetch_arxiv_metadata = lambda *args, **kwargs: None
    processor._extract_title_from_pdf = (
        lambda path: "Towards a Science of Scaling Agent Systems"
    )
    download_attempts = []
    processor._download_file = lambda *args, **kwargs: download_attempts.append(args) or True

    paper = processor.download_document(
        "https://arxiv.org/abs/2512.08296",
        "tweet-1",
        resume=True,
    )

    expected_name = "2512.08296-towards-a-science-of-scaling-agent-systems.pdf"
    expected_path = tmp_path / "papers" / expected_name
    assert paper is not None
    assert paper.filename == expected_name
    assert paper.downloaded is True
    assert expected_path.exists()
    assert not legacy_path.exists()
    assert download_attempts == []


def test_arxiv_processor_fresh_download_uses_metadata_title_not_pdf_fallback(tmp_path):
    processor = ArXivProcessorV2(output_dir=str(tmp_path))
    processor._fetch_arxiv_metadata = lambda *args, **kwargs: {
        "title": "Metadata Title",
        "abstract": "",
        "authors": [],
        "categories": [],
    }
    processor._extract_title_from_pdf = lambda path: "PDF Title"

    def fake_download(url, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4 fresh")
        return True

    processor._download_file = fake_download

    paper = processor.download_document(
        "https://arxiv.org/abs/2604.12345",
        "tweet-2",
        resume=False,
    )

    expected_name = "2604.12345-metadata-title.pdf"
    assert paper is not None
    assert paper.title == "Metadata Title"
    assert paper.filename == expected_name
    assert (tmp_path / "papers" / expected_name).exists()
