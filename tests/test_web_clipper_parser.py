from pathlib import Path

import pytest

from collectors.web_clipper_parser import (
    WebClipperFrontmatterError,
    WebClipperMarkdownError,
    parse_web_clipper_markdown,
)


def test_parse_web_clipper_markdown_extracts_frontmatter_and_body(tmp_path: Path):
    path = tmp_path / "note.md"
    text = (
        "---\n"
        "title: Example Clip\n"
        "url: https://example.com/article\n"
        "lang: en\n"
        "tags:\n"
        "  - research\n"
        "  - notes\n"
        "---\n"
        "\n"
        "# Example Clip\n"
        "\n"
        "Body text.\n"
    )

    parsed = parse_web_clipper_markdown(text, source_path=path)

    assert parsed.raw_content == text
    assert parsed.title == "Example Clip"
    assert parsed.body == "# Example Clip\n\nBody text.\n"
    assert parsed.frontmatter["url"] == "https://example.com/article"
    assert parsed.source_url == "https://example.com/article"
    assert parsed.source_language == "en"


def test_parse_web_clipper_markdown_uses_heading_fallback_for_title(tmp_path: Path):
    path = tmp_path / "clip.md"
    text = "---\nsource_url: https://example.com\n---\n\n# Heading Title\nContent.\n"

    parsed = parse_web_clipper_markdown(text, source_path=path)

    assert parsed.title == "Heading Title"
    assert parsed.source_url == "https://example.com"


def test_parse_web_clipper_markdown_normalizes_yaml_dates(tmp_path: Path):
    path = tmp_path / "clip.md"
    text = "---\npublished: 2026-04-04\n---\n\nBody\n"

    parsed = parse_web_clipper_markdown(text, source_path=path)

    assert parsed.frontmatter["published"] == "2026-04-04"


@pytest.mark.parametrize(
    "text, error_type, message",
    [
        ("# no frontmatter\n", WebClipperFrontmatterError, "Missing frontmatter"),
        (
            "---\ntitle: x\n",
            WebClipperFrontmatterError,
            "Missing closing frontmatter delimiter",
        ),
        (
            "---\n- not-a-mapping\n---\nbody\n",
            WebClipperFrontmatterError,
            "frontmatter must be a mapping",
        ),
        ("", WebClipperMarkdownError, "Empty Web Clipper note"),
    ],
)
def test_parse_web_clipper_markdown_fails_closed(
    tmp_path: Path,
    text: str,
    error_type: type[Exception],
    message: str,
):
    path = tmp_path / "note.md"

    with pytest.raises(error_type, match=message):
        parse_web_clipper_markdown(text, source_path=path)
