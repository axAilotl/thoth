"""Obsidian markdown note parsing: YAML frontmatter and wikilinks.

A note is parsed into its frontmatter mapping, body text, title, and
every ``[[wikilink]]`` / ``![[embed]]`` reference. Parsing fails closed:
a malformed frontmatter block raises :class:`NoteParseError` and the
importer skips the note (recording path + reason) instead of guessing at
the content. Notes without frontmatter are legal Obsidian and parse with
an empty mapping.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import yaml


class NoteParseError(ValueError):
    """Raised when a note cannot be parsed safely (malformed document)."""


class _FrontmatterLoader(yaml.SafeLoader):
    """SafeLoader that refuses anchors and aliases outright.

    YAML alias expansion is a billion-laughs vector: a few hundred bytes
    of nested anchors expand to gigabytes of composed nodes and minutes of
    CPU. Obsidian frontmatter has no legitimate need for either, so both
    fail closed as malformed documents.
    """

    def fetch_anchor(self):
        raise yaml.YAMLError("YAML anchors are not allowed in note frontmatter")

    def fetch_alias(self):
        raise yaml.YAMLError("YAML aliases are not allowed in note frontmatter")


@dataclass(frozen=True)
class NoteLink:
    """One ``[[...]]`` reference inside a note body or frontmatter."""

    target: str  # raw target text, heading/alias stripped
    embed: bool  # True for ![[...]] embeds


@dataclass
class ParsedNote:
    """Parsed view of one Obsidian markdown note."""

    frontmatter: dict
    body: str
    title: str
    links: list[NoteLink] = field(default_factory=list)
    excerpt: str = ""


_WIKILINK_RE = re.compile(r"(!?)\[\[([^\[\]|#]+)(?:#[^\[\]|]*)?(?:\|[^\[\]]*)?\]\]")
_FRONTMATTER_RE = re.compile(r"\A---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|\Z)", re.DOTALL)
_HEADING_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)

# Frontmatter scalar/list values may themselves contain [[wikilinks]]
# (Obsidian link-style frontmatter). They count as references too.
_MAX_TITLE = 200
_MAX_EXCERPT = 2000


def _frontmatter_links(value: object, embed: bool = False) -> list[NoteLink]:
    links: list[NoteLink] = []
    if isinstance(value, str):
        links.extend(
            NoteLink(target=match.group(2).strip(), embed=bool(match.group(1)))
            for match in _WIKILINK_RE.finditer(value)
        )
    elif isinstance(value, list):
        for item in value:
            links.extend(_frontmatter_links(item, embed=embed))
    elif isinstance(value, dict):
        for item in value.values():
            links.extend(_frontmatter_links(item, embed=embed))
    return links


def parse_note(text: str, *, fallback_title: str) -> ParsedNote:
    """Parse one note's markdown text. Fails closed on bad frontmatter.

    ``fallback_title`` (usually the file stem) is used when neither the
    frontmatter ``title`` nor a top-level heading names the note.
    """
    if not isinstance(text, str):
        raise NoteParseError(f"note text must be str, got {type(text).__name__}")

    frontmatter: dict = {}
    body = text
    if text.startswith("---"):
        match = _FRONTMATTER_RE.match(text)
        if match is None:
            raise NoteParseError("unterminated or malformed frontmatter block")
        raw = match.group(1)
        try:
            parsed = yaml.load(raw, Loader=_FrontmatterLoader)
        except yaml.YAMLError as exc:
            raise NoteParseError(f"frontmatter YAML error: {exc}") from exc
        except RecursionError as exc:
            raise NoteParseError(
                "frontmatter nesting exceeds the parser limit"
            ) from exc
        if parsed is not None and not isinstance(parsed, dict):
            raise NoteParseError(
                f"frontmatter must be a mapping, got {type(parsed).__name__}"
            )
        frontmatter = parsed or {}
        body = text[match.end():]

    links = [
        NoteLink(target=match.group(2).strip(), embed=bool(match.group(1)))
        for match in _WIKILINK_RE.finditer(body)
    ]
    links.extend(_frontmatter_links(frontmatter))

    title = frontmatter.get("title")
    if not isinstance(title, str) or not title.strip():
        heading = _HEADING_RE.search(body)
        title = heading.group("title") if heading else fallback_title
    title = title.strip()[:_MAX_TITLE]

    excerpt = " ".join(body.split())[:_MAX_EXCERPT]
    return ParsedNote(
        frontmatter=frontmatter, body=body, title=title, links=links, excerpt=excerpt
    )
