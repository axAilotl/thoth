"""Obsidian vault import into CCF (checklist section 9).

Maps an Obsidian vault segment — YAML-frontmatter markdown notes,
``[[wikilinks]]``, ``![[embeds]]``, and binary attachments — onto
canonical CCF objects through the producer -> admission path:

- ``notes``    frontmatter/wikilink parsing (fails closed on malformed docs)
- ``vault``    filesystem scan and classification (notes/binaries/git repos)
- ``importer`` the producer-bound import pass and its honest report
"""

from ccf.obsidian.importer import (
    DEFAULT_EMBED_CAP_BYTES,
    ImportReport,
    NoteRecord,
    ObsidianImportError,
    ObsidianImporter,
)
from ccf.obsidian.notes import NoteLink, NoteParseError, ParsedNote, parse_note
from ccf.obsidian.vault import VaultFile, VaultLayout, VaultScanError, scan_vault

__all__ = [
    "DEFAULT_EMBED_CAP_BYTES",
    "ImportReport",
    "NoteLink",
    "NoteParseError",
    "NoteRecord",
    "ObsidianImportError",
    "ObsidianImporter",
    "ParsedNote",
    "VaultFile",
    "VaultLayout",
    "VaultScanError",
    "parse_note",
    "scan_vault",
]
