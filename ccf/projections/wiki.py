"""Wiki/knowledge-base rebuild from canonical CCF state.

Wiki pages and summaries are projections (checklist phase 4: "Wiki
pages/summaries → projection or generated artifact, never source
replacement"). :func:`rebuild_wiki` regenerates the repo's wiki-style
markdown — an ``index.md`` navigation root plus one page per
``semantic.entity`` Record — purely from canonical archive state, into a
caller-chosen staging directory (never the live ``wiki/``).

Output is a pure function of canonical state: same archive, same bytes.
Page frontmatter follows the repo's compiled-wiki shape (see
``tests/fixtures/wiki/pages/``) and carries ``source_records`` with the
exact CCF Record URNs the page derives from. If a page is ever preserved
as a generated artifact rather than regenerated, it must be admitted as a
Record with those evidence links (spec 10.7).
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

from ccf.projections import WIKI
from ccf.projections.rebuild import finish_rebuild


class WikiRebuildError(RuntimeError):
    """Raised when a wiki rebuild cannot proceed safely."""


def _slugify(label: str, object_id: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-") or "entity"
    suffix = object_id.rsplit(":", 1)[-1].replace("-", "")[:8]
    return f"{base}-{suffix}"


def _frontmatter(fields: dict) -> str:
    lines = ["---"]
    for key, value in fields.items():
        if isinstance(value, list):
            lines.append(f"{key}:")
            lines.extend(f"  - {item}" for item in value)
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    return "\n".join(lines)


def rebuild_wiki(conn, archive_id: str, staging_dir: str | Path) -> dict:
    """Regenerate wiki markdown into ``staging_dir``; returns a report.

    The staging directory is managed wholesale: previous generated
    ``index.md`` and ``pages/`` content is replaced. Refuses to operate on
    a non-empty directory that does not look like a previous rebuild
    (fail closed against pointing this at a hand-written wiki).
    """
    staging = Path(staging_dir)
    pages_dir = staging / "pages"
    if staging.exists():
        if not staging.is_dir():
            raise WikiRebuildError(f"staging path {staging} is not a directory")
        markers = staging / "index.md"
        contents = [p for p in staging.iterdir()]
        if contents and not markers.exists():
            raise WikiRebuildError(
                f"staging dir {staging} is not empty and has no index.md; "
                "refusing to overwrite an unmanaged directory"
            )
        shutil.rmtree(pages_dir, ignore_errors=True)
        (staging / "index.md").unlink(missing_ok=True)
    pages_dir.mkdir(parents=True, exist_ok=True)

    entities = conn.execute(
        """
        SELECT oh.id,
               sem.plaintext_json -> 'payload' ->> 'label' AS label,
               sem.plaintext_json -> 'payload' ->> 'entity_kind' AS entity_kind,
               sem.plaintext_json -> 'payload' ->> 'description' AS description,
               a.admitted_at
        FROM object_header oh
        JOIN compartment s
          ON s.object_id = oh.id AND s.compartment = 'structural'
        LEFT JOIN compartment sem
          ON sem.object_id = oh.id AND sem.compartment = 'semantic'
             AND sem.state = 'plaintext'
        JOIN admission a ON a.archive_id = oh.archive_id AND a.object_id = oh.id
        WHERE oh.archive_id = %s AND oh.object_kind = 'record'
          AND s.state = 'plaintext'
          AND s.plaintext_json ->> 'type' = 'semantic.entity'
        ORDER BY oh.id
        """,
        (archive_id,),
    ).fetchall()

    pages: list[tuple[str, str]] = []  # (slug, title)
    for object_id, label, entity_kind, description, admitted_at in entities:
        title = label or object_id
        slug = _slugify(title, object_id)
        frontmatter = _frontmatter(
            {
                "thoth_type": "wiki_page",
                "title": title,
                "slug": slug,
                "kind": entity_kind or "entity",
                "summary": (description or "").splitlines()[0] if description else "",
                "source_records": [object_id],
                "language": "en",
                "created_at": admitted_at,
                "updated_at": admitted_at,
            }
        )
        body = f"{frontmatter}\n\n# {title}\n\n{description or ''}\n"
        (pages_dir / f"{slug}.md").write_text(body, encoding="utf-8")
        pages.append((slug, title))

    index_lines = [
        "# Thoth Wiki (CCF rebuild)",
        "",
        "Regenerated from canonical CCF state; do not edit by hand.",
        "",
        "## Pages",
        "",
    ]
    index_lines.extend(f"- [{title}](pages/{slug}.md)" for slug, title in pages)
    index_lines.append("")
    (staging / "index.md").write_text("\n".join(index_lines), encoding="utf-8")

    finish_rebuild(conn, archive_id=archive_id, projection_name=WIKI)
    return {"pages": len(pages), "staging_dir": str(staging)}
