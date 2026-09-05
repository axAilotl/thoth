"""Explicit-root, incremental keyword inventory and bounded embedding backfill."""

from __future__ import annotations

from pathlib import PurePosixPath
from datetime import datetime, timezone

from core.archivist_retrieval.inventory import (
    resolve_archivist_root_spec, scope_base, sync_archivist_inventory, iter_allowed_corpus_paths,
)
from core.archivist_retrieval.semantic import ensure_corpus_embeddings
from core.connector_budgets import start_connector_budget_run
from core.llm_interface import LLMInterface
from core.metadata_db import get_metadata_db
from core.path_layout import build_path_layout


def corpus_roots(config, layout, *, key="include_roots", required=True):
    """Validate explicit roots, rejecting traversal and symlink scope escapes."""
    specs = config.get(f"sources.corpus_index.{key}", [])
    if not isinstance(specs, (list, tuple)) or (required and not specs):
        raise ValueError(f"sources.corpus_index.{key} must be an explicit root list")
    roots = []
    for spec in specs:
        if (not isinstance(spec, str) or not spec.strip()
                or PurePosixPath(spec).is_absolute()
                or ".." in PurePosixPath(spec).parts
                or "\\" in spec):
            raise ValueError(f"Invalid corpus root: {spec!r}")
        root = resolve_archivist_root_spec(spec, layout=layout)
        if not root.path.resolve().is_relative_to(scope_base(root.scope, layout).resolve()):
            raise ValueError(f"Corpus root escapes its scope: {spec}")
        roots.append(root)
    return tuple(roots)


class CorpusIndexConnector:
    def __init__(self, config, *, layout=None, db=None, llm_interface=None):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.db = db or get_metadata_db()
        self.llm_interface = llm_interface

    async def collect(self, *, max_new_embeddings_per_run=None):
        roots = corpus_roots(self.config, self.layout)
        excluded = corpus_roots(self.config, self.layout, key="exclude_roots", required=False)
        enabled = self.config.get("sources.corpus_index.embeddings_enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("sources.corpus_index.embeddings_enabled must be boolean")
        maximum = self.config.get("sources.corpus_index.max_new_embeddings_per_run", 128)
        if max_new_embeddings_per_run is not None:
            maximum = max_new_embeddings_per_run
        if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 1:
            raise ValueError("max_new_embeddings_per_run must be a positive integer")
        budget = start_connector_budget_run(self.config, "corpus_index")
        seen = set()
        for root in roots:
            for path in iter_allowed_corpus_paths(root, exclude_roots=excluded):
                # Preflight the entire allowlist before inventory can read it.
                if path.is_symlink():
                    raise ValueError(f"Corpus index refuses symlinks: {path}")
                if (path.is_file() and path.suffix.lower() in {".md", ".markdown", ".txt", ".pdf"}
                        and path not in seen):
                    budget.add_file(path, count_input_tokens=False)
                    seen.add(path)
        inventory = sync_archivist_inventory(
            tuple(root.spec for root in roots),
            exclude_root_specs=tuple(root.spec for root in excluded),
            config=self.config, layout=self.layout, db=self.db,
        )
        result = {
            "documents": len(inventory.documents),
            "indexed_count": inventory.indexed_count,
            "reused_count": inventory.reused_count,
            "keyword_content_count": sum(bool(doc.content_text.strip()) for doc in inventory.documents),
            "empty_text_count": sum(not doc.content_text.strip() for doc in inventory.documents),
            "empty_pdf_paths": [doc.scope_relative_path for doc in inventory.documents
                                if doc.file_type == "pdf" and not doc.content_text.strip()],
            "scanned_roots": list(inventory.scanned_roots),
            "missing_roots": list(inventory.missing_roots),
            "embeddings_enabled": enabled,
        }
        if enabled:
            coverage = await ensure_corpus_embeddings(
                db=self.db, llm_interface=self.llm_interface or LLMInterface(self.config.get("llm", {})),
                documents=inventory.documents, max_new_embeddings_per_run=maximum, budget=budget,
            )
            result["embeddings"] = coverage.to_dict()
        result["budget"] = budget.summary()
        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.db.upsert_automation_state("corpus_index:last_result", result)
        return result
