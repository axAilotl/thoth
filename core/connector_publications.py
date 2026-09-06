"""Runtime-owned derived publication after successful connector intake.

Connectors only discover/store source data; they cannot write compiled wiki files.
This explicit opt-in stage follows the same ownership boundary as ingestion's
wiki projection, shared by API, CLI and scheduled connector execution.
"""
from .prompt_index import publish_prompt_index


def publish_connector_derivatives(name, result, *, config, layout, db):
    if name != "corpus_index" or config.get("sources.corpus_index.prompt_index_enabled", False) is not True:
        return
    report = publish_prompt_index(config=config, layout=layout, db=db)
    result["prompt_index"] = report
    db.upsert_automation_state("prompt_index:publication", report)
