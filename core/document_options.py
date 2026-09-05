"""Fail-closed opt-ins shared by document collection and enrichment."""


def document_boolean(config, key: str) -> bool:
    value = config.get(f"sources.web_clipper.{key}", False)
    if type(value) is not bool:
        raise ValueError(f"sources.web_clipper.{key} must be a boolean")
    return value


def validate_document_opt_ins(config) -> None:
    document_boolean(config, "queue_pdfs")
    document_boolean(config, "summarize")
