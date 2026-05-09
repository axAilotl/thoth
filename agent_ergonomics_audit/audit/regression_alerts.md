# Regression Alerts

No regressions detected in Pass 2 targeted validation.

Watch for future changes that:

- print human status lines to stdout during `--json` plan runs
- make `x-api-sync --plan` import or call network-bound sync code
- make `web-clipper --plan` write file metadata, queue rows, or staged assets
- make `ingest-queue --plan` mark rows processing or update wiki pages
