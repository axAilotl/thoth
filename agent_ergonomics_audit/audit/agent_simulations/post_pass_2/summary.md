# Post-Pass 2 Simulation

The first safe command is now explicit and parseable:

- `python thoth.py web-clipper --plan --json`
- `python thoth.py ingest-queue --plan --json --limit 25`
- `python thoth.py x-api-sync --plan --json --max-pages 1`

Each surface emits JSON on stdout, sends diagnostics to stderr, reports mutation flags as false, and keeps actual collection, queue processing, network calls, and checkpoint writes behind the non-plan invocation.
