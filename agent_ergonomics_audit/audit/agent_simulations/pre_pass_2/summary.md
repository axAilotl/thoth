# Pre-Pass 2 Simulation

An agent trying to safely inspect mutating ingestion commands had no reliable first command for Web Clipper, queue draining, or X API sync. `web-clipper` attempted configuration and collection immediately, `ingest-queue` started processing due rows, and `x-api-sync` contacted the X API. The agent had to infer risk from docs or source.
