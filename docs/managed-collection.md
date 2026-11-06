# Cissa-managed collection

`core.managed_execution.ManagedCollectionRun` is the execution envelope passed
by Cissa's authenticated worker. It is not exposed as an agent-supplied tool.
The worker resolves an integration's account, archive, selected resources,
credential handle, current lease and source settings before invoking Thoth.

Required fields: `run_id`, `idempotency_key`, `account_id`, `archive_id`,
`integration_id`, `connector`, `connector_version`, positive `lease_generation`,
`selected_resources`, `source_config`, `options`, UTC `window_start` and
`window_end` (`[start,end)`), and positive finite `budgets`. `cursor` is optional.
For discovery connectors resources are GitHub/HF usernames or arXiv
topics/categories. Credentials are forbidden in `source_config` and `options`.

The local worker constructs `AgentSurfaceService(config, layout=layout, db=db,
event_store=store, managed_run=request, account_root=root,
credentials={"token": resolved_token})` and calls
`run_connector(request.connector, execute=True)`. Account root and store handles
come from the worker's account binding, never the remote agent. Store raw roots,
all layout paths and the database must belong to that root. A configured
Postgres capture store must be supplied explicitly; managed collection cannot
resolve a global DSN. `managed.connector_version` in deployment settings must
equal the requested immutable version. Managed mode currently supports the
GitHub/HF/arXiv discovery adapters; unsupported connectors fail closed.

The source settings replace this connector's process configuration. Credentials
are passed separately to the collector and never included in the run envelope.
GitHub ignores ambient token/netrc configuration. Hugging Face receives an
explicit token or `False`, disabling the SDK's cached-token fallback. Run
history and capture-event provenance include Cissa's run and scope identity.

Cissa owns transport authentication, lease validation/revocation, Secret
Manager resolution, safe public receipts and per-account process provisioning.
Do not publish the standalone Thoth API or share one configured process between
accounts. An explicit context is not a replacement for that authorization.
