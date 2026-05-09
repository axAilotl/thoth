# Pass 2 Uplift Diff

| Surface | Before | After | Uplift |
| --- | ---: | ---: | ---: |
| `verb__web-clipper__plan` | 420 | 875 | +455 |
| `verb__ingest-queue__plan` | 430 | 865 | +435 |
| `verb__x-api-sync__plan` | 410 | 870 | +460 |

The main uplift is safety with recovery and output parseability: an agent can now preview ingestion mutation surfaces without changing queue rows, staging assets, contacting X, or writing checkpoints.
