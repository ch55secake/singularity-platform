# Operations

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `SINGULARITY_BIND` | `0.0.0.0:8080` | Listen address |
| `SINGULARITY_INGEST_TOKEN` | unset | Bearer token |
| `SINGULARITY_CREDENTIALS_PATH` | `data/collectors.json` | Hashed collector credentials |
| `SINGULARITY_SPOOL_PATH` | `data/salts.jsonl` | Durable input spool |
| `SINGULARITY_COLLECTOR_MAX_BATCH_SIZE` | `1000` | Maximum records per collector request |
| `SINGULARITY_COLLECTOR_RATE_LIMIT` | `60` | Requests per collector window |
| `SINGULARITY_COLLECTOR_RATE_WINDOW_SECONDS` | `60` | Rate-limit window |
| `RUST_LOG` | `singularity_platform=info` | Logging filter |

## Collector credentials

Provision credentials offline on the platform host:

```bash
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials create collector-name
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials rotate collector-name
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials revoke collector-name
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials list
```

The create and rotate commands print the new bearer token once. Store it in
the collector's private configuration and do not put it in source control.
Credential changes are written atomically, but the running platform reads the
file at startup; restart the service after creating, rotating, or revoking a
credential. The legacy `SINGULARITY_INGEST_TOKEN` remains available during
migration and is attributed as `legacy-global-token`.

## Recovery

The JSONL spool is append-only and synchronized before an ingest request is
acknowledged. On startup, the platform rebuilds its duplicate index from valid
complete records. An incomplete final line is treated as a crash tail and can
be left in place; earlier records remain recoverable.

Stop the platform before copying the spool, preserve the original file, and
replay records through the ingestion endpoint after a restore. Investigate
malformed complete-line warnings before manually repairing a spool.

## Retention

The baseline keeps only salt records. Once replay downloading is added, raw
metadata and demos should be deleted after successful parsing unless explicitly
retained for debugging.
