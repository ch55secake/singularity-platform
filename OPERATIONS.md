# Operations

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `SINGULARITY_BIND` | `0.0.0.0:8080` | Listen address |
| `SINGULARITY_INGEST_TOKEN` | unset | Bearer token |
| `SINGULARITY_SPOOL_PATH` | `data/salts.jsonl` | Durable input spool |
| `RUST_LOG` | `singularity_platform=info` | Logging filter |

## Recovery

The JSONL spool is append-only. Stop the platform before copying it, preserve
the original file, and replay records through the ingestion endpoint after a
restore.

## Retention

The baseline keeps only salt records. Once replay downloading is added, raw
metadata and demos should be deleted after successful parsing unless explicitly
retained for debugging.
