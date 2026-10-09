# Operations

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `SINGULARITY_BIND` | `0.0.0.0:8080` | Listen address |
| `SINGULARITY_INGEST_TOKEN` | unset | Bearer token |
| `SINGULARITY_SPOOL_PATH` | `data/salts.jsonl` | Durable input spool |
| `RUST_LOG` | `singularity_platform=info` | Logging filter |

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
