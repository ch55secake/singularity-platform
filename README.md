# Singularity Platform

Private ingestion and processing services for the Singularity Deadlock match
tracker.

## Baseline

The `v0.1.0-baseline` provides an authenticated salt-ingestion endpoint and a
durable JSONL spool. It deliberately does not include Steam Game Coordinator
access, replay downloading, or demo parsing yet.

Run locally:

```bash
SINGULARITY_INGEST_TOKEN=local-dev-token cargo run
```

Health check:

```bash
curl http://127.0.0.1:8080/health
```

Submit salts:

```bash
curl -X POST http://127.0.0.1:8080/ingest/salts \
  -H 'Authorization: Bearer local-dev-token' \
  -H 'Content-Type: application/json' \
  -d '[{"match_id":42,"cluster_id":1,"metadata_salt":2}]'
```

See [`SPEC.md`](SPEC.md) for the service contract.
