# Singularity Platform Specification

Status: baseline v0.1.0

## Goal

Provide a private, authenticated ingestion boundary for cache-derived Deadlock
match salts and a durable handoff point for later processing workers.

## Baseline endpoints

### `GET /health`

Unauthenticated liveness endpoint.

### `POST /ingest/salts`

Accepts a JSON array of one to 1,000 salt records. The request must contain:

```text
Authorization: Bearer <SINGULARITY_INGEST_TOKEN>
```

Authentication is disabled only when the server token is unset. Production
deployments must always set it.

## Record contract

```json
{
  "match_id": 42476710,
  "cluster_id": 183,
  "metadata_salt": 428480166,
  "replay_salt": null,
  "username": "ingest-tool:123456"
}
```

At least one salt is required. Match IDs are restricted to the current
realistic range used by the collector. Unknown JSON fields are ignored by the
baseline for forward compatibility.

## Baseline persistence

Accepted records are appended to `SINGULARITY_SPOOL_PATH` as JSONL. Duplicate
records are suppressed for the lifetime of the process. Persistent deduplication
and ClickHouse ingestion are next-stage work.

## Processing stages

Planned states:

```text
received → metadata_pending → metadata_processed
         → replay_pending   → replay_processed
         → analytics_written
```

Failures must be recorded with retry count and last error. The server must not
require a Steam account in any stage.

## Acceptance criteria

- Valid cache submissions are accepted and persisted.
- Invalid or empty batches are rejected.
- Missing/incorrect bearer tokens are rejected when configured.
- Duplicate submissions are safe.
- The process can restart without corrupting the spool.
- Health remains available independently of the processing worker.
