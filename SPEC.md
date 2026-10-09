# Singularity Platform Specification

Status: baseline v0.1.0

## Goal

Provide a private, authenticated ingestion boundary for cache-derived Deadlock
match salts and a durable handoff point for later processing workers.

## Baseline endpoints

### `GET /health`

Unauthenticated liveness endpoint.

### `POST /ingest/salts`

Accepts a JSON array of one to the configured collector batch limit (at most
1,000) salt records. The request must contain one of:

```text
Authorization: Bearer <SINGULARITY_INGEST_TOKEN>
```

or a bearer token provisioned for one collector with the offline
`credentials create` command. Authentication is disabled only when neither the
legacy token nor a credential record is configured; this development fallback
must not be used in production.

The platform applies a fixed-window request limit independently to each
collector and rejects request bodies larger than 512 KiB. The limits are
configured with `SINGULARITY_COLLECTOR_RATE_LIMIT`,
`SINGULARITY_COLLECTOR_RATE_WINDOW_SECONDS`, and
`SINGULARITY_COLLECTOR_MAX_BATCH_SIZE`.

## Record contract

```json
{
  "match_id": 42476710,
  "cluster_id": 183,
  "metadata_salt": 428480166,
  "replay_salt": null,
  "username": "ingest-tool:123456",
  "collector_id": "oscar-desktop"
}
```

At least one salt is required. Match IDs are restricted to the current
realistic range used by the collector. Unknown JSON fields are ignored by the
baseline for forward compatibility. `collector_id` is assigned from the
authenticated credential and cannot be supplied by the client.

## Baseline persistence

Accepted records are appended to `SINGULARITY_SPOOL_PATH` as JSONL. The file is
flushed and synchronized before the request is acknowledged. On startup, the
platform rebuilds its deduplication index from complete valid spool lines, so
replaying a request after a restart is reported as a duplicate.

An incomplete final line is treated as a crash tail and ignored; earlier valid
records remain recoverable. Malformed complete lines are skipped and logged
without logging their contents. Durable processing states, leases, retries,
dead-letter records, and ClickHouse ingestion remain next-stage work.

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
- Collector credentials can be created, rotated, revoked, and listed without
  storing plaintext tokens.
- Accepted records are attributed to the authenticated collector.
- Request bodies and per-collector request rates are bounded.
- Duplicate submissions are safe, including after a process restart.
- A truncated final JSONL line does not prevent recovery of earlier records.
- The process can restart without corrupting the spool.
- Health remains available independently of the processing worker.
