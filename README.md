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

For multiple collectors, provision independent credentials instead of sharing
one token:

```bash
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials create oscar-desktop
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials list
```

The create and rotate commands print a token once. Store it in the collector's
private configuration. Use `credentials rotate <id>` to invalidate the old
token or `credentials revoke <id>` for emergency revocation. Restart the
platform after changing the credential file; the file stores SHA-256 hashes,
never plaintext tokens.

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

Accepted records include the authenticated collector identity in the JSONL
spool. The legacy `SINGULARITY_INGEST_TOKEN` remains accepted during migration
and is recorded as `legacy-global-token`.

Run one metadata download pass with:

```bash
singularity-platform metadata-worker
```

It writes decompressed metadata handoff files and restart-safe job state under
`data/` by default. No Steam login or Steam credentials are used.

After applying `schema/002_matches.sql`, load those files into ClickHouse with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform metadata-parser
```

Refresh public name and image lookups with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform assets-sync
```

The read API is available after applying the match and asset schemas:

```bash
curl http://127.0.0.1:8080/api/matches
curl http://127.0.0.1:8080/api/matches/<uuid>
```

Set `SINGULARITY_READ_TOKEN` to protect reads with a separate bearer token.
Use `SINGULARITY_MEMBER_NAMES_JSON` to resolve consenting friends' account IDs
to display names; account IDs are never returned by the browser API.

See [`SPEC.md`](SPEC.md) for the service contract.
