# Singularity Platform

Private ingestion and processing services for the Singularity Deadlock match
tracker.

## Baseline

The platform provides an authenticated salt-ingestion endpoint, a durable JSONL
spool, metadata processing, and an opt-in replay worker. It never uses Steam
Game Coordinator access or server-side Steam credentials.

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

After applying `schema/002_matches.sql`, `schema/005_match_events.sql`, and
`schema/006_objectives.sql`, load those files into ClickHouse with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform metadata-parser
```

After applying `schema/004_replays.sql`, run one replay download and parse pass
with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform replay-worker
```

The worker handles replay-salt records, parses Deadlock player-pawn positions
with the pinned Haste revision, retries delayed or malformed downloads, and
deletes raw demos after a successful parse unless a bounded debug retention
window is configured.

Refresh public name and image lookups with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform assets-sync
```

The read API is available after applying the match and asset schemas:

```bash
curl http://127.0.0.1:8080/api/matches
curl http://127.0.0.1:8080/api/matches/<uuid>
curl http://127.0.0.1:8080/api/players/by-name/<member-name>
```

Set `SINGULARITY_READ_TOKEN` to protect reads with a separate bearer token.
Use `SINGULARITY_MEMBER_NAMES_JSON` to resolve consenting friends' account IDs
to display names; account IDs are never returned by the browser API.

Run the optional Discord bot separately from the platform HTTP service:

```bash
SINGULARITY_DISCORD_TOKEN=replace-me \
SINGULARITY_DISCORD_CHANNEL_ID=123456789012345678 \
SINGULARITY_DISCORD_GUILD_ID=123456789012345678 \
  singularity-platform discord-bot
```

Configure `SINGULARITY_DISCORD_READ_TOKEN` when the read API is protected. The
bot uses only the read API, posts newly processed group matches once, and
provides `/link`, `/last`, `/match`, `/stats`, and `/leaderboard`. Apply
`schema/005_match_events.sql` so the parser can publish the notification
cursor.

See [`SPEC.md`](SPEC.md) for the service contract.
