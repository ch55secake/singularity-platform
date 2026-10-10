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
| `SINGULARITY_METADATA_SPOOL_PATH` | `SINGULARITY_SPOOL_PATH` | Worker spool override |
| `SINGULARITY_METADATA_STATE_PATH` | `data/metadata-state.json` | Metadata job state |
| `SINGULARITY_METADATA_OUTPUT_DIR` | `data/metadata` | Decompressed metadata handoff files |
| `SINGULARITY_METADATA_PARSE_STATE_PATH` | `data/metadata-parse-state.json` | Parser idempotency state |
| `SINGULARITY_METADATA_BASE_URL` | `http://replay{cluster}.valve.net/1422450` | Metadata URL base |
| `SINGULARITY_METADATA_TIMEOUT_SECONDS` | `30` | Per-download timeout |
| `SINGULARITY_METADATA_RETRY_BASE_SECONDS` | `30` | Initial retry delay |
| `SINGULARITY_METADATA_RETRY_MAX_SECONDS` | `3600` | Maximum retry delay |
| `SINGULARITY_CLICKHOUSE_URL` | unset | ClickHouse HTTP endpoint for parser |
| `SINGULARITY_CLICKHOUSE_DATABASE` | `singularity` | ClickHouse database |
| `SINGULARITY_CLICKHOUSE_USER` | unset | ClickHouse user |
| `SINGULARITY_CLICKHOUSE_PASSWORD` | unset | ClickHouse password |
| `SINGULARITY_ASSETS_BASE_URL` | `https://api.deadlock-api.com/v1/assets` | Public asset API base |
| `SINGULARITY_ASSETS_STATE_PATH` | `data/assets-state.json` | Asset snapshot state |
| `SINGULARITY_ASSETS_TIMEOUT_SECONDS` | `30` | Asset request timeout |
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

## Metadata worker

Run one restart-safe metadata pass after ingestion, from a cron or deployment
worker:

```bash
singularity-platform metadata-worker
```

The worker reads `SINGULARITY_METADATA_SPOOL_PATH` or
`SINGULARITY_SPOOL_PATH`, downloads each eligible metadata file, accepts the
known zstd payload despite its `.bz2` suffix, and falls back to bzip2 when the
magic bytes indicate bzip2. Decompressed handoff files are written atomically
under `SINGULARITY_METADATA_OUTPUT_DIR`. The state file records
`metadata_pending`/`metadata_processed`, attempts, retry time, and a bounded
error code without storing the URL or salt. Failed downloads use exponential
backoff, including 404 and 5xx responses.

After a successful download, apply `schema/002_matches.sql` and run the
parser against the ClickHouse HTTP endpoint:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform metadata-parser
```

The parser writes match, player, item-timeline, and player-stat rows as JSONEachRow and
records processed job digests in `SINGULARITY_METADATA_PARSE_STATE_PATH`.
Timeline rows include the aim, kill-type, damage, mitigation, healing, power,
and health counters used by the derived views. `player_stat_averages` selects
the last snapshot for each player in each match before averaging across
matches. ReplacingMergeTree tables and the parser state make repeated parsing
idempotent.

Damage matrix samples are written to `match_damage`; query
`match_damage_player_reconciliation` to compare the normalized stat-type-0
totals against each player's final `player_damage` value. The raw matrix value
is retained because the protobuf encodes damage in doubled fixed-point units.

Death events are written to `match_deaths` with nullable position fields for
older or incomplete metadata. Use `match_deaths_with_players` for hero/team
labels and `match_death_count_reconciliation` to verify that recorded deaths
match the scoreboard.

Player paths are written to `match_player_positions` as world coordinates. A
sample match with 32 minutes, 12 players, and one-second samples produces
approximately 23,040 rows; ClickHouse's compact numeric schema keeps this at
well below a megabyte in the local smoke estimate before replication. To check
alignment, compare a death's `game_time_s` with the matching victim path
sample; the sample metadata's nearest point is within a few world units of
`death_pos`.

Apply `schema/003_assets.sql` after `schema/002_matches.sql` so
`match_player_items_with_assets` can resolve timeline IDs from the hero/item
snapshots. The view uses the asset payload's `type` field to distinguish
ability upgrades from shop upgrades.

## Asset lookups

Apply `schema/003_assets.sql`, then refresh the public hero, item, ability, and
map snapshots with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform assets-sync
```

The command classifies ability assets from the public items response, stores
the raw public payload for forward compatibility, and skips ClickHouse writes
when the endpoint snapshots have not changed. The map row includes the
minimap URL and `[-radius, radius]` world bounds for position conversion.

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
