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

### Read API

The platform exposes the browser-facing read contract at `/api/matches`,
`/api/matches/{uuid}`, `/api/matches/{uuid}/positions`,
`/api/matches/{uuid}/deaths`, and `/api/players/{account_id}`. Equivalent paths
without the `/api` prefix are available for direct LAN clients. Reads use a
separate optional `SINGULARITY_READ_TOKEN`; when it is unset, LAN-only network
isolation is the authentication boundary. Read queries are issued only when a
request arrives, so health and process startup do not require ClickHouse.

Match IDs in browser links are UUIDs generated once by the parser and persisted
in parser state. Numeric match IDs remain accepted for migration and direct
operator use. The response hides Steam account IDs, resolves heroes and items
through the asset snapshots, and maps configured account IDs to friend names
from `SINGULARITY_MEMBER_NAMES_JSON`.

The match detail response includes the final player-stat snapshot, every stats
timeline row, source-normalized damage, item/ability events, death events,
world-coordinate paths, and the current map asset. Missing optional metadata is
represented by nulls or empty arrays. A healthy service returns `503` for read
requests when ClickHouse is unavailable.

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

The one-shot `metadata-worker` reads eligible records from the spool and writes
decompressed metadata handoff files to `SINGULARITY_METADATA_OUTPUT_DIR`. Its
private state file tracks `metadata_pending` and `metadata_processed`, retry
attempts, next retry time, and bounded error codes. The worker does not require
Steam credentials.

The `metadata-parser` command decodes the protobuf handoff with the pinned
Deadlock `valveprotos` definitions and inserts match, player, and timeline rows
into ClickHouse using JSONEachRow. It tracks processed job digests separately
and uses the `schema/002_matches.sql` ReplacingMergeTree tables for idempotent
re-parsing. Timeline rows include the upstream aim, kill-type, damage,
mitigation, healing, power, and health counters. The derived views expose
accuracy, hero crit rate, mutually exclusive bullet/ability/melee kill shares,
headshot kill share, and damage taken per minute. Per-account averages use the
last snapshot in each match; `headshot_kills` is a subset of `bullet_kills` and
is therefore not included in the kill-share denominator.

The parser also writes every `players[].items[]` event to
`match_player_items`, including shop purchases, sales, imbued abilities, and
ability upgrades. `upgrade_info` preserves the packed source value and adds
the upstream unlock-bit and upgrade-star count decoding. The
`match_player_items_with_assets` view joins `item_id` to the public asset
snapshots: `type = ability` identifies ability upgrades and `type = upgrade`
identifies shop upgrades.

The parser expands `match_info.damage_matrix` into `match_damage`. Its aligned
`source_details.stat_type` and `source_details.source_name` arrays resolve each
`source_details_index`; stat type `0` is player damage. The protobuf matrix
stores damage in doubled fixed-point units, so `raw_cumulative_damage` retains
the wire value while `cumulative_damage` divides it by two. The totals view
uses the latest sample, and the reconciliation view filters to known player
targets before comparing against the final `player_damage` stat snapshot.

The parser expands each `players[].death_details[]` event into `match_deaths`,
preserving game time, time-to-kill, victim and killer positions, and death
duration. `match_deaths_with_players` joins both slots to scoreboard hero/team
data. `match_death_count_reconciliation` compares recorded victim counts with
the parsed scoreboard `deaths` totals.

The parser expands `match_info.match_paths.paths[]` into
`match_player_positions`. For each sample, world coordinates use
`world = min + position / resolution * (max - min)` independently for X and Y.
Combat types are `0=out`, `1=player`, `2=enemy NPC`, and `3=neutral`; movement
types are `0=normal`, `1=ability`, `2=ability debuff`, `3=ground dash`,
`4=slide`, `5=rope climbing`, `6=ziplining`, `7=in air`, and `8=air dash`.

The `assets-sync` command snapshots the public heroes, items, modifiers, and
map assets into the `schema/003_assets.sql` lookup tables. Snapshot hashes make
repeated refreshes idempotent, and the map snapshot records the minimap URL and
world bounds needed by position and death consumers.

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
