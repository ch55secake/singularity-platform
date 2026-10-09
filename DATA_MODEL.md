# Data Model

The baseline spool uses the collector's JSON contract. The planned ClickHouse
tables are:

| Table | Purpose |
|---|---|
| `match_salts` | Candidate cluster and metadata/replay salts |
| `match_jobs` | Download and parse state machine |
| `match_metadata` | Match-level protobuf fields |
| `match_player` | Detailed player and match statistics |
| `player_match_history` | Per-account history rows |
| `demo_player` | Demo-derived hero build and observed identity data |

Salt values are credentials for Valve replay files and must not be exposed in
logs or public API responses. Accepted salt records also retain the opaque
collector identity assigned by the platform credential; bearer tokens are not
stored with the record.
