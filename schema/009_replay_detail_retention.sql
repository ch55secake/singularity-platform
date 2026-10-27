-- Expire high-volume detail after 30 days while retaining summaries and sparse
-- match events. Existing parts receive a fresh retention date as ClickHouse
-- materializes this default column, giving the migration a 30-day grace period.

ALTER TABLE singularity.match_player_positions
    ADD COLUMN IF NOT EXISTS retention_date Date DEFAULT today();
ALTER TABLE singularity.match_player_positions
    MODIFY TTL retention_date + INTERVAL 30 DAY DELETE;

ALTER TABLE singularity.match_damage
    ADD COLUMN IF NOT EXISTS retention_date Date DEFAULT today();
ALTER TABLE singularity.match_damage
    MODIFY TTL retention_date + INTERVAL 30 DAY DELETE;

ALTER TABLE singularity.replay_positions
    ADD COLUMN IF NOT EXISTS retention_date Date DEFAULT today();
ALTER TABLE singularity.replay_positions
    MODIFY TTL retention_date + INTERVAL 30 DAY DELETE;
