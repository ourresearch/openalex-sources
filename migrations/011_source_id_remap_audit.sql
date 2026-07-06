-- 011 id-remap audit (oxjob #548, cutover)
-- At cutover, every app-minted id (id > 7407059451, the backfill max) is
-- re-assigned to continue sequentially from walden's final max id, so the
-- public S-id space has no jump to the 7.5B mitigation range. This table is
-- the permanent old->new record (scripts/remap_minted_ids.py writes it).

CREATE TABLE IF NOT EXISTS source_id_remap (
    old_id      BIGINT PRIMARY KEY,
    new_id      BIGINT NOT NULL UNIQUE,
    remapped_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
