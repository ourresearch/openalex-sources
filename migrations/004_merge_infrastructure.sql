-- 004 merge infrastructure (oxjob #548, Phase 3)
-- Merges are first-class operations: loser gets merge_into_id/merge_into_date
-- (already in 001), ISSNs move to the winner, and every merge is audited here.

CREATE TABLE IF NOT EXISTS source_merge (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    loser_id    BIGINT NOT NULL REFERENCES sources(id),
    winner_id   BIGINT NOT NULL REFERENCES sources(id),
    rule        TEXT NOT NULL,          -- 'auto_name_match', 'manual', ...
    source_feed TEXT,                   -- feed whose conflict surfaced the pair
    detail      JSONB,                  -- names, works counts, issue ids, ...
    merged_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_source_merge_winner ON source_merge(winner_id);
CREATE INDEX IF NOT EXISTS idx_source_merge_loser ON source_merge(loser_id);

-- conflict-queue resolution tracking
ALTER TABLE source_ingest_issue ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
ALTER TABLE source_ingest_issue ADD COLUMN IF NOT EXISTS resolution  TEXT;
CREATE INDEX IF NOT EXISTS idx_source_ingest_issue_unresolved
    ON source_ingest_issue(issue_type) WHERE resolved_at IS NULL;

-- operational works counts snapshotted from Databricks (winner-selection signal;
-- refresh at need, check as_of before trusting)
CREATE TABLE IF NOT EXISTS source_works_count (
    source_id   BIGINT PRIMARY KEY,
    works_count BIGINT,
    as_of       DATE
);
