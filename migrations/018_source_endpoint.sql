-- 018 endpoint -> source link (oxjob #548 endpoint step b)

-- Migrated from Databricks openalex.sources.endpoint_to_source (4,423 rows as of
-- 2026-07-08): the mapping CreateLocationsWithSources uses to attach repository
-- locations to sources. PK = one-endpoint-one-source (mirrors source_datacite_id).
-- The one duplicate endpoint in the Databricks table (ccab103ae1e4d0fc234 ->
-- {4306401821, 4406922899}) is resolved to 4306401821 at import time, matching
-- production behavior (the notebook's row_number() picked the lowest source id);
-- the pair is parked in source_ingest_issue as a merge candidate.
CREATE TABLE IF NOT EXISTS source_endpoint (
    endpoint_id TEXT PRIMARY KEY REFERENCES endpoint(id),
    source_id   BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_source_endpoint_source ON source_endpoint(source_id);
