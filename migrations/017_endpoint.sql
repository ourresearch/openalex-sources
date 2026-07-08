-- 017 endpoint table (oxjob #548, endpoint-table migration step a; Casey 2026-07-08)
--
-- 1:1 copy of openalex-ingest's public.endpoint (the OAI-PMH harvest registry,
-- ~6,200 endpoints). Schema only — data is copied separately. The openalex-ingest
-- table stays LIVE (repositories.py reads it and writes the health columns daily)
-- until the harvester is repointed at this database, so expect a data re-sync at
-- repoint time; until then this copy is a point-in-time snapshot.
--
-- Kept for fidelity, decide at repoint: legacy columns retry_interval / retry_at /
-- is_core (documented unused since the Jan 2026 harvester simplification) and
-- id_old (+ its index). Skipped: ingest's endpoint_id_idx, a redundant duplicate
-- of the primary-key index.

CREATE TABLE IF NOT EXISTS endpoint (
    id                                    TEXT PRIMARY KEY,
    pmh_url                               TEXT,
    last_harvest_started                  TIMESTAMP,
    most_recent_date_harvested            TIMESTAMP,
    name                                  TEXT,
    email                                 TEXT,
    last_harvest_finished                 TIMESTAMP,
    pmh_set                               TEXT,
    error                                 TEXT,
    earliest_timestamp                    TIMESTAMP,
    ready_to_run                          BOOLEAN,
    repo_unique_id                        TEXT,
    contacted                             TIMESTAMP,
    repo_request_id                       TEXT,
    harvest_identify_response             TEXT,
    harvest_test_initial_dates            TEXT,
    harvest_test_recent_dates             TEXT,
    sample_pmh_record                     TEXT,
    id_old                                TEXT,
    contacted_text                        TEXT,
    policy_promises_no_submitted          BOOLEAN,
    policy_promises_no_submitted_evidence TEXT,
    metadata_prefix                       TEXT NOT NULL DEFAULT 'oai_dc',
    green_scrape                          BOOLEAN NOT NULL DEFAULT TRUE,
    retry_interval                        INTERVAL,
    retry_at                              TIMESTAMP,
    rand                                  REAL NOT NULL DEFAULT random(),
    is_core                               BOOLEAN,
    in_walden                             BOOLEAN DEFAULT FALSE,
    last_health_status                    TEXT,
    last_health_check                     TIMESTAMP,
    last_response_time                    DOUBLE PRECISION,
    last_error_message                    TEXT
);

CREATE INDEX IF NOT EXISTS endpoint_repo_unique_id_idx ON endpoint (repo_unique_id);
CREATE INDEX IF NOT EXISTS endpoint_id_old_idx ON endpoint (id_old);

COMMENT ON TABLE endpoint IS 'OAI-PMH harvest registry (~6,200 endpoints), migrated from openalex-ingest PG (oxjob #548). Harvested daily by openalex-ingest repositories.py.';
COMMENT ON COLUMN endpoint.last_health_status IS 'Status from last harvest attempt: success, blocked, timeout, connection_error, malformed, oai_error';
COMMENT ON COLUMN endpoint.last_health_check IS 'Timestamp of the last harvest attempt';
COMMENT ON COLUMN endpoint.last_response_time IS 'Response time in seconds for the last harvest attempt';
COMMENT ON COLUMN endpoint.last_error_message IS 'Error message from the last failed harvest attempt';
COMMENT ON COLUMN endpoint.retry_interval IS 'LEGACY - unused since the Jan 2026 harvester simplification (old tiering/backoff system)';
COMMENT ON COLUMN endpoint.retry_at IS 'LEGACY - unused since the Jan 2026 harvester simplification (old tiering/backoff system)';
COMMENT ON COLUMN endpoint.is_core IS 'LEGACY - unused since the Jan 2026 harvester simplification (old tiering/backoff system)';
