-- 027: per-run record count on endpoint (oxjobs #804/#836, Jason 2026-08-30).
-- The nightly harvester (openalex-ingest repositories.py) already writes the
-- last_health_* receipt columns per attempt; this adds the one number it was
-- throwing away: how many records the last harvest attempt retrieved from the
-- feed. Written alongside last_health_check from the same code path; NULL for
-- rows never harvested since this shipped. On failed attempts it records the
-- count retrieved before the error (may be partial).
ALTER TABLE endpoint ADD COLUMN IF NOT EXISTS last_record_count BIGINT;
COMMENT ON COLUMN endpoint.last_record_count IS
  'Records retrieved from the feed in the harvest attempt stamped by last_health_check; partial on failures; NULL = not harvested since 2026-08-30.';
