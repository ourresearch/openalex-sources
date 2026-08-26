-- 026 historical publisher-original APC currencies (oxjob #571 phase 2).
--
-- `apc_usd_by_year` remains the normalized annual USD series.  The additive
-- `apc_prices_by_year` object carries only currencies identified as prices
-- actually advertised by the publisher for that journal-year:
--
--   {"2019": [{"price": 1535, "currency": "USD"},
--             {"price": 1250, "currency": "EUR"}]}
-- Exact-zero observations are represented as USD 0 only; currency choice is
-- meaningless for a free journal and Work consumers should be deterministic.
--
-- The Butler v2 file contains a per-record data_version whose legacy rows
-- need provenance reconciled against v1.  Preserve that upstream version and
-- record id in bronze; `dataset_version` remains the release-level ingest tag
-- (`butler_v1` / `butler_v2`).  Per-currency reconciliation evidence lives in
-- the existing schemaless `prices` JSONB entries.

ALTER TABLE sources ADD COLUMN IF NOT EXISTS apc_prices_by_year JSONB;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_sources_apc_prices_by_year_object'
          AND conrelid = 'public.sources'::regclass
    ) THEN
        ALTER TABLE sources
            ADD CONSTRAINT ck_sources_apc_prices_by_year_object
            CHECK (apc_prices_by_year IS NULL
                   OR jsonb_typeof(apc_prices_by_year) = 'object');
    END IF;
END $$;

ALTER TABLE butler_apc_journal_year
    ADD COLUMN IF NOT EXISTS source_record_id BIGINT;

ALTER TABLE butler_apc_journal_year
    ADD COLUMN IF NOT EXISTS source_data_version SMALLINT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_butler_apc_source_data_version'
          AND conrelid = 'public.butler_apc_journal_year'::regclass
    ) THEN
        ALTER TABLE butler_apc_journal_year
            ADD CONSTRAINT ck_butler_apc_source_data_version
            CHECK (source_data_version IS NULL
                   OR source_data_version IN (1, 2));
    END IF;
END $$;

COMMENT ON COLUMN sources.apc_prices_by_year IS
    'Observed-year publisher-original APC prices by currency; normalized USD remains in apc_usd_by_year.';
COMMENT ON COLUMN butler_apc_journal_year.source_record_id IS
    'record_id from the upstream Butler/ScholCommLab file, when present.';
COMMENT ON COLUMN butler_apc_journal_year.source_data_version IS
    'Per-record data_version from the upstream file; distinct from the release-level dataset_version.';
