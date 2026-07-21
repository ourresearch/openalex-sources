-- 020 per-year APC list prices (oxjob #571, Butler et al. dataset)
-- DRAFT — lives in apc-571-prep until approved; copy to migrations/ to apply.
-- Idempotent: safe to re-run.
--
-- Medallion split (Jason/Casey 2026-07-17):
--   bronze = butler_apc_journal_year (raw rows, ALL currencies, collection
--            metadata; the audit trail)
--   gold   = sources.apc_usd_by_year (USD-only, OBSERVED years only, no
--            fill in either direction -- Casey/Kyle 2026-07-21; written by
--            jobs/butler_apc.py)
-- Gold shape (convention, enforced by the job): JSONB object
--   {"2019": 1790, ..., "2023": 2390}
-- string year keys -> integer USD; self-describing (SCHEMA-DESIGN.md).
-- apc_usd is derived by the job as the most recent observed value (Casey
-- ack 2026-07-21). apc_prices is UNTOUCHED: walden parses it with a fixed
-- ARRAY<STRUCT<price INT, currency STRING>> schema; never change it.
-- (Comment updated 2026-07-21 to match the decided shape; SQL unchanged,
-- migration already applied as v46.)

ALTER TABLE sources ADD COLUMN IF NOT EXISTS apc_usd_by_year JSONB;

-- light shape guard: NULL or a JSON object (full convention is code-side)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ck_sources_apc_usd_by_year_object'
    ) THEN
        ALTER TABLE sources ADD CONSTRAINT ck_sources_apc_usd_by_year_object
            CHECK (apc_usd_by_year IS NULL
                   OR jsonb_typeof(apc_usd_by_year) = 'object');
    END IF;
END $$;

-- bronze: the Butler annual file, staged verbatim-but-normalized
-- (TRUNCATE+reload per dataset version fetch)
CREATE TABLE IF NOT EXISTS butler_apc_journal_year (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    unique_id     INTEGER NOT NULL,      -- dataset journal key (NOT dense, NOT ours)
    publisher     TEXT,
    issns         TEXT[]  NOT NULL,      -- normalized ISSN_1/ISSN_2
    journal       TEXT,
    oa_status     TEXT,                  -- Gold / Hybrid / _no_status_provided
    apc_provided  TEXT,                  -- yes / no / per page fee only
    apc_order     SMALLINT,              -- 1, 2 (mid-year transition), NULL = no price
    apc_year      SMALLINT NOT NULL,
    apc_date      DATE,                  -- collection / Wayback snapshot date
    prices        JSONB,                 -- [{"currency","price","original":bool}] originals only
    price_usd     NUMERIC,               -- dataset USD value (original or converted)
    apc_source    TEXT,                  -- Publisher website / Wayback Machine / ...
    dataset_version TEXT NOT NULL,       -- 'butler_v1' / 'butler_v2'
    fetched_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_butler_apc_jy_year ON butler_apc_journal_year(apc_year);
