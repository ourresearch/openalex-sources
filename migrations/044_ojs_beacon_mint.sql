-- 044 ojs_beacon_mint: journals minted from the PKP Beacon are OA by default (oxjob #1539)
-- Casey, 2026-10-04: a journal we minted because the Beacon saw it running OJS is open
-- unless we have evidence otherwise (80-page sample of the low-rate mints: 0 paywalls).
-- Membership here is the fifth input to is_oa_high_oa_rate in jobs/apply_oa_flags, so
-- CreateWorksBase colours its articles diamond/gold (walden reads is_oa_high_oa_rate and
-- is_in_doaj, never sources.is_oa). Rows come from jobs/ojs_beacon mint (go-forward) and
-- scripts/backfill_ojs_beacon_mint.py (the 2026-09-30 / 2026-10-04 receipts).

CREATE TABLE IF NOT EXISTS ojs_beacon_mint (
    source_id  BIGINT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    edition    TEXT NOT NULL,          -- Beacon edition the candidate came from (e.g. 2026-07-18)
    receipt    TEXT,                   -- data/ojs_beacon/receipt-*.csv that recorded the mint
    minted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
