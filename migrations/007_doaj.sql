-- 007 DOAJ staging (oxjob #548, Phase 2)
-- Full-snapshot staging for the public DOAJ journal metadata CSV (doaj.org/csv).
CREATE TABLE IF NOT EXISTS doaj_journal (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issns         TEXT[] NOT NULL,
    title         TEXT,
    publisher     TEXT,
    license       TEXT,        -- normalized: cc-by, cc-by-nc, ..., public-domain
    oa_start_year INTEGER,
    country       TEXT,
    fetched_at    TIMESTAMPTZ DEFAULT now()
);
