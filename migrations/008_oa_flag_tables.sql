-- 008 OA-flag mapping tables (oxjob #548)
-- Carried over from Databricks so flag recomputation is app-owned. The upstream
-- producers (J-STAGE crawl, OJS detection, OA-rate analytics) stay outside this
-- app; refresh = re-run scripts/import_oa_flag_tables.py, then jobs/apply_oa_flags.

CREATE TABLE IF NOT EXISTS jstage_journal (
    issn          TEXT PRIMARY KEY,
    oa_start_year INTEGER,
    oa_end_year   INTEGER        -- 9999 = open-ended
);

CREATE TABLE IF NOT EXISTS ojs_journal (
    issn  TEXT PRIMARY KEY,
    is_oa BOOLEAN
);

-- Effective high-OA-rate list: the Databricks base table with the unpaywall
-- journal-curation-request overlay already applied at export time.
CREATE TABLE IF NOT EXISTS high_oa_rate_issn (
    issn_l             TEXT PRIMARY KEY,
    is_oa_high_oa_rate BOOLEAN NOT NULL,
    start_year         INTEGER
);

-- Operational snapshot from Databricks sources_api (drives is_fully_open_in_jstage,
-- which compares the J-STAGE OA window against each source's publication span).
CREATE TABLE IF NOT EXISTS source_publication_years (
    source_id              BIGINT PRIMARY KEY,
    first_publication_year INTEGER,
    last_publication_year  INTEGER,
    as_of                  DATE
);
