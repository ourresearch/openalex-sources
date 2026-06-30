-- 003 Crossref journals staging + ingest-issue log (oxjob #548, Phase 2)

-- Full-snapshot staging table: each fetch TRUNCATEs and reloads the current
-- api.crossref.org/journals feed. The sync job reads this and upserts into sources.
CREATE TABLE IF NOT EXISTS crossref_journal (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issns       TEXT[] NOT NULL,
    issn_l      TEXT,
    title       TEXT,
    publisher   TEXT,
    raw         JSONB,
    fetched_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_crossref_journal_issn_l ON crossref_journal(issn_l);

-- Breadcrumb for anomalies the upsert primitive can't resolve automatically
-- (e.g. an incoming journal whose ISSNs map to >1 existing source = merge candidate).
CREATE TABLE IF NOT EXISTS source_ingest_issue (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_feed        TEXT NOT NULL,         -- 'crossref', 'issn', 'doaj', ...
    issue_type         TEXT NOT NULL,         -- 'multi_match', ...
    issns              TEXT[],
    matched_source_ids BIGINT[],
    detail             TEXT,
    created_at         TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_source_ingest_issue_feed_type
    ON source_ingest_issue(source_feed, issue_type);
