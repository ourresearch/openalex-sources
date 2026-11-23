-- 049 NDL's indexed-journal list as a registry feed (oxjob #2547, Casey 2026-11-23).
--
-- The National Diet Library publishes the list of journals its periodical-article index covers
-- (雑誌記事索引 採録誌一覧, https://www.ndl.go.jp/data/sakuin/sakuin_index, sairokushi_YYYYMM.tsv, ~27K titles:
-- NDL bib id, ISSNs, title, publisher, years). jobs/ndl_journals reloads it whole each run and fills, never
-- overwriting, the ISSN, ISSN-L, publisher text and country of the journals tied to a record.

CREATE TABLE IF NOT EXISTS ndl_journal (
    bibid          TEXT PRIMARY KEY,            -- NDL bibliographic id
    status         TEXT,                        -- c current, d ceased, u unknown
    issns          TEXT[] NOT NULL DEFAULT '{}',
    title          TEXT NOT NULL,               -- NDL's title statement
    publisher      TEXT,                        -- current publisher name (no place, years or parallel name)
    publisher_raw  TEXT,
    place_in_japan BOOLEAN NOT NULL DEFAULT false,
    first_year     INT,
    last_year      INT,
    edition        TEXT NOT NULL,               -- e.g. sairokushi_202610
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ndl_journal_issns_idx ON ndl_journal USING gin (issns);
COMMENT ON TABLE ndl_journal IS
  'oxjob #2547: NDL indexed-journal list (sairokushi). Full replace per run (jobs/ndl_journals).';

-- Which source each NDL record describes: 'issn' (the source owns one of the record's ISSNs, found by the job) or a
-- checked one-off bind (e.g. 'oxjob2547': title + publisher + years matched for ISSN-less MAG-era journals).
-- No FK on bibid: a record NDL drops simply stops contributing.
CREATE TABLE IF NOT EXISTS source_ndl_journal (
    source_id  BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    bibid      TEXT NOT NULL,
    matched_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, bibid)
);
CREATE INDEX IF NOT EXISTS source_ndl_journal_bibid_idx ON source_ndl_journal (bibid);
