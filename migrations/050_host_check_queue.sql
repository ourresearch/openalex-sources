-- 050 A change in Crossref's journal-level publisher queues the journal for a host check (oxjob #2693).
--
-- A source's publisher_id is written once and feeds only fill nulls, so a journal that moves to another publisher keeps
-- the old one (on 28 Nov 2026, 277 journals / ~906K works). Crossref's journal record (api.crossref.org/journals) names
-- the new publisher once the title transfers. jobs/sync_crossref_journals records the publisher string(s) Crossref
-- gives for each source here; when they change, the source goes on host_check_queue and jobs/host_check decides it
-- (moves only when Crossref's journal record, the registrant of the newest and of most of the journal's DOIs, and the
-- newest DOI's landing site all name one mapped publisher company; otherwise holds it with the reason).
-- Down: DROP TABLE host_check_queue; DROP TABLE source_crossref_publisher;

CREATE TABLE IF NOT EXISTS source_crossref_publisher (
    source_id   BIGINT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    publisher   TEXT NOT NULL,              -- Crossref journal-record publisher(s) for the source's ISSNs, sorted, ' | '-joined
    seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_at  TIMESTAMPTZ                 -- last run whose string differed from the one before
);

CREATE TABLE IF NOT EXISTS host_check_queue (
    id                 BIGSERIAL PRIMARY KEY,
    source_id          BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    crossref_before    TEXT,
    crossref_after     TEXT NOT NULL,
    publisher_id_then  BIGINT,              -- the source's publisher_id when queued
    queued_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    status             TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'moved', 'held', 'same_host')),
    checked_at         TIMESTAMPTZ,
    publisher_id_after BIGINT,              -- set when moved; revert = publisher_id back to publisher_id_then
    note               TEXT                 -- the evidence, or why it was held
);
CREATE UNIQUE INDEX IF NOT EXISTS host_check_queue_open ON host_check_queue (source_id) WHERE status = 'queued';
COMMENT ON TABLE host_check_queue IS
  'oxjob #2693: journals whose Crossref journal-record publisher changed; worked by jobs/host_check.';
