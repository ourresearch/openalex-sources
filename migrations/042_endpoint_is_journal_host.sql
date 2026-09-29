-- 042: journal-host flag on oai_pmh_endpoint (oxjob #1409, Casey 2026-09-28).
-- Marks a (pmh_url, pmh_set) row as the journal's own platform, e.g. a
-- single-journal OJS install. walden CreateSources mirrors it into
-- openalex.sources.endpoint_to_source, and CreateWorksBase treats repo locations
-- from flagged endpoints on a journal source as publisher copies
-- (host_type='publisher', version='publishedVersion'). This replaces the
-- hardcoded source-id lists in CreateWorksBase (oxjob #805), so adding a
-- journal is a row here, not a walden deploy.
--
-- Opt-in: default false, set only on endpoints verified to serve exactly one
-- journal. Nothing is derived from existing journal bindings, some of which
-- are aggregators bound to a journal source (Kew, Lincoln; the ~40 OJS-host
-- misbindings found by oxjob #1404); flagging those would label a whole
-- aggregator as that journal's publisher.
ALTER TABLE oai_pmh_endpoint
  ADD COLUMN IF NOT EXISTS is_journal_host BOOLEAN NOT NULL DEFAULT false;
COMMENT ON COLUMN oai_pmh_endpoint.is_journal_host IS
  'oxjob #1409: this (pmh_url, pmh_set) is the journal''s own platform; walden labels its locations publisher / publishedVersion. Opt-in; set only on endpoints verified to serve one journal.';

-- The seven endpoints gated by the hardcoded lists today (or about to be).
DO $$
DECLARE n int;
BEGIN
  UPDATE oai_pmh_endpoint SET is_journal_host = true
  WHERE id IN (
    'p4u8mjn3jyccjrrapwpq',  -- Chemical Engineering Transactions (#805 wave 1)
    'hvxusvo8txxz5cyjyvkd',  -- Journal of Mining Institute (#805 wave 2)
    '3ri5enmermi5hxf4zd2r',  -- Maestro y Sociedad (#1404 batch 1)
    'acwvqnicgc7uic72gudx',  -- RBONE (#1404 batch 1)
    'jvj5fm8fwj76svnt7u5o',  -- EXCLI Journal (#1404 batch 1)
    '83ace47s4gctbcnwhbph',  -- Alpine and Mediterranean Quaternary (#1404 batch 1)
    'td9eipcxzuejdm8snfue'   -- o-bib (#1404 batch 1)
  );
  GET DIAGNOSTICS n = ROW_COUNT;
  IF n <> 7 THEN
    -- no percent sign anywhere in this file, comments included: migrate.py runs it
    -- through exec_driver_sql, whose empty parameter dict makes psycopg2 read a
    -- percent sign as a bind marker (the first 042 release failed on RAISE's placeholder)
    RAISE EXCEPTION USING MESSAGE = '042: expected to flag 7 endpoints, flagged ' || n;
  END IF;
END $$;
