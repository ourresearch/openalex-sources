-- 036: rename the registry table to oai_pmh_endpoint. oxjob #83.13
--
-- Casey 09-01: "rename that table ... O-A-I underscore P-M-H underscore endpoint" (singular,
-- "Right now it's singular"); naming settled in the oxjob log 08-31. Casey's warning, verbatim:
-- "Second you rename that and it can't find the table, it's going to break." So:
--   * openalex-ingest is repointed to the new name in the same window (ORM __tablename__).
--   * walden CreateSources is repointed to openalex_sources.public.oai_pmh_endpoint.
--   * users-api /oaipmh-sets (Jason, 08-30) still says `endpoint`; a plain updatable VIEW named
--     endpoint stands in until he repoints, then a later migration drops the view.
-- Also drops repo_unique_id (Casey 09-01 delete list; 5,828 populated, indexed, zero readers:
-- not in the ingest ORM, not in users-api, not in walden). The other listed columns
-- (id_old, repo_request_id, contacted, contacted_text, name) wait for the ingest deploy and
-- Kyle's tools, in 037.
--
-- FKs (source_endpoint.endpoint_id) follow the rename. Index and constraint names keep their
-- endpoint_* prefix; renaming them buys nothing and would re-point 032/033 docs.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $precheck$
BEGIN
    IF pg_catalog.to_regclass('public.oai_pmh_endpoint') IS NOT NULL THEN
        RAISE EXCEPTION '036: public.oai_pmh_endpoint already exists';
    END IF;
    IF (SELECT relkind FROM pg_catalog.pg_class WHERE oid = 'public.endpoint'::regclass) <> 'r' THEN
        RAISE EXCEPTION '036: public.endpoint is not a plain table';
    END IF;
END;
$precheck$;

ALTER TABLE public.endpoint RENAME TO oai_pmh_endpoint;

DROP INDEX IF EXISTS public.endpoint_repo_unique_id_idx;
ALTER TABLE public.oai_pmh_endpoint DROP COLUMN repo_unique_id;

-- Transitional alias for consumers not yet repointed (users-api /oaipmh-sets). Simple
-- single-table view: automatically updatable, so any UPDATE/DELETE through it still works
-- and still fires the oai_pmh_endpoint triggers.
CREATE VIEW public.endpoint AS SELECT * FROM public.oai_pmh_endpoint;

COMMENT ON TABLE public.oai_pmh_endpoint IS
    'OAI-PMH endpoint registry (one row per (pmh_url, pmh_set), each bound to exactly one source). Renamed from endpoint by mig 036 (oxjob 83.13).';
COMMENT ON VIEW public.endpoint IS
    'TRANSITIONAL alias of oai_pmh_endpoint for consumers not yet repointed (users-api /oaipmh-sets). Drop when none remain. mig 036.';

-- Post-apply:
--   SELECT relkind FROM pg_class WHERE relname IN ('endpoint','oai_pmh_endpoint');  -- v, r
--   SELECT count(*) FROM endpoint; SELECT count(*) FROM oai_pmh_endpoint;           -- equal
-- Rollback (forward-only runner): DROP VIEW public.endpoint; ALTER TABLE public.oai_pmh_endpoint RENAME TO endpoint;
--   (repo_unique_id is not restorable; values exported in 8313-fresh-eyes-20260901/repo_unique_id-export-20260901.csv)
