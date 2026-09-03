-- 037: drop the legacy endpoint columns and the two legacy relationship stores. oxjob #83.13
--
-- Casey 09-01 delete list: repo_unique_id (done in 036), id_old, repo_request_id, contacted,
-- contacted_text ("contacted request ID columns"), source_endpoint ("source endpoint dot source
-- ID"), sources.endpoint_id ("source dot endpoint ID"). Casey 08-17: name ("not referred to at
-- all anymore"). Also drops the transitional `endpoint` view from 036 now that its last reader
-- (users-api /oaipmh-sets, ac69bb3 / v500) is on the new name.
--
-- Readers verified off these before apply: openalex-ingest (v143, ORM declares none of them),
-- walden CreateSources (4ca6046, reads oai_pmh_endpoint + endpoint_deletion_audit only),
-- users-api /oaipmh-sets (ac69bb3), jobs/delete_endpoints.py (patched same push). Kyle's local
-- retirement tool read `name` and `source_endpoint`; jobs/retire_endpoints.py replaces it.
--
-- Values are preserved: full exports of source_endpoint and of (id, id_old, repo_request_id,
-- contacted, contacted_text, name) and (sources.id, endpoint_id) are taken pre-apply into
-- ~/repos-prep/8313-fresh-eyes-20260901/legacy-exports-037/. The binding truth is
-- oai_pmh_endpoint.source_id (NOT NULL, FK) plus endpoint_deletion_audit for deleted ids.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $precheck$
BEGIN
    -- every active row is bound (035) and every source_endpoint link that still exists agrees
    -- with the scalar binding or points at a deleted endpoint; nothing is lost by dropping it.
    -- A disagreement is acceptable only where the scalar binding was written under review
    -- (a row in endpoint_source_backfill_audit): those are the corrections of wrong legacy links
    -- (Internet Archive, UMich quod, figshare global, the two Naturalis endpoints, ...).
    IF EXISTS (
        SELECT 1 FROM public.source_endpoint se
          JOIN public.oai_pmh_endpoint e ON e.id = se.endpoint_id
         WHERE e.source_id <> se.source_id
           AND NOT EXISTS (SELECT 1 FROM public.endpoint_source_backfill_audit a WHERE a.endpoint_id = e.id)
    ) THEN
        RAISE EXCEPTION '037: source_endpoint disagrees with an UNREVIEWED oai_pmh_endpoint.source_id; adjudicate before dropping';
    END IF;
END;
$precheck$;

DROP VIEW IF EXISTS public.endpoint;

DROP TABLE public.source_endpoint;

DROP INDEX IF EXISTS public.endpoint_id_old_idx;
ALTER TABLE public.oai_pmh_endpoint
    DROP COLUMN id_old,
    DROP COLUMN repo_request_id,
    DROP COLUMN contacted,
    DROP COLUMN contacted_text,
    DROP COLUMN name;

ALTER TABLE public.sources DROP COLUMN endpoint_id;

COMMENT ON TABLE public.oai_pmh_endpoint IS
    'OAI-PMH endpoint registry: one row per (pmh_url, pmh_set), each bound to exactly one source (source_id NOT NULL, FK). Legacy columns and the source_endpoint / sources.endpoint_id stores dropped by mig 037 (oxjob 83.13).';

-- Post-apply:
--   SELECT to_regclass('public.source_endpoint'), to_regclass('public.endpoint');   -- NULL, NULL
--   SELECT count(*) FROM information_schema.columns WHERE table_name='oai_pmh_endpoint'
--     AND column_name IN ('id_old','repo_request_id','contacted','contacted_text','name');  -- 0
--   SELECT count(*) FROM information_schema.columns WHERE table_name='sources' AND column_name='endpoint_id'; -- 0
-- Rollback: forward-only; values restorable from the pre-apply exports.
