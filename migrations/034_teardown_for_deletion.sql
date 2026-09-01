-- 034: make the plan of record (DELETE orphans, then NOT NULL) executable. oxjob #83.13
--
-- Casey's plan (2026-09-01 call) has no retirement state, no mint ledger and no route
-- table. Migrations 028-031 shipped those as guard machinery for the attended 08-31
-- writes, which are done and receipted. What they leave behind blocks the remaining
-- work:
--   * 028's trigger forbids DELETE of any status='retired' row and forbids leaving
--     'retired'. It also renamed endpoint_deletion_audit away, so jobs/delete_endpoints.py
--     fails its own preflight. The 455 quarantine rows carry "reversible" in their reason
--     while being irreversible.
--   * 029's five triggers on public.sources (a table written by crossref/doaj/datacite
--     sync, users-api curation and the repo-request automation) gate every insert/delete
--     against the mint ledger. Both mint runs are complete (45 receipts); the ledger stays
--     as history, the gates go.
--   * 030's endpoint_source_route has RESTRICT FKs onto endpoint and sources: 41 active
--     orphans and 81 retired rows cannot be deleted while it exists. Zero readers
--     (pg_stat_statements since 06-30). The scalar endpoint.source_id is the binding.
--
-- Preconditions (verified 2026-09-01, re-verify pre-apply):
--   SELECT count(*) FROM endpoint_source_mint_run;                     -- 2, both complete
--   SELECT count(*) FROM endpoint_source_mint_receipt;                 -- 45
--   -- CSV exports taken: route-adjudication-20260901.csv (84 rows),
--   --                    endpoint_source_route-full-export-20260901.csv (4,608 rows)
--   SELECT count(*) FROM endpoint_deletion_audit_archived_pre_retirement; -- 0
--
-- Audit tables (endpoint_retirement_audit, endpoint_source_mint_*,
-- endpoint_source_backfill_audit) are NOT touched: append-only history stays.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $precheck$
BEGIN
    IF (SELECT count(*) FROM public.endpoint_source_mint_run) <> 2
       OR (SELECT count(*) FROM public.endpoint_source_mint_receipt) <> 45 THEN
        RAISE EXCEPTION '034: mint ledger differs from the reviewed state (expect 2 runs / 45 receipts)';
    END IF;
    IF EXISTS (SELECT 1 FROM public.endpoint_deletion_audit_archived_pre_retirement) THEN
        RAISE EXCEPTION '034: archived deletion audit is not empty; review before restoring';
    END IF;
END;
$precheck$;

-- 1. sources: remove the mint gates. Ledger validation triggers on the mint tables stay.
DROP TRIGGER IF EXISTS endpoint_source_mint_source_require_audit_first   ON public.sources;
DROP TRIGGER IF EXISTS endpoint_source_mint_source_require_receipt       ON public.sources;
DROP TRIGGER IF EXISTS endpoint_source_mint_source_require_rollback      ON public.sources;
DROP TRIGGER IF EXISTS endpoint_source_mint_source_delete_require_receipt ON public.sources;
DROP TRIGGER IF EXISTS endpoint_source_mint_source_no_truncate           ON public.sources;

-- 2. endpoint: retirement stays a receipted transition INTO 'retired'; DELETE and leaving
--    'retired' become allowed (the deletion executor writes its own receipt).
CREATE OR REPLACE FUNCTION public.enforce_endpoint_retirement_receipt()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    receipt_matches BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status = 'retired' THEN
            RAISE EXCEPTION 'retired endpoint INSERT is forbidden';
        END IF;
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        -- 034: deletion is the exit, but never silent (Codex 09-01: active-row DELETEs were
        -- unaudited). The delete executor writes its receipt in the same transaction.
        IF NOT EXISTS (
            SELECT 1 FROM ONLY public.endpoint_deletion_audit d
             WHERE d.endpoint_id = OLD.id
               AND d.xmin = pg_catalog.pg_current_xact_id()::xid
        ) THEN
            RAISE EXCEPTION 'endpoint DELETE requires a same-transaction deletion receipt';
        END IF;
        RETURN NULL;
    END IF;
    IF OLD.status = 'retired' THEN
        RETURN NULL;                                  -- 034: un-retire / edits allowed
    END IF;
    IF NEW.status = 'retired' THEN
        SELECT EXISTS (
            SELECT 1
            FROM ONLY public.endpoint_retirement_audit a
            WHERE a.endpoint_id = OLD.id
              AND a.status_after = 'retired'
              AND a.retirement_reason = NEW.retirement_reason
              AND a.retired_at = NEW.retired_at
              AND a.database_name = pg_catalog.current_database()
              AND a.xmin = pg_catalog.pg_current_xact_id()::xid
        ) INTO receipt_matches;
        IF NOT receipt_matches THEN
            RAISE EXCEPTION 'retired endpoint requires a same-transaction receipt';
        END IF;
    END IF;
    RETURN NULL;
END;
$function$;

-- 2b. Fire the trigger only when identity/lifecycle columns change, not on the harvester's
--     ~300k health UPDATEs (Codex 09-01). Same function, narrower event list.
DROP TRIGGER IF EXISTS endpoint_retirement_requires_receipt ON public.endpoint;
CREATE CONSTRAINT TRIGGER endpoint_retirement_requires_receipt
AFTER INSERT OR DELETE OR UPDATE OF status, retirement_reason, retired_at, source_id, pmh_url, pmh_set, id
ON public.endpoint DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.enforce_endpoint_retirement_receipt();

-- 3. Restore the deletion audit table the delete executor expects.
DROP TRIGGER IF EXISTS endpoint_deletion_audit_archive_no_insert
    ON public.endpoint_deletion_audit_archived_pre_retirement;
ALTER TABLE public.endpoint_deletion_audit_archived_pre_retirement
    RENAME TO endpoint_deletion_audit;

-- 4. Route table: superseded by scalar endpoint.source_id. Exported before drop.
DROP TABLE public.endpoint_source_route;

-- 5. 031 made the mint dependency functions route-aware; drop the route key so they
--    remain callable (they are only used by the (now removed) sources triggers and by
--    the rollback executor, which must not run after this migration).
CREATE OR REPLACE FUNCTION public.endpoint_source_mint_live_dependencies(requested_id BIGINT)
RETURNS JSONB LANGUAGE plpgsql STABLE STRICT SET search_path = pg_catalog
AS $function$
DECLARE
    result JSONB;
    legacy_endpoint_count BIGINT := 0;
    scalar_endpoint_count BIGINT := 0;
BEGIN
    SELECT pg_catalog.jsonb_build_object(
        'source_exists', EXISTS (SELECT 1 FROM public.sources AS s WHERE s.id = requested_id),
        'source_issn_count', (SELECT count(*) FROM public.source_issn AS si WHERE si.source_id = requested_id),
        'source_datacite_id_count', (SELECT count(*) FROM public.source_datacite_id AS sd WHERE sd.source_id = requested_id),
        'source_merge_loser_count', (SELECT count(*) FROM public.source_merge AS sm WHERE sm.loser_id = requested_id),
        'source_merge_winner_count', (SELECT count(*) FROM public.source_merge AS sm WHERE sm.winner_id = requested_id),
        'source_works_count_count', (SELECT count(*) FROM public.source_works_count AS swc WHERE swc.source_id = requested_id),
        'source_publication_years_count', (SELECT count(*) FROM public.source_publication_years AS spy WHERE spy.source_id = requested_id),
        'source_oa_override_count', (SELECT count(*) FROM public.source_oa_override AS soo WHERE soo.source_id = requested_id)
    ) INTO result;
    IF pg_catalog.to_regclass('public.source_endpoint') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM public.source_endpoint WHERE source_id = $1'
           INTO legacy_endpoint_count USING requested_id;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = 'public.endpoint'::REGCLASS AND a.attname = 'source_id'
                  AND a.attnum > 0 AND NOT a.attisdropped) THEN
        EXECUTE 'SELECT count(*) FROM public.endpoint WHERE source_id = $1'
           INTO scalar_endpoint_count USING requested_id;
    END IF;
    RETURN result || pg_catalog.jsonb_build_object(
        'source_endpoint_count', legacy_endpoint_count,
        'endpoint_source_id_count', scalar_endpoint_count);
END;
$function$;

CREATE OR REPLACE FUNCTION public.endpoint_source_mint_dependencies_are_clear(value JSONB)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog
AS $function$
    SELECT value -> 'source_exists' = 'true'::JSONB
       AND value - 'source_exists' = pg_catalog.jsonb_build_object(
            'source_issn_count', 0, 'source_datacite_id_count', 0,
            'source_merge_loser_count', 0, 'source_merge_winner_count', 0,
            'source_works_count_count', 0, 'source_publication_years_count', 0,
            'source_oa_override_count', 0, 'source_endpoint_count', 0,
            'endpoint_source_id_count', 0)
$function$;

COMMENT ON TABLE public.endpoint_retirement_audit IS
    '83.13: append-only history of active->retired transitions (mig 028). Since mig 034 retired rows may be deleted or un-retired; deletions are receipted in endpoint_deletion_audit.';

-- Post-apply demonstration:
--   SELECT tgname FROM pg_trigger WHERE tgrelid='sources'::regclass AND NOT tgisinternal;  -- 0 rows
--   SELECT to_regclass('endpoint_deletion_audit'), to_regclass('endpoint_source_route');    -- table, NULL
--   BEGIN; DELETE FROM endpoint WHERE id='<any id>'; ROLLBACK;  -- FAILS: requires a receipt (expected)
--   (the executor jobs/delete_endpoints.py writes the receipt in-transaction and succeeds)
-- Rollback: forward-only; the dropped triggers/table are re-creatable from 029/030 text,
-- route rows from endpoint_source_route-full-export-20260901.csv.
