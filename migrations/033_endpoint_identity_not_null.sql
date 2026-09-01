-- 033: close the status=NULL bypass of migration 032 (oxjob #83.13).
--
-- 032's unique indexes are partial on status='active'. Codex (gpt-5.6-sol,
-- 2026-09-01) demonstrated that two rows with identical (pmh_url, pmh_set) and an
-- explicit status=NULL are both accepted, because NULL never matches the index
-- predicate. This migration makes status mandatory and confines it to the two
-- lifecycle values the retirement executor (mig 028) already enforces in code.
--
-- It also requires every ACTIVE row to carry a non-blank pmh_url. NOT NULL on
-- pmh_url globally is NOT applied: 9 retired rows carry NULL pmh_url today
-- (all status='retired', in_walden=false; one still bound to source 4306400370).
-- Those are cruft for the box-7 cleanup pass, not this migration's job.
--
-- Preconditions verified against prod 2026-09-01 (post-032):
--   SELECT count(*) FROM endpoint WHERE status IS NULL;                      -- 0
--   SELECT status, count(*) FROM endpoint GROUP BY 1;                        -- active 5307 / retired 920, nothing else
--   SELECT count(*) FROM endpoint WHERE status='active'
--     AND (pmh_url IS NULL OR btrim(pmh_url)='');                            -- 0
--
-- Same transactional constraints as 032: migrate.py runs this inside one
-- transaction; the table is ~6.2k rows, so the ACCESS EXCLUSIVE lock for
-- SET NOT NULL is milliseconds. Every object fully qualified (mig 028 convention).

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

-- Fail closed if prod drifted since the preconditions above were measured.
DO $precheck$
BEGIN
    IF EXISTS (SELECT 1 FROM public.endpoint WHERE status IS NULL) THEN
        RAISE EXCEPTION '033: endpoint rows with NULL status exist; resolve before applying';
    END IF;
    IF EXISTS (SELECT 1 FROM public.endpoint WHERE status NOT IN ('active','retired')) THEN
        RAISE EXCEPTION '033: endpoint rows with unknown status exist; resolve before applying';
    END IF;
    IF EXISTS (SELECT 1 FROM public.endpoint
               WHERE status = 'active' AND (pmh_url IS NULL OR btrim(pmh_url) = '')) THEN
        RAISE EXCEPTION '033: active endpoint rows with blank pmh_url exist; resolve before applying';
    END IF;
END;
$precheck$;

ALTER TABLE public.endpoint
    ALTER COLUMN status SET DEFAULT 'active',
    ALTER COLUMN status SET NOT NULL;

-- No new value CHECK: mig 028's endpoint_status_allowed already limits status to
-- ('active','retired') and only admitted NULL; NOT NULL above closes that gap.

ALTER TABLE public.endpoint
    ADD CONSTRAINT endpoint_active_requires_url_check
        CHECK (status <> 'active' OR (pmh_url IS NOT NULL AND btrim(pmh_url) <> ''));

-- Codex 09-01: (url, NULL) and (url, '') would land in different 032 indexes and coexist.
-- 0 rows have a blank or untrimmed pmh_set today (verified). Same rule 030 had on routes.
ALTER TABLE public.endpoint
    ADD CONSTRAINT endpoint_pmh_set_nonblank_check
        CHECK (pmh_set IS NULL OR (btrim(pmh_set) <> '' AND pmh_set = btrim(pmh_set)));

COMMENT ON COLUMN public.endpoint.status IS
    'Endpoint lifecycle: active or retired. NOT NULL since mig 033 (closes the status=NULL bypass of the 032 unique indexes).';
COMMENT ON CONSTRAINT endpoint_active_requires_url_check ON public.endpoint IS
    '83.13: an active endpoint must have a non-blank pmh_url. mig 033.';

-- Post-apply demonstration (run manually, expect BOTH to fail):
--   INSERT INTO endpoint (id, pmh_url, status) VALUES ('t1', 'https://x/oai', NULL);
--     -> null value in column "status" violates not-null constraint
--   INSERT INTO endpoint (id, pmh_url, status) VALUES ('t2', '', 'active');
--     -> violates check constraint "endpoint_active_requires_url_check"
--
-- Rollback (forward-only runner: author as a new numbered migration):
--   ALTER TABLE public.endpoint DROP CONSTRAINT IF EXISTS endpoint_pmh_set_nonblank_check;
--   ALTER TABLE public.endpoint DROP CONSTRAINT IF EXISTS endpoint_active_requires_url_check;
--   ALTER TABLE public.endpoint ALTER COLUMN status DROP NOT NULL;
