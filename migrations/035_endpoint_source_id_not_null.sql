-- 035: every endpoint row has a valid source. oxjob #83.13, ACCEPTANCE boxes 2-4.
--
-- Casey 08-17: "Every repo endpoint must have a source." Casey 09-01: "source_id definitely
-- can't be null. There's the foreign key constraint." Jason 08-29: "every active endpoint has
-- a source (FK-enforced)". The FK exists since migration 022 (endpoint_source_id_fkey,
-- ON DELETE RESTRICT). This migration adds NOT NULL, which makes an insert without a source
-- fail at the database.
--
-- Precondition reached 2026-09-01 ~14:35 PT after the receipted runs of that afternoon:
--   bind 45 minted (run 322ca80a), delete 1,445 (run f5e54874), mint+bind 113 (run 34877ee7),
--   mint+bind 48 (run 72aaad31), delete DataCite OAI (run 1097f578).
--   SELECT count(*) FROM endpoint WHERE source_id IS NULL;   -- 0 (all statuses)
--
-- Retired rows keep their binding (retirement preserves source_id), so the constraint is
-- column-wide, not partial. Same transactional conventions as 032-034.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $precheck$
BEGIN
    IF EXISTS (SELECT 1 FROM public.endpoint WHERE source_id IS NULL) THEN
        RAISE EXCEPTION '035: endpoint rows with NULL source_id exist; resolve or delete before applying';
    END IF;
    IF EXISTS (SELECT 1 FROM public.endpoint e LEFT JOIN public.sources s ON s.id = e.source_id WHERE s.id IS NULL) THEN
        RAISE EXCEPTION '035: endpoint rows reference a missing source; FK integrity broken';
    END IF;
END;
$precheck$;

ALTER TABLE public.endpoint
    ALTER COLUMN source_id SET NOT NULL;

COMMENT ON COLUMN public.endpoint.source_id IS
    '83.13: the one Source this endpoint row (pmh_url, pmh_set) feeds. FK to sources.id (mig 022), NOT NULL since mig 035.';

-- Post-apply demonstration (expect failure):
--   INSERT INTO endpoint (id, pmh_url, status) VALUES ('t035', 'https://example.org/oai', 'active');
--     -> null value in column "source_id" violates not-null constraint
-- Rollback (forward-only runner): ALTER TABLE public.endpoint ALTER COLUMN source_id DROP NOT NULL;
