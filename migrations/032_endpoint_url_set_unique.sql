-- 032: enforce the 83.13 binding key on the endpoint table itself.
-- UNIQUE (pmh_url, pmh_set) among ACTIVE endpoints (partial indexes; retired/
-- quarantined rows keep their history without blocking re-use of a URL).
-- Per Casey's 2026-09-01 ask ("unique on PMH URL + PMH set, no duplicates")
-- and the Jason-approved shared doc.
--
-- Two partial indexes, not one composite: most endpoints are set-less, and a
-- plain UNIQUE (pmh_url, pmh_set) would treat NULL sets as distinct and allow
-- duplicate (same_url, NULL) rows -- exactly the case being banned.
--
-- NOT CONCURRENTLY: migrate.py applies each file inside a transaction, and
-- CREATE INDEX CONCURRENTLY cannot run in a transaction block (it would fail
-- the release phase and roll back the deploy). The table is ~6.2k rows, so the
-- build lock is single-digit milliseconds. Do not "optimize" this back.
--
-- Precondition verified against prod immediately pre-apply: zero duplicate
-- (pmh_url, coalesce(pmh_set,'')) groups among status='active' rows.
-- NOTE: written against current table name `endpoint`; the decided rename to
-- `oai_pmh_endpoint` is a separate migration (033+) and must re-point these
-- index names.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Base identity: one active row per bare URL when no set is scoped.
CREATE UNIQUE INDEX IF NOT EXISTS endpoint_active_url_base_key
    ON public.endpoint (pmh_url)
    WHERE status = 'active' AND pmh_set IS NULL;

-- Scoped identity: one active row per (URL, set).
CREATE UNIQUE INDEX IF NOT EXISTS endpoint_active_url_set_key
    ON public.endpoint (pmh_url, pmh_set)
    WHERE status = 'active' AND pmh_set IS NOT NULL;

COMMENT ON INDEX public.endpoint_active_url_base_key IS
    '83.13: unique active bare-URL endpoint (no set scope). mig 032.';
COMMENT ON INDEX public.endpoint_active_url_set_key IS
    '83.13: unique active (pmh_url, pmh_set) endpoint. mig 032.';

-- Rollback (forward-only runner: author as a new numbered migration):
--   DROP INDEX IF EXISTS public.endpoint_active_url_base_key;
--   DROP INDEX IF EXISTS public.endpoint_active_url_set_key;
