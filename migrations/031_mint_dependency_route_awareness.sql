-- 031: make the mint dependency inspection route-aware (oxjob #647)
--
-- Migration 030 introduced public.endpoint_source_route with a RESTRICT FK
-- onto public.sources. A minted Source that has been routed must therefore be
-- a CLEAN ROLLBACK REFUSAL, not a mid-DELETE foreign-key abort: this migration
-- extends the two 029 dependency functions so the route count is part of the
-- dependency snapshot and of the clear-set definition. Replacing the bodies
-- here (rather than patching 029) keeps 029 replayable on fresh chains; the
-- executor pins the two bodies from THIS file once it is present.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE FUNCTION public.endpoint_source_mint_live_dependencies(requested_id BIGINT)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
STRICT
SET search_path = pg_catalog
AS $function$
DECLARE
    result JSONB;
    legacy_endpoint_count BIGINT := 0;
    scalar_endpoint_count BIGINT := 0;
BEGIN
    SELECT pg_catalog.jsonb_build_object(
        'source_exists', EXISTS (
            SELECT 1 FROM public.sources AS s WHERE s.id = requested_id
        ),
        'source_issn_count', (
            SELECT count(*) FROM public.source_issn AS si
             WHERE si.source_id = requested_id
        ),
        'source_datacite_id_count', (
            SELECT count(*) FROM public.source_datacite_id AS sd
             WHERE sd.source_id = requested_id
        ),
        'source_merge_loser_count', (
            SELECT count(*) FROM public.source_merge AS sm
             WHERE sm.loser_id = requested_id
        ),
        'source_merge_winner_count', (
            SELECT count(*) FROM public.source_merge AS sm
             WHERE sm.winner_id = requested_id
        ),
        'source_works_count_count', (
            SELECT count(*) FROM public.source_works_count AS swc
             WHERE swc.source_id = requested_id
        ),
        'source_publication_years_count', (
            SELECT count(*) FROM public.source_publication_years AS spy
             WHERE spy.source_id = requested_id
        ),
        'source_oa_override_count', (
            SELECT count(*) FROM public.source_oa_override AS soo
             WHERE soo.source_id = requested_id
        ),
        'endpoint_source_route_count', (
            SELECT count(*) FROM public.endpoint_source_route AS esr
             WHERE esr.source_id = requested_id
        )
    ) INTO result;

    -- These two stores are transitional and may be removed by a later schema
    -- migration. Dynamic inspection treats them only as deletion dependencies;
    -- it does not interpret either one as this mint's relationship.
    IF pg_catalog.to_regclass('public.source_endpoint') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM public.source_endpoint WHERE source_id = $1'
           INTO legacy_endpoint_count USING requested_id;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute AS a
         WHERE a.attrelid = 'public.endpoint'::REGCLASS
           AND a.attname = 'source_id' AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        EXECUTE 'SELECT count(*) FROM public.endpoint WHERE source_id = $1'
           INTO scalar_endpoint_count USING requested_id;
    END IF;
    RETURN result || pg_catalog.jsonb_build_object(
        'source_endpoint_count', legacy_endpoint_count,
        'endpoint_source_id_count', scalar_endpoint_count
    );
END;
$function$;

CREATE OR REPLACE FUNCTION public.endpoint_source_mint_dependencies_are_clear(value JSONB)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $function$
    SELECT value -> 'source_exists' = 'true'::JSONB
       AND value - 'source_exists' = pg_catalog.jsonb_build_object(
            'source_issn_count', 0,
            'source_datacite_id_count', 0,
            'source_merge_loser_count', 0,
            'source_merge_winner_count', 0,
            'source_works_count_count', 0,
            'source_publication_years_count', 0,
            'source_oa_override_count', 0,
            'source_endpoint_count', 0,
            'endpoint_source_id_count', 0,
            'endpoint_source_route_count', 0
       )
$function$;
