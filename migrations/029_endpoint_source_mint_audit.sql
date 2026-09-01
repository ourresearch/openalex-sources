-- 029: append-only, source-only endpoint Source mint ledger (oxjob #647)
--
-- This migration is schema and guard logic only. It never mints a Source and
-- never changes either endpoint relationship store. The durable product of a
-- mint is an immutable receipt mapping endpoint_id to minted_source_id. A
-- later, separately reviewed route migration may consume those receipts when
-- the canonical (endpoint, set) relationship schema exists.
--
-- A mint transaction must insert one run header and every reviewed intent
-- before its first Source INSERT. Deferred checks require the exact intent and
-- receipt sets at commit, so an authorization cannot be committed for later
-- use. Rollback follows the same run -> all intents -> deletes -> receipts
-- protocol and never updates or deletes endpoint rows. All snapshots are
-- materialized JSONB; no warehouse time travel is used.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE FUNCTION public.endpoint_source_mint_jsonb_sha256(value JSONB)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $function$
    SELECT pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to(value::TEXT, 'UTF8')),
        'hex'
    )
$function$;

CREATE FUNCTION public.endpoint_source_mint_live_source_snapshot(requested_id BIGINT)
RETURNS JSONB
LANGUAGE sql
STABLE
STRICT
SET search_path = pg_catalog
AS $function$
    SELECT pg_catalog.jsonb_build_object(
        'source', pg_catalog.to_jsonb(s),
        'source_issns', COALESCE(
            (
                SELECT pg_catalog.jsonb_agg(
                    pg_catalog.to_jsonb(si) ORDER BY si.issn
                )
                  FROM public.source_issn AS si
                 WHERE si.source_id = s.id
            ),
            '[]'::JSONB
        )
    )
      FROM public.sources AS s
     WHERE s.id = requested_id
$function$;

CREATE FUNCTION public.endpoint_source_mint_live_dependencies(requested_id BIGINT)
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

CREATE FUNCTION public.endpoint_source_mint_live_transitional_routes(
    requested_endpoint_id TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
STRICT
SET search_path = pg_catalog
AS $function$
DECLARE
    scalar_source_id BIGINT := NULL;
    normalized_source_id BIGINT := NULL;
    legacy_source_ids JSONB := '[]'::JSONB;
BEGIN
    -- These are deprecated route surfaces, not the mint output. Observe them
    -- only to prevent a redundant Source when an endpoint is already routed.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute AS a
         WHERE a.attrelid = 'public.endpoint'::REGCLASS
           AND a.attname = 'source_id' AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        EXECUTE 'SELECT source_id FROM public.endpoint WHERE id = $1'
           INTO scalar_source_id USING requested_endpoint_id;
    END IF;
    IF pg_catalog.to_regclass('public.source_endpoint') IS NOT NULL THEN
        EXECUTE 'SELECT source_id FROM public.source_endpoint WHERE endpoint_id = $1'
           INTO normalized_source_id USING requested_endpoint_id;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute AS a
         WHERE a.attrelid = 'public.sources'::REGCLASS
           AND a.attname = 'endpoint_id' AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        EXECUTE
            'SELECT COALESCE(jsonb_agg(id ORDER BY id), ''[]''::jsonb) '
            'FROM public.sources WHERE endpoint_id = $1'
           INTO legacy_source_ids USING requested_endpoint_id;
    END IF;
    RETURN pg_catalog.jsonb_build_object(
        'scalar_source_id', scalar_source_id,
        'source_endpoint_source_id', normalized_source_id,
        'legacy_source_ids', legacy_source_ids
    );
END;
$function$;

CREATE FUNCTION public.endpoint_source_mint_transitional_routes_are_clear(value JSONB)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $function$
    SELECT value = pg_catalog.jsonb_build_object(
        'scalar_source_id', NULL,
        'source_endpoint_source_id', NULL,
        'legacy_source_ids', '[]'::JSONB
    )
$function$;

CREATE TABLE public.endpoint_source_mint_run (
    run_id                       UUID        PRIMARY KEY,
    expected_row_count           INTEGER     NOT NULL,
    approved_plan                JSONB       NOT NULL,
    approved_scope               JSONB       NOT NULL,
    prewrite_snapshot            JSONB       NOT NULL,
    manifest_sha256              TEXT        NOT NULL,
    live_review_sha256           TEXT        NOT NULL,
    plan_sha256                  TEXT        NOT NULL,
    scope_sha256                 TEXT        NOT NULL,
    prewrite_snapshot_sha256     TEXT        NOT NULL,
    run_approval_ref             TEXT        NOT NULL,
    change_ref                   TEXT        NOT NULL,
    writer_pause_ref             TEXT        NOT NULL,
    actor                        TEXT        NOT NULL,
    hash_algorithm               TEXT        NOT NULL DEFAULT 'oa-jsonb-sha256-v1',
    audit_schema_version         SMALLINT    NOT NULL DEFAULT 1,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT endpoint_source_mint_run_count CHECK (expected_row_count > 0),
    CONSTRAINT endpoint_source_mint_run_plan_shape CHECK (
        jsonb_typeof(approved_plan) = 'object'
        AND jsonb_typeof(approved_plan -> 'rows') = 'array'
        AND jsonb_array_length(approved_plan -> 'rows') = expected_row_count
    ),
    CONSTRAINT endpoint_source_mint_run_scope_shape CHECK (
        jsonb_typeof(approved_scope) = 'object'
        AND jsonb_typeof(approved_scope -> 'endpoint_ids') = 'array'
        AND jsonb_array_length(approved_scope -> 'endpoint_ids') = expected_row_count
    ),
    CONSTRAINT endpoint_source_mint_run_prewrite_shape CHECK (
        jsonb_typeof(prewrite_snapshot) = 'object'
        AND jsonb_typeof(prewrite_snapshot -> 'rows') = 'array'
        AND jsonb_array_length(prewrite_snapshot -> 'rows') = expected_row_count
    ),
    CONSTRAINT endpoint_source_mint_run_manifest_hash
        CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT endpoint_source_mint_run_live_review_hash
        CHECK (live_review_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT endpoint_source_mint_run_plan_hash CHECK (
        plan_sha256 ~ '^[0-9a-f]{64}$'
        AND plan_sha256 = public.endpoint_source_mint_jsonb_sha256(approved_plan)
    ),
    CONSTRAINT endpoint_source_mint_run_scope_hash CHECK (
        scope_sha256 ~ '^[0-9a-f]{64}$'
        AND scope_sha256 = public.endpoint_source_mint_jsonb_sha256(approved_scope)
    ),
    CONSTRAINT endpoint_source_mint_run_prewrite_hash CHECK (
        prewrite_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND prewrite_snapshot_sha256 =
            public.endpoint_source_mint_jsonb_sha256(prewrite_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_run_refs CHECK (
        btrim(run_approval_ref) <> '' AND btrim(change_ref) <> ''
        AND btrim(writer_pause_ref) <> '' AND btrim(actor) <> ''
    ),
    CONSTRAINT endpoint_source_mint_run_algorithm
        CHECK (hash_algorithm = 'oa-jsonb-sha256-v1'),
    CONSTRAINT endpoint_source_mint_run_version CHECK (audit_schema_version = 1)
);

CREATE TABLE public.endpoint_source_mint_intent (
    run_id                       UUID        NOT NULL,
    endpoint_id                  TEXT        NOT NULL,
    endpoint_snapshot            JSONB       NOT NULL,
    endpoint_snapshot_sha256     TEXT        NOT NULL,
    transitional_route_snapshot  JSONB       NOT NULL,
    transitional_route_sha256    TEXT        NOT NULL,
    source_spec                  JSONB       NOT NULL,
    source_spec_sha256           TEXT        NOT NULL,
    evidence_sha256              TEXT        NOT NULL,
    confidence                   TEXT        NOT NULL,
    manifest_sha256              TEXT        NOT NULL,
    live_review_sha256           TEXT        NOT NULL,
    plan_sha256                  TEXT        NOT NULL,
    scope_sha256                 TEXT        NOT NULL,
    hash_algorithm               TEXT        NOT NULL DEFAULT 'oa-jsonb-sha256-v1',
    audit_schema_version         SMALLINT    NOT NULL DEFAULT 1,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT endpoint_source_mint_intent_pkey PRIMARY KEY (run_id, endpoint_id),
    CONSTRAINT endpoint_source_mint_intent_run_fkey FOREIGN KEY (run_id)
        REFERENCES public.endpoint_source_mint_run(run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_mint_intent_endpoint_once UNIQUE (endpoint_id),
    CONSTRAINT endpoint_source_mint_intent_spec_once UNIQUE (source_spec_sha256),
    CONSTRAINT endpoint_source_mint_intent_endpoint_shape CHECK (
        jsonb_typeof(endpoint_snapshot) = 'object'
        AND endpoint_snapshot ->> 'id' = endpoint_id
    ),
    CONSTRAINT endpoint_source_mint_intent_endpoint_hash CHECK (
        endpoint_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND endpoint_snapshot_sha256 =
            public.endpoint_source_mint_jsonb_sha256(endpoint_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_intent_route_shape CHECK (
        jsonb_typeof(transitional_route_snapshot) = 'object'
        AND transitional_route_snapshot ?& ARRAY[
            'scalar_source_id', 'source_endpoint_source_id', 'legacy_source_ids'
        ]::TEXT[]
        AND jsonb_typeof(transitional_route_snapshot -> 'legacy_source_ids') = 'array'
    ),
    CONSTRAINT endpoint_source_mint_intent_route_hash CHECK (
        transitional_route_sha256 ~ '^[0-9a-f]{64}$'
        AND transitional_route_sha256 =
            public.endpoint_source_mint_jsonb_sha256(transitional_route_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_intent_spec_shape CHECK (
        jsonb_typeof(source_spec) = 'object'
        AND source_spec ?& ARRAY[
            'display_name', 'homepage_url', 'type', 'country_code', 'institution_id'
        ]::TEXT[]
        AND source_spec - ARRAY[
            'display_name', 'homepage_url', 'type', 'country_code', 'institution_id'
        ]::TEXT[] = '{}'::JSONB
        AND jsonb_typeof(source_spec -> 'display_name') = 'string'
        AND btrim(source_spec ->> 'display_name') <> ''
        AND jsonb_typeof(source_spec -> 'homepage_url') = 'string'
        AND btrim(source_spec ->> 'homepage_url') ~* '^https?://[^[:space:]]+$'
        AND jsonb_typeof(source_spec -> 'type') = 'string'
        AND btrim(source_spec ->> 'type') <> ''
        AND (
            jsonb_typeof(source_spec -> 'country_code') = 'null'
            OR source_spec ->> 'country_code' ~ '^[A-Z]{2}$'
        )
        AND (
            jsonb_typeof(source_spec -> 'institution_id') = 'null'
            OR (
                jsonb_typeof(source_spec -> 'institution_id') = 'number'
                AND source_spec ->> 'institution_id' ~ '^[1-9][0-9]*$'
            )
        )
    ),
    CONSTRAINT endpoint_source_mint_intent_spec_hash CHECK (
        source_spec_sha256 ~ '^[0-9a-f]{64}$'
        AND source_spec_sha256 =
            public.endpoint_source_mint_jsonb_sha256(source_spec)
    ),
    CONSTRAINT endpoint_source_mint_intent_hashes CHECK (
        evidence_sha256 ~ '^[0-9a-f]{64}$'
        AND manifest_sha256 ~ '^[0-9a-f]{64}$'
        AND live_review_sha256 ~ '^[0-9a-f]{64}$'
        AND plan_sha256 ~ '^[0-9a-f]{64}$'
        AND scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT endpoint_source_mint_intent_confidence
        CHECK (confidence IN ('HIGH', 'MED', 'LOW')),
    CONSTRAINT endpoint_source_mint_intent_algorithm
        CHECK (hash_algorithm = 'oa-jsonb-sha256-v1'),
    CONSTRAINT endpoint_source_mint_intent_version CHECK (audit_schema_version = 1)
);

CREATE TABLE public.endpoint_source_mint_receipt (
    run_id                       UUID        NOT NULL,
    endpoint_id                  TEXT        NOT NULL,
    minted_source_id             BIGINT      NOT NULL,
    after_source_snapshot        JSONB       NOT NULL,
    after_source_snapshot_sha256 TEXT        NOT NULL,
    source_spec_sha256           TEXT        NOT NULL,
    evidence_sha256              TEXT        NOT NULL,
    manifest_sha256              TEXT        NOT NULL,
    live_review_sha256           TEXT        NOT NULL,
    plan_sha256                  TEXT        NOT NULL,
    scope_sha256                 TEXT        NOT NULL,
    hash_algorithm               TEXT        NOT NULL DEFAULT 'oa-jsonb-sha256-v1',
    audit_schema_version         SMALLINT    NOT NULL DEFAULT 1,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT endpoint_source_mint_receipt_pkey PRIMARY KEY (run_id, endpoint_id),
    CONSTRAINT endpoint_source_mint_receipt_intent_fkey
        FOREIGN KEY (run_id, endpoint_id)
        REFERENCES public.endpoint_source_mint_intent(run_id, endpoint_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_mint_receipt_source_once UNIQUE (minted_source_id),
    CONSTRAINT endpoint_source_mint_receipt_source_positive CHECK (minted_source_id > 0),
    CONSTRAINT endpoint_source_mint_receipt_snapshot_shape CHECK (
        jsonb_typeof(after_source_snapshot) = 'object'
        AND after_source_snapshot ?& ARRAY['source', 'source_issns']::TEXT[]
        AND after_source_snapshot - ARRAY['source', 'source_issns']::TEXT[] = '{}'::JSONB
        AND jsonb_typeof(after_source_snapshot -> 'source') = 'object'
        AND jsonb_typeof(after_source_snapshot -> 'source_issns') = 'array'
        AND (after_source_snapshot -> 'source' ->> 'id')::BIGINT = minted_source_id
    ),
    CONSTRAINT endpoint_source_mint_receipt_snapshot_hash CHECK (
        after_source_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND after_source_snapshot_sha256 =
            public.endpoint_source_mint_jsonb_sha256(after_source_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_receipt_hashes CHECK (
        source_spec_sha256 ~ '^[0-9a-f]{64}$'
        AND evidence_sha256 ~ '^[0-9a-f]{64}$'
        AND manifest_sha256 ~ '^[0-9a-f]{64}$'
        AND live_review_sha256 ~ '^[0-9a-f]{64}$'
        AND plan_sha256 ~ '^[0-9a-f]{64}$'
        AND scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT endpoint_source_mint_receipt_algorithm
        CHECK (hash_algorithm = 'oa-jsonb-sha256-v1'),
    CONSTRAINT endpoint_source_mint_receipt_version CHECK (audit_schema_version = 1)
);

CREATE TABLE public.endpoint_source_mint_rollback_run (
    rollback_run_id              UUID        PRIMARY KEY,
    expected_row_count           INTEGER     NOT NULL,
    approved_rollback_plan       JSONB       NOT NULL,
    approved_rollback_scope      JSONB       NOT NULL,
    predelete_snapshot           JSONB       NOT NULL,
    rollback_plan_sha256         TEXT        NOT NULL,
    rollback_scope_sha256        TEXT        NOT NULL,
    predelete_snapshot_sha256    TEXT        NOT NULL,
    approval_ref                 TEXT        NOT NULL,
    change_ref                   TEXT        NOT NULL,
    writer_pause_ref             TEXT        NOT NULL,
    actor                        TEXT        NOT NULL,
    hash_algorithm               TEXT        NOT NULL DEFAULT 'oa-jsonb-sha256-v1',
    audit_schema_version         SMALLINT    NOT NULL DEFAULT 1,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT endpoint_source_mint_rollback_run_count CHECK (expected_row_count > 0),
    CONSTRAINT endpoint_source_mint_rollback_run_plan_shape CHECK (
        jsonb_typeof(approved_rollback_plan) = 'object'
        AND jsonb_typeof(approved_rollback_plan -> 'rows') = 'array'
        AND jsonb_array_length(approved_rollback_plan -> 'rows') = expected_row_count
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_scope_shape CHECK (
        jsonb_typeof(approved_rollback_scope) = 'object'
        AND jsonb_typeof(approved_rollback_scope -> 'endpoint_ids') = 'array'
        AND jsonb_array_length(approved_rollback_scope -> 'endpoint_ids') =
            expected_row_count
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_predelete_shape CHECK (
        jsonb_typeof(predelete_snapshot) = 'object'
        AND jsonb_typeof(predelete_snapshot -> 'rows') = 'array'
        AND jsonb_array_length(predelete_snapshot -> 'rows') = expected_row_count
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_plan_hash CHECK (
        rollback_plan_sha256 ~ '^[0-9a-f]{64}$'
        AND rollback_plan_sha256 =
            public.endpoint_source_mint_jsonb_sha256(approved_rollback_plan)
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_scope_hash CHECK (
        rollback_scope_sha256 ~ '^[0-9a-f]{64}$'
        AND rollback_scope_sha256 =
            public.endpoint_source_mint_jsonb_sha256(approved_rollback_scope)
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_predelete_hash CHECK (
        predelete_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND predelete_snapshot_sha256 =
            public.endpoint_source_mint_jsonb_sha256(predelete_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_refs CHECK (
        btrim(approval_ref) <> '' AND btrim(change_ref) <> ''
        AND btrim(writer_pause_ref) <> '' AND btrim(actor) <> ''
    ),
    CONSTRAINT endpoint_source_mint_rollback_run_algorithm
        CHECK (hash_algorithm = 'oa-jsonb-sha256-v1'),
    CONSTRAINT endpoint_source_mint_rollback_run_version
        CHECK (audit_schema_version = 1)
);

CREATE TABLE public.endpoint_source_mint_rollback_intent (
    rollback_run_id              UUID        NOT NULL,
    mint_run_id                  UUID        NOT NULL,
    endpoint_id                  TEXT        NOT NULL,
    minted_source_id             BIGINT      NOT NULL,
    before_source_snapshot       JSONB       NOT NULL,
    before_source_sha256         TEXT        NOT NULL,
    dependency_snapshot          JSONB       NOT NULL,
    dependency_snapshot_sha256   TEXT        NOT NULL,
    reason                       TEXT        NOT NULL,
    rollback_plan_sha256         TEXT        NOT NULL,
    rollback_scope_sha256        TEXT        NOT NULL,
    hash_algorithm               TEXT        NOT NULL DEFAULT 'oa-jsonb-sha256-v1',
    audit_schema_version         SMALLINT    NOT NULL DEFAULT 1,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT endpoint_source_mint_rollback_intent_pkey
        PRIMARY KEY (rollback_run_id, endpoint_id),
    CONSTRAINT endpoint_source_mint_rollback_intent_run_fkey
        FOREIGN KEY (rollback_run_id)
        REFERENCES public.endpoint_source_mint_rollback_run(rollback_run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_mint_rollback_intent_receipt_fkey
        FOREIGN KEY (mint_run_id, endpoint_id)
        REFERENCES public.endpoint_source_mint_receipt(run_id, endpoint_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_mint_rollback_intent_original_once
        UNIQUE (mint_run_id, endpoint_id),
    CONSTRAINT endpoint_source_mint_rollback_intent_source_once
        UNIQUE (minted_source_id),
    CONSTRAINT endpoint_source_mint_rollback_intent_source_positive
        CHECK (minted_source_id > 0),
    CONSTRAINT endpoint_source_mint_rollback_intent_source_shape CHECK (
        jsonb_typeof(before_source_snapshot) = 'object'
        AND (before_source_snapshot -> 'source' ->> 'id')::BIGINT = minted_source_id
    ),
    CONSTRAINT endpoint_source_mint_rollback_intent_source_hash CHECK (
        before_source_sha256 ~ '^[0-9a-f]{64}$'
        AND before_source_sha256 =
            public.endpoint_source_mint_jsonb_sha256(before_source_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_rollback_intent_dependency_hash CHECK (
        dependency_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND dependency_snapshot_sha256 =
            public.endpoint_source_mint_jsonb_sha256(dependency_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_rollback_intent_reason CHECK (btrim(reason) <> ''),
    CONSTRAINT endpoint_source_mint_rollback_intent_hashes CHECK (
        rollback_plan_sha256 ~ '^[0-9a-f]{64}$'
        AND rollback_scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT endpoint_source_mint_rollback_intent_algorithm
        CHECK (hash_algorithm = 'oa-jsonb-sha256-v1'),
    CONSTRAINT endpoint_source_mint_rollback_intent_version
        CHECK (audit_schema_version = 1)
);

CREATE TABLE public.endpoint_source_mint_rollback_receipt (
    rollback_run_id              UUID        NOT NULL,
    mint_run_id                  UUID        NOT NULL,
    endpoint_id                  TEXT        NOT NULL,
    minted_source_id             BIGINT      NOT NULL,
    before_source_snapshot       JSONB       NOT NULL,
    before_source_sha256         TEXT        NOT NULL,
    dependency_snapshot          JSONB       NOT NULL,
    dependency_snapshot_sha256   TEXT        NOT NULL,
    source_absence_confirmed     BOOLEAN     NOT NULL,
    reason                       TEXT        NOT NULL,
    approval_ref                 TEXT        NOT NULL,
    change_ref                   TEXT        NOT NULL,
    writer_pause_ref             TEXT        NOT NULL,
    actor                        TEXT        NOT NULL,
    rollback_plan_sha256         TEXT        NOT NULL,
    rollback_scope_sha256        TEXT        NOT NULL,
    hash_algorithm               TEXT        NOT NULL DEFAULT 'oa-jsonb-sha256-v1',
    audit_schema_version         SMALLINT    NOT NULL DEFAULT 1,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT endpoint_source_mint_rollback_receipt_pkey
        PRIMARY KEY (rollback_run_id, endpoint_id),
    CONSTRAINT endpoint_source_mint_rollback_receipt_intent_fkey
        FOREIGN KEY (rollback_run_id, endpoint_id)
        REFERENCES public.endpoint_source_mint_rollback_intent(
            rollback_run_id, endpoint_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_mint_rollback_receipt_original_once
        UNIQUE (mint_run_id, endpoint_id),
    CONSTRAINT endpoint_source_mint_rollback_receipt_source_once
        UNIQUE (minted_source_id),
    CONSTRAINT endpoint_source_mint_rollback_receipt_absence
        CHECK (source_absence_confirmed),
    CONSTRAINT endpoint_source_mint_rollback_receipt_source_hash CHECK (
        before_source_sha256 ~ '^[0-9a-f]{64}$'
        AND before_source_sha256 =
            public.endpoint_source_mint_jsonb_sha256(before_source_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_rollback_receipt_dependency_hash CHECK (
        dependency_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND dependency_snapshot_sha256 =
            public.endpoint_source_mint_jsonb_sha256(dependency_snapshot)
    ),
    CONSTRAINT endpoint_source_mint_rollback_receipt_refs CHECK (
        btrim(reason) <> '' AND btrim(approval_ref) <> '' AND btrim(change_ref) <> ''
        AND btrim(writer_pause_ref) <> '' AND btrim(actor) <> ''
    ),
    CONSTRAINT endpoint_source_mint_rollback_receipt_hashes CHECK (
        rollback_plan_sha256 ~ '^[0-9a-f]{64}$'
        AND rollback_scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT endpoint_source_mint_rollback_receipt_algorithm
        CHECK (hash_algorithm = 'oa-jsonb-sha256-v1'),
    CONSTRAINT endpoint_source_mint_rollback_receipt_version
        CHECK (audit_schema_version = 1)
);

COMMENT ON TABLE public.endpoint_source_mint_receipt IS
  'Immutable source-only mint receipt: endpoint identity to minted Source identity; not an endpoint relationship row.';
COMMENT ON TABLE public.endpoint_source_mint_rollback_receipt IS
  'Immutable receipt for exact guarded deletion of a minted Source; endpoint rows are untouched.';

CREATE FUNCTION public.endpoint_source_mint_dependencies_are_clear(value JSONB)
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
            'endpoint_source_id_count', 0
       )
$function$;

CREATE FUNCTION public.validate_endpoint_source_mint_intent_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    run_row RECORD;
    live_endpoint JSONB;
    live_routes JSONB;
    current_count INTEGER;
BEGIN
    SELECT * INTO run_row
      FROM public.endpoint_source_mint_run
     WHERE run_id = NEW.run_id
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'mint intent has no run header';
    END IF;
    IF (NEW.manifest_sha256, NEW.live_review_sha256, NEW.plan_sha256,
        NEW.scope_sha256) IS DISTINCT FROM
       (run_row.manifest_sha256, run_row.live_review_sha256,
        run_row.plan_sha256, run_row.scope_sha256) THEN
        RAISE EXCEPTION 'mint intent hashes differ from run';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            run_row.approved_scope -> 'endpoint_ids'
        ) AS scoped(endpoint_id)
        WHERE scoped.endpoint_id = NEW.endpoint_id
    ) THEN
        RAISE EXCEPTION 'mint intent absent from approved scope';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(
            run_row.approved_plan -> 'rows'
        ) AS planned(row_value)
        WHERE planned.row_value ->> 'endpoint_id' = NEW.endpoint_id
          AND planned.row_value -> 'source_spec' = NEW.source_spec
          AND planned.row_value ->> 'source_spec_sha256' = NEW.source_spec_sha256
          AND planned.row_value ->> 'evidence_sha256' = NEW.evidence_sha256
          AND planned.row_value ->> 'confidence' = NEW.confidence
          AND planned.row_value -> 'transitional_route_snapshot' =
              NEW.transitional_route_snapshot
          AND planned.row_value ->> 'transitional_route_sha256' =
              NEW.transitional_route_sha256
    ) THEN
        RAISE EXCEPTION 'mint intent differs from approved plan';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(
            run_row.prewrite_snapshot -> 'rows'
        ) AS snapshotted(row_value)
        WHERE snapshotted.row_value ->> 'endpoint_id' = NEW.endpoint_id
          AND snapshotted.row_value -> 'endpoint_snapshot' = NEW.endpoint_snapshot
          AND snapshotted.row_value ->> 'endpoint_snapshot_sha256' =
              NEW.endpoint_snapshot_sha256
          AND snapshotted.row_value -> 'transitional_route_snapshot' =
              NEW.transitional_route_snapshot
          AND snapshotted.row_value ->> 'transitional_route_sha256' =
              NEW.transitional_route_sha256
    ) THEN
        RAISE EXCEPTION 'mint intent differs from prewrite snapshot';
    END IF;
    SELECT pg_catalog.to_jsonb(e) INTO live_endpoint
      FROM public.endpoint AS e
     WHERE e.id = NEW.endpoint_id
     FOR SHARE;
    IF NOT FOUND OR live_endpoint IS DISTINCT FROM NEW.endpoint_snapshot THEN
        RAISE EXCEPTION 'mint endpoint differs from exact observation';
    END IF;
    live_routes := public.endpoint_source_mint_live_transitional_routes(
        NEW.endpoint_id
    );
    IF live_routes IS DISTINCT FROM NEW.transitional_route_snapshot
       OR NOT public.endpoint_source_mint_transitional_routes_are_clear(live_routes) THEN
        RAISE EXCEPTION 'mint endpoint already has a transitional route';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.source_type AS st
         WHERE st.source_type_id = NEW.source_spec ->> 'type'
    ) THEN
        RAISE EXCEPTION 'mint Source type absent from vocabulary';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.sources AS s
         WHERE s.display_name IS NOT DISTINCT FROM NEW.source_spec ->> 'display_name'
           AND s.homepage_url IS NOT DISTINCT FROM NEW.source_spec ->> 'homepage_url'
           AND s.type IS NOT DISTINCT FROM NEW.source_spec ->> 'type'
           AND s.country_code IS NOT DISTINCT FROM NEW.source_spec ->> 'country_code'
           AND s.institution_id IS NOT DISTINCT FROM
               (NEW.source_spec ->> 'institution_id')::BIGINT
           AND NOT EXISTS (
               SELECT 1 FROM public.source_merge AS sm WHERE sm.loser_id = s.id
           )
    ) THEN
        RAISE EXCEPTION 'exact active Source already exists';
    END IF;
    SELECT count(*) INTO current_count
      FROM public.endpoint_source_mint_intent WHERE run_id = NEW.run_id;
    IF current_count >= run_row.expected_row_count THEN
        RAISE EXCEPTION 'mint intent count exceeds approved count';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE FUNCTION public.require_endpoint_source_mint_run_complete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    intent_count INTEGER;
    receipt_count INTEGER;
    bad_count INTEGER;
BEGIN
    SELECT count(*) INTO intent_count
      FROM public.endpoint_source_mint_intent WHERE run_id = NEW.run_id;
    SELECT count(*) INTO receipt_count
      FROM public.endpoint_source_mint_receipt WHERE run_id = NEW.run_id;
    IF intent_count <> NEW.expected_row_count
       OR receipt_count <> NEW.expected_row_count THEN
        RAISE EXCEPTION 'mint run incomplete';
    END IF;
    SELECT count(*) INTO bad_count
      FROM public.endpoint_source_mint_intent AS i
      LEFT JOIN public.endpoint_source_mint_receipt AS r
        ON r.run_id = i.run_id AND r.endpoint_id = i.endpoint_id
      LEFT JOIN public.sources AS s ON s.id = r.minted_source_id
     WHERE i.run_id = NEW.run_id
       AND (
           r.endpoint_id IS NULL
           OR public.endpoint_source_mint_live_source_snapshot(s.id)
                IS DISTINCT FROM r.after_source_snapshot
           OR (SELECT pg_catalog.to_jsonb(e) FROM public.endpoint AS e
                WHERE e.id = i.endpoint_id) IS DISTINCT FROM i.endpoint_snapshot
           OR public.endpoint_source_mint_live_transitional_routes(i.endpoint_id)
                IS DISTINCT FROM i.transitional_route_snapshot
           OR NOT public.endpoint_source_mint_transitional_routes_are_clear(
                i.transitional_route_snapshot
           )
           OR NOT public.endpoint_source_mint_dependencies_are_clear(
                public.endpoint_source_mint_live_dependencies(r.minted_source_id)
           )
       );
    IF bad_count <> 0 THEN
        RAISE EXCEPTION 'mint run has stale intent/receipt state';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE FUNCTION public.require_endpoint_source_mint_audit_before_source_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    inserted_spec JSONB;
    inserted_hash TEXT;
    intent_row RECORD;
    expected_count INTEGER;
    actual_count INTEGER;
BEGIN
    inserted_spec := pg_catalog.jsonb_build_object(
        'display_name', NEW.display_name,
        'homepage_url', NEW.homepage_url,
        'type', NEW.type,
        'country_code', NEW.country_code,
        'institution_id', NEW.institution_id
    );
    inserted_hash := public.endpoint_source_mint_jsonb_sha256(inserted_spec);
    SELECT * INTO intent_row
      FROM public.endpoint_source_mint_intent
     WHERE source_spec_sha256 = inserted_hash
     FOR KEY SHARE;
    IF NOT FOUND THEN
        IF EXISTS (
            SELECT 1 FROM public.endpoint_source_mint_intent AS i
             WHERE i.source_spec ->> 'display_name' IS NOT DISTINCT FROM NEW.display_name
               AND i.source_spec ->> 'homepage_url' IS NOT DISTINCT FROM NEW.homepage_url
               AND i.source_spec ->> 'type' IS NOT DISTINCT FROM NEW.type
        ) THEN
            RAISE EXCEPTION 'Source INSERT differs from reviewed mint spec';
        END IF;
        RETURN NEW;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.endpoint_source_mint_receipt AS r
         WHERE r.run_id = intent_row.run_id AND r.endpoint_id = intent_row.endpoint_id
    ) THEN
        RAISE EXCEPTION 'reviewed mint intent was already consumed';
    END IF;
    SELECT r.expected_row_count,
           (SELECT count(*) FROM public.endpoint_source_mint_intent AS i
             WHERE i.run_id = intent_row.run_id)
      INTO expected_count, actual_count
      FROM public.endpoint_source_mint_run AS r
     WHERE r.run_id = intent_row.run_id;
    IF actual_count <> expected_count THEN
        RAISE EXCEPTION 'mint requires all intents before first Source write';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE FUNCTION public.validate_endpoint_source_mint_receipt_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    intent_row RECORD;
    source_row RECORD;
    live_source JSONB;
    live_dependencies JSONB;
BEGIN
    SELECT * INTO intent_row
      FROM public.endpoint_source_mint_intent
     WHERE run_id = NEW.run_id AND endpoint_id = NEW.endpoint_id
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'mint receipt has no exact intent';
    END IF;
    IF (NEW.source_spec_sha256, NEW.evidence_sha256, NEW.manifest_sha256,
        NEW.live_review_sha256, NEW.plan_sha256, NEW.scope_sha256)
       IS DISTINCT FROM
       (intent_row.source_spec_sha256, intent_row.evidence_sha256,
        intent_row.manifest_sha256, intent_row.live_review_sha256,
        intent_row.plan_sha256, intent_row.scope_sha256) THEN
        RAISE EXCEPTION 'mint receipt hashes differ from intent';
    END IF;
    SELECT * INTO source_row FROM public.sources AS s
     WHERE s.id = NEW.minted_source_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'minted Source absent for receipt';
    END IF;
    live_source := public.endpoint_source_mint_live_source_snapshot(NEW.minted_source_id);
    IF live_source IS DISTINCT FROM NEW.after_source_snapshot THEN
        RAISE EXCEPTION 'minted Source differs from exact receipt';
    END IF;
    IF source_row.display_name IS DISTINCT FROM intent_row.source_spec ->> 'display_name'
       OR source_row.homepage_url IS DISTINCT FROM intent_row.source_spec ->> 'homepage_url'
       OR source_row.type IS DISTINCT FROM intent_row.source_spec ->> 'type'
       OR source_row.country_code IS DISTINCT FROM intent_row.source_spec ->> 'country_code'
       OR source_row.institution_id IS DISTINCT FROM
          (intent_row.source_spec ->> 'institution_id')::BIGINT THEN
        RAISE EXCEPTION 'minted Source differs from approved structured spec';
    END IF;
    IF pg_catalog.jsonb_strip_nulls(
           (live_source -> 'source') - ARRAY[
               'id', 'display_name', 'type', 'institution_id', 'homepage_url',
               'country_code', 'is_oa', 'created_date', 'updated_date'
           ]::TEXT[]
       ) <> '{}'::JSONB
       OR source_row.is_oa IS DISTINCT FROM FALSE
       OR live_source -> 'source' -> 'created_date' = 'null'::JSONB
       OR live_source -> 'source' -> 'updated_date' = 'null'::JSONB THEN
        RAISE EXCEPTION 'minted Source has unreviewed non-spec fields or defaults';
    END IF;
    live_dependencies :=
        public.endpoint_source_mint_live_dependencies(NEW.minted_source_id);
    IF NOT public.endpoint_source_mint_dependencies_are_clear(live_dependencies) THEN
        RAISE EXCEPTION 'source-only mint unexpectedly has dependencies';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.sources AS other
         WHERE other.id <> NEW.minted_source_id
           AND other.display_name IS NOT DISTINCT FROM source_row.display_name
           AND other.homepage_url IS NOT DISTINCT FROM source_row.homepage_url
           AND NOT EXISTS (
               SELECT 1 FROM public.source_merge AS sm WHERE sm.loser_id = other.id
           )
    ) THEN
        RAISE EXCEPTION 'active duplicate Source appeared during mint';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE FUNCTION public.require_endpoint_source_mint_receipt_for_source_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    inserted_spec JSONB;
    inserted_hash TEXT;
    intent_row RECORD;
    receipt_row RECORD;
BEGIN
    inserted_spec := pg_catalog.jsonb_build_object(
        'display_name', NEW.display_name,
        'homepage_url', NEW.homepage_url,
        'type', NEW.type,
        'country_code', NEW.country_code,
        'institution_id', NEW.institution_id
    );
    inserted_hash := public.endpoint_source_mint_jsonb_sha256(inserted_spec);
    SELECT * INTO intent_row FROM public.endpoint_source_mint_intent
     WHERE source_spec_sha256 = inserted_hash;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT * INTO receipt_row FROM public.endpoint_source_mint_receipt
     WHERE run_id = intent_row.run_id
       AND endpoint_id = intent_row.endpoint_id
       AND minted_source_id = NEW.id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'reviewed Source INSERT lacks exact mint receipt';
    END IF;
    IF public.endpoint_source_mint_live_source_snapshot(NEW.id)
       IS DISTINCT FROM receipt_row.after_source_snapshot THEN
        RAISE EXCEPTION 'reviewed Source INSERT differs from receipt';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE FUNCTION public.validate_endpoint_source_mint_rollback_intent_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    rollback_run RECORD;
    original_receipt RECORD;
    live_source JSONB;
    live_dependencies JSONB;
    current_count INTEGER;
BEGIN
    SELECT * INTO rollback_run FROM public.endpoint_source_mint_rollback_run
     WHERE rollback_run_id = NEW.rollback_run_id FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'rollback intent has no run';
    END IF;
    SELECT * INTO original_receipt FROM public.endpoint_source_mint_receipt
     WHERE run_id = NEW.mint_run_id AND endpoint_id = NEW.endpoint_id
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'rollback intent has no original receipt';
    END IF;
    IF NEW.minted_source_id <> original_receipt.minted_source_id
       OR NEW.before_source_snapshot IS DISTINCT FROM
          original_receipt.after_source_snapshot
       OR NEW.before_source_sha256 IS DISTINCT FROM
          original_receipt.after_source_snapshot_sha256 THEN
        RAISE EXCEPTION 'rollback intent differs from mint receipt';
    END IF;
    IF (NEW.rollback_plan_sha256, NEW.rollback_scope_sha256) IS DISTINCT FROM
       (rollback_run.rollback_plan_sha256, rollback_run.rollback_scope_sha256) THEN
        RAISE EXCEPTION 'rollback intent hashes differ from run';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            rollback_run.approved_rollback_scope -> 'endpoint_ids'
        ) AS scoped(endpoint_id) WHERE scoped.endpoint_id = NEW.endpoint_id
    ) THEN
        RAISE EXCEPTION 'rollback intent absent from scope';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(
            rollback_run.approved_rollback_plan -> 'rows'
        ) AS planned(row_value)
        WHERE planned.row_value ->> 'mint_run_id' = NEW.mint_run_id::TEXT
          AND planned.row_value ->> 'endpoint_id' = NEW.endpoint_id
          AND planned.row_value ->> 'minted_source_id' = NEW.minted_source_id::TEXT
          AND planned.row_value -> 'before_source_snapshot' = NEW.before_source_snapshot
          AND planned.row_value ->> 'before_source_sha256' = NEW.before_source_sha256
          AND planned.row_value -> 'dependency_snapshot' = NEW.dependency_snapshot
          AND planned.row_value ->> 'dependency_snapshot_sha256' =
              NEW.dependency_snapshot_sha256
          AND planned.row_value ->> 'reason' = NEW.reason
    ) THEN
        RAISE EXCEPTION 'rollback intent differs from approved plan';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(
            rollback_run.predelete_snapshot -> 'rows'
        ) AS snapshotted(row_value)
        WHERE snapshotted.row_value ->> 'endpoint_id' = NEW.endpoint_id
          AND snapshotted.row_value ->> 'minted_source_id' = NEW.minted_source_id::TEXT
          AND snapshotted.row_value -> 'source_snapshot' = NEW.before_source_snapshot
          AND snapshotted.row_value ->> 'source_snapshot_sha256' =
              NEW.before_source_sha256
          AND snapshotted.row_value -> 'dependency_snapshot' = NEW.dependency_snapshot
          AND snapshotted.row_value ->> 'dependency_snapshot_sha256' =
              NEW.dependency_snapshot_sha256
    ) THEN
        RAISE EXCEPTION 'rollback intent differs from predelete snapshot';
    END IF;
    PERFORM 1 FROM public.sources AS s
     WHERE s.id = NEW.minted_source_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'rollback Source absent before authorization';
    END IF;
    live_source := public.endpoint_source_mint_live_source_snapshot(NEW.minted_source_id);
    live_dependencies :=
        public.endpoint_source_mint_live_dependencies(NEW.minted_source_id);
    IF live_source IS DISTINCT FROM NEW.before_source_snapshot
       OR live_dependencies IS DISTINCT FROM NEW.dependency_snapshot
       OR NOT public.endpoint_source_mint_dependencies_are_clear(live_dependencies) THEN
        RAISE EXCEPTION 'rollback Source/dependency drift';
    END IF;
    SELECT count(*) INTO current_count FROM public.endpoint_source_mint_rollback_intent
     WHERE rollback_run_id = NEW.rollback_run_id;
    IF current_count >= rollback_run.expected_row_count THEN
        RAISE EXCEPTION 'rollback intent count exceeds approved count';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE FUNCTION public.require_endpoint_source_mint_rollback_before_source_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    original_receipt RECORD;
    rollback_intent RECORD;
    expected_count INTEGER;
    actual_count INTEGER;
    live_source JSONB;
    live_dependencies JSONB;
BEGIN
    SELECT * INTO original_receipt FROM public.endpoint_source_mint_receipt
     WHERE minted_source_id = OLD.id;
    IF NOT FOUND THEN
        RETURN OLD;
    END IF;
    SELECT ri.* INTO rollback_intent
      FROM public.endpoint_source_mint_rollback_intent AS ri
     WHERE ri.mint_run_id = original_receipt.run_id
       AND ri.endpoint_id = original_receipt.endpoint_id
       AND NOT EXISTS (
           SELECT 1 FROM public.endpoint_source_mint_rollback_receipt AS done
            WHERE done.rollback_run_id = ri.rollback_run_id
              AND done.endpoint_id = ri.endpoint_id
       )
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'receipted mint Source delete lacks rollback intent';
    END IF;
    SELECT rr.expected_row_count,
           (SELECT count(*) FROM public.endpoint_source_mint_rollback_intent AS all_i
             WHERE all_i.rollback_run_id = rollback_intent.rollback_run_id)
      INTO expected_count, actual_count
      FROM public.endpoint_source_mint_rollback_run AS rr
     WHERE rr.rollback_run_id = rollback_intent.rollback_run_id;
    IF actual_count <> expected_count THEN
        RAISE EXCEPTION 'rollback requires all intents before first delete';
    END IF;
    live_source := public.endpoint_source_mint_live_source_snapshot(OLD.id);
    live_dependencies := public.endpoint_source_mint_live_dependencies(OLD.id);
    IF live_source IS DISTINCT FROM rollback_intent.before_source_snapshot
       OR live_dependencies IS DISTINCT FROM rollback_intent.dependency_snapshot
       OR NOT public.endpoint_source_mint_dependencies_are_clear(live_dependencies) THEN
        RAISE EXCEPTION 'receipted mint Source has dependencies or drift';
    END IF;
    RETURN OLD;
END;
$function$;

CREATE FUNCTION public.validate_endpoint_source_mint_rollback_receipt_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    intent_row RECORD;
    run_row RECORD;
BEGIN
    SELECT * INTO intent_row FROM public.endpoint_source_mint_rollback_intent
     WHERE rollback_run_id = NEW.rollback_run_id
       AND endpoint_id = NEW.endpoint_id FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'rollback receipt has no exact intent';
    END IF;
    SELECT * INTO run_row FROM public.endpoint_source_mint_rollback_run
     WHERE rollback_run_id = NEW.rollback_run_id FOR KEY SHARE;
    IF (NEW.mint_run_id, NEW.minted_source_id, NEW.before_source_snapshot,
        NEW.before_source_sha256, NEW.dependency_snapshot,
        NEW.dependency_snapshot_sha256, NEW.reason,
        NEW.rollback_plan_sha256, NEW.rollback_scope_sha256)
       IS DISTINCT FROM
       (intent_row.mint_run_id, intent_row.minted_source_id,
        intent_row.before_source_snapshot, intent_row.before_source_sha256,
        intent_row.dependency_snapshot, intent_row.dependency_snapshot_sha256,
        intent_row.reason, intent_row.rollback_plan_sha256,
        intent_row.rollback_scope_sha256) THEN
        RAISE EXCEPTION 'rollback receipt differs from intent';
    END IF;
    IF (NEW.approval_ref, NEW.change_ref, NEW.writer_pause_ref, NEW.actor)
       IS DISTINCT FROM
       (run_row.approval_ref, run_row.change_ref,
        run_row.writer_pause_ref, run_row.actor) THEN
        RAISE EXCEPTION 'rollback receipt refs differ from run';
    END IF;
    IF EXISTS (SELECT 1 FROM public.sources AS s WHERE s.id = NEW.minted_source_id) THEN
        RAISE EXCEPTION 'rollback Source still exists';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE FUNCTION public.require_endpoint_source_mint_rollback_receipt_for_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    receipt_count INTEGER;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.endpoint_source_mint_receipt AS original
         WHERE original.minted_source_id = OLD.id
    ) THEN
        -- The rollback ledger governs only Sources minted by this contract.
        RETURN NULL;
    END IF;
    SELECT count(*) INTO receipt_count
      FROM public.endpoint_source_mint_rollback_receipt AS done
      JOIN public.endpoint_source_mint_rollback_intent AS ri
        ON ri.rollback_run_id = done.rollback_run_id
       AND ri.endpoint_id = done.endpoint_id
     WHERE ri.minted_source_id = OLD.id;
    IF receipt_count <> 1 THEN
        RAISE EXCEPTION 'mint Source delete requires one final rollback receipt';
    END IF;
    IF EXISTS (SELECT 1 FROM public.sources AS s WHERE s.id = OLD.id) THEN
        RAISE EXCEPTION 'rollback did not remove Source';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE FUNCTION public.require_endpoint_source_mint_rollback_run_complete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    intent_count INTEGER;
    receipt_count INTEGER;
    present_count INTEGER;
BEGIN
    SELECT count(*) INTO intent_count FROM public.endpoint_source_mint_rollback_intent
     WHERE rollback_run_id = NEW.rollback_run_id;
    SELECT count(*) INTO receipt_count FROM public.endpoint_source_mint_rollback_receipt
     WHERE rollback_run_id = NEW.rollback_run_id;
    SELECT count(*) INTO present_count
      FROM public.endpoint_source_mint_rollback_intent AS ri
      JOIN public.sources AS s ON s.id = ri.minted_source_id
     WHERE ri.rollback_run_id = NEW.rollback_run_id;
    IF intent_count <> NEW.expected_row_count
       OR receipt_count <> NEW.expected_row_count
       OR present_count <> 0 THEN
        RAISE EXCEPTION 'rollback run incomplete';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE FUNCTION public.reject_endpoint_source_mint_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    RAISE EXCEPTION 'endpoint Source mint history is append-only';
END;
$function$;

CREATE FUNCTION public.reject_endpoint_source_mint_sources_truncate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    RAISE EXCEPTION 'TRUNCATE public.sources is forbidden after endpoint Source mint audit installation';
END;
$function$;

CREATE TRIGGER endpoint_source_mint_intent_validate_insert
BEFORE INSERT ON public.endpoint_source_mint_intent
FOR EACH ROW EXECUTE FUNCTION public.validate_endpoint_source_mint_intent_insert();

CREATE CONSTRAINT TRIGGER endpoint_source_mint_run_require_complete
AFTER INSERT ON public.endpoint_source_mint_run
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.require_endpoint_source_mint_run_complete();

CREATE TRIGGER endpoint_source_mint_source_require_audit_first
BEFORE INSERT ON public.sources
FOR EACH ROW EXECUTE FUNCTION public.require_endpoint_source_mint_audit_before_source_insert();

CREATE TRIGGER endpoint_source_mint_receipt_validate_insert
BEFORE INSERT ON public.endpoint_source_mint_receipt
FOR EACH ROW EXECUTE FUNCTION public.validate_endpoint_source_mint_receipt_insert();

CREATE CONSTRAINT TRIGGER endpoint_source_mint_source_require_receipt
AFTER INSERT ON public.sources
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.require_endpoint_source_mint_receipt_for_source_insert();

CREATE TRIGGER endpoint_source_mint_rollback_intent_validate_insert
BEFORE INSERT ON public.endpoint_source_mint_rollback_intent
FOR EACH ROW EXECUTE FUNCTION public.validate_endpoint_source_mint_rollback_intent_insert();

CREATE TRIGGER endpoint_source_mint_source_require_rollback
BEFORE DELETE ON public.sources
FOR EACH ROW EXECUTE FUNCTION public.require_endpoint_source_mint_rollback_before_source_delete();

CREATE TRIGGER endpoint_source_mint_rollback_receipt_validate_insert
BEFORE INSERT ON public.endpoint_source_mint_rollback_receipt
FOR EACH ROW EXECUTE FUNCTION public.validate_endpoint_source_mint_rollback_receipt_insert();

CREATE CONSTRAINT TRIGGER endpoint_source_mint_source_delete_require_receipt
AFTER DELETE ON public.sources
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.require_endpoint_source_mint_rollback_receipt_for_delete();

CREATE TRIGGER endpoint_source_mint_source_no_truncate
BEFORE TRUNCATE ON public.sources
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_sources_truncate();

CREATE CONSTRAINT TRIGGER endpoint_source_mint_rollback_run_require_complete
AFTER INSERT ON public.endpoint_source_mint_rollback_run
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.require_endpoint_source_mint_rollback_run_complete();

CREATE TRIGGER endpoint_source_mint_run_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_source_mint_run
FOR EACH ROW EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_run_no_truncate
BEFORE TRUNCATE ON public.endpoint_source_mint_run
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_intent_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_source_mint_intent
FOR EACH ROW EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_intent_no_truncate
BEFORE TRUNCATE ON public.endpoint_source_mint_intent
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_receipt_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_source_mint_receipt
FOR EACH ROW EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_receipt_no_truncate
BEFORE TRUNCATE ON public.endpoint_source_mint_receipt
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_rollback_run_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_source_mint_rollback_run
FOR EACH ROW EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_rollback_run_no_truncate
BEFORE TRUNCATE ON public.endpoint_source_mint_rollback_run
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_rollback_intent_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_source_mint_rollback_intent
FOR EACH ROW EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_rollback_intent_no_truncate
BEFORE TRUNCATE ON public.endpoint_source_mint_rollback_intent
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_rollback_receipt_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_source_mint_rollback_receipt
FOR EACH ROW EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();
CREATE TRIGGER endpoint_source_mint_rollback_receipt_no_truncate
BEFORE TRUNCATE ON public.endpoint_source_mint_rollback_receipt
FOR EACH STATEMENT EXECUTE FUNCTION public.reject_endpoint_source_mint_history_mutation();

REVOKE ALL PRIVILEGES ON TABLE
    public.endpoint_source_mint_run,
    public.endpoint_source_mint_intent,
    public.endpoint_source_mint_receipt,
    public.endpoint_source_mint_rollback_run,
    public.endpoint_source_mint_rollback_intent,
    public.endpoint_source_mint_rollback_receipt
FROM PUBLIC;

REVOKE ALL PRIVILEGES ON FUNCTION
    public.endpoint_source_mint_jsonb_sha256(JSONB),
    public.endpoint_source_mint_live_source_snapshot(BIGINT),
    public.endpoint_source_mint_live_dependencies(BIGINT),
    public.endpoint_source_mint_dependencies_are_clear(JSONB),
    public.endpoint_source_mint_live_transitional_routes(TEXT),
    public.endpoint_source_mint_transitional_routes_are_clear(JSONB),
    public.validate_endpoint_source_mint_intent_insert(),
    public.require_endpoint_source_mint_run_complete(),
    public.require_endpoint_source_mint_audit_before_source_insert(),
    public.validate_endpoint_source_mint_receipt_insert(),
    public.require_endpoint_source_mint_receipt_for_source_insert(),
    public.validate_endpoint_source_mint_rollback_intent_insert(),
    public.require_endpoint_source_mint_rollback_before_source_delete(),
    public.validate_endpoint_source_mint_rollback_receipt_insert(),
    public.require_endpoint_source_mint_rollback_receipt_for_delete(),
    public.require_endpoint_source_mint_rollback_run_complete(),
    public.reject_endpoint_source_mint_history_mutation(),
    public.reject_endpoint_source_mint_sources_truncate()
FROM PUBLIC;
