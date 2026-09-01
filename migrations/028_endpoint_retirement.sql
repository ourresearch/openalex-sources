-- 028: endpoint lifecycle and append-only retirement audit (oxjob #83.13)
--
-- Retirement preserves endpoint identity, endpoint.name, both endpoint-to-Source
-- relationship stores, and sources.endpoint_id. The executor is responsible for
-- setting ready_to_run and in_walden false in the same guarded transaction so a
-- retired endpoint cannot continue harvesting.
--
-- Lifecycle columns deliberately remain nullable: this migration introduces no
-- global NOT NULL. Existing rows receive the 'active' default; the retirement
-- executor refuses NULL or unknown status. A possible disabled/quarantine state
-- remains a separate, unapproved governance decision and is not introduced here.
--
-- The trusted Heroku registry schema is public. Every migration object is fully
-- qualified, and pg_temp is explicitly last so temporary or earlier decoy
-- schemas cannot redirect relation or function resolution.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $environment$
BEGIN
    IF pg_catalog.current_setting('session_replication_role') <> 'origin' THEN
        RAISE EXCEPTION 'endpoint retirement migration requires session_replication_role=origin';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtenabled <> 'D'
    ) THEN
        RAISE EXCEPTION 'endpoint retirement migration refuses enabled event triggers';
    END IF;
END;
$environment$;

LOCK TABLE public.sources, public.source_endpoint, public.endpoint
    IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.endpoint_deletion_audit IN ACCESS EXCLUSIVE MODE;
LOCK TABLE public.schema_migrations IN SHARE ROW EXCLUSIVE MODE;

DO $contract$
DECLARE
    endpoint_constraints TEXT[];
    source_endpoint_constraints TEXT[];
    deletion_audit_constraints TEXT[];
    migration_ledger_constraints TEXT[];
    deletion_audit_indexes TEXT[];
    migration_ledger_indexes TEXT[];
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = ANY (ARRAY[
            'public.endpoint'::pg_catalog.regclass,
            'public.source_endpoint'::pg_catalog.regclass,
            'public.sources'::pg_catalog.regclass
        ])
          AND NOT t.tgisinternal
    ) THEN
        RAISE EXCEPTION 'operational pre-retirement trigger set is not canonical';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = 'public.schema_migrations'::pg_catalog.regclass
          AND NOT t.tgisinternal
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_rewrite r
        WHERE r.ev_class = ANY (ARRAY[
            'public.endpoint'::pg_catalog.regclass,
            'public.source_endpoint'::pg_catalog.regclass,
            'public.sources'::pg_catalog.regclass,
            'public.schema_migrations'::pg_catalog.regclass
        ])
          AND r.rulename <> '_RETURN'
    ) THEN
        RAISE EXCEPTION 'operational or migration-ledger trigger/rule set is not canonical';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc p
        WHERE p.pronamespace = (
            SELECT c.relnamespace
            FROM pg_catalog.pg_class c
            WHERE c.oid = 'public.endpoint'::pg_catalog.regclass
        )
          AND p.proname IN (
              'enforce_endpoint_retirement_receipt',
              'reject_endpoint_retirement_audit_mutation'
          )
    ) THEN
        RAISE EXCEPTION 'endpoint retirement function namespace is not clean';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        WHERE c.oid = ANY (ARRAY[
            'public.endpoint'::pg_catalog.regclass,
            'public.source_endpoint'::pg_catalog.regclass,
            'public.sources'::pg_catalog.regclass,
            'public.endpoint_deletion_audit'::pg_catalog.regclass,
            'public.schema_migrations'::pg_catalog.regclass
        ])
          AND (
              c.relkind <> 'r' OR c.relpersistence <> 'p' OR c.relispartition
              OR c.relrowsecurity OR c.relforcerowsecurity
          )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_inherits i
        WHERE i.inhrelid = ANY (ARRAY[
            'public.endpoint'::pg_catalog.regclass,
            'public.source_endpoint'::pg_catalog.regclass,
            'public.sources'::pg_catalog.regclass,
            'public.endpoint_deletion_audit'::pg_catalog.regclass,
            'public.schema_migrations'::pg_catalog.regclass
        ])
           OR i.inhparent = ANY (ARRAY[
            'public.endpoint'::pg_catalog.regclass,
            'public.source_endpoint'::pg_catalog.regclass,
            'public.sources'::pg_catalog.regclass,
            'public.endpoint_deletion_audit'::pg_catalog.regclass,
            'public.schema_migrations'::pg_catalog.regclass
        ])
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_policy p
        WHERE p.polrelid = ANY (ARRAY[
            'public.endpoint'::pg_catalog.regclass,
            'public.source_endpoint'::pg_catalog.regclass,
            'public.sources'::pg_catalog.regclass,
            'public.endpoint_deletion_audit'::pg_catalog.regclass,
            'public.schema_migrations'::pg_catalog.regclass
        ])
    ) THEN
        RAISE EXCEPTION 'endpoint retirement migration requires ordinary non-RLS non-inherited protected tables';
    END IF;

    IF (
        SELECT pg_catalog.array_agg(column_name::TEXT ORDER BY ordinal_position)
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'schema_migrations'
    ) <> ARRAY['version', 'applied_at']::TEXT[] OR EXISTS (
        SELECT 1
        FROM (VALUES
            ('version', 'text', 'NO', NULL::TEXT),
            ('applied_at', 'timestamp with time zone', 'YES', 'now()')
        ) AS expected(column_name, data_type, is_nullable, column_default)
        LEFT JOIN information_schema.columns actual
          ON actual.table_schema = 'public'
         AND actual.table_name = 'schema_migrations'
         AND actual.column_name = expected.column_name
        WHERE actual.column_name IS NULL
           OR actual.data_type <> expected.data_type
           OR actual.is_nullable <> expected.is_nullable
           OR actual.column_default IS DISTINCT FROM expected.column_default
           OR actual.is_identity <> 'NO'
           OR actual.identity_generation IS NOT NULL
           OR actual.is_generated <> 'NEVER'
           OR actual.generation_expression IS NOT NULL
           OR (
               expected.data_type = 'text'
               AND actual.collation_name IS NOT NULL
           )
    ) THEN
        RAISE EXCEPTION 'schema_migrations column contract is not canonical';
    END IF;

    SELECT pg_catalog.array_agg(conname ORDER BY conname)
    INTO migration_ledger_constraints
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'public.schema_migrations'::pg_catalog.regclass;
    IF migration_ledger_constraints <> ARRAY['schema_migrations_pkey']::TEXT[]
       OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_constraint
           WHERE conrelid = 'public.schema_migrations'::pg_catalog.regclass
             AND (
                 conname <> 'schema_migrations_pkey'
                 OR NOT convalidated OR condeferrable OR condeferred
                 OR pg_catalog.pg_get_constraintdef(oid) <> 'PRIMARY KEY (version)'
             )
       )
    THEN
        RAISE EXCEPTION 'schema_migrations constraint contract is not canonical';
    END IF;

    SELECT pg_catalog.array_agg(ic.relname::TEXT ORDER BY ic.relname)
    INTO migration_ledger_indexes
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
    WHERE i.indrelid = 'public.schema_migrations'::pg_catalog.regclass;
    IF migration_ledger_indexes <> ARRAY['schema_migrations_pkey']::TEXT[]
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_index i
           JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
           JOIN pg_catalog.pg_am am ON am.oid = ic.relam
           WHERE i.indrelid = 'public.schema_migrations'::pg_catalog.regclass
             AND (
                 ic.relname <> 'schema_migrations_pkey'
                 OR am.amname <> 'btree'
                 OR NOT i.indisunique OR NOT i.indisprimary
                 OR i.indisexclusion OR NOT i.indimmediate
                 OR NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive
                 OR i.indisclustered OR i.indisreplident
                 OR i.indnkeyatts <> 1 OR i.indnatts <> 1
                 OR i.indoption::TEXT <> '0'
                 OR i.indpred IS NOT NULL OR i.indexprs IS NOT NULL
                 OR ARRAY(
                     SELECT a.attname::TEXT
                     FROM pg_catalog.unnest(i.indkey)
                         WITH ORDINALITY AS key(attnum, position)
                     JOIN pg_catalog.pg_attribute a
                       ON a.attrelid = i.indrelid AND a.attnum = key.attnum
                     WHERE key.position <= i.indnkeyatts
                     ORDER BY key.position
                 ) <> ARRAY['version']::TEXT[]
                 OR ARRAY(
                     SELECT opc.opcname::TEXT
                     FROM pg_catalog.unnest(i.indclass)
                         WITH ORDINALITY AS key(opclass_oid, position)
                     JOIN pg_catalog.pg_opclass opc
                       ON opc.oid = key.opclass_oid
                     WHERE key.position <= i.indnkeyatts
                     ORDER BY key.position
                 ) <> ARRAY['text_ops']::TEXT[]
                 OR ARRAY(
                     SELECT collation_oid::OID
                     FROM pg_catalog.unnest(i.indcollation)
                         WITH ORDINALITY AS key(collation_oid, position)
                     WHERE key.position <= i.indnkeyatts
                     ORDER BY key.position
                 ) <> ARRAY(
                     SELECT a.attcollation
                     FROM pg_catalog.unnest(i.indkey)
                         WITH ORDINALITY AS key(attnum, position)
                     JOIN pg_catalog.pg_attribute a
                       ON a.attrelid = i.indrelid AND a.attnum = key.attnum
                     WHERE key.position <= i.indnkeyatts
                     ORDER BY key.position
                 )
                 OR pg_catalog.pg_get_indexdef(i.indexrelid) <>
                    'CREATE UNIQUE INDEX schema_migrations_pkey ON ' ||
                    pg_catalog.quote_ident((
                        SELECT n.nspname FROM pg_catalog.pg_namespace n
                        JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
                        WHERE c.oid = i.indrelid
                    )) || '.' || pg_catalog.quote_ident('schema_migrations') ||
                    ' USING btree (version)'
             )
       )
       OR (
           SELECT pg_catalog.count(*)
           FROM ONLY public.schema_migrations
           WHERE version = '025'
       ) <> 1
    THEN
        RAISE EXCEPTION 'schema_migrations index/history contract is not canonical';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('endpoint', 'id', 'text', 'NO'),
            ('endpoint', 'name', 'text', 'YES'),
            ('endpoint', 'pmh_url', 'text', 'YES'),
            ('endpoint', 'pmh_set', 'text', 'YES'),
            ('endpoint', 'source_id', 'bigint', 'YES'),
            ('endpoint', 'ready_to_run', 'boolean', 'YES'),
            ('endpoint', 'in_walden', 'boolean', 'YES'),
            ('source_endpoint', 'endpoint_id', 'text', 'NO'),
            ('source_endpoint', 'source_id', 'bigint', 'NO'),
            ('sources', 'id', 'bigint', 'NO'),
            ('sources', 'endpoint_id', 'text', 'YES')
        ) AS expected(table_name, column_name, data_type, is_nullable)
        LEFT JOIN information_schema.columns actual
          ON actual.table_schema = 'public'
         AND actual.table_name = expected.table_name
         AND actual.column_name = expected.column_name
        WHERE actual.column_name IS NULL
           OR actual.data_type <> expected.data_type
           OR actual.is_nullable <> expected.is_nullable
           OR (
               expected.data_type = 'text'
               AND actual.collation_name IS NOT NULL
           )
           OR actual.is_generated <> 'NEVER'
           OR actual.generation_expression IS NOT NULL
           OR (
               expected.table_name = 'sources' AND expected.column_name = 'id'
               AND (
                   actual.is_identity <> 'YES'
                   OR actual.identity_generation <> 'ALWAYS'
               )
           )
           OR (
               NOT (expected.table_name = 'sources' AND expected.column_name = 'id')
               AND (
                   actual.is_identity <> 'NO'
                   OR actual.identity_generation IS NOT NULL
               )
           )
    ) OR EXISTS (
        SELECT 1
        FROM information_schema.columns actual
        WHERE actual.table_schema = 'public'
          AND actual.table_name IN ('endpoint', 'source_endpoint', 'sources')
          AND (
              actual.is_generated <> 'NEVER'
              OR actual.generation_expression IS NOT NULL
              OR (
                  actual.table_name = 'sources' AND actual.column_name = 'id'
                  AND (
                      actual.is_identity <> 'YES'
                      OR actual.identity_generation <> 'ALWAYS'
                  )
              )
              OR (
                  NOT (
                      actual.table_name = 'sources'
                      AND actual.column_name = 'id'
                  )
                  AND (
                      actual.is_identity <> 'NO'
                      OR actual.identity_generation IS NOT NULL
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION 'operational column contract is not canonical';
    END IF;

    SELECT pg_catalog.array_agg(conname ORDER BY conname)
    INTO endpoint_constraints
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'public.endpoint'::pg_catalog.regclass;
    IF endpoint_constraints <> ARRAY[
        'endpoint_pkey', 'endpoint_source_id_fkey'
    ]::TEXT[] OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.endpoint'::pg_catalog.regclass
          AND (
              NOT convalidated OR condeferrable OR condeferred
              OR (conname = 'endpoint_pkey' AND
                  pg_catalog.pg_get_constraintdef(oid) <> 'PRIMARY KEY (id)')
              OR (conname = 'endpoint_source_id_fkey' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE RESTRICT')
          )
    ) THEN
        RAISE EXCEPTION 'endpoint pre-retirement constraint contract is not canonical';
    END IF;

    SELECT pg_catalog.array_agg(conname ORDER BY conname)
    INTO source_endpoint_constraints
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'public.source_endpoint'::pg_catalog.regclass;
    IF source_endpoint_constraints <> ARRAY[
        'source_endpoint_endpoint_id_fkey',
        'source_endpoint_pkey',
        'source_endpoint_source_id_fkey'
    ]::TEXT[] OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.source_endpoint'::pg_catalog.regclass
          AND (
              NOT convalidated OR condeferrable OR condeferred
              OR (conname = 'source_endpoint_pkey' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'PRIMARY KEY (endpoint_id)')
              OR (conname = 'source_endpoint_endpoint_id_fkey' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'FOREIGN KEY (endpoint_id) REFERENCES endpoint(id)')
              OR (conname = 'source_endpoint_source_id_fkey' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE')
          )
    ) THEN
        RAISE EXCEPTION 'source_endpoint constraint contract is not canonical';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.sources'::pg_catalog.regclass
          AND conname = 'sources_pkey'
          AND convalidated AND NOT condeferrable AND NOT condeferred
          AND pg_catalog.pg_get_constraintdef(oid) = 'PRIMARY KEY (id)'
    ) THEN
        RAISE EXCEPTION 'sources primary-key contract is not canonical';
    END IF;

    SELECT pg_catalog.array_agg(conname ORDER BY conname)
    INTO deletion_audit_constraints
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'public.endpoint_deletion_audit'::pg_catalog.regclass;
    IF deletion_audit_constraints <> ARRAY[
        'endpoint_deletion_audit_executed_by_present',
        'endpoint_deletion_audit_manifest_hash_present',
        'endpoint_deletion_audit_pkey',
        'endpoint_deletion_audit_reason_present',
        'endpoint_deletion_audit_repo_record_count_nonnegative',
        'endpoint_deletion_audit_review_ref_present'
    ]::TEXT[] OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.endpoint_deletion_audit'::pg_catalog.regclass
          AND (
              NOT convalidated OR condeferrable OR condeferred
              OR (conname = 'endpoint_deletion_audit_pkey' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'PRIMARY KEY (run_id, endpoint_id)')
              OR (conname = 'endpoint_deletion_audit_repo_record_count_nonnegative'
                  AND pg_catalog.pg_get_constraintdef(oid) <>
                  'CHECK ((repo_record_count_at_delete >= 0))')
              OR (conname = 'endpoint_deletion_audit_reason_present' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'CHECK ((btrim(reason) <> ''''::text))')
              OR (conname = 'endpoint_deletion_audit_manifest_hash_present' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'CHECK ((btrim(manifest_hash) <> ''''::text))')
              OR (conname = 'endpoint_deletion_audit_review_ref_present' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'CHECK ((btrim(review_ref) <> ''''::text))')
              OR (conname = 'endpoint_deletion_audit_executed_by_present' AND
                  pg_catalog.pg_get_constraintdef(oid) <>
                  'CHECK ((btrim(executed_by) <> ''''::text))')
          )
    ) THEN
        RAISE EXCEPTION 'migration-025 audit constraint contract is not canonical';
    END IF;

    SELECT pg_catalog.array_agg(ic.relname::TEXT ORDER BY ic.relname)
    INTO deletion_audit_indexes
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
    WHERE i.indrelid = 'public.endpoint_deletion_audit'::pg_catalog.regclass;
    IF deletion_audit_indexes <> ARRAY[
        'endpoint_deletion_audit_pkey',
        'idx_endpoint_deletion_audit_endpoint'
    ]::TEXT[] OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = ic.relam
        WHERE i.indrelid = 'public.endpoint_deletion_audit'::pg_catalog.regclass
          AND (
              am.amname <> 'btree'
              OR i.indisexclusion OR NOT i.indimmediate
              OR NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive
              OR i.indisclustered OR i.indisreplident
              OR i.indpred IS NOT NULL OR i.indexprs IS NOT NULL
              OR (
                  ic.relname = 'endpoint_deletion_audit_pkey'
                  AND (
                      NOT i.indisunique OR NOT i.indisprimary
                      OR i.indnkeyatts <> 2 OR i.indnatts <> 2
                      OR i.indoption::TEXT <> '0 0'
                      OR ARRAY(
                          SELECT a.attname::TEXT
                          FROM pg_catalog.unnest(i.indkey)
                              WITH ORDINALITY AS key(attnum, position)
                          JOIN pg_catalog.pg_attribute a
                            ON a.attrelid = i.indrelid
                           AND a.attnum = key.attnum
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      ) <> ARRAY['run_id', 'endpoint_id']::TEXT[]
                      OR ARRAY(
                          SELECT opc.opcname::TEXT
                          FROM pg_catalog.unnest(i.indclass)
                              WITH ORDINALITY AS key(opclass_oid, position)
                          JOIN pg_catalog.pg_opclass opc
                            ON opc.oid = key.opclass_oid
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      ) <> ARRAY['uuid_ops', 'text_ops']::TEXT[]
                      OR ARRAY(
                          SELECT collation_oid::OID
                          FROM pg_catalog.unnest(i.indcollation)
                              WITH ORDINALITY AS key(collation_oid, position)
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      ) <> ARRAY(
                          SELECT a.attcollation
                          FROM pg_catalog.unnest(i.indkey)
                              WITH ORDINALITY AS key(attnum, position)
                          JOIN pg_catalog.pg_attribute a
                            ON a.attrelid = i.indrelid
                           AND a.attnum = key.attnum
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      )
                      OR pg_catalog.pg_get_indexdef(i.indexrelid) <>
                         'CREATE UNIQUE INDEX endpoint_deletion_audit_pkey ON ' ||
                         pg_catalog.quote_ident((
                             SELECT n.nspname FROM pg_catalog.pg_namespace n
                             JOIN pg_catalog.pg_class c
                               ON c.relnamespace = n.oid
                             WHERE c.oid = i.indrelid
                         )) || '.' ||
                         pg_catalog.quote_ident('endpoint_deletion_audit') ||
                         ' USING btree (run_id, endpoint_id)'
                  )
              )
              OR (
                  ic.relname = 'idx_endpoint_deletion_audit_endpoint'
                  AND (
                      i.indisunique OR i.indisprimary
                      OR i.indnkeyatts <> 1 OR i.indnatts <> 1
                      OR i.indoption::TEXT <> '0'
                      OR ARRAY(
                          SELECT a.attname::TEXT
                          FROM pg_catalog.unnest(i.indkey)
                              WITH ORDINALITY AS key(attnum, position)
                          JOIN pg_catalog.pg_attribute a
                            ON a.attrelid = i.indrelid
                           AND a.attnum = key.attnum
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      ) <> ARRAY['endpoint_id']::TEXT[]
                      OR ARRAY(
                          SELECT opc.opcname::TEXT
                          FROM pg_catalog.unnest(i.indclass)
                              WITH ORDINALITY AS key(opclass_oid, position)
                          JOIN pg_catalog.pg_opclass opc
                            ON opc.oid = key.opclass_oid
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      ) <> ARRAY['text_ops']::TEXT[]
                      OR ARRAY(
                          SELECT collation_oid::OID
                          FROM pg_catalog.unnest(i.indcollation)
                              WITH ORDINALITY AS key(collation_oid, position)
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      ) <> ARRAY(
                          SELECT a.attcollation
                          FROM pg_catalog.unnest(i.indkey)
                              WITH ORDINALITY AS key(attnum, position)
                          JOIN pg_catalog.pg_attribute a
                            ON a.attrelid = i.indrelid
                           AND a.attnum = key.attnum
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position
                      )
                      OR pg_catalog.pg_get_indexdef(i.indexrelid) <>
                         'CREATE INDEX idx_endpoint_deletion_audit_endpoint ON ' ||
                         pg_catalog.quote_ident((
                             SELECT n.nspname FROM pg_catalog.pg_namespace n
                             JOIN pg_catalog.pg_class c
                               ON c.relnamespace = n.oid
                             WHERE c.oid = i.indrelid
                         )) || '.' ||
                         pg_catalog.quote_ident('endpoint_deletion_audit') ||
                         ' USING btree (endpoint_id)'
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION 'migration-025 audit index contract is not canonical';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        WHERE c.oid = 'public.endpoint_deletion_audit'::pg_catalog.regclass
          AND c.relacl IS NOT NULL
          AND ARRAY(
              SELECT privilege_type::TEXT
              FROM pg_catalog.aclexplode(c.relacl)
              ORDER BY privilege_type
          ) = ARRAY(
              SELECT privilege_type::TEXT
              FROM pg_catalog.aclexplode(
                  pg_catalog.acldefault('r', c.relowner)
              )
              ORDER BY privilege_type
          )
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.aclexplode(c.relacl)
              WHERE grantee <> c.relowner OR grantor <> c.relowner
                 OR is_grantable
          )
    ) THEN
        RAISE EXCEPTION 'migration-025 audit ACL is not canonical owner-only access';
    END IF;

    IF (
        SELECT pg_catalog.array_agg(column_name::TEXT ORDER BY ordinal_position)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'endpoint_deletion_audit'
    ) <> ARRAY[
        'run_id', 'endpoint_id', 'pmh_url', 'pmh_set',
        'source_id_at_delete', 'legacy_source_endpoint_source_id',
        'repo_record_count_at_delete', 'reason', 'manifest_hash',
        'review_ref', 'executed_by', 'executed_at'
    ]::TEXT[] OR EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'endpoint_deletion_audit'
          AND (
              column_default IS DISTINCT FROM
                  CASE WHEN column_name = 'executed_at' THEN 'now()' END
              OR is_identity <> 'NO'
              OR identity_generation IS NOT NULL
              OR is_generated <> 'NEVER'
              OR generation_expression IS NOT NULL
              OR (data_type = 'text' AND collation_name IS NOT NULL)
          )
    ) THEN
        RAISE EXCEPTION 'migration-025 audit column contract is not canonical';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('run_id', 'uuid', 'NO'),
            ('endpoint_id', 'text', 'NO'),
            ('pmh_url', 'text', 'YES'),
            ('pmh_set', 'text', 'YES'),
            ('source_id_at_delete', 'bigint', 'YES'),
            ('legacy_source_endpoint_source_id', 'bigint', 'YES'),
            ('repo_record_count_at_delete', 'bigint', 'NO'),
            ('reason', 'text', 'NO'),
            ('manifest_hash', 'text', 'NO'),
            ('review_ref', 'text', 'NO'),
            ('executed_by', 'text', 'NO'),
            ('executed_at', 'timestamp with time zone', 'NO')
        ) AS expected(column_name, data_type, is_nullable)
        LEFT JOIN information_schema.columns actual
          ON actual.table_schema = 'public'
         AND actual.table_name = 'endpoint_deletion_audit'
         AND actual.column_name = expected.column_name
        WHERE actual.column_name IS NULL
           OR actual.data_type <> expected.data_type
           OR actual.is_nullable <> expected.is_nullable
    ) THEN
        RAISE EXCEPTION 'migration-025 audit column types are not canonical';
    END IF;

    IF (
        SELECT pg_catalog.array_agg(t.tgname::TEXT ORDER BY t.tgname)
        FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = 'public.endpoint_deletion_audit'::pg_catalog.regclass
          AND NOT t.tgisinternal
    ) <> ARRAY[
        'endpoint_deletion_audit_no_row_mutation',
        'endpoint_deletion_audit_no_truncate'
    ]::TEXT[] OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger t
        JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
        JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid
        JOIN pg_catalog.pg_language l ON l.oid = p.prolang
        WHERE t.tgrelid = 'public.endpoint_deletion_audit'::pg_catalog.regclass
          AND NOT t.tgisinternal
          AND (
              t.tgenabled <> 'O' OR t.tgqual IS NOT NULL
              OR t.tgattr::TEXT <> '' OR t.tgnargs <> 0
              OR t.tgtype <> CASE t.tgname
                  WHEN 'endpoint_deletion_audit_no_row_mutation' THEN 27
                  WHEN 'endpoint_deletion_audit_no_truncate' THEN 34
              END
              OR p.proname <> 'reject_endpoint_deletion_audit_mutation'
              OR p.pronamespace <> c.relnamespace
              OR pg_catalog.btrim(pg_catalog.regexp_replace(
                  p.prosrc, '\s+', ' ', 'g'
              )) <> 'BEGIN RAISE EXCEPTION ''endpoint_deletion_audit is append-only''; END;'
              OR l.lanname <> 'plpgsql' OR p.provolatile <> 'v'
              OR p.prosecdef OR p.proleakproof OR p.proparallel <> 'u'
              OR p.proconfig IS NOT NULL
              OR pg_catalog.pg_get_function_identity_arguments(p.oid) <> ''
          )
    ) THEN
        RAISE EXCEPTION 'migration-025 audit trigger contract is not canonical';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sources'
          AND column_name = 'id' AND data_type = 'bigint'
          AND is_identity = 'YES' AND identity_generation = 'ALWAYS'
          AND is_generated = 'NEVER'
    ) THEN
        RAISE EXCEPTION 'sources.id must be GENERATED ALWAYS identity';
    END IF;
END;
$contract$;

-- Disable the pre-retirement hard-delete executor fail-closed. Its schema check
-- requires the old table name and therefore refuses before DELETE statements.
-- Preserve its immutable historical rows under an explicit archive name.
DO $rules$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_rewrite r
        WHERE r.ev_class = 'public.endpoint_deletion_audit'::pg_catalog.regclass
          AND r.rulename <> '_RETURN'
    ) THEN
        RAISE EXCEPTION 'endpoint_deletion_audit has unsupported rewrite rules';
    END IF;
END;
$rules$;

ALTER TABLE public.endpoint_deletion_audit
    RENAME TO endpoint_deletion_audit_archived_pre_retirement;

CREATE TRIGGER endpoint_deletion_audit_archive_no_insert
BEFORE INSERT ON public.endpoint_deletion_audit_archived_pre_retirement
FOR EACH ROW
EXECUTE FUNCTION public.reject_endpoint_deletion_audit_mutation();

REVOKE INSERT
    ON public.endpoint_deletion_audit_archived_pre_retirement
    FROM PUBLIC;

ALTER TABLE public.endpoint
    ADD COLUMN status TEXT DEFAULT 'active',
    ADD COLUMN retirement_reason TEXT,
    ADD COLUMN retired_at TIMESTAMPTZ;

DO $lifecycle_dependencies$
BEGIN
    IF EXISTS (
        WITH updated_columns AS (
            SELECT attnum
            FROM pg_catalog.pg_attribute
            WHERE attrelid = 'public.endpoint'::pg_catalog.regclass
              AND attname IN (
                  'status', 'retirement_reason', 'retired_at',
                  'ready_to_run', 'in_walden'
              )
        )
        SELECT 1
        FROM pg_catalog.pg_index idx
        WHERE idx.indrelid = 'public.endpoint'::pg_catalog.regclass
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_depend d
              JOIN updated_columns u ON u.attnum = d.refobjsubid
              WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                AND d.objid = idx.indexrelid
                AND d.refobjid = idx.indrelid
          )
    ) OR EXISTS (
        WITH updated_columns AS (
            SELECT attnum
            FROM pg_catalog.pg_attribute
            WHERE attrelid = 'public.endpoint'::pg_catalog.regclass
              AND attname IN (
                  'status', 'retirement_reason', 'retired_at',
                  'ready_to_run', 'in_walden'
              )
        )
        SELECT 1
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_attrdef ad
          ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE a.attrelid = 'public.endpoint'::pg_catalog.regclass
          AND a.attgenerated <> ''
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_depend d
              JOIN updated_columns u ON u.attnum = d.refobjsubid
              WHERE d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                AND d.objid = ad.oid
                AND d.refobjid = a.attrelid
          )
    ) THEN
        RAISE EXCEPTION 'endpoint retirement fields have unsafe index/generated dependencies';
    END IF;
END;
$lifecycle_dependencies$;

ALTER TABLE public.endpoint
    ADD CONSTRAINT endpoint_status_allowed
        CHECK (status IS NULL OR status IN ('active', 'retired')),
    ADD CONSTRAINT endpoint_retirement_fields_coherent
        CHECK (
            CASE
                WHEN status IS NULL THEN
                    retirement_reason IS NULL AND retired_at IS NULL
                WHEN status = 'retired' THEN
                    retirement_reason IS NOT NULL
                    AND btrim(retirement_reason) <> ''
                    AND retired_at IS NOT NULL
                    AND ready_to_run IS FALSE
                    AND in_walden IS FALSE
                ELSE
                    retirement_reason IS NULL AND retired_at IS NULL
            END
        );

COMMENT ON COLUMN public.endpoint.status IS
  'Endpoint lifecycle: active or retired. Nullable by design; writers should use an explicit supported value.';
COMMENT ON COLUMN public.endpoint.retirement_reason IS
  'Reviewed reason for retirement; required exactly when status is retired.';
COMMENT ON COLUMN public.endpoint.retired_at IS
  'Database transaction time when status was changed to retired.';

CREATE TABLE public.endpoint_retirement_audit (
    endpoint_id                       TEXT        PRIMARY KEY,
    run_id                            UUID        NOT NULL,
    name                              TEXT,
    pmh_url                           TEXT,
    pmh_set                           TEXT,
    source_id_before                  BIGINT,
    legacy_source_endpoint_source_id  BIGINT,
    legacy_sources_endpoint_source_ids_before BIGINT[] NOT NULL,
    status_before                     TEXT        NOT NULL,
    retirement_reason_before          TEXT,
    retired_at_before                 TIMESTAMPTZ,
    ready_to_run_before               BOOLEAN,
    in_walden_before                  BOOLEAN,
    status_after                      TEXT        NOT NULL,
    retirement_reason                 TEXT        NOT NULL,
    retired_at                        TIMESTAMPTZ NOT NULL,
    reason                            TEXT        NOT NULL,
    database_name                     TEXT        NOT NULL,
    schema_name                       TEXT        NOT NULL,
    manifest_hash                     TEXT        NOT NULL,
    plan_hash                         TEXT        NOT NULL,
    review_ref                        TEXT        NOT NULL,
    executed_by                       TEXT        NOT NULL,
    executed_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT endpoint_retirement_audit_legacy_sources_ids_no_nulls
        CHECK (array_position(legacy_sources_endpoint_source_ids_before, NULL) IS NULL),
    CONSTRAINT endpoint_retirement_audit_transition
        CHECK (
            status_before = 'active'
            AND retirement_reason_before IS NULL
            AND retired_at_before IS NULL
            AND status_after = 'retired'
        ),
    CONSTRAINT endpoint_retirement_audit_reason_matches
        CHECK (retirement_reason = reason),
    CONSTRAINT endpoint_retirement_audit_reason_present
        CHECK (btrim(reason) <> ''),
    CONSTRAINT endpoint_retirement_audit_database_name_present
        CHECK (btrim(database_name) <> ''),
    CONSTRAINT endpoint_retirement_audit_schema_name_present
        CHECK (btrim(schema_name) <> ''),
    CONSTRAINT endpoint_retirement_audit_manifest_hash_present
        CHECK (btrim(manifest_hash) <> ''),
    CONSTRAINT endpoint_retirement_audit_plan_hash_present
        CHECK (btrim(plan_hash) <> ''),
    CONSTRAINT endpoint_retirement_audit_review_ref_present
        CHECK (btrim(review_ref) <> ''),
    CONSTRAINT endpoint_retirement_audit_executed_by_present
        CHECK (btrim(executed_by) <> '')
);

-- PostgreSQL places an unqualified index name in its explicitly qualified
-- table's schema; CREATE INDEX does not accept a schema-qualified index name.
CREATE INDEX idx_endpoint_retirement_audit_run
    ON public.endpoint_retirement_audit(run_id);

COMMENT ON TABLE public.endpoint_retirement_audit IS
  'One append-only receipt per endpoint retirement. No FK: history must survive later operational cleanup.';
COMMENT ON COLUMN public.endpoint_retirement_audit.review_ref IS
  'Structured execution approval, approved plan hash, and row-level manifest review reference.';

CREATE FUNCTION public.reject_endpoint_retirement_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'endpoint_retirement_audit is append-only';
END;
$function$;

CREATE TRIGGER endpoint_retirement_audit_no_row_mutation
BEFORE UPDATE OR DELETE ON public.endpoint_retirement_audit
FOR EACH ROW
EXECUTE FUNCTION public.reject_endpoint_retirement_audit_mutation();

CREATE TRIGGER endpoint_retirement_audit_no_truncate
BEFORE TRUNCATE ON public.endpoint_retirement_audit
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_endpoint_retirement_audit_mutation();

REVOKE UPDATE, DELETE, TRUNCATE
    ON public.endpoint_retirement_audit
    FROM PUBLIC;

-- Default privileges can grant this newly created table to named roles. Fail
-- the whole migration rather than committing an audit table that the executor
-- and rollback procedures will refuse as noncanonical.
DO $retirement_audit_acl$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        WHERE c.oid = 'public.endpoint_retirement_audit'::pg_catalog.regclass
          AND c.relacl IS NOT NULL
          AND ARRAY(
              SELECT privilege_type::TEXT
              FROM pg_catalog.aclexplode(c.relacl)
              ORDER BY privilege_type
          ) = ARRAY(
              SELECT privilege_type::TEXT
              FROM pg_catalog.aclexplode(
                  pg_catalog.acldefault('r', c.relowner)
              )
              ORDER BY privilege_type
          )
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.aclexplode(c.relacl)
              WHERE grantee <> c.relowner OR grantor <> c.relowner
                 OR is_grantable
          )
    ) THEN
        RAISE EXCEPTION 'endpoint_retirement_audit ACL is not canonical owner-only access';
    END IF;
END;
$retirement_audit_acl$;

CREATE FUNCTION public.enforce_endpoint_retirement_receipt()
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
        IF OLD.status = 'retired' THEN
            RAISE EXCEPTION 'retired endpoint DELETE is forbidden';
        END IF;
        RETURN NULL;
    END IF;
    IF OLD.status = 'retired' AND NEW.status IS DISTINCT FROM 'retired' THEN
        RAISE EXCEPTION 'receipted retired endpoint cannot leave retired status';
    END IF;

    IF OLD.status = 'retired' AND NEW.status = 'retired' THEN
        IF NEW.id IS DISTINCT FROM OLD.id
            OR NEW.name IS DISTINCT FROM OLD.name
            OR NEW.pmh_url IS DISTINCT FROM OLD.pmh_url
            OR NEW.pmh_set IS DISTINCT FROM OLD.pmh_set
            OR NEW.source_id IS DISTINCT FROM OLD.source_id
            OR NEW.retirement_reason IS DISTINCT FROM OLD.retirement_reason
            OR NEW.retired_at IS DISTINCT FROM OLD.retired_at
        THEN
            RAISE EXCEPTION 'retired endpoint lifecycle and identity are immutable';
        END IF;
        RETURN NULL;
    END IF;
    IF NEW.status = 'retired' THEN
        SELECT EXISTS (
            SELECT 1
            FROM ONLY public.endpoint_retirement_audit a
            WHERE a.endpoint_id = OLD.id
              AND a.status_before = OLD.status
              AND a.retirement_reason_before IS NOT DISTINCT FROM OLD.retirement_reason
              AND a.retired_at_before IS NOT DISTINCT FROM OLD.retired_at
              AND a.ready_to_run_before IS NOT DISTINCT FROM OLD.ready_to_run
              AND a.in_walden_before IS NOT DISTINCT FROM OLD.in_walden
              AND a.name IS NOT DISTINCT FROM OLD.name
              AND a.pmh_url IS NOT DISTINCT FROM OLD.pmh_url
              AND a.pmh_set IS NOT DISTINCT FROM OLD.pmh_set
              AND a.source_id_before IS NOT DISTINCT FROM OLD.source_id
              AND NEW.name IS NOT DISTINCT FROM OLD.name
              AND NEW.pmh_url IS NOT DISTINCT FROM OLD.pmh_url
              AND NEW.pmh_set IS NOT DISTINCT FROM OLD.pmh_set
              AND NEW.source_id IS NOT DISTINCT FROM OLD.source_id
              AND NEW.id IS NOT DISTINCT FROM OLD.id
              AND a.legacy_source_endpoint_source_id IS NOT DISTINCT FROM (
                  SELECT se.source_id
                  FROM ONLY public.source_endpoint se
                  WHERE se.endpoint_id = NEW.id
              )
              AND a.legacy_sources_endpoint_source_ids_before = ARRAY(
                  SELECT s.id
                  FROM ONLY public.sources s
                  WHERE s.endpoint_id = NEW.id
                  ORDER BY s.id
              )
              AND a.status_after = 'retired'
              AND a.retirement_reason = NEW.retirement_reason
              AND a.reason = NEW.retirement_reason
              AND a.retired_at = NEW.retired_at
              AND a.executed_at = NEW.retired_at
              AND a.database_name = pg_catalog.current_database()
              AND a.schema_name = TG_TABLE_SCHEMA
              AND (
                  OLD.status = 'retired'
                  OR a.xmin = pg_catalog.pg_current_xact_id()::xid
              )
        ) INTO receipt_matches;
        IF NOT receipt_matches THEN
            RAISE EXCEPTION 'retired endpoint requires exact immutable receipt';
        END IF;
    END IF;
    RETURN NULL;
END;
$function$;

CREATE CONSTRAINT TRIGGER endpoint_retirement_requires_receipt
AFTER INSERT OR UPDATE OR DELETE ON public.endpoint
NOT DEFERRABLE
INITIALLY IMMEDIATE
FOR EACH ROW
EXECUTE FUNCTION public.enforce_endpoint_retirement_receipt();
