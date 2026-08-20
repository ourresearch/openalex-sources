-- 023: append-only endpoint Source backfill audit
-- oxjob 83.13; engineering safeguard for the reviewed endpoint.source_id fill
--
-- This migration creates only the audit ledger required by
-- jobs/backfill_endpoint_source.py. It does not populate endpoint.source_id,
-- delete endpoints, change readers or writers, or enforce NOT NULL.
--
-- There are deliberately no foreign keys from this history table to endpoint or
-- sources. Audit evidence must survive later deletion of either operational row.
-- The primary key makes a run retry fail loudly for an endpoint already committed.
-- Row and truncate triggers make ordinary DML attempts fail loudly, including
-- attempts made through the table-owner credential used by the application. They
-- are not privilege isolation: that owner could deliberately disable/drop them.
--
-- SET LOCAL depends on migrate.py executing each numbered file inside one
-- engine.begin() transaction. Re-review the timeouts if that runner changes.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE endpoint_source_backfill_audit (
    run_id                              UUID        NOT NULL,
    endpoint_id                         TEXT        NOT NULL,
    before_source_id                    BIGINT,
    after_source_id                     BIGINT      NOT NULL,
    observed_source_endpoint_source_id  BIGINT,
    observed_legacy_source_ids          JSONB       NOT NULL,
    reason                              TEXT        NOT NULL,
    manifest_hash                       TEXT        NOT NULL,
    plan_hash                           TEXT        NOT NULL,
    scope_hash                          TEXT        NOT NULL,
    review_ref                          TEXT        NOT NULL,
    executed_by                         TEXT        NOT NULL,
    executed_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT endpoint_source_backfill_audit_pkey
        PRIMARY KEY (run_id, endpoint_id),
    CONSTRAINT endpoint_source_backfill_audit_legacy_ids_array
        CHECK (jsonb_typeof(observed_legacy_source_ids) = 'array'),
    CONSTRAINT endpoint_source_backfill_audit_reason_present
        CHECK (btrim(reason) <> ''),
    CONSTRAINT endpoint_source_backfill_audit_manifest_hash_present
        CHECK (btrim(manifest_hash) <> ''),
    CONSTRAINT endpoint_source_backfill_audit_plan_hash_present
        CHECK (btrim(plan_hash) <> ''),
    CONSTRAINT endpoint_source_backfill_audit_scope_hash_present
        CHECK (btrim(scope_hash) <> ''),
    CONSTRAINT endpoint_source_backfill_audit_review_ref_present
        CHECK (btrim(review_ref) <> ''),
    CONSTRAINT endpoint_source_backfill_audit_executed_by_present
        CHECK (btrim(executed_by) <> '')
);

CREATE INDEX idx_endpoint_source_backfill_audit_endpoint
    ON endpoint_source_backfill_audit(endpoint_id);

COMMENT ON TABLE endpoint_source_backfill_audit IS
  'Append-only receipt for reviewed endpoint.source_id backfill writes (oxjob 83.13). Operational endpoint and Source deletion must not erase this history.';

COMMENT ON COLUMN endpoint_source_backfill_audit.review_ref IS
  'Durable plan approval reference plus the execution-scope hash and any row-level approval reference.';

CREATE FUNCTION reject_endpoint_source_backfill_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'endpoint_source_backfill_audit is append-only';
END;
$function$;

CREATE TRIGGER endpoint_source_backfill_audit_no_row_mutation
BEFORE UPDATE OR DELETE ON endpoint_source_backfill_audit
FOR EACH ROW
EXECUTE FUNCTION reject_endpoint_source_backfill_audit_mutation();

CREATE TRIGGER endpoint_source_backfill_audit_no_truncate
BEFORE TRUNCATE ON endpoint_source_backfill_audit
FOR EACH STATEMENT
EXECUTE FUNCTION reject_endpoint_source_backfill_audit_mutation();

REVOKE UPDATE, DELETE, TRUNCATE
    ON endpoint_source_backfill_audit
    FROM PUBLIC;
