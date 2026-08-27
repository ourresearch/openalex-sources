-- 025: append-only endpoint deletion audit
-- oxjob 83.13; durable receipts for reviewed endpoint registry cleanup
--
-- This migration creates only the audit ledger required by
-- jobs/delete_endpoints.py. It does not delete endpoints or change either
-- endpoint-to-Source relationship store.
--
-- There are deliberately no foreign keys from this history table to endpoint or
-- sources. Audit evidence must survive deletion of the operational rows. The
-- primary key makes a run retry fail loudly for an endpoint already committed.
-- Row and truncate triggers make ordinary DML attempts fail loudly, including
-- attempts made through the table-owner credential used by the application. They
-- are not privilege isolation: that owner could deliberately disable/drop them.
--
-- SET LOCAL depends on migrate.py executing each numbered file inside one
-- engine.begin() transaction. Re-review the timeouts if that runner changes.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE endpoint_deletion_audit (
    run_id                            UUID        NOT NULL,
    endpoint_id                       TEXT        NOT NULL,
    pmh_url                           TEXT,
    pmh_set                           TEXT,
    source_id_at_delete               BIGINT,
    legacy_source_endpoint_source_id  BIGINT,
    repo_record_count_at_delete       BIGINT      NOT NULL,
    reason                            TEXT        NOT NULL,
    manifest_hash                     TEXT        NOT NULL,
    review_ref                        TEXT        NOT NULL,
    executed_by                       TEXT        NOT NULL,
    executed_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT endpoint_deletion_audit_pkey
        PRIMARY KEY (run_id, endpoint_id),
    CONSTRAINT endpoint_deletion_audit_repo_record_count_nonnegative
        CHECK (repo_record_count_at_delete >= 0),
    CONSTRAINT endpoint_deletion_audit_reason_present
        CHECK (btrim(reason) <> ''),
    CONSTRAINT endpoint_deletion_audit_manifest_hash_present
        CHECK (btrim(manifest_hash) <> ''),
    CONSTRAINT endpoint_deletion_audit_review_ref_present
        CHECK (btrim(review_ref) <> ''),
    CONSTRAINT endpoint_deletion_audit_executed_by_present
        CHECK (btrim(executed_by) <> '')
);

CREATE INDEX idx_endpoint_deletion_audit_endpoint
    ON endpoint_deletion_audit(endpoint_id);

COMMENT ON TABLE endpoint_deletion_audit IS
  'Append-only receipt for reviewed endpoint and source_endpoint deletions (oxjob 83.13).';

COMMENT ON COLUMN endpoint_deletion_audit.repo_record_count_at_delete IS
  'Count of legacy sources.endpoint_id pointers observed immediately before endpoint deletion.';

COMMENT ON COLUMN endpoint_deletion_audit.review_ref IS
  'Structured execution approval, approved plan hash, and row-level manifest review reference.';

CREATE FUNCTION reject_endpoint_deletion_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'endpoint_deletion_audit is append-only';
END;
$function$;

CREATE TRIGGER endpoint_deletion_audit_no_row_mutation
BEFORE UPDATE OR DELETE ON endpoint_deletion_audit
FOR EACH ROW
EXECUTE FUNCTION reject_endpoint_deletion_audit_mutation();

CREATE TRIGGER endpoint_deletion_audit_no_truncate
BEFORE TRUNCATE ON endpoint_deletion_audit
FOR EACH STATEMENT
EXECUTE FUNCTION reject_endpoint_deletion_audit_mutation();

REVOKE UPDATE, DELETE, TRUNCATE
    ON endpoint_deletion_audit
    FROM PUBLIC;
