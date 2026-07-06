-- 012 sources.id -> GENERATED ALWAYS AS IDENTITY (oxjob #548)
-- The DEFAULT nextval() setup let any INSERT silently supply its own id and
-- leave the sequence behind MAX(id) (bitten 2026-07-06: a dry-run setval stuck
-- because sequences are non-transactional). As an identity column, explicit ids
-- require OVERRIDING SYSTEM VALUE, so only the two scripts that legitimately
-- write specific S-ids (initial backfill, cutover walden-mint import) can --
-- and each resyncs the sequence to MAX(id) afterwards. Uniqueness itself was
-- always enforced by the PK.

ALTER TABLE sources ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS source_id_seq;
ALTER TABLE sources ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;
SELECT setval(pg_get_serial_sequence('sources', 'id'), (SELECT MAX(id) FROM sources));
