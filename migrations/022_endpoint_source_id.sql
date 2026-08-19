-- 022: endpoint.source_id -- expand-only
-- oxjob 83.13; core relationship decided at the 2026-08-18 10:59 meeting
--
-- Add the future canonical endpoint-to-Source relationship as a nullable
-- foreign key. This migration intentionally performs no backfill, does not
-- enforce NOT NULL, and does not remove either legacy relationship store.
-- Those changes follow only after endpoint assignments are reviewed and all
-- writers and consumers have moved to endpoint.source_id.
--
-- Cardinality: sources.id 1 <- N endpoint.source_id. source_id is deliberately
-- not unique: every retained endpoint will eventually have exactly one Source,
-- while one Source may have many endpoints. endpoint.id remains the durable
-- endpoint identity used in harvested-data lineage.
--
-- SET LOCAL depends on migrate.py executing each migration inside
-- engine.begin(). Fail the release instead of waiting indefinitely for locks.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE endpoint
    ADD COLUMN source_id BIGINT;

ALTER TABLE endpoint
    ADD CONSTRAINT endpoint_source_id_fkey
    FOREIGN KEY (source_id)
    REFERENCES sources(id)
    ON DELETE RESTRICT;

CREATE INDEX idx_endpoint_source_id ON endpoint(source_id);

COMMENT ON COLUMN endpoint.source_id IS
  'Future canonical Source for this endpoint. Many endpoints may reference one Source. Nullable only during the reviewed transition; a later migration enforces NOT NULL.';
