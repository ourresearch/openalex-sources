-- 021: D1 tombstone removal — schema record (oxjob #629 endgame, applied 2026-08-13)
--
-- The attended D1 migration (run 2026-08-13, transaction-wrapped, assert-guarded;
-- package in rohan's sources-work/d1_plan/) deleted all 31,855 tombstoned rows,
-- re-pointed their datacite links to terminal winners, and dropped the merge
-- columns. This migration records that state so a fresh-database rebuild
-- (001..020 recreate the legacy merge schema) converges to the post-D1 shape.
-- Every statement is idempotent: a no-op on the already-migrated production
-- database, a real drop on a fresh rebuild (where no tombstone data can exist,
-- since migrations run before any ingest).
--
-- NOTE for migrate.py: plain DDL only — no DO blocks, no RAISE, no '%'
-- characters; the runner executes this file raw via exec_driver_sql and
-- percent signs break psycopg2 parameter handling.
--
-- The source_merge ledger is KEPT — it is now the sole record of merges and
-- the anti-join filter used by the walden mirror (CreateSources).

ALTER TABLE source_merge DROP CONSTRAINT IF EXISTS source_merge_loser_id_fkey;
ALTER TABLE source_merge DROP CONSTRAINT IF EXISTS source_merge_winner_id_fkey;
ALTER TABLE sources      DROP CONSTRAINT IF EXISTS sources_merge_into_id_fkey;

DROP INDEX IF EXISTS idx_sources_merge_into_id;
DROP INDEX IF EXISTS idx_sources_active;

ALTER TABLE sources DROP COLUMN IF EXISTS merge_into_id;
ALTER TABLE sources DROP COLUMN IF EXISTS merge_into_date;

COMMENT ON TABLE source_merge IS
  'Immutable merge ledger (loser_id -> winner_id). Post-D1 (2026-08-13) this is the sole merge record: loser rows are deleted from sources at merge time; consumers anti-join this table. Unmerge-by-restore must also delete the restored loser''s rows here.';
