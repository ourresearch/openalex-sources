-- ============================================================================
-- 021_d1_tombstone_removal.sql — record D1 (merge-column removal) in the
-- forward-only migration history. Drafted 2026-08-12 (dual-model validation,
-- ship-validation-20260812 blocker 3); ships in the Step-7 registry-app
-- deploy commit as migrations/021_d1_tombstone_removal.sql.
--
-- WHY THIS FILE EXISTS: the Procfile release phase replays migrate.py on
-- every deploy, and migrations stop at 020 — without a 021, a fresh database
-- rebuild would resurrect the merge columns / FKs / indexes that the attended
-- D1 migration (d1_migration.sql) dropped, and prod's manual D1 would be
-- unrecorded in schema_migrations.
--
-- FAIL-SAFE PATTERN (this file is NOT the D1 migration and must never act
-- as one):
--   * On a database that still contains tombstoned rows, it REFUSES —
--     RAISE EXCEPTION tells the operator to run the attended D1 migration
--     (d1_migration.sql, with its export-fed asserts) first. The exception
--     aborts migrate.py's transaction, so the release fails loudly and
--     nothing is recorded.
--   * On an empty/fresh rebuild (or any DB with zero tombstones), it drops
--     the obsolete tombstone schema idempotently (IF EXISTS guards
--     throughout): the two merge columns, their FKs, the two merge-column
--     indexes, and re-documents source_merge as the dangling-id ledger.
--   * On prod, the attended D1 run already did all of this — the operator
--     inserts the schema_migrations row for 021 MANUALLY right after the
--     attended run (see GO-CHECKLIST.md Step 7; migrate.py row format:
--     INSERT INTO schema_migrations (version) VALUES ('021'), applied_at
--     defaults to now()) so this file is skipped as already-applied. If it
--     ever runs anyway, every statement is a no-op by the IF EXISTS guards.
-- ============================================================================

DO $$
DECLARE n bigint;
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'public' AND table_name = 'sources'
                AND column_name = 'merge_into_id') THEN
    EXECUTE 'SELECT count(*) FROM sources WHERE merge_into_id IS NOT NULL' INTO n;
    IF n > 0 THEN
      RAISE EXCEPTION '021_d1_tombstone_removal: % tombstoned rows still present. This '
        'migration must NOT delete data — run the attended D1 migration '
        '(~/sources-work/d1_plan/d1_migration.sql, with the same-day '
        'd1_pre_export.sh values) first, then insert the schema_migrations '
        'row for 021 manually per GO-CHECKLIST.md Step 7.', n;
    END IF;
  END IF;
END $$;

-- Zero tombstones (fresh rebuild, or D1 already ran): drop the obsolete
-- tombstone schema. Same object set as d1_migration.sql S2/S5, idempotent.
ALTER TABLE source_merge DROP CONSTRAINT IF EXISTS source_merge_loser_id_fkey;
ALTER TABLE source_merge DROP CONSTRAINT IF EXISTS source_merge_winner_id_fkey;
ALTER TABLE sources      DROP CONSTRAINT IF EXISTS sources_merge_into_id_fkey;
DROP INDEX IF EXISTS idx_sources_merge_into_id;
DROP INDEX IF EXISTS idx_sources_active;
ALTER TABLE sources DROP COLUMN IF EXISTS merge_into_id;
ALTER TABLE sources DROP COLUMN IF EXISTS merge_into_date;

COMMENT ON TABLE source_merge IS
  'Historical merge ledger (D1, 2026-07: loser rows are DELETED from sources at merge close-out; '
  'loser_id/winner_id intentionally have no FK and may reference deleted ids. '
  'Winner may itself appear as a later loser (chains). Preserves the works-style '
  '301-redirect option. Pre-deletion field snapshot: ~/sources-work/d1_plan/exports/ '
  'tombstoned_sources_<stamp>.tsv (the unmerge evidence — see UNMERGE-RUNBOOK.md).';
