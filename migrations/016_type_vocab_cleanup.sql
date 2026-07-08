-- 016 source_type vocabulary cleanup (oxjob #548; Casey 2026-07-07)
--
-- igsnCatalog and raidRegistry are DataCite CLIENT types, not OpenAlex source
-- types. They leaked in via the walden backfill (all 47 rows created
-- 2026-06-30, zero works hosted) and the backfill-derived vocabulary
-- enshrined them. The app's own mint path was never affected:
-- CLIENT_TYPE_TO_SOURCE_TYPE defaults unknown client types to 'repository'.
--
--   igsnCatalog (35)  -> repository  (physical-sample DOI repositories: SESAR,
--                                     GFZ, PANGAEA core repositories, ...)
--   raidRegistry (12) -> other       (research-activity ID registries; they
--                                     host no documents at all)
--
-- Also: the 6 remaining NULL-type rows (2024-03-05 walden era) are all
-- journals by inspection (Physik-Journal, Collagen and Leather, Frontiers of
-- Mathematics, 2x Journal of Umm Al-Qura University, Medical history.
-- Supplement) -- set them, then lock the column NOT NULL so a type-less
-- source can't reappear.

UPDATE sources SET type = 'repository', updated_date = now() WHERE type = 'igsnCatalog';
UPDATE sources SET type = 'other',      updated_date = now() WHERE type = 'raidRegistry';
UPDATE sources SET type = 'journal',    updated_date = now() WHERE type IS NULL;

DELETE FROM source_type WHERE source_type_id IN ('igsnCatalog', 'raidRegistry');

ALTER TABLE sources ALTER COLUMN type SET NOT NULL;
