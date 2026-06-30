-- 002 make sources.id auto-mint from source_id_seq on insert (oxjob #548)
--
-- A plain INSERT (no id) now mints the next S-id from source_id_seq. Explicit-id
-- inserts (e.g. the initial backfill / any re-import) still work and bypass the
-- sequence -- the loader calls setval(source_id_seq, MAX(id)) afterward to keep it
-- ahead of the highest assigned id.
ALTER SEQUENCE source_id_seq OWNED BY sources.id;
ALTER TABLE sources ALTER COLUMN id SET DEFAULT nextval('source_id_seq');
