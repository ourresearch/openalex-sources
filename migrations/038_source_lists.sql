-- 038 source lists: "this source appears on external list X" (oxjob #1205)
--
-- Replaces the one-boolean-per-list pattern (is_core, is_in_doaj, ...) with a
-- multivalued, non-normative membership column. `source_list` is the registry of
-- lists we know about (public id, maintainer, URL, loaded edition);
-- `source_list_member` holds ISSN-keyed membership for lists loaded from a file
-- (one row per ISSN). `sources.listed_in` is DERIVED — the single writer is
-- sources_lib.recompute_listed_in(), which ORs file-loaded memberships (via
-- source_issn) with the legacy booleans (is_core -> 'cwts-core',
-- is_in_doaj -> 'doaj'). Walden mirrors the column verbatim (CreateSources).
--
-- Updating a file-loaded list is a manual, by-hand step (no scheduled job, by
-- decision): python -m jobs.load_source_list --list <id> --csv <file> --version <date>

CREATE TABLE IF NOT EXISTS source_list (
    id            TEXT PRIMARY KEY,        -- public value in sources.listed_in (kebab-case)
    display_name  TEXT NOT NULL,
    maintainer    TEXT,
    url           TEXT,
    scope         TEXT,
    list_version  DATE,                    -- edition currently loaded (file-loaded lists)
    loaded_at     TIMESTAMPTZ
);
COMMENT ON TABLE source_list IS 'External source lists exposed via sources.listed_in (oxjob #1205). Non-normative: membership only.';

CREATE TABLE IF NOT EXISTS source_list_member (
    list_id            TEXT NOT NULL REFERENCES source_list(id) ON DELETE CASCADE,
    issn               TEXT NOT NULL,
    name               TEXT,
    active             BOOLEAN NOT NULL DEFAULT TRUE,
    withdrawn_date     DATE,
    withdrawal_reason  TEXT,
    PRIMARY KEY (list_id, issn)
);
CREATE INDEX IF NOT EXISTS idx_source_list_member_issn ON source_list_member (issn);
COMMENT ON TABLE source_list_member IS 'ISSN-keyed membership for file-loaded source lists; joins to source_issn. Inactive rows are kept for history and do not count as members.';

ALTER TABLE sources ADD COLUMN IF NOT EXISTS listed_in TEXT[];
COMMENT ON COLUMN sources.listed_in IS 'Derived (sources_lib.recompute_listed_in): ids of external lists this source appears on — cwts-core (is_core), doaj (is_in_doaj), plus active source_list_member rows. NULL = none. Non-normative.';

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('cwts-core', 'CWTS Core sources',
   'Centre for Science and Technology Studies (CWTS), Leiden University',
   'https://zenodo.org/records/13879982',
   'Sources included in the Leiden Ranking Open Edition; derived from sources.is_core (one-time 2024 load).'),
  ('doaj', 'Directory of Open Access Journals',
   'DOAJ', 'https://doaj.org/',
   'Fully open-access journals vetted by DOAJ; derived from sources.is_in_doaj (weekly jobs/doaj).'),
  ('doyens', 'Liste de revues recommandables (CDD / CNU Santé)',
   'Conférence des Doyens de Médecine and Conseil National des Universités – Santé (France)',
   'https://conferencedesdoyensdemedecine.org/la-conference-des-doyens-de-medecine-et-du-cnu-sante-luttent-contre-les-revues-predatrices/',
   'Health, medicine and biology journals in French and English; updated quarterly by the maintainers, loaded by hand from the published spreadsheet.')
ON CONFLICT (id) DO NOTHING;
