-- 013 source-level OA curation overrides (oxjob #548 post-cutover review, finding F3)
-- The retired CreateSources DLT applied users-api prototype curations
-- (openalex.curations.approved_curations, properties is_oa / oa_flip_year) as the
-- FINAL override on is_oa_high_oa_rate / high_oa_rate_start_year. The app never
-- re-established that, so the first apply_oa_flags runs reverted the curated
-- values (arXiv / Zenodo / PubMed Central briefly served is_oa=false at cutover).
-- This table freezes the curation intent; jobs/apply_oa_flags.py applies it with
-- the DLT's exact precedence. Phase-4 curations-apply will supersede this table.

CREATE TABLE IF NOT EXISTS source_oa_override (
    source_id        BIGINT PRIMARY KEY,
    curated_is_oa    BOOLEAN,   -- when set: final word on is_oa_high_oa_rate
    curated_flip_year INTEGER,  -- when set (and is_oa not false): start_year = flip+1
    note             TEXT
);

INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) VALUES
  (24112332,   TRUE,  NULL, 'Pakistan Journal of Pharmaceutical Sciences'),
  (69273698,   TRUE,  2026, 'Digestive and Liver Disease'),
  (191833357,  FALSE, NULL, 'The American Biology Teacher'),
  (202381698,  FALSE, 2005, 'PLoS ONE (is_oa=false wins; flip ignored per DLT precedence)'),
  (2764455111, TRUE,  NULL, 'PubMed Central'),
  (4306400194, TRUE,  NULL, 'arXiv (Cornell University)'),
  (4306400487, TRUE,  NULL, 'Infoscience (EPFL)'),
  (4306400562, TRUE,  NULL, 'Zenodo (CERN)'),
  (4387288460, FALSE, 2020, 'Proceeding B-ICON (is_oa=false wins)'),
  (7407055455, TRUE,  NULL, 'Federal Open Science Repository of Canada')
ON CONFLICT (source_id) DO NOTHING;
