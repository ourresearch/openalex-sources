-- 040 source lists: one list per level (oxjob #1205, Jason 2026-09-18).
-- The level registers (Norway, JUFO, JPPS) exist to get away from the
-- in-or-out binary of WoS/Scopus; collapsing them to one list would lose the
-- point. So: norway-1/norway-2, jufo-1/2/3, jpps-1/2/3, each its own list.
-- The flat `norway` and `jufo` lists loaded earlier today are dropped (members
-- cascade); the next jobs.load_source_list run recomputes sources.listed_in.
-- Also registers Latindex Catálogo 2.0. Non-normative throughout.
-- Naming (Jason, 2026-09-18): ids and display names keep the maintainer's own
-- level labels (norway-2, jufo-3, jpps-1). All three count up (higher = more
-- selective), so no direction tag; revisit only if a list that counts the
-- other way (ABDC A*, CNRS rank 1) is ever loaded.

DELETE FROM source_list WHERE id IN ('norway', 'jufo');

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('norway-1', 'Norwegian Register, level 1',
   'Norwegian Directorate for Higher Education and Skills (HK-dir)',
   'https://kanalregister.hkdir.no/',
   'Journals and series at level 1 (standard) in the Norwegian Register for Scientific Journals, Series and Publishers, all fields; also used by Sweden.'),
  ('norway-2', 'Norwegian Register, level 2',
   'Norwegian Directorate for Higher Education and Skills (HK-dir)',
   'https://kanalregister.hkdir.no/',
   'Journals and series at level 2 (the most selective tier, about 20% of publications) in the Norwegian Register for Scientific Journals, Series and Publishers, all fields.'),
  ('jufo-1', 'Publication Forum (JUFO), level 1',
   'Federation of Finnish Learned Societies (TSV)',
   'https://julkaisufoorumi.fi/en',
   'Journals and series rated level 1 (basic) by the Finnish Publication Forum, all fields.'),
  ('jufo-2', 'Publication Forum (JUFO), level 2',
   'Federation of Finnish Learned Societies (TSV)',
   'https://julkaisufoorumi.fi/en',
   'Journals and series rated level 2 (leading) by the Finnish Publication Forum, all fields.'),
  ('jufo-3', 'Publication Forum (JUFO), level 3',
   'Federation of Finnish Learned Societies (TSV)',
   'https://julkaisufoorumi.fi/en',
   'Journals and series rated level 3 (top) by the Finnish Publication Forum, all fields.'),
  ('jpps-1', 'JPPS, one star',
   'African Journals Online (AJOL) and INASP',
   'https://www.journalquality.info/',
   'Journals assessed at one star under the Journal Publishing Practices and Standards framework, on the AJOL, NepJOL, BanglaJOL, CamJOL, MongoliaJOL and SLJOL platforms (Global South).'),
  ('jpps-2', 'JPPS, two stars',
   'African Journals Online (AJOL) and INASP',
   'https://www.journalquality.info/',
   'Journals assessed at two stars under the Journal Publishing Practices and Standards framework, on the AJOL, NepJOL, BanglaJOL, CamJOL, MongoliaJOL and SLJOL platforms (Global South).'),
  ('jpps-3', 'JPPS, three stars',
   'African Journals Online (AJOL) and INASP',
   'https://www.journalquality.info/',
   'Journals assessed at three stars under the Journal Publishing Practices and Standards framework, on the AJOL, NepJOL, BanglaJOL, CamJOL, MongoliaJOL and SLJOL platforms (Global South).'),
  ('latindex', 'Latindex Catálogo 2.0',
   'Latindex (UNAM and partner institutions)',
   'https://www.latindex.org/',
   'Current journals in Latindex Catálogo 2.0, the quality-criteria catalogue for Latin America, the Caribbean, Spain and Portugal.')
ON CONFLICT (id) DO NOTHING;
