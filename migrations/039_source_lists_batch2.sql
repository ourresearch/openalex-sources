-- 039 source lists, batch 2 (oxjob #1205 phase 4): register five more external
-- allow lists so jobs.load_source_list can load them. Membership rows come from
-- the CSVs written by jobs.fetch_source_list; this migration only adds the
-- list metadata. Non-normative throughout: a row says who maintains the list
-- and what it covers, never that OpenAlex endorses it.
--
-- Deliberately NOT included: Scopus (Jason, 2026-09-17: the boolean
-- is_indexed_in_scopus stays, unadvertised; no list) and any deny list.
--
-- Licence status at the time of writing (verify before the first load):
--   medline    US public domain; NLM asks for "Courtesy of the U.S. National Library of Medicine"
--   norway     NLOD 2.0 (API terms) + CC BY 4.0 (site footer)
--   scielo     site footer CC BY 4.0; not stated for ArticleMeta metadata
--   jufo       not stated for the data (site material CC BY 4.0) — ask julkaisufoorumi@tsv.fi
--   erih-plus  NLOD via the kanalregister API terms vs CC BY-NC 4.0 on erihplus.hkdir.no — ask HK-dir

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('medline', 'MEDLINE',
   'U.S. National Library of Medicine (NLM)',
   'https://www.nlm.nih.gov/medline/medline_overview.html',
   'Journals currently indexed for MEDLINE (NLM Catalog "currently indexed"), biomedicine and life sciences. Courtesy of the U.S. National Library of Medicine.'),
  ('norway', 'Norwegian Register for Scientific Journals, Series and Publishers',
   'Norwegian Directorate for Higher Education and Skills (HK-dir)',
   'https://kanalregister.hkdir.no/',
   'Journals and series approved at level 1 or 2 in the Norwegian register (Kanalregisteret), all fields; also used by Sweden. Level is not exposed.'),
  ('jufo', 'Publication Forum (JUFO)',
   'Federation of Finnish Learned Societies (TSV)',
   'https://julkaisufoorumi.fi/en',
   'Journals and series rated level 1, 2 or 3 by the Finnish Publication Forum, all fields. Level is not exposed.'),
  ('erih-plus', 'ERIH PLUS',
   'Norwegian Directorate for Higher Education and Skills (HK-dir)',
   'https://erihplus.hkdir.no/',
   'European Reference Index for the Humanities and Social Sciences: approved journals in the humanities and social sciences.'),
  ('scielo', 'SciELO',
   'SciELO (Scientific Electronic Library Online)',
   'https://www.scielo.org/',
   'Current journals in the certified SciELO network collections (Ibero-America and South Africa), via ArticleMeta. Distinct from sources.is_in_scielo, which flags DOIs registered through SciELO.')
ON CONFLICT (id) DO NOTHING;
