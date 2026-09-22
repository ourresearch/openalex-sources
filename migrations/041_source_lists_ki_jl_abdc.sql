-- 041 source lists: Karolinska Institutet Journal List (KI-JL) and the ABDC
-- Journal Quality List (oxjob #1288, suggested by Reese Richardson; Jason OK
-- 2026-09-22). One list per level, as in 040. Non-normative throughout.
--
-- KI-JL: levels 1-3; level 0 ("not recommended") is a non-list state and is
-- not loaded. ABDC: A*, A, B, C. ABDC is the first list whose top tier is not
-- the highest number; ids and display names keep ABDC's own labels and the
-- scope text says which tier is top (the 040 note about direction tags).
--
-- Licence: neither maintainer states a data licence. Same handling as JUFO:
-- maintainer + URL credited on the entity doc, GUI and help center.

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('ki-jl-1', 'KI Journal List, level 1',
   'Karolinska Institutet',
   'https://staff.ki.se/research-support/karolinska-institutet-journal-list-kijl',
   'Journals at level 1 (meets the criteria for scientific publishing) in the Karolinska Institutet Journal List (KI-JL), medicine and health sciences.'),
  ('ki-jl-2', 'KI Journal List, level 2',
   'Karolinska Institutet',
   'https://staff.ki.se/research-support/karolinska-institutet-journal-list-kijl',
   'Journals at level 2 (high standard) in the Karolinska Institutet Journal List (KI-JL), medicine and health sciences.'),
  ('ki-jl-3', 'KI Journal List, level 3',
   'Karolinska Institutet',
   'https://staff.ki.se/research-support/karolinska-institutet-journal-list-kijl',
   'Journals at level 3 (the highest level) in the Karolinska Institutet Journal List (KI-JL), medicine and health sciences.'),
  ('abdc-a-star', 'ABDC Journal Quality List, A*',
   'Australian Business Deans Council (ABDC)',
   'https://abdc.edu.au/abdc-journal-quality-list/',
   'Journals rated A* (the top tier) in the ABDC Journal Quality List, business, economics and related fields.'),
  ('abdc-a', 'ABDC Journal Quality List, A',
   'Australian Business Deans Council (ABDC)',
   'https://abdc.edu.au/abdc-journal-quality-list/',
   'Journals rated A (second tier, below A*) in the ABDC Journal Quality List, business, economics and related fields.'),
  ('abdc-b', 'ABDC Journal Quality List, B',
   'Australian Business Deans Council (ABDC)',
   'https://abdc.edu.au/abdc-journal-quality-list/',
   'Journals rated B (third tier) in the ABDC Journal Quality List, business, economics and related fields.'),
  ('abdc-c', 'ABDC Journal Quality List, C',
   'Australian Business Deans Council (ABDC)',
   'https://abdc.edu.au/abdc-journal-quality-list/',
   'Journals rated C (fourth tier) in the ABDC Journal Quality List, business, economics and related fields.')
ON CONFLICT (id) DO NOTHING;
