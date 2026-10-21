-- 047 source lists: the Diamond Discovery Hub (DDH), the European Diamond
-- Capacity Hub's directory of vetted Diamond OA journals. Requested by Johan
-- Rooryck and Pierre Mounier (OPERAS) on 2026-10-21 in the OA-colours thread.
-- One flat list (no levels). Membership is harvested over DDH's OAI-PMH
-- endpoint (jobs/fetch_source_list ddh) and keyed by ISSN like every other
-- file-loaded list. Same conventions as 038-046.

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('ddh', 'Diamond Discovery Hub',
   'European Diamond Capacity Hub (EDCH), coordinated by OPERAS',
   'https://ddh.edch.eu/',
   'Diamond open access journals (no fees for authors or readers, community-owned, openly licensed) vetted by the Diamond Discovery Hub against its operational Diamond criteria, all fields.')
ON CONFLICT (id) DO NOTHING;
