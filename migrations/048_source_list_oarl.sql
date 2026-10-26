-- 048 source lists: the International OA Journal Recommended List (OARL),
-- run by Tsinghua University Library, the Society of China University Journals
-- (CUJS) and CNPIEC. Offered by OARL on 2026-10-19 (Zendesk 26281), accepted by
-- Kyle 2026-10-24; 2025 edition (2,062 journals, Title/ISSN/EISSN, no tiers)
-- received 2026-10-25 with permission to publish membership openly (CC0).
-- One flat list (no levels), updated annually by email. Same conventions as 038-047.

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('oarl', 'International OA Journal Recommended List (OARL)',
   'Tsinghua University Library, Society of China University Journals (CUJS) and China National Publications Import & Export (Group) Co., Ltd. (CNPIEC)',
   'https://oarl.org.cn/en/applicationGuide',
   'Fully open access journals, all fields and languages, recommended after data-driven screening and independent expert evaluation of scholarly quality, publishing transparency and research integrity.')
ON CONFLICT (id) DO NOTHING;
