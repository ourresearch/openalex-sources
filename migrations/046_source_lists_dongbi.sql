-- 046 source lists: the Dongbi Index global high-quality journal list (oxjob
-- #1615; suggested by Ross Mounce, Jason OK 2026-10-09). Grades A (top) to D,
-- assigned per subject; a journal is listed once, under its best grade. Same
-- conventions as 040/041/045. No data licence stated; launched as "free to
-- search and download" (Xinhua, 2026-03-24). Credited with maintainer + URL.

INSERT INTO source_list (id, display_name, maintainer, url, scope) VALUES
  ('dongbi-a', 'Dongbi Index, grade A',
   'Dongbi Technology Data (Shenzhen), with the Institute of Medical Information, Chinese Academy of Medical Sciences',
   'https://www.dbdata.com/dongbiindex/',
   'Journals graded A (the top of four grades) in at least one subject of the Dongbi Index global high-quality journal list (China), 2025 edition, all fields.'),
  ('dongbi-b', 'Dongbi Index, grade B',
   'Dongbi Technology Data (Shenzhen), with the Institute of Medical Information, Chinese Academy of Medical Sciences',
   'https://www.dbdata.com/dongbiindex/',
   'Journals whose best grade in the Dongbi Index global high-quality journal list (China), 2025 edition, is B (second of four grades), all fields.'),
  ('dongbi-c', 'Dongbi Index, grade C',
   'Dongbi Technology Data (Shenzhen), with the Institute of Medical Information, Chinese Academy of Medical Sciences',
   'https://www.dbdata.com/dongbiindex/',
   'Journals whose best grade in the Dongbi Index global high-quality journal list (China), 2025 edition, is C (third of four grades), all fields.'),
  ('dongbi-d', 'Dongbi Index, grade D',
   'Dongbi Technology Data (Shenzhen), with the Institute of Medical Information, Chinese Academy of Medical Sciences',
   'https://www.dbdata.com/dongbiindex/',
   'Journals whose best grade in the Dongbi Index global high-quality journal list (China), 2025 edition, is D (fourth of four grades: emerging and specialized fields), all fields.')
ON CONFLICT (id) DO NOTHING;
