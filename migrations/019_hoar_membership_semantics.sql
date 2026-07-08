-- 019 high_oa_rate_issn: membership IS the flag

-- The is_oa_high_oa_rate boolean was true on all but 2 of 35,042 rows — an
-- artifact of the one-time Databricks export, where the unpaywall curation
-- overlay produced explicit-false rows for curator force-excludes. Delete
-- those first (0910-6340 Analytical Sciences, 2986-027X Proceeding B-ICON;
-- keeping them through a plain column drop would silently flip them to OA),
-- then drop the column: presence in the table now means high OA rate.
DELETE FROM high_oa_rate_issn WHERE NOT is_oa_high_oa_rate;
ALTER TABLE high_oa_rate_issn DROP COLUMN IF EXISTS is_oa_high_oa_rate;
