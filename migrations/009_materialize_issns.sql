-- 009 materialize sources.issns (oxjob #548)
-- The issns array becomes a derived column on sources (ISSN-L first, then
-- alphabetical), refreshed by sources_lib at the two write paths for
-- source_issn (insert_issns, merge_source) -- the same pattern as
-- sources.datacite_ids. This makes the sources table self-contained so
-- Databricks can read it directly; the source_export view is dropped in 010.
-- source_issn remains the authority (UNIQUE(issn) lives there).

ALTER TABLE sources ADD COLUMN IF NOT EXISTS issns TEXT[];

UPDATE sources s
SET issns = sub.arr
FROM (
    SELECT source_id, array_agg(issn ORDER BY is_issn_l DESC, issn) AS arr
    FROM source_issn
    GROUP BY source_id
) sub
WHERE sub.source_id = s.id
  AND s.issns IS DISTINCT FROM sub.arr;
