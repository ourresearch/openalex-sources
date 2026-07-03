-- 010 drop the source_export view (oxjob #548)
-- sources.issns is now a materialized derived column (009), so the sources
-- table is self-contained and Databricks reads it directly. The two legacy
-- renames the view provided (issn_l -> issn, homepage_url -> webpage) move
-- into the walden consumers' SELECTs at cutover, where those queries are
-- being edited anyway.
DROP VIEW IF EXISTS source_export;
