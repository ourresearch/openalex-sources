-- 014 ISSN-L membership invariant (oxjob #548, 2026-07-07 post-cutover audit)
--
-- sources.issn_l could historically be ANY ISSN -- including one OWNED BY A
-- DIFFERENT SOURCE. That was the one route by which the same ISSN reached
-- consumers on two sources (28 active collisions, mostly walden-import twins
-- plus 8 duplicate mints the cascade couldn't see because it matched only on
-- direct ISSN membership). After this migration, issn_l must be one of the
-- source's OWN ISSNs (composite FK below); via UNIQUE(issn) on source_issn
-- that transitively forbids pointing at another source's ISSN, ever.
--
-- Order matters: queue the collision evidence (1) before repair (3) destroys
-- it; repair everything before the FK (7) validates.

-- 1) queue every active issn_l collision pair as a merge candidate; the daily
--    resolve_conflicts drains them (exact-name pairs auto-merge, the rest park
--    as needs_review)
INSERT INTO source_ingest_issue (source_feed, issue_type, issns, matched_source_ids, detail)
SELECT 'issnl_audit', 'multi_match',
       ARRAY[s.issn_l],
       ARRAY[LEAST(s.id, o.id), GREATEST(s.id, o.id)],
       'issn_l collision: ' || COALESCE(s.display_name, '?') || ' | '
           || COALESCE(o.display_name, '?') || ' | issn_l=' || s.issn_l
FROM sources s
JOIN source_issn si ON si.issn = s.issn_l AND si.source_id <> s.id
JOIN sources o ON o.id = si.source_id
WHERE s.merge_into_id IS NULL AND o.merge_into_id IS NULL
ON CONFLICT (source_feed, issue_type, matched_source_ids) DO NOTHING;

-- 2) adopt registry-known, unowned linking ISSNs as owned rows (~68 sources):
--    the ISSN IC map says this is the same journal's linking ISSN, and owning
--    it also makes the source matchable by future feed rows that carry it
INSERT INTO source_issn (source_id, issn, is_issn_l)
SELECT s.id, s.issn_l, FALSE
FROM sources s
WHERE s.merge_into_id IS NULL
  AND s.issn_l ~ '^[0-9]{4}-[0-9]{3}[0-9X]$'
  AND NOT EXISTS (SELECT 1 FROM source_issn si WHERE si.issn = s.issn_l)
  AND (EXISTS (SELECT 1 FROM issn_to_issnl m WHERE m.issn = s.issn_l)
       OR EXISTS (SELECT 1 FROM issn_to_issnl m WHERE m.issn_l = s.issn_l))
ON CONFLICT (issn) DO NOTHING;

-- 3) re-point every remaining non-member issn_l (collision pointers queued in
--    step 1, map-unknown or malformed values) into the source's own set:
--    map-preferred member first, else first owned ISSN, else NULL
UPDATE sources s
SET issn_l = COALESCE(
        (SELECT m.issn_l FROM issn_to_issnl m
          JOIN source_issn a ON a.source_id = s.id AND a.issn = m.issn
          JOIN source_issn b ON b.source_id = s.id AND b.issn = m.issn_l
          ORDER BY m.issn LIMIT 1),
        (SELECT si.issn FROM source_issn si WHERE si.source_id = s.id
          ORDER BY si.issn LIMIT 1)),
    updated_date = now()
WHERE s.merge_into_id IS NULL AND s.issn_l IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM source_issn si
                  WHERE si.source_id = s.id AND si.issn = s.issn_l);

-- 4) redirect rows carry no issn_l: their ISSNs moved to the winner at merge
--    (merge_source clears it going forward; this catches the 3.5K pre-existing)
UPDATE sources SET issn_l = NULL, updated_date = now()
WHERE merge_into_id IS NOT NULL AND issn_l IS NOT NULL;

-- 5) re-sync is_issn_l flags to the now member-only issn_l -- fixes the 1,005
--    drifted rows (incl. 591 double-flagged sources from the old enrich path)
UPDATE source_issn si
SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l)
FROM sources s
WHERE s.id = si.source_id
  AND si.is_issn_l IS DISTINCT FROM (si.issn IS NOT DISTINCT FROM s.issn_l);

-- 6) refresh the derived issns arrays (element set and ordering both depend on
--    the rows/flags fixed above)
UPDATE sources s
SET issns = sub.arr, updated_date = now()
FROM (SELECT source_id, array_agg(issn ORDER BY is_issn_l DESC, issn) AS arr
      FROM source_issn GROUP BY source_id) sub
WHERE sub.source_id = s.id AND s.issns IS DISTINCT FROM sub.arr;

-- 7) enforce: issn_l is one of the source's own ISSNs. DEFERRABLE because
--    mint_source writes the sources row before its source_issn rows, and
--    merge_source moves rows before the commit-time state is consistent.
ALTER TABLE sources ADD CONSTRAINT fk_sources_issn_l_member
    FOREIGN KEY (id, issn_l) REFERENCES source_issn (source_id, issn)
    DEFERRABLE INITIALLY DEFERRED;
