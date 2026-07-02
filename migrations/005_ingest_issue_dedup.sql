-- 005 conflict-queue dedup (oxjob #548, Phase 3)
-- The sync jobs re-encounter persisting conflicts on every run (daily for
-- Crossref); without a uniqueness guard the queue re-accumulates the same
-- pair each day. One row per (feed, matched id-set), ever: once logged --
-- whether unresolved, auto-merged, or parked as needs_review -- the same
-- conflict is never logged again. A truly new conflict always has a
-- different id-set (merged losers lose their ISSNs and can't re-match).

-- collapse duplicates from the initial run (same feed + id-set), keeping the
-- earliest row (it may carry a resolution)
DELETE FROM source_ingest_issue a
USING source_ingest_issue b
WHERE a.source_feed = b.source_feed
  AND a.matched_source_ids = b.matched_source_ids
  AND a.issue_type = b.issue_type
  AND a.id > b.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_source_ingest_issue_feed_set
    ON source_ingest_issue (source_feed, issue_type, matched_source_ids);
