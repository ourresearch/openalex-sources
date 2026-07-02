-- 006 DataCite feed (oxjob #548, Phase 2/3)

-- Normalized DataCite-client -> source link (mirrors source_issn: the PK is
-- the one-client-one-source invariant). sources.datacite_ids JSONB stays as
-- the derived export-compatible column; this table is the authority.
CREATE TABLE IF NOT EXISTS source_datacite_id (
    datacite_id TEXT PRIMARY KEY,
    source_id   BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_source_datacite_id_source ON source_datacite_id(source_id);

-- seed from the backfilled JSONB arrays; links on merged losers follow the redirect
INSERT INTO source_datacite_id (datacite_id, source_id)
SELECT DISTINCT ON (x) x, COALESCE(s.merge_into_id, s.id)
FROM sources s, jsonb_array_elements_text(s.datacite_ids) x
WHERE s.datacite_ids IS NOT NULL
ORDER BY x, (s.merge_into_id IS NULL) DESC, s.id
ON CONFLICT (datacite_id) DO NOTHING;

-- full-snapshot staging for api.datacite.org/clients
CREATE TABLE IF NOT EXISTS datacite_client (
    id            TEXT PRIMARY KEY,     -- client id, e.g. 'crui.unipd'
    display_name  TEXT,
    issns         TEXT[],
    url           TEXT,
    client_type   TEXT,                 -- repository | periodical | ...
    provider_id   TEXT,
    provider_name TEXT,
    raw           JSONB,
    fetched_at    TIMESTAMPTZ DEFAULT now()
);
