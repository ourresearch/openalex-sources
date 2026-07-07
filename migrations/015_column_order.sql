-- 015 logical column order (oxjob #548; cosmetic, Casey 2026-07-07)
--
-- Postgres can't reorder columns in place, so rebuild the table with columns
-- grouped logically: identity/naming first (issns moves up from dead last),
-- then organization, external identifiers, APC/society, OA flags, and the
-- lifecycle/audit columns (merge_into_*, created/updated_date) at the end.
-- Every consumer selects by name (CreateSources, feeds, federation), so this
-- changes nothing functionally; it exists for humans browsing the table.
--
-- Runs in one transaction (migrate.py). Inventory preserved exactly: 4
-- outbound + 4 inbound FKs, 6 secondary indexes (incl. the partial active
-- index), GENERATED ALWAYS identity with its sequence position and name,
-- column defaults. No views depend on sources; single-role grants (owner).

CREATE TABLE sources_new (
    -- identity & naming
    id                            BIGINT GENERATED ALWAYS AS IDENTITY,
    display_name                  TEXT,
    type                          TEXT,
    issn_l                        TEXT,
    issns                         TEXT[],
    alternate_titles              JSONB,
    display_name_before_override  TEXT,
    override_timestamp            TIMESTAMPTZ,
    -- organization & location
    publisher                     TEXT,
    publisher_id                  BIGINT,
    institution_id                BIGINT,
    homepage_url                  TEXT,
    country                       TEXT,
    country_code                  TEXT,
    -- external identifiers
    wikidata_id                   TEXT,
    fatcat_id                     TEXT,
    crossref_id                   TEXT,
    datacite_id                   TEXT,
    datacite_ids                  JSONB,
    endpoint_id                   TEXT,
    sample_pmh_record             TEXT,
    -- APC & society
    apc_usd                       INTEGER,
    apc_prices                    JSONB,
    societies                     JSONB,
    is_society_journal            BOOLEAN,
    -- OA flags
    is_oa                         BOOLEAN,
    is_in_doaj                    BOOLEAN,
    is_in_doaj_start_year         INTEGER,
    doaj_license                  TEXT,
    is_in_scielo                  BOOLEAN,
    is_ojs                        BOOLEAN,
    is_oa_high_oa_rate            BOOLEAN,
    high_oa_rate_start_year       BIGINT,
    is_fully_open_in_jstage       BOOLEAN,
    is_core                       BOOLEAN,
    is_preprint_repository        BOOLEAN,
    -- lifecycle / audit
    merge_into_id                 BIGINT,
    merge_into_date               TIMESTAMPTZ,
    created_date                  TIMESTAMPTZ DEFAULT now(),
    updated_date                  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO sources_new (
    id, display_name, type, issn_l, issns, alternate_titles,
    display_name_before_override, override_timestamp,
    publisher, publisher_id, institution_id, homepage_url, country, country_code,
    wikidata_id, fatcat_id, crossref_id, datacite_id, datacite_ids,
    endpoint_id, sample_pmh_record,
    apc_usd, apc_prices, societies, is_society_journal,
    is_oa, is_in_doaj, is_in_doaj_start_year, doaj_license, is_in_scielo,
    is_ojs, is_oa_high_oa_rate, high_oa_rate_start_year,
    is_fully_open_in_jstage, is_core, is_preprint_repository,
    merge_into_id, merge_into_date, created_date, updated_date)
OVERRIDING SYSTEM VALUE
SELECT
    id, display_name, type, issn_l, issns, alternate_titles,
    display_name_before_override, override_timestamp,
    publisher, publisher_id, institution_id, homepage_url, country, country_code,
    wikidata_id, fatcat_id, crossref_id, datacite_id, datacite_ids,
    endpoint_id, sample_pmh_record,
    apc_usd, apc_prices, societies, is_society_journal,
    is_oa, is_in_doaj, is_in_doaj_start_year, doaj_license, is_in_scielo,
    is_ojs, is_oa_high_oa_rate, high_oa_rate_start_year,
    is_fully_open_in_jstage, is_core, is_preprint_repository,
    merge_into_id, merge_into_date, created_date, updated_date
FROM sources;

-- carry the sequence position over exactly (not MAX(id): values consumed by
-- rolled-back mints must not be reissued)
SELECT setval('sources_new_id_seq', (SELECT last_value FROM sources_id_seq));

ALTER TABLE source_issn        DROP CONSTRAINT source_issn_source_id_fkey;
ALTER TABLE source_merge       DROP CONSTRAINT source_merge_loser_id_fkey;
ALTER TABLE source_merge       DROP CONSTRAINT source_merge_winner_id_fkey;
ALTER TABLE source_datacite_id DROP CONSTRAINT source_datacite_id_source_id_fkey;

DROP TABLE sources;  -- drops its owned sources_id_seq with it
ALTER TABLE sources_new RENAME TO sources;
ALTER SEQUENCE sources_new_id_seq RENAME TO sources_id_seq;

-- constraints back under their original names
ALTER TABLE sources ADD CONSTRAINT sources_pkey PRIMARY KEY (id);
ALTER TABLE sources ADD CONSTRAINT sources_type_fkey
    FOREIGN KEY (type) REFERENCES source_type (source_type_id);
ALTER TABLE sources ADD CONSTRAINT sources_merge_into_id_fkey
    FOREIGN KEY (merge_into_id) REFERENCES sources (id);
ALTER TABLE sources ADD CONSTRAINT fk_sources_issn_l_member
    FOREIGN KEY (id, issn_l) REFERENCES source_issn (source_id, issn)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE source_issn ADD CONSTRAINT source_issn_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES sources (id) ON DELETE CASCADE;
ALTER TABLE source_merge ADD CONSTRAINT source_merge_loser_id_fkey
    FOREIGN KEY (loser_id) REFERENCES sources (id);
ALTER TABLE source_merge ADD CONSTRAINT source_merge_winner_id_fkey
    FOREIGN KEY (winner_id) REFERENCES sources (id);
ALTER TABLE source_datacite_id ADD CONSTRAINT source_datacite_id_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES sources (id) ON DELETE CASCADE;

CREATE INDEX idx_sources_issn_l        ON sources (issn_l);
CREATE INDEX idx_sources_type          ON sources (type);
CREATE INDEX idx_sources_publisher_id  ON sources (publisher_id);
CREATE INDEX idx_sources_endpoint_id   ON sources (endpoint_id);
CREATE INDEX idx_sources_merge_into_id ON sources (merge_into_id);
CREATE INDEX idx_sources_active        ON sources (id) WHERE merge_into_id IS NULL;

ANALYZE sources;
