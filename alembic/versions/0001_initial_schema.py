"""initial sources schema (oxjob #548)

Revision ID: 0001
Revises:
Create Date: 2026-06-30
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- controlled vocab for sources.type -------------------------------
    op.execute(
        """
        CREATE TABLE source_type (
            source_type_id TEXT PRIMARY KEY,
            display_name   TEXT,
            description    TEXT
        );
        """
    )

    # --- the sources registry --------------------------------------------
    op.execute(
        """
        CREATE TABLE sources (
            id                            BIGINT PRIMARY KEY,
            display_name                  TEXT,
            type                          TEXT REFERENCES source_type(source_type_id),
            issn_l                        TEXT,
            publisher                     TEXT,
            publisher_id                  BIGINT,
            institution_id                BIGINT,
            homepage_url                  TEXT,
            country                       TEXT,
            country_code                  TEXT,
            apc_usd                       INTEGER,
            apc_prices                    JSONB,
            societies                     JSONB,
            is_society_journal            BOOLEAN,
            alternate_titles              JSONB,
            wikidata_id                   TEXT,
            fatcat_id                     TEXT,
            crossref_id                   TEXT,
            datacite_id                   TEXT,
            datacite_ids                  JSONB,
            endpoint_id                   TEXT,
            sample_pmh_record             TEXT,
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
            merge_into_id                 BIGINT REFERENCES sources(id),
            merge_into_date               TIMESTAMPTZ,
            display_name_before_override  TEXT,
            override_timestamp            TIMESTAMPTZ,
            created_date                  TIMESTAMPTZ DEFAULT now(),
            updated_date                  TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    op.execute("CREATE INDEX idx_sources_issn_l ON sources(issn_l);")
    op.execute("CREATE INDEX idx_sources_type ON sources(type);")
    op.execute("CREATE INDEX idx_sources_publisher_id ON sources(publisher_id);")
    op.execute("CREATE INDEX idx_sources_endpoint_id ON sources(endpoint_id);")
    op.execute("CREATE INDEX idx_sources_merge_into_id ON sources(merge_into_id);")
    # fast scan of live (non-redirected) sources
    op.execute(
        "CREATE INDEX idx_sources_active ON sources(id) WHERE merge_into_id IS NULL;"
    )

    # --- normalized ISSN membership (UNIQUE(issn) = dedup invariant) ------
    op.execute(
        """
        CREATE TABLE source_issn (
            source_id BIGINT  NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
            issn      TEXT    NOT NULL,
            is_issn_l BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (source_id, issn),
            CONSTRAINT uq_source_issn_issn UNIQUE (issn)
        );
        """
    )

    # --- ISSN -> ISSN-L map (port of guts journal_issn_to_issnl) ---------
    op.execute(
        """
        CREATE TABLE issn_to_issnl (
            issn         TEXT PRIMARY KEY,
            issn_l       TEXT,
            note         TEXT,
            updated_date TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    op.execute("CREATE INDEX idx_issn_to_issnl_issn_l ON issn_to_issnl(issn_l);")

    # --- go-forward S-id minting sequence (seeded above current max) -----
    # Reset to MAX(id) by the loader after the initial backfill.
    op.execute("CREATE SEQUENCE source_id_seq START WITH 7407059452;")

    # --- Databricks-facing export view (the federation read contract) ----
    # Reassembles the flat shape of openalex.sources.sources: restores the
    # `issn`/`issns`/`webpage` names the pipeline currently expects.
    op.execute(
        """
        CREATE VIEW source_export AS
        SELECT
            s.id,
            s.display_name,
            s.type,
            s.issn_l AS issn,
            (SELECT array_agg(si.issn ORDER BY si.is_issn_l DESC, si.issn)
               FROM source_issn si WHERE si.source_id = s.id) AS issns,
            s.publisher,
            s.publisher_id,
            s.institution_id,
            s.homepage_url AS webpage,
            s.country,
            s.country_code,
            s.apc_usd,
            s.apc_prices,
            s.societies,
            s.is_society_journal,
            s.alternate_titles,
            s.wikidata_id,
            s.fatcat_id,
            s.crossref_id,
            s.datacite_id,
            s.datacite_ids,
            s.endpoint_id,
            s.sample_pmh_record,
            s.is_oa,
            s.is_in_doaj,
            s.is_in_doaj_start_year,
            s.doaj_license,
            s.is_in_scielo,
            s.is_ojs,
            s.is_oa_high_oa_rate,
            s.high_oa_rate_start_year,
            s.is_fully_open_in_jstage,
            s.is_core,
            s.is_preprint_repository,
            s.merge_into_id,
            s.merge_into_date,
            s.display_name_before_override,
            s.override_timestamp,
            s.created_date,
            s.updated_date
        FROM sources s;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS source_export;")
    op.execute("DROP SEQUENCE IF EXISTS source_id_seq;")
    op.execute("DROP TABLE IF EXISTS issn_to_issnl;")
    op.execute("DROP TABLE IF EXISTS source_issn;")
    op.execute("DROP TABLE IF EXISTS sources;")
    op.execute("DROP TABLE IF EXISTS source_type;")
