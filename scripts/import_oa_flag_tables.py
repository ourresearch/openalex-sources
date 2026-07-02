"""Import the OA-flag mapping tables from Databricks into the app Postgres.

Run LOCALLY (needs Databricks credentials via openalex-walden's utils):

  python scripts/import_oa_flag_tables.py

Loads: jstage_journal, ojs_journal, high_oa_rate_issn (with the unpaywall
curation-request overlay applied in-query, replicating CreateSources), and
source_publication_years. Then run jobs/apply_oa_flags (on Heroku) to recompute
source flags.
"""
import io
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, "/Users/caseymeyer/vs code projects/openalex-walden")

from db import engine
from utils.databricks_sql import run_query

TODAY = date.today().isoformat()

EXPORTS = {
    # table -> (columns, query)
    "jstage_journal": (
        ["issn", "oa_start_year", "oa_end_year"],
        """SELECT UPPER(issn) AS issn, MIN(jstage_oa_start_year) AS oa_start_year,
                  MAX(jstage_oa_end_year) AS oa_end_year
           FROM (SELECT explode(issns) AS issn, jstage_oa_start_year, jstage_oa_end_year
                 FROM openalex.sources.jstage_oa)
           WHERE issn IS NOT NULL GROUP BY 1""",
    ),
    "ojs_journal": (
        ["issn", "is_oa"],
        """SELECT UPPER(issn) AS issn, MAX(is_oa) AS is_oa
           FROM openalex.sources.ojs_journals WHERE issn IS NOT NULL GROUP BY 1""",
    ),
    "high_oa_rate_issn": (
        ["issn_l", "is_oa_high_oa_rate", "start_year"],
        # base list outer-joined with the latest approved unpaywall curation request
        # per ISSN; curation takes full priority (CreateSources parity)
        """WITH base AS (
             SELECT DISTINCT issn_l, oa_year FROM openalex.sources.high_oa_rate_issns
           ), cur AS (
             SELECT issn AS issn_l, new_is_oa AS is_oa, CAST(new_oa_date AS INT) AS start_year
             FROM (SELECT *, row_number() OVER (PARTITION BY issn ORDER BY ingestion_timestamp DESC) rn
                   FROM openalex.unpaywall.journal_curation_requests WHERE approved = 'yes')
             WHERE rn = 1
           )
           SELECT COALESCE(cur.issn_l, base.issn_l) AS issn_l,
                  CASE WHEN cur.is_oa IS NOT NULL THEN cur.is_oa
                       ELSE base.oa_year IS NOT NULL END AS is_oa_high_oa_rate,
                  CASE WHEN cur.issn_l IS NOT NULL THEN cur.start_year
                       ELSE base.oa_year END AS start_year
           FROM base FULL OUTER JOIN cur ON base.issn_l = cur.issn_l""",
    ),
    "source_publication_years": (
        ["source_id", "first_publication_year", "last_publication_year"],
        """SELECT id AS source_id, CAST(first_publication_year AS INT),
                  CAST(last_publication_year AS INT)
           FROM openalex.sources.sources_api
           WHERE first_publication_year IS NOT NULL OR last_publication_year IS NOT NULL""",
    ),
}


def main():
    for table, (cols, query) in EXPORTS.items():
        rows = run_query(query, size="xlarge")
        buf = io.StringIO()
        stamped = table == "source_publication_years"
        for r in rows:
            vals = ["\\N" if r[c] is None else str(r[c]) for c in cols]
            if stamped:
                vals.append(TODAY)
            buf.write("\t".join(vals) + "\n")
        buf.seek(0)
        copy_cols = cols + (["as_of"] if stamped else [])
        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.execute(f"TRUNCATE {table}")
                cur.copy_expert(f"COPY {table} ({', '.join(copy_cols)}) FROM STDIN", buf)
            raw.commit()
        finally:
            raw.close()
        print(f"{table}: loaded {len(rows)} rows")


if __name__ == "__main__":
    main()
