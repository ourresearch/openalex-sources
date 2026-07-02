"""Recompute the OA flags from the imported mapping tables (CreateSources parity):

  is_ojs                  -- any source ISSN in ojs_journal
  is_oa_high_oa_rate      -- publisher prefix rule (mdpi/academic journals/edorium)
                             OR is_in_scielo OR an OA OJS match
                             OR the effective high_oa_rate_issn list
  is_fully_open_in_jstage -- a J-STAGE OA window covers the source's whole
                             publication span (source_publication_years snapshot)
  is_oa                   -- is_in_doaj OR is_fully_open_in_jstage OR is_in_scielo
                             OR is_oa_high_oa_rate

Only changed rows are written. Run after scripts/import_oa_flag_tables.py refreshes
the mapping tables, and weekly to pick up new mints + publication-span drift.

  python -m jobs.apply_oa_flags [--dry-run]
"""
import argparse

from sqlalchemy import text

from db import engine

PUBLISHER_OA_RULE = "^(mdpi|academic journals|edorium journals)"


def run(dry_run=False):
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TEMP TABLE _oa_flags ON COMMIT DROP AS
            WITH per_source AS (
                SELECT s.id,
                       BOOL_OR(oj.issn IS NOT NULL) AS matched_ojs,
                       BOOL_OR(oj.is_oa) AS ojs_is_oa,
                       BOOL_OR(h.is_oa_high_oa_rate) AS hoar_listed,
                       MIN(h.start_year) FILTER (WHERE h.is_oa_high_oa_rate) AS hoar_start,
                       BOOL_OR(j.issn IS NOT NULL
                               AND p.first_publication_year IS NOT NULL
                               AND p.last_publication_year IS NOT NULL
                               AND j.oa_start_year <= p.first_publication_year
                               AND j.oa_end_year >= p.last_publication_year) AS jstage_full
                FROM sources s
                LEFT JOIN source_issn si ON si.source_id = s.id
                LEFT JOIN ojs_journal oj ON oj.issn = si.issn
                LEFT JOIN high_oa_rate_issn h ON h.issn_l = si.issn
                LEFT JOIN jstage_journal j ON j.issn = si.issn
                LEFT JOIN source_publication_years p ON p.source_id = s.id
                WHERE s.merge_into_id IS NULL
                GROUP BY s.id
            )
            SELECT s.id,
                   COALESCE(ps.matched_ojs, FALSE) AS new_is_ojs,
                   (COALESCE(s.publisher ~* :pub_rule, FALSE)
                    OR COALESCE(s.is_in_scielo, FALSE)
                    OR COALESCE(ps.ojs_is_oa, FALSE)
                    OR COALESCE(ps.hoar_listed, FALSE)) AS new_hoar,
                   CASE WHEN COALESCE(s.publisher ~* :pub_rule, FALSE)
                          OR COALESCE(s.is_in_scielo, FALSE)
                          OR COALESCE(ps.ojs_is_oa, FALSE) THEN NULL
                        ELSE ps.hoar_start END AS new_hoar_start,
                   COALESCE(ps.jstage_full, FALSE) AS new_jstage_full
            FROM sources s
            JOIN per_source ps ON ps.id = s.id
        """), {"pub_rule": PUBLISHER_OA_RULE})

        changes = conn.execute(text("""
            SELECT COUNT(*) FROM sources s JOIN _oa_flags f ON f.id = s.id
            WHERE s.is_ojs IS DISTINCT FROM f.new_is_ojs
               OR s.is_oa_high_oa_rate IS DISTINCT FROM f.new_hoar
               OR (s.high_oa_rate_start_year IS DISTINCT FROM f.new_hoar_start AND f.new_hoar)
               OR s.is_fully_open_in_jstage IS DISTINCT FROM f.new_jstage_full
               OR s.is_oa IS DISTINCT FROM (COALESCE(s.is_in_doaj, FALSE) OR f.new_jstage_full
                                            OR COALESCE(s.is_in_scielo, FALSE) OR f.new_hoar)
        """)).scalar()
        total = conn.execute(text("SELECT COUNT(*) FROM _oa_flags")).scalar()
        print(f"{total} active sources evaluated; {changes} need updates; dry_run={dry_run}",
              flush=True)
        if dry_run:
            return

        conn.execute(text("""
            UPDATE sources s SET
                is_ojs = f.new_is_ojs,
                is_oa_high_oa_rate = f.new_hoar,
                high_oa_rate_start_year = CASE WHEN f.new_hoar THEN f.new_hoar_start END,
                is_fully_open_in_jstage = f.new_jstage_full,
                is_oa = (COALESCE(s.is_in_doaj, FALSE) OR f.new_jstage_full
                         OR COALESCE(s.is_in_scielo, FALSE) OR f.new_hoar),
                updated_date = now()
            FROM _oa_flags f
            WHERE f.id = s.id
              AND (s.is_ojs IS DISTINCT FROM f.new_is_ojs
                   OR s.is_oa_high_oa_rate IS DISTINCT FROM f.new_hoar
                   OR (s.high_oa_rate_start_year IS DISTINCT FROM f.new_hoar_start AND f.new_hoar)
                   OR s.is_fully_open_in_jstage IS DISTINCT FROM f.new_jstage_full
                   OR s.is_oa IS DISTINCT FROM (COALESCE(s.is_in_doaj, FALSE) OR f.new_jstage_full
                                                OR COALESCE(s.is_in_scielo, FALSE) OR f.new_hoar))
        """))
    print("applied (DONE)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
