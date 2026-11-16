"""Recompute the OA flags from the imported mapping tables (CreateSources parity):

  is_ojs                  -- any source ISSN in ojs_beacon_issn (PKP Beacon), via
                             sources_lib.recompute_is_ojs (oxjob #1424)
  is_oa_high_oa_rate      -- publisher prefix rule (mdpi/academic journals/edorium)
                             OR is_in_scielo OR an OA match in the legacy ojs_journal
                             table (its is_oa column; no longer drives is_ojs)
                             OR the effective high_oa_rate_issn list
                             OR minted from the PKP Beacon (ojs_beacon_mint: OJS journals
                             are OA by default -- Casey, oxjob #1539)
  is_fully_open_in_jstage -- a J-STAGE OA window covers the source's whole
                             publication span (source_publication_years snapshot)

is_oa itself is then re-derived by sources_lib.recompute_is_oa (the single
writer of is_oa: is_in_doaj OR jstage_full OR is_in_scielo OR high_oa_rate).

Only changed rows are written. Run after scripts/import_oa_flag_tables.py refreshes
the mapping tables, and weekly to pick up new mints + publication-span drift.

  python -m jobs.apply_oa_flags [--dry-run]
"""
import argparse

from sqlalchemy import text

from db import engine
from sources_lib import recompute_is_oa, recompute_is_ojs

PUBLISHER_OA_RULE = "^(mdpi|academic journals|edorium journals)"


# The new flags per source, as a CTE ``flags`` (no temp table: the registry's
# lanes login has no TEMP privilege, oxjob #1224). {scope} limits it to some
# sources. The curation override (source_oa_override) is the FINAL word, with
# the retired DLT's exact precedence: is_oa beats flip_year beats the computed
# value.
_FLAGS_CTE = """
WITH per_source AS (
    SELECT s.id,
           BOOL_OR(oj.is_oa) AS ojs_is_oa,
           BOOL_OR(h.issn_l IS NOT NULL) AS hoar_listed,
           BOOL_OR(bm.source_id IS NOT NULL) AS beacon_minted,
           MIN(h.start_year) AS hoar_start,
           BOOL_OR(j.issn IS NOT NULL
                   AND p.first_publication_year IS NOT NULL
                   AND p.last_publication_year IS NOT NULL
                   AND j.oa_start_year <= p.first_publication_year
                   AND j.oa_end_year >= p.last_publication_year) AS jstage_full
    FROM sources s
    LEFT JOIN source_issn si ON si.source_id = s.id
    LEFT JOIN ojs_journal oj ON oj.issn = si.issn
    LEFT JOIN high_oa_rate_issn h ON h.issn_l = si.issn
    LEFT JOIN ojs_beacon_mint bm ON bm.source_id = s.id
    LEFT JOIN jstage_journal j ON j.issn = si.issn
    LEFT JOIN source_publication_years p ON p.source_id = s.id
    WHERE TRUE{scope}
    GROUP BY s.id
), flags AS (
    SELECT s.id,
           CASE WHEN o.curated_is_oa IS NOT NULL THEN o.curated_is_oa
                WHEN o.curated_flip_year IS NOT NULL THEN TRUE
                ELSE (COALESCE(s.publisher ~* :pub_rule, FALSE)
                      OR COALESCE(s.is_in_scielo, FALSE)
                      OR COALESCE(ps.ojs_is_oa, FALSE)
                      OR COALESCE(ps.hoar_listed, FALSE)
                      OR COALESCE(ps.beacon_minted, FALSE)) END AS new_hoar,
           CASE WHEN o.curated_is_oa = FALSE THEN NULL
                WHEN o.curated_flip_year IS NOT NULL THEN o.curated_flip_year + 1
                WHEN COALESCE(s.publisher ~* :pub_rule, FALSE)
                  OR COALESCE(s.is_in_scielo, FALSE)
                  OR COALESCE(ps.ojs_is_oa, FALSE)
                  OR COALESCE(ps.beacon_minted, FALSE) THEN NULL
                ELSE ps.hoar_start END AS new_hoar_start,
           COALESCE(ps.jstage_full, FALSE) AS new_jstage_full
    FROM sources s
    JOIN per_source ps ON ps.id = s.id
    LEFT JOIN source_oa_override o ON o.source_id = s.id
)
"""

_FLAGS_DIFFER = """(s.is_oa_high_oa_rate IS DISTINCT FROM f.new_hoar
       OR (s.high_oa_rate_start_year IS DISTINCT FROM f.new_hoar_start AND f.new_hoar)
       OR s.is_fully_open_in_jstage IS DISTINCT FROM f.new_jstage_full)"""


def apply(conn, dry_run=False, source_ids=None):
    """Re-derive is_oa_high_oa_rate, its start year and is_fully_open_in_jstage in
    ``conn``'s transaction; returns the rows updated. ``source_ids``: only these
    sources (jobs.apply_source_curations, oxjob #1224); None = every source.
    is_oa itself is left to recompute_is_oa."""
    cte = _FLAGS_CTE.format(scope="" if source_ids is None else " AND s.id = ANY(:ids)")
    params = {"pub_rule": PUBLISHER_OA_RULE, "ids": source_ids}
    total, changes = conn.execute(text(f"""{cte}
        SELECT COUNT(*), COUNT(*) FILTER (WHERE {_FLAGS_DIFFER}
            OR s.is_oa IS DISTINCT FROM (COALESCE(s.is_in_doaj, FALSE) OR f.new_jstage_full
                                         OR COALESCE(s.is_in_scielo, FALSE) OR f.new_hoar))
        FROM sources s JOIN flags f ON f.id = s.id
    """), params).one()
    print(f"{total} active sources evaluated; {changes} need updates; dry_run={dry_run}",
          flush=True)
    if dry_run:
        ojs_changes = conn.execute(text("""
            SELECT COUNT(*) FROM sources s
            WHERE s.is_ojs IS DISTINCT FROM EXISTS (
                SELECT 1 FROM source_issn si
                JOIN ojs_beacon_issn b ON b.issn = si.issn
                WHERE si.source_id = s.id)
        """)).scalar()
        print(f"is_ojs would change on {ojs_changes}", flush=True)
        return 0

    return conn.execute(text(f"""{cte}
        UPDATE sources s SET
            is_oa_high_oa_rate = f.new_hoar,
            high_oa_rate_start_year = CASE WHEN f.new_hoar THEN f.new_hoar_start END,
            is_fully_open_in_jstage = f.new_jstage_full,
            updated_date = now()
        FROM flags f
        WHERE f.id = s.id AND {_FLAGS_DIFFER}
    """), params).rowcount


def run(dry_run=False):
    with engine.begin() as conn:
        apply(conn, dry_run)
        if dry_run:
            return
        oa = recompute_is_oa(conn)
        ojs = recompute_is_ojs(conn)
    print(f"applied (DONE); is_oa recomputed on {oa}; is_ojs on {ojs}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
