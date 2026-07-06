"""Cutover import of walden's post-export mints at their PUBLIC ids (oxjob #548).

After the 2026-06-30 backfill, walden's CreateSources kept minting daily
(id > 7407059451). Those sources are live on the public API and are referenced
by walden works, so they keep their ids. Run scripts/remap_minted_ids.py FIRST
(it vacates this id range), then this.

Input: a Parquet export of the walden rows, same column contract as the initial
backfill export — produce it in walden with:

  SELECT id, display_name, type, issn AS issn_l, publisher, publisher_id,
         institution_id, webpage AS homepage_url, country, country_code, apc_usd,
         to_json(apc_prices) AS apc_prices_json, to_json(societies) AS societies_json,
         is_society_journal, to_json(alternate_titles) AS alternate_titles_json,
         wikidata_id, fatcat_id, crossref_id, datacite_id,
         to_json(datacite_ids) AS datacite_ids_json, endpoint_id, sample_pmh_record,
         is_oa, is_in_doaj, is_in_doaj_start_year, doaj_license, is_in_scielo,
         is_ojs, is_oa_high_oa_rate, high_oa_rate_start_year,
         is_fully_open_in_jstage, is_core, is_preprint_repository, merge_into_id,
         merge_into_date, display_name_before_override, override_timestamp,
         created_date, updated_date, to_json(issns) AS issns_json
  FROM openalex.sources.sources WHERE id > 7407059451

An imported ISSN may already belong to an app source (both systems minted the
same journal). The ISSN stays with its current owner — UNIQUE(issn) is the
invariant — and the pair is logged to source_ingest_issue
('walden_import', 'multi_match') for the ordinary merge queue to resolve.

Usage:
  python scripts/import_walden_mints.py --parquet PATH [--execute]
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from sqlalchemy import text

from db import engine
from sources_lib import normalize_issns, refresh_issns_column
from load_initial_sources import SOURCE_COLUMNS, build_source_row, _clean, _to_int, _parse_json

BACKFILL_MAX = 7407059451


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--execute", action="store_true", help="commit (default: dry-run)")
    args = ap.parse_args()

    df = pd.read_parquet(args.parquet)
    records = df.to_dict("records")
    bad = [r for r in records if _to_int(r.get("id")) <= BACKFILL_MAX]
    if bad:
        sys.exit(f"{len(bad)} rows have id <= {BACKFILL_MAX}; this script only "
                 "imports walden's post-export mints")
    print(f"read {len(records)} walden mints from {args.parquet}")

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            ids = [_to_int(r["id"]) for r in records]
            occupied = conn.execute(
                text("SELECT COUNT(*) FROM sources WHERE id = ANY(:ids)"), {"ids": ids}
            ).scalar()
            if occupied:
                sys.exit(f"{occupied} incoming ids already exist in sources — run "
                         "scripts/remap_minted_ids.py first")

            types = sorted({_clean(r.get("type")) for r in records} - {None})
            for t in types:
                conn.execute(text(
                    "INSERT INTO source_type (source_type_id, display_name) "
                    "VALUES (:t, :t) ON CONFLICT DO NOTHING"), {"t": t})

            cols_sql = ", ".join(SOURCE_COLUMNS)
            params_sql = ", ".join(f":{c}" for c in SOURCE_COLUMNS)
            insert_source = text(
                f"INSERT INTO sources ({cols_sql}) OVERRIDING SYSTEM VALUE VALUES ({params_sql})")

            n_issns = n_conflicts = 0
            for rec in records:
                row = dict(zip(SOURCE_COLUMNS, build_source_row(rec)))
                sid = row["id"]
                conn.execute(insert_source, row)

                issn_l = _clean(rec.get("issn_l"))
                for issn in normalize_issns(_parse_json(rec.get("issns_json")) or []):
                    owner = conn.execute(
                        text("SELECT source_id FROM source_issn WHERE issn = :i"),
                        {"i": issn}).scalar()
                    if owner is not None:
                        n_conflicts += 1
                        conn.execute(text(
                            "INSERT INTO source_ingest_issue "
                            "(source_feed, issue_type, issns, matched_source_ids, detail) "
                            "VALUES ('walden_import', 'multi_match', :i, :m, :d) "
                            "ON CONFLICT (source_feed, issue_type, matched_source_ids) DO NOTHING"
                        ), {"i": [issn], "m": sorted([owner, sid]),
                            "d": _clean(rec.get("display_name"))})
                        continue
                    conn.execute(text(
                        "INSERT INTO source_issn (source_id, issn, is_issn_l) "
                        "VALUES (:sid, :i, :l)"),
                        {"sid": sid, "i": issn, "l": issn == issn_l})
                    n_issns += 1
                refresh_issns_column(conn, sid)

            print(f"imported {len(records)} sources, attached {n_issns} issns, "
                  f"queued {n_conflicts} issn conflicts for the merge queue")
            if args.execute:
                trans.commit()
                print("COMMITTED")
            else:
                trans.rollback()
                print("dry-run: rolled back")
        except Exception:
            trans.rollback()
            raise


if __name__ == "__main__":
    main()
