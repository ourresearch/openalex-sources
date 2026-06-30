"""One-time initial backfill of the sources registry (oxjob #548, Phase 1).

Reads a Parquet export of Databricks `openalex.sources.sources` (produced by the
migration export step) and loads it into the Heroku Postgres, PRESERVING every
S-id. Normalizes the `issns` arrays into `source_issn`, enforcing the
one-ISSN-one-source invariant via UNIQUE(issn): the ~85 ISSNs that the Spark build
left on two sources are resolved lowest-source-id-wins, and the dropped pairs are
written to a collision report.

Idempotent: truncates the four tables first, so it can be re-run.

Usage:
  python scripts/load_initial_sources.py --parquet PATH [--report PATH]
"""
import argparse
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from psycopg2.extras import Json, execute_values

from db import engine

SOURCE_COLUMNS = [
    "id", "display_name", "type", "issn_l", "publisher", "publisher_id",
    "institution_id", "homepage_url", "country", "country_code", "apc_usd",
    "apc_prices", "societies", "is_society_journal", "alternate_titles",
    "wikidata_id", "fatcat_id", "crossref_id", "datacite_id", "datacite_ids",
    "endpoint_id", "sample_pmh_record", "is_oa", "is_in_doaj",
    "is_in_doaj_start_year", "doaj_license", "is_in_scielo", "is_ojs",
    "is_oa_high_oa_rate", "high_oa_rate_start_year", "is_fully_open_in_jstage",
    "is_core", "is_preprint_repository", "merge_into_id", "merge_into_date",
    "display_name_before_override", "override_timestamp", "created_date",
    "updated_date",
]
INT_COLS = {
    "id", "publisher_id", "institution_id", "apc_usd", "is_in_doaj_start_year",
    "high_oa_rate_start_year", "merge_into_id",
}
BOOL_COLS = {
    "is_society_journal", "is_oa", "is_in_doaj", "is_in_scielo", "is_ojs",
    "is_oa_high_oa_rate", "is_fully_open_in_jstage", "is_core",
    "is_preprint_repository",
}
JSON_COLS = {"apc_prices", "societies", "alternate_titles", "datacite_ids"}
# *_json parquet columns holding serialized arrays/structs
JSON_SRC = {
    "apc_prices": "apc_prices_json",
    "societies": "societies_json",
    "alternate_titles": "alternate_titles_json",
    "datacite_ids": "datacite_ids_json",
}


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def _to_int(v):
    v = _clean(v)
    return int(v) if v is not None else None


def _to_bool(v):
    v = _clean(v)
    return bool(v) if v is not None else None


def _parse_json(v):
    v = _clean(v)
    if v is None or v == "null":
        return None
    return json.loads(v) if isinstance(v, str) else v


def build_source_row(rec):
    out = []
    for col in SOURCE_COLUMNS:
        if col == "merge_into_id":
            out.append(None)  # set in second pass to avoid self-FK ordering issues
        elif col in INT_COLS:
            out.append(_to_int(rec.get(col)))
        elif col in BOOL_COLS:
            out.append(_to_bool(rec.get(col)))
        elif col in JSON_COLS:
            parsed = _parse_json(rec.get(JSON_SRC[col]))
            out.append(Json(parsed) if parsed is not None else None)
        else:
            out.append(_clean(rec.get(col)))
    return out


def resolve_issns(records):
    """Return (rows, collisions). rows = [(source_id, issn, is_issn_l)] with each
    issn assigned to its lowest source_id; collisions = dropped (issn, kept, dropped)."""
    owner = {}        # issn -> lowest source_id
    issn_l_of = {}    # source_id -> issn_l
    pairs = []        # (source_id, issn)
    for rec in records:
        sid = _to_int(rec.get("id"))
        issn_l_of[sid] = _clean(rec.get("issn_l"))
        issns = _parse_json(rec.get("issns_json")) or []
        for issn in issns:
            if not issn:
                continue
            pairs.append((sid, issn))
            if issn not in owner or sid < owner[issn]:
                owner[issn] = sid

    rows, collisions = [], []
    seen = set()
    for sid, issn in pairs:
        keep = owner[issn]
        if sid != keep:
            collisions.append((issn, keep, sid))
            continue
        if (keep, issn) in seen:
            continue
        seen.add((keep, issn))
        rows.append((keep, issn, issn == issn_l_of.get(keep)))
    return rows, collisions


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--report", default="issn_collisions.csv")
    args = ap.parse_args()

    df = pd.read_parquet(args.parquet)
    records = df.to_dict("records")
    print(f"read {len(records):,} source rows from {args.parquet}")

    source_rows = [build_source_row(r) for r in records]
    merge_pairs = [
        (_to_int(r["id"]), _to_int(r.get("merge_into_id")))
        for r in records
        if _to_int(r.get("merge_into_id")) is not None
    ]
    types = sorted({_clean(r.get("type")) for r in records} - {None})
    issn_rows, collisions = resolve_issns(records)
    print(f"source_issn rows: {len(issn_rows):,}; ISSN collisions resolved: {len(collisions)}")

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("TRUNCATE source_issn, issn_to_issnl, sources, source_type CASCADE;")

        execute_values(
            cur,
            "INSERT INTO source_type (source_type_id, display_name) VALUES %s",
            [(t, t) for t in types],
        )

        cols_sql = ", ".join(SOURCE_COLUMNS)
        execute_values(
            cur,
            f"INSERT INTO sources ({cols_sql}) VALUES %s",
            source_rows,
            page_size=1000,
        )

        # second pass: self-referential redirects
        execute_values(
            cur,
            "UPDATE sources AS s SET merge_into_id = v.merge_into_id "
            "FROM (VALUES %s) AS v(id, merge_into_id) WHERE s.id = v.id",
            merge_pairs,
            page_size=1000,
        )

        execute_values(
            cur,
            "INSERT INTO source_issn (source_id, issn, is_issn_l) VALUES %s",
            issn_rows,
            page_size=1000,
        )

        cur.execute("SELECT setval('source_id_seq', (SELECT MAX(id) FROM sources));")
        seq = cur.fetchone()[0]
        raw.commit()
        print(f"committed. source_id_seq -> {seq}")
    finally:
        raw.close()

    if collisions:
        rep = pd.DataFrame(collisions, columns=["issn", "kept_source_id", "dropped_source_id"])
        rep.to_csv(args.report, index=False)
        print(f"wrote {len(collisions)} collision rows -> {args.report}")


if __name__ == "__main__":
    main()
