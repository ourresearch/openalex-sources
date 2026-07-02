"""DOAJ journal feed: download the public metadata CSV (doaj.org/csv, ~23K
journals, no credentials), stage it, and apply DOAJ status to sources:

  is_in_doaj / is_in_doaj_start_year / doaj_license, plus delistings (flag off
  when a journal leaves DOAJ) and a guarded is_oa recompute on changed rows.

  python -m jobs.doaj [--dry-run] [--skip-fetch]
"""
import argparse
import csv
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from sqlalchemy import text

from db import engine

DOAJ_CSV_URL = "https://doaj.org/csv"

LICENSE_MAP = {
    "CC BY": "cc-by",
    "CC BY-NC": "cc-by-nc",
    "CC BY-NC-ND": "cc-by-nc-nd",
    "CC BY-NC-SA": "cc-by-nc-sa",
    "CC BY-SA": "cc-by-sa",
    "CC BY-ND": "cc-by-nd",
    "Public domain": "public-domain",
}


def fetch():
    r = requests.get(DOAJ_CSV_URL, timeout=300)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    rows = []
    for rec in reader:
        issns = [
            v.strip().upper()
            for v in (rec.get("Journal ISSN (print version)"),
                      rec.get("Journal EISSN (online version)"))
            if v and v.strip()
        ]
        if not issns:
            continue
        year = (rec.get("When did the journal start to publish all content using an open license?") or "").strip()
        rows.append({
            "issns": issns,
            "title": (rec.get("Journal title") or "").strip() or None,
            "publisher": (rec.get("Publisher") or "").strip() or None,
            "license": LICENSE_MAP.get((rec.get("Journal license") or "").strip()),
            "oa_start_year": int(year) if year.isdigit() else None,
            "country": (rec.get("Country of publisher") or "").strip() or None,
        })
    insert = text(
        "INSERT INTO doaj_journal (issns, title, publisher, license, oa_start_year, country) "
        "VALUES (:issns, :title, :publisher, :license, :oa_start_year, :country)"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE doaj_journal"))
        for i in range(0, len(rows), 5000):
            conn.execute(insert, rows[i:i + 5000])
    print(f"staged {len(rows)} DOAJ journals", flush=True)


def apply(dry_run=False):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMP TABLE _doaj_flags ON COMMIT DROP AS
            SELECT si.source_id,
                   MIN(d.oa_start_year) AS start_year,
                   MIN(d.license) AS license
            FROM doaj_journal d
            CROSS JOIN LATERAL unnest(d.issns) AS di(issn)
            JOIN source_issn si ON si.issn = di.issn
            GROUP BY si.source_id
        """))
        n_match = conn.execute(text("SELECT COUNT(*) FROM _doaj_flags")).scalar()

        turned_on = conn.execute(text("""
            SELECT COUNT(*) FROM sources s JOIN _doaj_flags f ON f.source_id = s.id
            WHERE s.is_in_doaj IS DISTINCT FROM TRUE
               OR s.is_in_doaj_start_year IS DISTINCT FROM f.start_year
               OR s.doaj_license IS DISTINCT FROM f.license
        """)).scalar()
        turned_off = conn.execute(text("""
            SELECT COUNT(*) FROM sources s
            WHERE s.is_in_doaj = TRUE AND s.merge_into_id IS NULL
              AND NOT EXISTS (SELECT 1 FROM _doaj_flags f WHERE f.source_id = s.id)
        """)).scalar()
        print(f"{n_match} sources matched in DOAJ; {turned_on} to set/update, "
              f"{turned_off} delistings; dry_run={dry_run}", flush=True)
        if dry_run:
            return

        conn.execute(text("""
            UPDATE sources s SET
                is_in_doaj = TRUE,
                is_in_doaj_start_year = f.start_year,
                doaj_license = f.license,
                is_oa = TRUE,
                updated_date = now()
            FROM _doaj_flags f
            WHERE f.source_id = s.id
              AND (s.is_in_doaj IS DISTINCT FROM TRUE
                   OR s.is_in_doaj_start_year IS DISTINCT FROM f.start_year
                   OR s.doaj_license IS DISTINCT FROM f.license)
        """))
        # delistings: flag off and recompute is_oa from the remaining OA signals
        conn.execute(text("""
            UPDATE sources s SET
                is_in_doaj = FALSE,
                is_in_doaj_start_year = NULL,
                doaj_license = NULL,
                is_oa = (COALESCE(s.is_in_scielo, FALSE)
                         OR COALESCE(s.is_oa_high_oa_rate, FALSE)
                         OR COALESCE(s.is_fully_open_in_jstage, FALSE)),
                updated_date = now()
            WHERE s.is_in_doaj = TRUE AND s.merge_into_id IS NULL
              AND NOT EXISTS (SELECT 1 FROM _doaj_flags f WHERE f.source_id = s.id)
        """))
    print("applied (DONE)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-fetch", action="store_true", help="apply from existing staging")
    args = ap.parse_args()
    if not args.skip_fetch:
        fetch()
    apply(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
