"""DOAJ journal feed: download the public metadata CSV (doaj.org/csv, ~23K
journals, no credentials), stage it, and apply DOAJ status to sources:

  is_in_doaj / is_in_doaj_start_year / doaj_license, plus delistings (flag off
  when a journal leaves DOAJ) and a guarded is_oa recompute on changed rows.

With --mint, DOAJ also ADDS journals the registry lacks (DOAJ is human-vetted,
and we already harvest DOAJ articles into the works pipeline — their locations
need these sources to match against). Same cascade as the DataCite sync:
ISSN match -> already known; unique exact-normalized-name match -> attach the
DOAJ ISSNs to that source; ambiguous name -> conflict row; no match -> mint.

  python -m jobs.doaj [--dry-run] [--skip-fetch] [--mint]
"""
import argparse
import csv
import io
from collections import Counter, defaultdict

import requests
from sqlalchemy import text

from db import engine
from sources_lib import (
    insert_issns,
    normalize_issns,
    normalize_name,
    resolve_issn_l,
    upsert_journal_by_issn,
)

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


def mint_missing(dry_run=False, batch=200):
    """Add DOAJ journals whose ISSNs match no source (guarded; see module doc)."""
    with engine.connect() as conn:
        staged = conn.execute(text(
            "SELECT issns, title, publisher FROM doaj_journal ORDER BY id")).fetchall()
        issn_to_sid = dict(conn.execute(text(
            "SELECT issn, source_id FROM source_issn")).fetchall())
        name_index = defaultdict(list)
        for sid, name in conn.execute(text(
                "SELECT id, display_name FROM sources WHERE merge_into_id IS NULL")).fetchall():
            name_index[normalize_name(name)].append(sid)

    counts = Counter()
    conn = engine.connect()
    trans = conn.begin()
    done = 0
    try:
        for row in staged:
            issns = normalize_issns(list(row.issns or []))
            if not issns or any(i in issn_to_sid for i in issns):
                counts["already_known"] += 1
                continue
            candidates = name_index.get(normalize_name(row.title), [])
            if len(candidates) == 1:
                counts["linked_by_name"] += 1
                if not dry_run:
                    sid = candidates[0]
                    issn_l = resolve_issn_l(conn, issns)
                    insert_issns(conn, sid, issns, issn_l)
                    for i in issns:
                        issn_to_sid[i] = sid
            elif len(candidates) > 1:
                counts["conflict"] += 1
                if not dry_run:
                    conn.execute(text(
                        "INSERT INTO source_ingest_issue "
                        "(source_feed, issue_type, issns, matched_source_ids, detail) "
                        "VALUES ('doaj', 'multi_match', :i, :m, :d) "
                        "ON CONFLICT (source_feed, issue_type, matched_source_ids) DO NOTHING"
                    ), {"i": issns, "m": sorted(candidates), "d": row.title})
            else:
                counts["added"] += 1
                if not dry_run:
                    _, sid = upsert_journal_by_issn(
                        conn, issns, display_name=row.title,
                        publisher=row.publisher, source_feed="doaj")
                    if sid:
                        for i in issns:
                            issn_to_sid[i] = sid
                        name_index[normalize_name(row.title)].append(sid)
            done += 1
            if done % batch == 0 and not dry_run:
                trans.commit()
                trans = conn.begin()
        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()
    print("mint summary:", dict(counts), flush=True)
    return counts


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
    ap.add_argument("--mint", action="store_true", help="add journals the registry lacks")
    args = ap.parse_args()
    if not args.skip_fetch:
        fetch()
    if args.mint:
        mint_missing(dry_run=args.dry_run)  # before apply, so new mints get flagged
    apply(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
