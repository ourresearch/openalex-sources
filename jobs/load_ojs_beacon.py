"""Load (or reload) the PKP Beacon's OJS journal ISSNs into ojs_beacon_issn and
recompute sources.is_ojs (oxjob #1424).

BY HAND, when PKP publishes a new Beacon edition (yearly, Harvard Dataverse
doi:10.7910/DVN/OCZNVY). Convert it to the CSV shape below (the builder is
build_csv.py in oxjob #1424), drop it in data/ojs_beacon/, and run this once.
Full replace (TRUNCATE + INSERT, one transaction), then
sources_lib.recompute_is_ojs(). Idempotent. Touches is_ojs only: not
source_list, not listed_in, not is_oa.

CSV columns (header required; the load_source_list shape, so one reader):
  name               Beacon journal name (informational)
  issns              one or more ISSNs, ';'-separated, NNNN-NNNX
  active             rows with false are skipped (the Beacon file has none)

  python -m jobs.load_ojs_beacon --csv data/ojs_beacon/ojs-beacon-2026-07-18.csv [--dry-run]
"""
import argparse

from sqlalchemy import text

from db import engine
from jobs.load_source_list import read_rows
from sources_lib import recompute_is_ojs


def run(csv_path, dry_run=False):
    names = {}
    for r in read_rows(csv_path):
        if not r["active"]:
            continue
        for issn in r["issns"]:
            names.setdefault(issn, r["name"])
    print(f"{csv_path}: {len(names)} distinct ISSNs", flush=True)

    conn = engine.connect()
    trans = conn.begin()
    try:
        before = conn.execute(text("SELECT COUNT(*) FROM ojs_beacon_issn")).scalar()
        is_ojs_before = conn.execute(text("SELECT COUNT(*) FROM sources WHERE is_ojs")).scalar()
        conn.execute(text("CREATE TEMP TABLE _ojs_before ON COMMIT DROP AS "
                          "SELECT id FROM sources WHERE is_ojs"))
        conn.execute(text("TRUNCATE ojs_beacon_issn"))
        conn.execute(text("""
            INSERT INTO ojs_beacon_issn (issn, name)
            SELECT * FROM UNNEST(CAST(:issns AS text[]), CAST(:names AS text[]))
        """), {"issns": list(names), "names": list(names.values())})
        changed = recompute_is_ojs(conn)
        is_ojs_after = conn.execute(text("SELECT COUNT(*) FROM sources WHERE is_ojs")).scalar()
        gained, lost = conn.execute(text("""
            SELECT COUNT(*) FILTER (WHERE s.is_ojs AND b.id IS NULL),
                   COUNT(*) FILTER (WHERE NOT s.is_ojs AND b.id IS NOT NULL)
            FROM sources s LEFT JOIN _ojs_before b ON b.id = s.id
        """)).one()
        unmatched = conn.execute(text("""
            SELECT COUNT(*) FROM ojs_beacon_issn b
            WHERE NOT EXISTS (SELECT 1 FROM source_issn si WHERE si.issn = b.issn)
        """)).scalar()

        print(f"ojs_beacon_issn rows: {before} -> {len(names)} ({unmatched} match no registry source)")
        print(f"sources.is_ojs: {is_ojs_before} -> {is_ojs_after} "
              f"({changed} rows changed: +{gained} / -{lost})")

        if dry_run:
            trans.rollback()
            print("DRY RUN — rolled back")
        else:
            trans.commit()
            print("committed")
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    run(a.csv, dry_run=a.dry_run)


if __name__ == "__main__":
    main()
