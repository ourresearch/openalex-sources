"""OA start years for the Diamond Discovery Hub sources that were still not OA after
ddh-oa-overrides-2026-10-23 (Kyle, 2026-10-23).

The 180 remaining ddh sources looked flipped / unclear in our own data. Their start years
come from each journal's web pages (About / history / policy pages, platform catalogue
pages; ~/MacDemes/research/ddh-oa/oa_since_review.csv, with URL + quote per journal).
Kyle's rule: count the WHOLE free run, i.e. back issues free on the journal's site or on an
official archive it links to (Persee, Hrcak, Biblioteka Nauki, dLib, Bazhum...) count.
Rows: curated_is_oa = TRUE plus curated_flip_year = first free year - 1 (apply_oa_flags makes
high_oa_rate_start_year = flip + 1, so works from the first free year on are OA via the
source and older ones keep their own evidence); journals open since their first issue get no
flip year. Five journals are held for DDH (one charges a EUR 300 APC; four unclear).

Manifest: ddh-oa-start-years-2026-10-23.csv next to this file. Guards per row: the source
exists, is on ddh, and has no source_oa_override row yet. One transaction, dry run by
default, receipt printed after RECEIPT. Flags change when jobs.apply_oa_flags runs.

    heroku run -a openalex-sources --no-tty "python handoff/ddh-oa-start-years-2026-10-23.py [--execute]"
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
MANIFEST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ddh-oa-start-years-2026-10-23.csv")


class DryRun(Exception):
    pass


def main():
    rows = list(csv.DictReader(open(MANIFEST, newline="", encoding="utf-8")))
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "inserted": [], "skipped": []}
    try:
        with engine.begin() as conn:
            for r in rows:
                sid = int(r["source_id"])
                flip = int(r["curated_flip_year"]) if r["curated_flip_year"] else None
                s = conn.execute(text("SELECT id, 'ddh' = ANY(listed_in) AS on_ddh FROM sources WHERE id = :id"),
                                 {"id": sid}).fetchone()
                existing = conn.execute(text("SELECT curated_is_oa, curated_flip_year, note FROM source_oa_override "
                                             "WHERE source_id = :id"), {"id": sid}).fetchone()
                why = ("missing" if s is None else "not on ddh" if not s.on_ddh
                       else f"override exists {tuple(existing)}" if existing else None)
                if why:
                    receipt["skipped"].append({"source_id": sid, "name": r["name"], "why": why})
                    print(f"SKIP {sid} {r['name']!r}: {why}", flush=True)
                    continue
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                                  "VALUES (:s, TRUE, :f, :n)"), {"s": sid, "f": flip, "n": r["note"]})
                receipt["inserted"].append({"source_id": sid, "name": r["name"], "oa_since": r["oa_since"] or "born_open"})
            print(f"overrides inserted {len(receipt['inserted'])} "
                  f"({sum(1 for x in receipt['inserted'] if x['oa_since'] != 'born_open')} with a start year), "
                  f"skipped {len(receipt['skipped'])} (of {len(rows)})", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
