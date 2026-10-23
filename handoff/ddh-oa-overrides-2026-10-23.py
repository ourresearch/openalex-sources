"""OA overrides for Diamond Discovery Hub sources that OpenAlex does not flag OA (Kyle, 2026-10-23).

Of the 4,291 ddh sources, 509 have is_oa = false, and every non-OA ddh work sits in one of
them (works in OA-flagged ddh sources are 100% OA). A per-year look at each source's works
(share with a free copy at the publisher; ~/MacDemes/research/ddh-oa) sorted them into:
always open since their first year (217), too small to tell (111), a visible flip (123),
no publisher evidence (43), mixed (15). This script covers the first two groups only, minus
the 47 sources minted on 2026-10-22 that already carry an override: 281 rows,
curated_is_oa = TRUE with no flip year (OA for every year). The flipped / unclear sources
need a start year first (journal web pages, then DDH).

The manifest is ddh-oa-overrides-2026-10-23.csv next to this file. Guards per row: the source
exists, is on ddh, and has no source_oa_override row yet (an existing curation is never
overwritten). One transaction, dry run by default, receipt printed after RECEIPT. The flags
change when jobs.apply_oa_flags next runs (weekly, Mon 05:55 UTC, or by hand).

    heroku run -a openalex-sources --no-tty "python handoff/ddh-oa-overrides-2026-10-23.py [--execute]"
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
MANIFEST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ddh-oa-overrides-2026-10-23.csv")


class DryRun(Exception):
    pass


def main():
    rows = list(csv.DictReader(open(MANIFEST, newline="", encoding="utf-8")))
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
               "inserted": [], "skipped": []}
    try:
        with engine.begin() as conn:
            for r in rows:
                sid = int(r["source_id"])
                s = conn.execute(text("SELECT id, 'ddh' = ANY(listed_in) AS on_ddh, is_oa FROM sources WHERE id = :id"),
                                 {"id": sid}).fetchone()
                existing = conn.execute(text("SELECT curated_is_oa, curated_flip_year, note FROM source_oa_override "
                                             "WHERE source_id = :id"), {"id": sid}).fetchone()
                why = ("missing" if s is None else "not on ddh" if not s.on_ddh
                       else f"override exists {tuple(existing)}" if existing else None)
                if why:
                    receipt["skipped"].append({"source_id": sid, "name": r["name"], "why": why})
                    print(f"SKIP {sid} {r['name']!r}: {why}", flush=True)
                    continue
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, TRUE, :n)"),
                             {"s": sid, "n": r["note"]})
                receipt["inserted"].append({"source_id": sid, "name": r["name"], "class": r["class"]})
            print(f"overrides inserted {len(receipt['inserted'])}, skipped {len(receipt['skipped'])} "
                  f"(of {len(rows)})", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
