"""Retype 1,195 Japanese and Korean society meeting series from journal to conference (oxjob #2525, 22 Nov 2026).

Why: these sources are meeting abstract books and proceedings (講演要旨集, 講演概要集, 学術講演梗概集, 講演論文集 /
학술대회 논문집, 학술발표논문집, 초록집), mostly single MAG-era records (S4306...), and 122 records minted this month
from CiNii (S7407...), all registered as journals. help.openalex.org (Source types) defines `conference` as
"Proceedings and abstracts from academic conferences"; oxjob #2494 retyped the same class where the works were already
typed as conference papers or abstracts. Their ~1.69M works are typed article; the same oxjob types them by the
series' own name (rows in openalex.works.record_declared_types, platform cjk-meeting-series).

Input: handoff/oxjob2525-cjk-meeting-series-retype-2026-11-22.json (ids, names, works on night 2026-11-22). Built by
oxjob #2525 (work/build_lists.py): every journal-typed source whose name says a Japanese or Korean meeting series, all
1,208 names read by hand; 13 left out (People's Congress gazettes the name rule caught, two journal section records,
two book series, three lecture series). Bar: a random 60 of the list (seed 25251) 60/60 meeting series.

What it does, in ONE transaction (dry run by default; rolls back unless --execute). Rows only, no DDL:
  UPDATE sources SET type = 'conference', updated_date = now() WHERE id = ANY(list) AND type = 'journal'.
A row whose type is no longer journal is skipped and listed. Expect 1,195 changed (tolerance 5).
Walden 26c57f6c (oxjob #2494) already reads conference series as journals in the OA and venue-copy rules, so no work
loses its OA status or venue pick from this retype.

Run from an openalex-sources checkout, NOT while the End 2 End is running (05:00 UTC until it publishes).
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2525-cjk-meeting-series-retype-2026-11-22.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2525-cjk-meeting-series-retype-2026-11-22.py --execute
Save the JSON after RECEIPT as handoff/oxjob2525-cjk-meeting-series-retype-2026-11-22-receipt.json and commit both files.
Revert: run_registry.py handoff/oxjob2525-cjk-meeting-series-retype-2026-11-22.py --revert handoff/oxjob2525-cjk-meeting-series-retype-2026-11-22-receipt.json [--execute]
(puts type 'journal' back on every row this run changed that is still 'conference'); with it, DELETE FROM
openalex.works.record_declared_types WHERE oxjob = 2525 on Databricks.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
LIST_FILE = os.path.join(HERE, "oxjob2525-cjk-meeting-series-retype-2026-11-22.json")
EXPECT, TOL = 1195, 5


class DryRun(Exception):
    pass


def main():
    ids = [s["id"] for s in json.load(open(LIST_FILE, encoding="utf-8"))["sources"]]
    receipt = {"oxjob": 2525, "run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
               "revert_of": REVERT, "changed": [], "skipped": {}}
    try:
        with engine.begin() as conn:
            if REVERT:
                back = json.load(open(REVERT, encoding="utf-8"))["changed"]
                rows = conn.execute(text("UPDATE sources SET type = 'journal', updated_date = now() "
                                         "WHERE id = ANY(:ids) AND type = 'conference' RETURNING id"),
                                    {"ids": back}).fetchall()
                receipt["changed"] = sorted(r.id for r in rows)
                for i in set(back) - set(receipt["changed"]):
                    receipt["skipped"][str(i)] = "no longer conference: edited since"
            else:
                before = {r.id: r.type for r in conn.execute(
                    text("SELECT id, type FROM sources WHERE id = ANY(:ids)"), {"ids": ids})}
                for i in ids:
                    if i not in before:
                        receipt["skipped"][str(i)] = "missing (merged or deleted since 22 Nov)"
                    elif before[i] != "journal":
                        receipt["skipped"][str(i)] = f"type is {before[i]!r}, not journal"
                rows = conn.execute(text("UPDATE sources SET type = 'conference', updated_date = now() "
                                         "WHERE id = ANY(:ids) AND type = 'journal' RETURNING id"),
                                    {"ids": ids}).fetchall()
                receipt["changed"] = sorted(r.id for r in rows)
                n = len(receipt["changed"])
                if abs(n - EXPECT) > TOL:
                    raise RuntimeError(f"changed {n}, expected {EXPECT} +/- {TOL}: rolled back")
            after = dict(conn.execute(text("SELECT type, count(*) FROM sources WHERE id = ANY(:ids) GROUP BY type"),
                                      {"ids": ids}).fetchall())
            print(f"changed {len(receipt['changed'])}, skipped {len(receipt['skipped'])}; list now by type: {after}",
                  flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
