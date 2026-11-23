"""Retype book and monograph series registered as journals from journal to book series (oxjob #2556, 23 Nov 2026).

Why: these sources are typed `journal`, yet at least 80% of their visible works are Crossref book chapters or books whose
own Crossref records carry the series ISSN, and none of their ISSNs is in Crossref's journal title list
(api.crossref.org/journals/{issn} answers 404 for book series, 200 for journals). Examples: Mathematical Surveys and
Monographs (AMS), Frontiers in Artificial Intelligence and Applications (IOS Press), Lecture Notes of the Institute for
Computer Sciences, Social Informatics and Telecommunications Engineering (Springer), Frontiers Research Topics, Palgrave
and Springer monograph series, UN iLibrary report series. help.openalex.org (Source types) defines `book series` as
"Numbered collections of scholarly books published under a shared series title"; their siblings (Lecture Notes in
Computer Science, Communications in Computer and Information Science, Springer Proceedings in Physics) are already
typed book series. Most came in through bulk ISSN imports that defaulted to journal (Feb 2022, Apr 2024, Nov 2024).

Input: handoff/oxjob2556-book-series-retype-2026-11-23.json (ids, names, works on 23 Nov). Built by oxjob #2556
(work/build_list.py): the rule above, minus the exclusion list (work/exclusion_list.json: no ISSN, yearbooks /
Jahrbücher / annuals / bulletins / journal-named records, journal Sonderheft / Supplementum series, single-conference
proceedings, any ISSN Crossref lists as a journal). Every source over 5K works checked by hand; bar on a random 60
(work/bar_sample.json).

What it does, in ONE transaction (dry run by default; rolls back unless --execute). Rows only, no DDL:
  UPDATE sources SET type = 'book series', updated_date = now() WHERE id = ANY(list) AND type = 'journal'.
A row whose type is no longer journal is skipped and listed. Expect EXPECT changed (tolerance 5).
No walden change: the retyped series take the rules book series already have (exposure measured in the oxjob:
<= ~70 works' OA status, <= ~640 venue-copy picks, ~2.2K organisation-author seats filled by oxjob #1346's rule).

Run from an openalex-sources checkout, NOT while the End 2 End is running (05:00 UTC until it publishes).
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2556-book-series-retype-2026-11-23.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2556-book-series-retype-2026-11-23.py --execute
Save the JSON after RECEIPT as handoff/oxjob2556-book-series-retype-2026-11-23-receipt.json and commit both files.
Revert: run_registry.py handoff/oxjob2556-book-series-retype-2026-11-23.py --revert handoff/oxjob2556-book-series-retype-2026-11-23-receipt.json [--execute]
(puts type 'journal' back on every row this run changed that is still 'book series').
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
LIST_FILE = os.path.join(HERE, "oxjob2556-book-series-retype-2026-11-23.json")
NEW, OLD = "book series", "journal"


class DryRun(Exception):
    pass


def main():
    listing = json.load(open(LIST_FILE, encoding="utf-8"))
    ids = [s["id"] for s in listing["sources"]]
    expect, tol = len(ids), 5
    receipt = {"oxjob": 2556, "run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
               "revert_of": REVERT, "changed": [], "skipped": {}}
    try:
        with engine.begin() as conn:
            if REVERT:
                back = json.load(open(REVERT, encoding="utf-8"))["changed"]
                rows = conn.execute(text("UPDATE sources SET type = :old, updated_date = now() "
                                         "WHERE id = ANY(:ids) AND type = :new RETURNING id"),
                                    {"ids": back, "old": OLD, "new": NEW}).fetchall()
                receipt["changed"] = sorted(r.id for r in rows)
                for i in set(back) - set(receipt["changed"]):
                    receipt["skipped"][str(i)] = f"no longer {NEW}: edited since"
            else:
                before = {r.id: r.type for r in conn.execute(
                    text("SELECT id, type FROM sources WHERE id = ANY(:ids)"), {"ids": ids})}
                for i in ids:
                    if i not in before:
                        receipt["skipped"][str(i)] = "missing (merged or deleted since 23 Nov)"
                    elif before[i] != OLD:
                        receipt["skipped"][str(i)] = f"type is {before[i]!r}, not {OLD}"
                rows = conn.execute(text("UPDATE sources SET type = :new, updated_date = now() "
                                         "WHERE id = ANY(:ids) AND type = :old RETURNING id"),
                                    {"ids": ids, "old": OLD, "new": NEW}).fetchall()
                receipt["changed"] = sorted(r.id for r in rows)
                n = len(receipt["changed"])
                if abs(n - expect) > tol:
                    raise RuntimeError(f"changed {n}, expected {expect} +/- {tol}: rolled back")
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
