"""Retype 1,288 conference proceedings and meeting-abstract series from journal to conference (oxjob #2494, 20 Nov 2026).

Why: these sources are typed `journal` although at least 90% of their visible works are conference papers or meeting
abstracts by OpenAlex's own work type (Proceedings of SPIE, AIP Conference Proceedings, Journal of Physics: Conference
Series, Bulletin of the American Physical Society, AGU Fall Meeting Abstracts, ECS Meeting Abstracts, Procedia series,
and 959 single-edition MAG records such as "Structures Congress 2012"). help.openalex.org (Source types) defines
`conference` as "Proceedings and abstracts from academic conferences, like IEEE and IOP Conference Series"; the sources
policy line of 5 Oct 2026 (oxjob #1528) retypes conference serials registered as journals. Premium ticket 22707 (CWTS)
asked for it; their core-journal selection keys on type journal.

Input: handoff/oxjob2494-conference-retype-2026-11-20.json (ids, names, works on 20 Nov). Built by oxjob #2494
(work/retype_list.json): journal-typed sources with >= 100 visible works, >= 90% conference papers or abstracts, minus
27 exclusions (real journals with mistyped items, journal supplements, abstracting journals, 4 book series), every
source over 20K works checked by hand, a random 60 of the rest 59/60 conference series or editions (seed 24941; the
miss, ACS Symposium Series, moved to the exclusions).

What it does, in ONE transaction (dry run by default; rolls back unless --execute). Rows only, no DDL:
  UPDATE sources SET type = 'conference', updated_date = now() WHERE id = ANY(list) AND type = 'journal'.
A row whose type is no longer journal is skipped and listed. Expect 1,288 changed (tolerance 5).
Walden 26c57f6c (same night) reads conference series as journals in the OA / venue-copy rules, so no work loses its
gold status, publisher copy or venue pick from this retype.

Run from the openalex-sources checkout, NOT while the End 2 End is running (05:00 UTC until it publishes).
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2494-conference-retype-2026-11-20.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2494-conference-retype-2026-11-20.py --execute
Save the JSON after RECEIPT as handoff/oxjob2494-conference-retype-2026-11-20-receipt.json and commit both files.
Revert: run_registry.py handoff/oxjob2494-conference-retype-2026-11-20.py --revert handoff/oxjob2494-conference-retype-2026-11-20-receipt.json [--execute]
(puts type 'journal' back on every row this run changed that is still 'conference'); then git revert walden 26c57f6c.
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
LIST_FILE = os.path.join(HERE, "oxjob2494-conference-retype-2026-11-20.json")
EXPECT, TOL = 1288, 5


class DryRun(Exception):
    pass


def main():
    ids = [s["id"] for s in json.load(open(LIST_FILE, encoding="utf-8"))["sources"]]
    receipt = {"oxjob": 2494, "run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
               "revert_of": REVERT, "changed": [], "skipped": {}}
    try:
        with engine.begin() as conn:
            if REVERT:
                prior = json.load(open(REVERT, encoding="utf-8"))
                back = prior["changed"]
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
                        receipt["skipped"][str(i)] = "missing (merged or deleted since 20 Nov)"
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
