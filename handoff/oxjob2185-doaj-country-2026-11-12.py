"""Country for DOAJ journals that have none (oxjob #2185, 2026-11-12): the first fill, with a receipt.

3,482 sources flagged is_in_doaj serve country_code NULL though DOAJ's journal record names the publisher's
country (staged all along in doaj_journal.country, never applied). This runs jobs.doaj.fill_country once, in one
transaction, and writes the receipt; after it, the scheduled `jobs.doaj` run fills new NULLs the same way.

Only NULL country_codes are filled; no existing value changes. `country` (the name) is filled only where NULL.
Works don't embed a source's country (the API joins it at query time): 0 works re-stamped.

    python handoff/oxjob2185-doaj-country-2026-11-12.py              # dry run, rolled back
    python handoff/oxjob2185-doaj-country-2026-11-12.py --execute    # write + print the receipt
    python handoff/oxjob2185-doaj-country-2026-11-12.py --revert handoff/oxjob2185-doaj-country-2026-11-12-receipt.json [--execute]

Revert sets each receipt row back to NULL only if it still carries the code this run wrote (a later edit is kept),
and clears `country` only where this run filled it.
"""
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from jobs.doaj import fill_country

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
NULLS = ("SELECT COUNT(*) FILTER (WHERE country_code IS NULL AND is_in_doaj), "
         "COUNT(*) FILTER (WHERE country_code IS NOT NULL) FROM sources")


class DryRun(Exception):
    pass


def fill(conn, receipt):
    filled = fill_country(conn)
    receipt["changed"] = {str(sid): [None, code] for sid, code, _, _ in filled}
    receipt["country_name_filled"] = [sid for sid, _, _, name_filled in filled if name_filled]
    print("top codes:", Counter(f[1] for f in filled).most_common(10), flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    named = set(prior["country_name_filled"])
    wrote = {int(sid): code for sid, (_, code) in prior["changed"].items()}
    done = {r.id for r in conn.execute(text("""
        UPDATE sources s SET country_code = NULL, country = CASE WHEN v.named THEN NULL ELSE s.country END,
                             updated_date = now()
        FROM unnest(CAST(:ids AS bigint[]), CAST(:codes AS text[]), CAST(:named AS boolean[])) AS v(id, code, named)
        WHERE s.id = v.id AND s.country_code = v.code
        RETURNING s.id
    """), {"ids": list(wrote), "codes": list(wrote.values()), "named": [sid in named for sid in wrote]})}
    receipt["skipped"] = {str(sid): f"no longer {code}" for sid, code in wrote.items() if sid not in done}
    receipt["reverted"] = len(done)
    print(f"reverted {len(done)} of {len(wrote)}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT}
    try:
        with engine.begin() as conn:
            receipt["before"] = list(conn.execute(text(NULLS)).fetchone())
            (revert if REVERT else fill)(conn, receipt)
            receipt["after"] = list(conn.execute(text(NULLS)).fetchone())
            print(f"DOAJ sources with NULL country_code {receipt['before'][0]} -> {receipt['after'][0]}; "
                  f"sources with a country_code {receipt['before'][1]} -> {receipt['after'][1]}", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
