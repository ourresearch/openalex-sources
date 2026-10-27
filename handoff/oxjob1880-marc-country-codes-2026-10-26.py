"""ISO country codes for 4,498 sources that carry Library of Congress (MARC) codes (oxjob #1880, 2026-10-26).

The 20-25 Nov 2024 import copied MARC country codes into `country_code` and the MARC name into `country`:
  - 614 three-letter codes for a state, province or nation (CAU California, NYU New York, ENK England, ONC Ontario,
    VRA Victoria, XXU/XXK/XXC). Not ISO at all: `filter=country_code:US` misses them. Mapped by the MARC Code List
    for Countries: *U -> US, *C -> CA, *K -> GB, *A -> AU.
  - 3,884 two-letter MARC codes that look like ISO but name another country: IO Indonesia (ISO IO = British Indian
    Ocean Territory, 1,961 sources), BL Brazil, II India, GW Germany, UA Egypt (ISO UA = Ukraine), AU Austria (ISO AU
    = Australia), SA South Africa (ISO SA = Saudi Arabia) ... The row's own `country` holds the MARC name, so each
    row states what it meant: the new code is ISO 3166-1 alpha-2 for that name (pycountry + MARC spellings).
CSV handoff/oxjob1880-marc-country-codes-2026-10-26.csv was built by oxjob #1880 work/cc_map.py from the 26 Oct
mirror (openalex.sources.sources). Rows whose `country` is empty or 'International organization' are not in it.

Guard per row: the source exists and still carries old_code (never overwrites a later edit). Only country_code and
updated_date change; `country` keeps the import's name. Works don't embed a source's country: 0 works re-stamped.
Revert: the receipt lists every (source_id, old_code); set them back the same way.

One transaction, dry run by default, receipt after RECEIPT.
    python handoff/oxjob1880-marc-country-codes-2026-10-26.py [--execute]
"""
import csv
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "oxjob1880-marc-country-codes-2026-10-26.csv")
EXECUTE = "--execute" in sys.argv
ISO2 = re.compile(r"^[A-Z]{2}$")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "changed": {}, "skipped": {}}


class DryRun(Exception):
    pass


def counts(conn):
    return conn.execute(text(
        "SELECT COUNT(*) FILTER (WHERE country_code !~ '^[A-Z]{2}$'), "
        "COUNT(*) FILTER (WHERE country_code = 'IO'), COUNT(*) FILTER (WHERE country_code = 'ID') "
        "FROM sources WHERE country_code IS NOT NULL")).fetchone()


def main():
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    assert all(ISO2.match(r["new_code"]) for r in rows), "new codes must be ISO alpha-2"
    try:
        with engine.begin() as conn:
            b = counts(conn)
            print(f"before: non-ISO-shaped codes {b[0]}, IO {b[1]}, ID {b[2]}", flush=True)
            cur = {r.id: r.country_code for r in conn.execute(
                text("SELECT id, country_code FROM sources WHERE id = ANY(:i)"),
                {"i": [int(r["source_id"]) for r in rows]})}
            by_pair = Counter()
            for r in rows:
                sid = int(r["source_id"])
                now = cur.get(sid, "<gone>")
                if now != r["old_code"]:
                    receipt["skipped"][str(sid)] = f"now {now}, expected {r['old_code']}"
                    continue
                k = conn.execute(text("UPDATE sources SET country_code = :n, updated_date = now() "
                                      "WHERE id = :s AND country_code = :o"),
                                 {"n": r["new_code"], "o": r["old_code"], "s": sid}).rowcount
                assert k == 1, sid
                receipt["changed"][str(sid)] = [r["old_code"], r["new_code"]]
                by_pair[f"{r['old_code']}->{r['new_code']}"] += 1
            a = counts(conn)
            print(f"changed {len(receipt['changed'])} of {len(rows)}; skipped {len(receipt['skipped'])}", flush=True)
            print(f"after: non-ISO-shaped codes {a[0]}, IO {a[1]}, ID {a[2]}", flush=True)
            print("top pairs:", by_pair.most_common(12), flush=True)
            receipt["before"], receipt["after"] = list(b), list(a)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
