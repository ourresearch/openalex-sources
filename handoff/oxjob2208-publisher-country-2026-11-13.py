"""Country for journals and book platforms that have none though their publisher's other titles all share one
(oxjob #2208, 2026-11-13).

country_code is NULL on about 3,700 journals, ebook platforms and book series whose host (publisher or institution)
puts every other title of its own in one country: Japanese Circulation Journal (S150042853, 91,876 works; Japanese
Circulation Society, 4 of 4 other titles JP), IEEJ Transactions on Power and Energy (S53799065). OpenAlex defines a
source's country as its home country, typically its publisher's, so source-country filters, continent and
is_global_south miss their works.

handoff/oxjob2208-publisher-country-2026-11-13.csv (built by oxjob #2208, work/build_manifest.py) lists per source
the country to set. A source is in it only when its host has >= 3 other titles with a country and they agree
(the share needed is in the oxjob README), the publisher record's own country does not disagree, and its works'
authors are not >= 80% from one other country (the sign of a wrong host). DOAJ journals held by #2185 are out.
Bar: the oxjob README.

Only NULL country_codes are filled; no existing value changes. `country` (the name) is filled only where NULL.
Works don't embed a source's country (the API joins it at query time): 0 works re-stamped.

Guard per row, re-checked inside the UPDATE: country_code is still NULL and the source still has the host the
manifest was built from.

    python handoff/oxjob2208-publisher-country-2026-11-13.py              # dry run, rolled back
    python handoff/oxjob2208-publisher-country-2026-11-13.py --execute    # write + print the receipt
    python handoff/oxjob2208-publisher-country-2026-11-13.py --revert handoff/oxjob2208-publisher-country-2026-11-13-receipt.json [--execute]

Revert sets each receipt row back to NULL only if it still carries the code this run wrote (a later edit is kept),
and clears `country` only where this run filled it.
"""
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = sys.argv[sys.argv.index("--manifest") + 1] if "--manifest" in sys.argv else os.path.join(HERE, "oxjob2208-publisher-country-2026-11-13.csv")
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
COUNTS = ("SELECT COUNT(*) FILTER (WHERE country_code IS NULL), "
          "COUNT(*) FILTER (WHERE country_code IS NOT NULL) FROM sources")
NAMES = {code: name for name, code in json.load(open(os.path.join(os.path.dirname(HERE), "data", "iso3166_country_names.json"), encoding="utf-8")).items()}


class DryRun(Exception):
    pass


def fill(conn, receipt):
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    ids = [int(r["source_id"]) for r in rows]
    assert len(set(ids)) == len(ids), "duplicate source ids in the manifest"
    unknown = sorted({r["country_code"] for r in rows if r["country_code"] not in NAMES})
    assert not unknown, f"not ISO 3166-1 alpha-2: {unknown}"
    hosts = [r["host_id"].rsplit("/", 1)[-1] for r in rows]          # P4310313287 or I4210128461
    done = {r.id: r.name_filled for r in conn.execute(text("""
        WITH v AS (SELECT * FROM unnest(CAST(:ids AS bigint[]), CAST(:codes AS text[]), CAST(:names AS text[]),
                                        CAST(:kinds AS text[]), CAST(:hids AS bigint[])) AS v(id, code, name, kind, hid)),
        t AS (SELECT s.id, v.code, v.name, s.country IS NULL AS name_filled
              FROM sources s JOIN v ON v.id = s.id
              WHERE s.country_code IS NULL
                AND ((v.kind = 'P' AND s.publisher_id = v.hid) OR (v.kind = 'I' AND s.institution_id = v.hid))
              FOR UPDATE OF s)
        UPDATE sources s SET country_code = t.code, country = COALESCE(s.country, t.name), updated_date = now()
        FROM t WHERE s.id = t.id
        RETURNING s.id, t.name_filled
    """), {"ids": ids, "codes": [r["country_code"] for r in rows], "names": [NAMES[r["country_code"]] for r in rows],
           "kinds": [h[0] for h in hosts], "hids": [int(h[1:]) for h in hosts]})}
    receipt["changed"] = {r["source_id"]: [None, r["country_code"]] for r in rows if int(r["source_id"]) in done}
    receipt["country_name_filled"] = sorted(sid for sid, named in done.items() if named)
    receipt["skipped"] = {r["source_id"]: "missing, country already set, or host changed"
                          for r in rows if int(r["source_id"]) not in done}
    print(f"filled {len(done)} of {len(rows)}; skipped {len(receipt['skipped'])}", flush=True)
    print("top codes:", Counter(r["country_code"] for r in rows if int(r["source_id"]) in done).most_common(10), flush=True)


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
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT, "manifest": os.path.basename(CSV)}
    try:
        with engine.begin() as conn:
            receipt["before"] = list(conn.execute(text(COUNTS)).fetchone())
            (revert if REVERT else fill)(conn, receipt)
            receipt["after"] = list(conn.execute(text(COUNTS)).fetchone())
            print(f"sources with NULL country_code {receipt['before'][0]} -> {receipt['after'][0]}; "
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
