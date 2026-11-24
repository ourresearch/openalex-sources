"""Country for journals their publisher owns outright, where the registry carries another country (oxjob #2576,
2026-11-24). 23 IET journals coded US (Electronics Letters, IET Generation Transmission & Distribution, IET
Communications ...) become GB, and MDPI's Molecules, coded DE, becomes CH.

Why: a source's country is its home country, typically its publisher's (help.openalex.org, sources country_code).
The IET (London / Stevenage) owns these 23 journals outright and Wiley's UK company publishes them; MDPI (Basel) has
owned Molecules since its first issue in 1996. The US and DE values are legacy MAG-import values (created 2016-06-24),
so works filtered by source country (primary_location.source.country_code) put ~161K of their works in the wrong
country. Two witnesses per journal, every row checked (no sample): the journal's own site (for the IET journals the
copyright line names the IET alone; Molecules' History page shows no founding society and no transfer) and DOAJ
(publisher country GB / CH, and no other owning organisation besides the IET). Held, not in the manifest: six IET
journals co-owned with a Chinese partner (CAAI Transactions on Intelligence Technology and five more; which country is
right for a co-owned or society journal is a policy question), and MDPI titles another body founded or owns (Medicina,
Current Oncology, JRFM, MCA, SIUJ, Cardiogenetics). Evidence: oxjob #2576 EXPLORE.md.

handoff/oxjob2576-owner-country-2026-11-24.csv lists per source the old and the new country_code and the host it was
checked under. `country` (the name) is set to the new country's name.
Works don't embed a source's country (the API joins it at query time): 0 works re-stamped.

Guard per row, re-checked inside the UPDATE: country_code still equals the old code and the source still has the
host the manifest was built from.

    python handoff/oxjob2576-owner-country-2026-11-24.py              # dry run, rolled back
    python handoff/oxjob2576-owner-country-2026-11-24.py --execute    # write; receipt -> handoff/oxjob2576-owner-country-2026-11-24-receipt.json
    python handoff/oxjob2576-owner-country-2026-11-24.py --revert handoff/oxjob2576-owner-country-2026-11-24-receipt.json [--execute]

--execute refuses unless all 24 rows change (US->GB 23, DE->CH 1): any skip rolls the whole transaction back. The
receipt is written to disk inside the transaction, before the commit, and marked committed after it; --execute refuses
to overwrite an existing receipt. Revert accepts only a committed forward receipt, and puts a row back to its old
country_code and old `country` name only where it still carries exactly what this run wrote (code, name, host); a
later edit is kept. A revert writes its own receipt (-revert-receipt.json).
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
CSV = os.path.join(HERE, "oxjob2576-owner-country-2026-11-24.csv")
RECEIPT = os.path.join(HERE, "oxjob2576-owner-country-2026-11-24-receipt.json")
EXPECTED = {"US->GB": 23, "DE->CH": 1}
EXECUTE = "--execute" in sys.argv
REVERT = None
if "--revert" in sys.argv:
    i = sys.argv.index("--revert")
    if i + 1 >= len(sys.argv) or sys.argv[i + 1].startswith("--"):
        sys.exit("--revert needs the receipt path")
    REVERT = sys.argv[i + 1]
_ISO = json.load(open(os.path.join(os.path.dirname(HERE), "data", "iso3166_country_names.json"), encoding="utf-8"))
NAMES = {code: name for name, code in _ISO.items()}
if len(NAMES) != len(_ISO):
    sys.exit("iso3166_country_names.json is not one name per code")
COUNTS = "SELECT country_code, COUNT(*) FROM sources WHERE country_code IN ('US', 'DE', 'GB', 'CH') GROUP BY 1 ORDER BY 1"


class DryRun(Exception):
    pass


def change(conn, receipt):
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    ids = [int(r["source_id"]) for r in rows]
    if len(set(ids)) != len(ids):
        raise SystemExit("duplicate source ids in the manifest")
    if not all(r["country_code"] in NAMES and r["old_country_code"] in NAMES for r in rows):
        raise SystemExit("not ISO 3166-1 alpha-2")
    if dict(Counter(f"{r['old_country_code']}->{r['country_code']}" for r in rows)) != EXPECTED:
        raise SystemExit(f"manifest is not the reviewed one: expected {EXPECTED}")
    done = {r.id: (r.old_name,) for r in conn.execute(text("""
        WITH v AS (SELECT * FROM unnest(CAST(:ids AS bigint[]), CAST(:olds AS text[]), CAST(:codes AS text[]),
                                        CAST(:names AS text[]), CAST(:hids AS bigint[])) AS v(id, old, code, name, hid)),
        t AS (SELECT s.id, v.code, v.name, s.country AS old_name
              FROM sources s JOIN v ON v.id = s.id
              WHERE s.country_code = v.old AND s.publisher_id = v.hid
              FOR UPDATE OF s)
        UPDATE sources s SET country_code = t.code, country = t.name, updated_date = now()
        FROM t WHERE s.id = t.id
        RETURNING s.id, t.old_name
    """), {"ids": ids, "olds": [r["old_country_code"] for r in rows], "codes": [r["country_code"] for r in rows],
           "names": [NAMES[r["country_code"]] for r in rows], "hids": [int(r["host_id"].lstrip("P")) for r in rows]})}
    # source id -> [old code, new code, old name, new name, host]
    receipt["changed"] = {r["source_id"]: [r["old_country_code"], r["country_code"], done[int(r["source_id"])][0],
                                           NAMES[r["country_code"]], r["host_id"]]
                          for r in rows if int(r["source_id"]) in done}
    receipt["skipped"] = {r["source_id"]: "missing, country no longer the old code, or host changed"
                          for r in rows if int(r["source_id"]) not in done}
    print(f"changed {len(done)} of {len(rows)}; skipped {len(receipt['skipped'])}", flush=True)
    print("moves:", Counter(f"{r['old_country_code']}->{r['country_code']}" for r in rows if int(r["source_id"]) in done).most_common(),
          "names written:", sorted({NAMES[r["country_code"]] for r in rows}), flush=True)
    if receipt["skipped"]:
        raise SystemExit(f"refusing a partial change: {receipt['skipped']}")


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    if not (prior.get("execute") is True and prior.get("committed") is True and not prior.get("revert_of")):
        raise SystemExit("not a committed forward receipt")
    ch = {int(sid): v for sid, v in prior["changed"].items()}  # id -> [old code, new code, old name, new name, host]
    done = {r.id for r in conn.execute(text("""
        UPDATE sources s SET country_code = v.old, country = v.old_name, updated_date = now()
        FROM unnest(CAST(:ids AS bigint[]), CAST(:olds AS text[]), CAST(:news AS text[]), CAST(:old_names AS text[]),
                    CAST(:new_names AS text[]), CAST(:hids AS bigint[]))
             AS v(id, old, new, old_name, new_name, hid)
        WHERE s.id = v.id AND s.country_code = v.new AND s.country IS NOT DISTINCT FROM v.new_name
          AND s.publisher_id = v.hid
        RETURNING s.id
    """), {"ids": list(ch), "olds": [v[0] for v in ch.values()], "news": [v[1] for v in ch.values()],
           "old_names": [v[2] for v in ch.values()], "new_names": [v[3] for v in ch.values()],
           "hids": [int(v[4].lstrip("P")) for v in ch.values()]})}
    receipt["reverted_ids"] = sorted(done)
    receipt["skipped"] = {str(sid): f"no longer {v[1]}" for sid, v in ch.items() if sid not in done}
    receipt["reverted"] = len(done)
    print(f"reverted {len(done)} of {len(ch)}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def save(path, receipt):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(receipt, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "committed": False,
               "revert_of": REVERT, "manifest": os.path.basename(CSV)}
    path = RECEIPT.replace("-receipt.json", "-revert-receipt.json") if REVERT else RECEIPT
    if EXECUTE and os.path.exists(path):
        raise SystemExit(f"{path} exists: refusing to overwrite a receipt")
    try:
        with engine.begin() as conn:
            receipt["before"] = dict(conn.execute(text(COUNTS)).fetchall())
            (revert if REVERT else change)(conn, receipt)
            receipt["after"] = dict(conn.execute(text(COUNTS)).fetchall())
            print(f"sources by code before {receipt['before']} after {receipt['after']}", flush=True)
            if not EXECUTE:
                raise DryRun()
            save(path, receipt)                      # on disk before the commit
        receipt["committed"] = True
        save(path, receipt)
        print(f"COMMITTED; receipt {path}")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
