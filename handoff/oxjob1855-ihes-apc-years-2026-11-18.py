"""Registry fix for ZD 27823 (oxjob #1855, support factory #2254): Publications mathematiques de l'IHES S25010850 keeps
Springer's 2022-2025 list prices in its per-year APC series, though authors paid no APC from 2021 on. Written off desk
2026-11-18 09:12 CT; RUN ON DESK (sources-registry rows, lanes_registry route, #1831).

What and why (each value is the journal's own statement; policy 'Upstream is the source of truth', #1164, as #188's
IHES write of 17 Nov):
  S25010850 apc_usd_by_year   {"2022": 3385, "2023": 3290, "2024": 3390, "2025": 3590}
                           -> {"2022": 0,    "2023": 0,    "2024": 0,    "2025": 0}
            apc_prices_by_year  each of those years [USD, EUR, GBP Springer list prices] -> [{"price": 0, "currency": "USD"}]
            (butler_apc's own convention: "Exact-zero years use a single USD 0 entry: zero means no fee").
  Evidence (receipts in oxjobs #1855 work/ticket_27823_recheck.md, fetched 18 Nov 2026):
    - https://link.springer.com/journal/10240 : "Article processing charges are not the responsibility of any individual
      researcher." "Became a fully open access journal since the beginning of 2021."
    - https://pmihes.centre-mersenne.org/page/about/ : "It became a fully open access journal in the beginning of 2021";
      "Fees : For authors, there are no charges for submission, processing or publication of papers in this journal."
  The Butler dataset (jobs/butler_apc.py, #571) recorded Springer's nominal list price, which IHES paid; the authors
  did not. apc_usd / apc_prices are already 0 (#188, 17 Nov 19:50 CT) and are not touched here.

Effect (walden CreateWorksBase apc_list takes the exact-year hit from apc_usd_by_year): the 31 IHES works of 2022-2025
go from apc_list 3,290-3,590 USD to apc_list 0 USD, like the journal's other 578 works. oa_status is unchanged (all 609
read diamond from night 2026-11-18 on, from #188's apc_usd 0); is_oa unchanged. 31 works re-stamped: far under any
guardrail. /sources/S25010850 apc_usd_by_year shows the zeros after the next Sync Sources.

Guards: the source must still hold the 'before' values below (else it stops, nothing written; already at 'after' is
reported "already done"); apc_usd must be 0 (else #188's write was undone: stop and look); is_oa recomputes to no change.

Run on desk from the openalex-sources checkout (copy this file to handoff/oxjob1855-ihes-apc-years-2026-11-18.py):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1855-ihes-apc-years-2026-11-18.py [--execute]
Dry run by default (ONE transaction, rolled back); prints the RECEIPT; --execute writes
handoff/oxjob1855-ihes-apc-years-receipt-<UTC>.json.
Timing: before ~22:30 CT, or after that night's End 2 End publishes (the 05:00 UTC run reads the registry).
Revert: UPDATE sources SET apc_usd_by_year = '<receipt before.apc_usd_by_year>'::jsonb,
        apc_prices_by_year = '<receipt before.apc_prices_by_year>'::jsonb WHERE id = 25010850;
Gotcha: jobs.butler_apc (manual reruns only) rewrites apc_usd_by_year / apc_prices_by_year for every journal its
dataset covers, and (Springer's rows end 2025, inside its recency window) apc_usd / apc_prices too: a rerun puts
Springer's IHES prices back. Re-apply this script and #188's after any butler_apc rerun until #1855 ships a guard.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

SID = 25010850
NOTE = ("ZD 27823 IHES: no author APC from 2021 (link.springer.com/journal/10240: 'Article processing charges are not "
        "the responsibility of any individual researcher'; pmihes.centre-mersenne.org/page/about: 'no charges')")
YEARS = ["2022", "2023", "2024", "2025"]


def prices(*triples):
    return [{"price": p, "currency": c} for p, c in triples]


BEFORE = {
    "apc_usd_by_year": {"2022": 3385, "2023": 3290, "2024": 3390, "2025": 3590},
    "apc_prices_by_year": {
        "2022": prices((3385, "USD"), (2750, "EUR"), (2310, "GBP")),
        "2023": prices((3290, "USD"), (2690, "EUR"), (2290, "GBP")),
        "2024": prices((3390, "USD"), (2790, "EUR"), (2390, "GBP")),
        "2025": prices((3590, "USD"), (2890, "EUR"), (2490, "GBP")),
    },
}
AFTER = {
    "apc_usd_by_year": {y: 0 for y in YEARS},
    "apc_prices_by_year": {y: prices((0, "USD")) for y in YEARS},
}
COLS = ["id", "display_name", "apc_usd", "apc_prices", "apc_usd_by_year", "apc_prices_by_year", "is_oa", "is_in_doaj",
        "publisher_id"]


def _load(v):
    return json.loads(v) if isinstance(v, str) else v


def norm(k, v):
    """Comparable form of one column value (JSONB arrives as dict/list, or str through some drivers)."""
    v = _load(v)
    if v is None:
        return None
    if k == "apc_usd_by_year":
        return sorted((str(y), int(round(float(p)))) for y, p in v.items())
    if k == "apc_prices_by_year":
        return sorted((str(y), sorted((int(round(float(p["price"]))), p["currency"].upper()) for p in ps))
                      for y, ps in v.items())
    if k == "apc_prices":
        return sorted((int(round(float(p["price"]))), p["currency"].upper()) for p in v) or None
    return v


def same(k, a, b):
    return norm(k, a) == norm(k, b)


def decide(r):
    """'done' | 'write'; raises AssertionError (nothing written) on drift or a state this script does not expect."""
    assert r is not None, f"S{SID} not in sources (merged or deleted): stop and look"
    assert r["is_oa"] is True, f"S{SID} is not OA: stop and look"
    assert r["apc_usd"] is not None and int(r["apc_usd"]) == 0 and same("apc_prices", r["apc_prices"], prices((0, "USD"))), \
        f"S{SID} apc_usd/apc_prices are no longer #188's 0 (apc_usd={r['apc_usd']}, apc_prices={r['apc_prices']}): " \
        "a butler_apc rerun? stop and look"
    if all(same(k, r[k], v) for k, v in AFTER.items()):
        return "done"
    drift = {k: (r[k], v) for k, v in BEFORE.items() if not same(k, r[k], v)}
    assert not drift, f"S{SID} drifted from what this script expects: {drift}; nothing written"
    return "write"


def main():
    from sqlalchemy import text
    from db import engine
    from sources_lib import recompute_is_oa

    execute = "--execute" in sys.argv
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "job": "oxjob #1855, ZD 27823",
               "before": None, "after": None, "actions": []}

    def log(msg):
        print(msg, flush=True)
        receipt["actions"].append(msg)

    def row(conn):
        r = conn.execute(text(f"SELECT {', '.join(COLS)} FROM sources WHERE id = :id FOR UPDATE"),
                         {"id": SID}).mappings().fetchone()
        return None if r is None else {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}

    with engine.begin() as conn:
        stray = recompute_is_oa(conn)
        assert stray == 0, f"is_oa out of step on {stray} sources before this batch: stop and look"
        r = row(conn)
        receipt["before"] = r
        what = decide(r)
        if what == "done":
            log(f"S{SID}: already done ({NOTE})")
        else:
            n = conn.execute(text("UPDATE sources SET apc_usd_by_year = CAST(:u AS JSONB), "
                                  "apc_prices_by_year = CAST(:p AS JSONB), updated_date = now() WHERE id = :id"),
                             {"u": json.dumps(AFTER["apc_usd_by_year"]), "p": json.dumps(AFTER["apc_prices_by_year"]),
                              "id": SID}).rowcount
            assert n == 1, n
            log(f"UPDATE sources {SID}: apc_usd_by_year {json.dumps(_load(r['apc_usd_by_year']))} -> "
                f"{json.dumps(AFTER['apc_usd_by_year'])}; apc_prices_by_year 2022-2025 Springer USD/EUR/GBP -> "
                f"single USD 0 entries  ({NOTE})")
        changed = recompute_is_oa(conn)
        assert changed == 0, f"an APC write must not change is_oa (changed {changed})"
        receipt["after"] = row(conn)
        assert decide(receipt["after"]) == "done", "after-state check failed"
        log("written 1 of 1; is_oa unchanged" if what == "write" else "written 0 of 1 (already done); is_oa unchanged")
        print("RECEIPT")
        print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))
        if not execute:
            raise RuntimeError("dry run: rolled back (pass --execute to write)")

    out = os.path.join(HERE, f"oxjob1855-ihes-apc-years-receipt-{datetime.now(timezone.utc):%Y%m%dT%H%MZ}.json")
    json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False, default=str)
    print("receipt written:", out)


if __name__ == "__main__":
    main()
