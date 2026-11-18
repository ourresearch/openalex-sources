"""Registry APC fixes for the source tickets re-pointed to oxjob #188 on 17 Nov (support factory #2254), written off
desk 2026-11-17 19:55 CT; RUN ON DESK (sources-registry rows, lanes_registry route, #1831).

Each value is the journal's own statement (policy: 'Upstream is the source of truth', #1164, as SCIRP on 16 Nov):
  ZD 8610   S2898597390 Revista Colombiana de Ortopedia y Traumatologia: USD 3,000 -> 0. About page: "The journal does
            not charge any fees for the submission, processing, and publication of articles" (receipt #188
            work/receipts/rccot_about_2026-11-17.txt). OA from 2023 (oa_flip_year 2022, unchanged): its 93 gold works
            (2023-2026) turn diamond; pre-2023 Elsevier works are not source-OA and keep their colour.
  ZD 22451  S25010850 Publications mathematiques de l'IHES: Springer's USD 3,590 / EUR 2,890 / GBP 2,490 -> 0. Journal
            site: "Starting in 2026 ... diamond open access, free of charge for both authors (no publication fees) and
            readers"; 1959-2020 issues opened in 2021 under CC BY-NC-ND (receipt work/receipts/pmihes_home_2026-11-17.txt);
            2021-2025 were OA at Springer with no APC (DDH evidence, openalex-sources
            handoff/ddh-oa-start-years-2026-10-23-evidence.csv). OA override flip 1958 (DDH, ruling #1239) unchanged: all
            608 works gold -> diamond.
  ZD 26103  S7407064897 Energy Demand (eceee): none -> EUR 1,200 (eceee member organisations EUR 950; list price kept).
            Submissions page "Regular article fee: EUR1,200" (Wayback 2026-08-31 copy, receipt
            work/receipts/energydemand_submissions_wayback_20260831.txt; the editor's ticket says the same). apc_usd
            1324 = EUR 1,200 x 1.1030 (open.er-api 2026-11-16, receipt work/receipts/open-er-api_usd_2026-11-16.json).
            First volume 2026, so one fee is right for every year: 20 diamond -> gold.
  ZD 24703  S5407050313 Invasive Physiotherapy and Musculoskeletal Medicine (JIPMM): none -> 0 (editor: diamond, APC 0;
            #1899's 17 Nov follow-up re-pointed it to #188). Colour unchanged (null already reads diamond); the record
            shows "no fee" instead of nothing.
Not here: SAJAS S114429563 (ZD 5213; tiered fees in an attachment a person must read), IGI Global (ZD 27072; its own
script, handoff/oxjob188-igi-apc-fill-2026-11-17.py, needs each journal's fee page read on desk first).

Why the APC sets the colour: walden CreateWorksBase calls an OA source's work diamond when ZEROIFNULL(apc_usd) = 0, else
gold. ~1,180 works get a new stamp (RCOT 522, IHES 609, Energy Demand 20, JIPMM 26): far under any guardrail.

Guards: each source must still hold the 'before' values below (else it stops, nothing written; a source already at its
'after' values is reported "already done" and skipped); is_oa recomputes to no change before and after.

Run on desk from the openalex-sources checkout (copy this file to handoff/oxjob188-apc-tickets-2026-11-17.py):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-apc-tickets-2026-11-17.py [--execute]
Dry run by default (ONE transaction, rolled back); prints the RECEIPT; --execute writes
handoff/oxjob188-apc-tickets-receipt-<UTC>.json.
Revert: per receipt 'before' row: UPDATE sources SET apc_usd = <before.apc_usd>, apc_prices = <before.apc_prices>
WHERE id = <id>.
Gotcha: jobs.butler_apc (manual reruns only) writes apc_usd/apc_prices for journals its dataset saw recently; a rerun
could put Springer's IHES price back. Its recency guard should skip IHES once Springer's rows stop at 2025.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import recompute_is_oa  # noqa: E402

EXECUTE = "--execute" in sys.argv


def usd(p):
    return [{"price": p, "currency": "USD"}]


NONE = "none"  # apc_prices NULL or [] (both mean none)
# id: (expected before, after, note)
FIXES = {
    2898597390: (dict(apc_usd=3000, apc_prices=usd(3000)),
                 dict(apc_usd=0, apc_prices=usd(0)),
                 "ZD 8610 RCOT: 'does not charge any fees' (revistasccot.org/index.php/rccot/about)"),
    25010850: (dict(apc_usd=3590, apc_prices=[{"price": 3590, "currency": "USD"}, {"price": 2890, "currency": "EUR"},
                                              {"price": 2490, "currency": "GBP"}]),
               dict(apc_usd=0, apc_prices=usd(0)),
               "ZD 22451 IHES: diamond, 'no publication fees' (pmihes.centre-mersenne.org)"),
    7407064897: (dict(apc_usd=None, apc_prices=NONE),
                 dict(apc_usd=1324, apc_prices=[{"price": 1200, "currency": "EUR"}]),
                 "ZD 26103 Energy Demand: regular article fee EUR 1,200 (energydemand.org/about/submissions)"),
    5407050313: (dict(apc_usd=None, apc_prices=NONE),
                 dict(apc_usd=0, apc_prices=usd(0)),
                 "ZD 24703 JIPMM: editor states diamond, APC 0"),
}
COLS = ["id", "display_name", "apc_usd", "apc_prices", "is_oa", "is_in_doaj", "publisher_id", "institution_id"]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "before": {}, "after": {},
           "actions": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def norm(k, v):
    if k == "apc_prices":
        if v is None or v == [] or v == NONE:
            return NONE
        v = json.loads(v) if isinstance(v, str) else v
        return sorted((int(p["price"]), p["currency"].upper()) for p in v)
    return None if v is None else int(v)


def same(k, a, b):
    return norm(k, a) == norm(k, b)


def row(conn, sid):
    r = conn.execute(text(f"SELECT {', '.join(COLS)} FROM sources WHERE id = :id FOR UPDATE"),
                     {"id": sid}).mappings().fetchone()
    assert r is not None, f"S{sid} not in sources (merged or deleted): stop and look"
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}


with engine.begin() as conn:
    stray = recompute_is_oa(conn)
    assert stray == 0, f"is_oa out of step on {stray} sources before this batch: stop and look"
    written = 0
    for sid, (expect, new, note) in FIXES.items():
        r = row(conn, sid)
        receipt["before"][str(sid)] = r
        if all(same(k, r[k], v) for k, v in new.items()):
            log(f"S{sid}: already done ({note})")
            continue
        drift = {k: (r[k], v) for k, v in expect.items() if not same(k, r[k], v)}
        assert not drift, f"S{sid} drifted from what this script expects: {drift}; nothing written"
        assert r["is_oa"] is True, (sid, "not OA: the APC would not change its colour; stop and look")
        n = conn.execute(text("UPDATE sources SET apc_usd = :u, apc_prices = CAST(:p AS JSONB), updated_date = now() "
                              "WHERE id = :id"),
                         {"u": new["apc_usd"], "p": json.dumps(new["apc_prices"]), "id": sid}).rowcount
        assert n == 1, (sid, n)
        written += 1
        log(f"UPDATE sources {sid}: apc_usd {r['apc_usd']} -> {new['apc_usd']}, apc_prices {r['apc_prices']} -> "
            f"{new['apc_prices']}  ({note})")
    changed = recompute_is_oa(conn)
    assert changed == 0, f"an APC write must not change is_oa (changed {changed})"
    for sid in FIXES:
        receipt["after"][str(sid)] = row(conn, sid)
    log(f"written {written} of {len(FIXES)}; is_oa unchanged")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))
    if not EXECUTE:
        raise RuntimeError("dry run: rolled back (pass --execute to write)")

out = os.path.join(HERE, f"oxjob188-apc-tickets-receipt-{datetime.now(timezone.utc):%Y%m%dT%H%MZ}.json")
json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False, default=str)
print("receipt written:", out)
