"""IGI Global open-access journals, batch 2: fill the article processing charge (APC) for the 41 journals batch 1 left.
oxjob #188 (diamond vs gold, null APC), support factory #2254, written off desk 2026-11-19; RUN ON DESK.

Batch 1 (openalex-sources 0675c78, 18 Nov, live 19 Nov) filled 20 of IGI Global's 61 fully-OA journals; their works
went diamond 3,351 -> 0. These are the other 41: all apc_usd NULL, is_oa true, host P4310320424; 3,187 post-flip works
read diamond (live 12:52 CT 19 Nov, receipt #188 work/receipts/igi41_before_live_2026-11-19.json). Each fee is
"APC: US $X" on the journal's own IGI page as the Wayback Machine saved it (mostly 18 Nov 2025; IJLIS Apr 2025, IJER
Mar 2024, JTA Feb 2024, IJDSGBT Oct 2024), US$1,050-2,750, URL in the CSV's apc_page; receipt #188
work/receipts/igi_apc_wayback_2026-11-19.jsonl. IGI title ids came from IGI's distributor title list (Wayback,
24 Jan 2026; IJLIS and IJER from the 23 Feb 2024 list). At the flip year the pages say "converting from Hybrid Open
Access to full Gold Open Access on January 1, 2022" with an APC (US$1,000-2,000 then): no waiver years.
Policy choice: follows 'Upstream is the source of truth' (#1164): a fee the publisher states is a fact, so these
journals are gold under today's rule (as SCIRP 16 Nov and IGI batch 1 18 Nov).

Input: handoff/oxjob188-igi-apc-2026-11-19.csv (copy of #188 work/igi_apc_fill_2026-11-19.csv, 41 rows).
Written only where the source still has no fee (apc_usd NULL, apc_prices NULL or []).

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-igi-apc-fill-2026-11-19.py [--execute]
Expect: dry run "IGI fill: 41 of 41 written; skipped 0"; a re-run after --execute writes 0 (all skipped).
Revert: per igi_filled row in the receipt: UPDATE sources SET apc_usd = NULL, apc_prices = <apc_prices_before> WHERE id = <id>.
"""
import csv
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
CSV = os.path.join(HERE, "oxjob188-igi-apc-2026-11-19.csv")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "igi_filled": [], "igi_skipped": []}

with engine.begin() as conn:
    assert recompute_is_oa(conn) == 0, "is_oa out of step before this batch: stop and look"
    rows = [r for r in csv.DictReader(open(CSV)) if r["new_apc_usd"]]
    for r in rows:
        sid, fee = int(r["source"].lstrip("S")), int(r["new_apc_usd"])
        prices = json.loads(r["new_apc_prices"]) if r["new_apc_prices"] else [{"price": fee, "currency": "USD"}]
        assert 0 < fee < 10000 and all(set(p) == {"price", "currency"} and isinstance(p["price"], int) for p in prices), r
        old = conn.execute(text("SELECT apc_prices, publisher_id FROM sources WHERE id = :id FOR UPDATE"),
                           {"id": sid}).fetchone()
        n = conn.execute(text(
            "UPDATE sources SET apc_usd = :u, apc_prices = CAST(:p AS JSONB), updated_date = now() "
            "WHERE id = :id AND apc_usd IS NULL AND (apc_prices IS NULL OR apc_prices = '[]'::jsonb)"),
            {"u": fee, "p": json.dumps(prices), "id": sid}).rowcount
        (receipt["igi_filled"] if n == 1 else receipt["igi_skipped"]).append(
            {"id": sid, "fee": fee, "prices": prices, "page": r["apc_page"],
             "apc_prices_before": old[0] if old else "missing", "publisher_id": old[1] if old else None})
    print(f"IGI fill: {len(receipt['igi_filled'])} of {len(rows)} written; skipped {len(receipt['igi_skipped'])}")
    assert recompute_is_oa(conn) == 0, "an APC write must not change is_oa"
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))
    if not EXECUTE:
        raise RuntimeError("dry run: rolled back (pass --execute to write)")

out = os.path.join(HERE, f"oxjob188-igi-apc-receipt-{datetime.now(timezone.utc):%Y%m%dT%H%MZ}.json")
json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False, default=str)
print("receipt written:", out)
