"""Ticket fee fixes, 3 journals (ZD 5070, 26822/26823, and #2308's 26464). oxjob #188 (diamond vs gold), support factory
#2254, written off desk 2026-11-23 (send lane, wake 188-1123-1200); RUN ON DESK. Same as oxjob188-apc-fill-2026-11-21.py
except: a zero fee is allowed (SFT), and the file names.

- S4210231393 Safety & Fire Technology (ZD 5070): we hold 800 PLN (184 USD); the journal page says "Publication in SFT
  is free of charge" (https://sft.cnbop.pl/en/for-authors, 23 Nov 09:22 CT); the 800 PLN is DOAJ's record for the Polish-language
  title (ISSN 1895-8443; SFT publishes in two language versions), the English title's DOAJ record (2657-8808) says no APC. -> explicit zero
  (apc_usd 0, apc_prices [{price 0, USD}], as SCOAP3's journals). 530 works gold -> diamond (D32 ruling: diamond =
  fully OA, no fee on record).
- S4210226426 Future Cities and Environment (ZD 26822, 26823): we hold GBP 500 (613 USD); the journal's APC page says
  GBP 582.00 (23 Nov 09:24 CT). -> GBP 582, 762 USD. Works stay gold.
- S2738172632 GMS Journal for Medical Education (#2308's ZD 26464): no fee on record; publisso's guidelines say EUR 800
  (EUR 400 for GMA members), DOAJ max 800 EUR. -> EUR 800, 878 USD. 20 diamond works -> gold.
USD at open.er-api's 23 Nov rates (GBP 0.763577, EUR 0.911134 per USD).
Policy choice: 'Upstream is the source of truth' (#1164): a fee the journal states is a fact.

Input: handoff/oxjob188-apc-fill-2026-11-23.csv (copy of #188 work/apc_fill_2026-11-23.csv, 3 rows).
Each row is written only where the source's apc_usd still equals expect_apc_usd_before (empty = NULL, and then
apc_prices must be NULL or []).

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-apc-fill-2026-11-23.py [--execute]
Expect: dry run "fee fill: 3 of 3 written; skipped 0"; a re-run after --execute writes 0 (all skipped).
Revert: per filled row in the receipt: UPDATE sources SET apc_usd = <apc_usd_before>, apc_prices = <apc_prices_before> WHERE id = <id>;
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
CSV = os.path.join(HERE, "oxjob188-apc-fill-2026-11-23.csv")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "filled": [], "skipped": []}

with engine.begin() as conn:
    assert recompute_is_oa(conn) == 0, "is_oa out of step before this batch: stop and look"
    rows = [r for r in csv.DictReader(open(CSV)) if r["new_apc_usd"]]
    for r in rows:
        sid, fee = int(r["source"].lstrip("S")), int(r["new_apc_usd"])
        before = int(r["expect_apc_usd_before"]) if r["expect_apc_usd_before"] else None
        prices = json.loads(r["new_apc_prices"])
        assert 0 <= fee < 10000 and all(set(p) == {"price", "currency"} and isinstance(p["price"], int) for p in prices), r
        old = conn.execute(text("SELECT apc_usd, apc_prices FROM sources WHERE id = :id FOR UPDATE"),
                           {"id": sid}).fetchone()
        guard = ("apc_usd IS NULL AND (apc_prices IS NULL OR apc_prices = '[]'::jsonb)" if before is None
                 else "apc_usd = :before")
        n = conn.execute(text(
            "UPDATE sources SET apc_usd = :u, apc_prices = CAST(:p AS JSONB), updated_date = now() "
            f"WHERE id = :id AND {guard}"),
            {"u": fee, "p": json.dumps(prices), "id": sid, **({} if before is None else {"before": before})}).rowcount
        (receipt["filled"] if n == 1 else receipt["skipped"]).append(
            {"id": sid, "ticket": r["ticket"], "fee": fee, "prices": prices, "page": r["apc_page"],
             "apc_usd_before": old[0] if old else "missing", "apc_prices_before": old[1] if old else "missing"})
        hp = r.get("new_homepage_url") or ""
        if hp:
            h = conn.execute(text("UPDATE sources SET homepage_url = :h, updated_date = now() "
                                  "WHERE id = :id AND homepage_url IS NULL"), {"h": hp, "id": sid}).rowcount
            receipt.setdefault("homepage_filled" if h == 1 else "homepage_skipped", []).append({"id": sid, "homepage_url": hp})
    print(f"fee fill: {len(receipt['filled'])} of {len(rows)} written; skipped {len(receipt['skipped'])}")
    assert recompute_is_oa(conn) == 0, "an APC write must not change is_oa"
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))
    if not EXECUTE:
        raise RuntimeError("dry run: rolled back (pass --execute to write)")

out = os.path.join(HERE, f"oxjob188-apc-fill-receipt-{datetime.now(timezone.utc):%Y%m%dT%H%MZ}.json")
json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False, default=str)
print("receipt written:", out)
