"""Ticket fee fills: Lyra Science's two journals (ZD 26817) and SAJAS (ZD 5213).
oxjob #188 (diamond vs gold, null APC), support factory #2254, written off desk 2026-11-20; RUN ON DESK.

- S7407089804 LCSAI and S7407089770 JSPE (Lyra Science, Barcelona): is_oa true since 19 Nov, no fee on record, so their
  works read diamond. The publisher's fee page (https://lyrascience.org/apc, read 20 Nov 10:50 CT) says "Standard APC
  EUR 200", charged on acceptance; the managing editor asked for gold on 19 Nov. EUR 200 -> USD 221 at open.er-api's
  16 Nov rate (#188 work/receipts/open-er-api_usd_2026-11-16.json), as Energy Demand's EUR 1,200 -> 1324 on 17 Nov.
- S114429563 SAJAS: fee USD 500 / ZAR 6,000 -> USD 750 / ZAR 10,000 (non-member rates in the editor's document,
  ZD 5213, as our 19 Nov reply quoted them). Gold before and after: no oa_status change. Confirm in the .docx first.
- Lyra homepages from the editor (ZD 26817, 19 Nov): lyrascience.org/lcsai and /jspe, written only where homepage_url
  is NULL (new_homepage_url column). The publisher record (Lyra Science) is #1223/#1239's (ruling D12), not here.
Policy choice: follows 'Upstream is the source of truth' (#1164): a fee the publisher states is a fact.

Input: handoff/oxjob188-apc-fill-2026-11-20.csv (copy of #188 work/apc_fill_2026-11-20.csv, 3 rows).
Each row is written only where the source's apc_usd still equals expect_apc_usd_before (empty = NULL, and then
apc_prices must be NULL or []).

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-apc-fill-2026-11-20.py [--execute]
Expect: dry run "fee fill: 3 of 3 written; skipped 0" and homepage_filled 2 in the RECEIPT; a re-run after --execute writes 0 (all skipped).
Revert: per filled row in the receipt: UPDATE sources SET apc_usd = <apc_usd_before>, apc_prices = <apc_prices_before> WHERE id = <id>;
per homepage_filled row: UPDATE sources SET homepage_url = NULL WHERE id = <id>.
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
CSV = os.path.join(HERE, "oxjob188-apc-fill-2026-11-20.csv")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "filled": [], "skipped": []}

with engine.begin() as conn:
    assert recompute_is_oa(conn) == 0, "is_oa out of step before this batch: stop and look"
    rows = [r for r in csv.DictReader(open(CSV)) if r["new_apc_usd"]]
    for r in rows:
        sid, fee = int(r["source"].lstrip("S")), int(r["new_apc_usd"])
        before = int(r["expect_apc_usd_before"]) if r["expect_apc_usd_before"] else None
        prices = json.loads(r["new_apc_prices"])
        assert 0 < fee < 10000 and all(set(p) == {"price", "currency"} and isinstance(p["price"], int) for p in prices), r
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
