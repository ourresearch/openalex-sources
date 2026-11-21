"""Publisher fee fill: Science Publishing Group (SciencePG, P4310319272), 319 journals from its one price list.
oxjob #188 (diamond vs gold, null APC), support factory #2254, written off desk 2026-11-21; RUN ON DESK.

- SciencePG publishes one APC price list for all 337 of its journals
  (https://www.sciencepublishinggroup.com/information/article-processing-charges, read 21 Nov 06:42 CT: 337 titles,
  370-1,070 USD; identical to the list Casey's paper trace #2058 saved at 01:54 CT, 0 diffs). Receipt: #188
  work/receipts/sciencepg_apc_list_2026-11-21T0642.json.
- None of SciencePG's 333 OpenAlex sources carries a fee, so the works of its OA journals read diamond: 37,777 diamond
  works on the 265 matched OA journals, live 21 Nov 06:43 CT (receipt work/receipts/sciencepg319_before_live_2026-11-21T0643.json).
- 319 sources match a price-list title by name (all hosted by P4310319272, no duplicate names; 296 exactly, 23 differ
  only in commas, case or a trailing period). 265 are is_oa true (their diamond works go gold), 54 are is_oa false
  (the fee is a fact either way; their works stay hybrid until their OA flag is fixed, which is not this script).
- Fee in USD as listed: apc_usd = price, apc_prices [{price, USD}]. Per-article waivers (Author Support Program) don't
  change the list price, as for IGI and SCIRP. No homepages here.
Policy choice: follows 'Upstream is the source of truth' (#1164): a fee the publisher states is a fact.

Input: handoff/oxjob188-sciencepg-apc-fill-2026-11-21.csv (copy of #188 work/sciencepg_apc_fill_2026-11-21.csv, 319 rows).
Each row is written only where the source's apc_usd is still NULL and apc_prices NULL or [].

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-sciencepg-apc-fill-2026-11-21.py [--execute]
Expect: dry run "fee fill: 319 of 319 written; skipped 0" in the RECEIPT, is_oa recompute 0; a re-run after --execute writes 0 (all skipped).
Revert: per filled row in the receipt: UPDATE sources SET apc_usd = NULL, apc_prices = <apc_prices_before> WHERE id = <id>.
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
CSV = os.path.join(HERE, "oxjob188-sciencepg-apc-fill-2026-11-21.csv")
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

out = os.path.join(HERE, f"oxjob188-sciencepg-apc-receipt-{datetime.now(timezone.utc):%Y%m%dT%H%MZ}.json")
json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False, default=str)
print("receipt written:", out)
