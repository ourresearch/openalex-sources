"""Ticket fee fill: Lyra Science's third journal (ZD 26817), Lyra Journal of Biomedical and Health Sciences.
oxjob #188 (diamond vs gold, null APC), support factory #2254, written off desk 2026-11-21; RUN ON DESK.

- S7407122396 LJBHS (Lyra Science, Barcelona; eISSN 3138-0867): no fee on record. The journal page
  (https://lyrascience.org/ljbhs, read 21 Nov 06:37 CT) says "Standard APC EUR 200 in every journal", charged on
  acceptance, CC BY 4.0; the managing editor asked for it on ZD 26817 (20 Nov 21:32 UTC). EUR 200 -> USD 221 at
  open.er-api's 16 Nov rate, exactly as its sister journals LCSAI and JSPE on 20 Nov (openalex-sources 8d6b9ad).
- Homepage lyrascience.org/ljbhs, written only where homepage_url is NULL.
- is_oa: false today; the curation cur-8HuHz4PQGbLB (is_oa true, posted 21 Nov 06:37 CT) flips it through
  apply_source_curations (daily 02:00 UTC), not this script. Without the fee its work would read diamond once is_oa
  flips; with both it reads gold. The publisher record (Lyra Science) is #1223/#1239's (ruling D12), not here.
Policy choice: follows 'Upstream is the source of truth' (#1164): a fee the publisher states is a fact.

Input: handoff/oxjob188-apc-fill-2026-11-21.csv (copy of #188 work/apc_fill_2026-11-21.csv, 1 row).
Each row is written only where the source's apc_usd still equals expect_apc_usd_before (empty = NULL, and then
apc_prices must be NULL or []).

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-apc-fill-2026-11-21.py [--execute]
Expect: dry run "fee fill: 1 of 1 written; skipped 0" and homepage_filled 1 in the RECEIPT; a re-run after --execute writes 0 (all skipped).
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
CSV = os.path.join(HERE, "oxjob188-apc-fill-2026-11-21.csv")
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
