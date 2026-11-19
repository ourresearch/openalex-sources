"""IGI Global open-access journals: fill the article processing charge (APC) where the registry has none.
oxjob #188 (diamond vs gold, null APC), support factory #2254, written off desk 2026-11-17; RUN ON DESK.

Why: walden CreateWorksBase calls an OA source's work diamond when ZEROIFNULL(apc_usd) = 0, else gold. IGI Global
(P4310320424) has 61 fully-OA journals (flipped 2020-2022, oa_flip_year set), all with apc_usd NULL, so their 6,536
post-flip works read diamond (live 2026-11-17). IGI charges authors: its open-access page says "If Accepted, an OA
Processing Charge Will Be Requested" (receipt #188 work/receipts/igi_open_access_page_2026-11-17.html.txt), and
OpenAPC records institutions paying IGI APCs in its full-OA journals every year 2021-2025, e.g. EUR 1,379 in 2024 for
International Journal of Online Pedagogy and Course Design (ISSN 2155-6873, ZD 27072, premium; receipt
work/receipts/openapc_igi_global_2026-11-17.json). Policy choice: follows 'Upstream is the source of truth' (#1164):
a fee the publisher states is a fact, so those journals are gold under today's rule (as SCIRP, 16 Nov).

Pre-flip works are not source-OA (oa_flip_year), so one current fee changes only the post-flip works' colour. Before
running, check on the publisher's site that the journals charged from their flip year (no blanket waiver years);
if a journal waived its APC for its first years, leave it blank here and note it.

Input: handoff/oxjob188-igi-apc-2026-11-17.csv (copy of #188 work/igi_apc_fill_2026-11-17.csv) with new_apc_usd,
new_apc_prices ([{"price": int, "currency": "USD"}]) and apc_page filled from each journal's own IGI page (bot-walled
off desk; on desk use agent-browser --headed as LANE.md says). Only rows with new_apc_usd are written.
Written only where the source still has no fee (apc_usd NULL, apc_prices NULL or []).

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-igi-apc-fill-2026-11-17.py [--execute]
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
CSV = os.path.join(HERE, "oxjob188-igi-apc-2026-11-17.csv")
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
