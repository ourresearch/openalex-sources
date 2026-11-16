"""Registry edit for Zendesk ticket 27229 (oxjob #2100, support factory lane, 2026-11-16 CT): two machine-learning
proceedings sources read "not open access" though every paper they publish is free to read, with no fee to authors.

  S4306419637  International Conference on Learning Representations (ICLR)  source_oa_override TRUE, no flip year
  S4363608721  Proceedings of Machine Learning Research (PMLR)              source_oa_override TRUE, no flip year

-> is_oa_high_oa_rate TRUE (all years), is_oa TRUE. No APC (none is charged), so walden colours their works diamond
(ZEROIFNULL(apc_usd) = 0 AND source_is_oa).

Evidence (oxjob #2100 work/oa_pdf_fetch_2026-11-16.txt, fetched from desk with no login on 2026-11-16): ICLR's own
proceedings site proceedings.iclr.cc serves every 2024-2026 paper (2,260 + 3,703 + 5,351) with a free PDF: 6 of 6
sampled PDFs (2024, 2025 x2, 2026 x3, incl. both ticket papers) returned 200 application/pdf. Earlier years are on
openreview.net, which has published every ICLR paper openly since 2013 (forum pages 200; its PDF links answer a bot
challenge to curl, not a paywall). PMLR: the ICML 2025 paper in the ticket (v267 adnan25a) has a free PDF (200, %PDF);
PMLR has been open since it began as JMLR W&CP in 2007. Neither venue charges authors.
Policy choice: follows RULINGS § 3 (#1239: fully-OA claims accepted after we verify them) and the support-factory
charter § 0 rule 9 (the lane checks the pages itself; receipts saved).

Run from this checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Revert: DELETE FROM source_oa_override WHERE source_id IN (4306419637, 4363608721) AND note LIKE 'ZD 27229 #2100%';
UPDATE sources SET is_oa_high_oa_rate = <before>, high_oa_rate_start_year = <before>, is_oa = <before>
WHERE id IN (4306419637, 4363608721);  (= the receipt's `before`)
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.getcwd())  # the openalex-sources checkout
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

EXECUTE = "--execute" in sys.argv
SOURCES = {
    4306419637: ("International Conference on Learning Representations",
                 "ZD 27229 #2100: ICLR papers are all free to read with no author fee (proceedings.iclr.cc 2024-2026, "
                 "openreview.net since 2013); 6/6 sampled PDFs fetched 2026-11-16"),
    4363608721: ("Proceedings of Machine Learning Research",
                 "ZD 27229 #2100: PMLR papers are all free to read with no author fee (proceedings.mlr.press, since "
                 "JMLR W&CP 2007); ticket PDF v267 adnan25a fetched 2026-11-16"),
}
COLS = ("display_name", "type", "publisher", "publisher_id", "is_oa", "is_oa_high_oa_rate", "high_oa_rate_start_year",
        "is_in_doaj", "is_in_scielo", "is_fully_open_in_jstage", "apc_usd", "apc_prices")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "ticket": 27229, "oxjob": 2100,
           "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn, sid):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    return {k: (None if r[k] is None else (r[k] if k == "apc_prices" else str(r[k]))) for k in COLS} if r else None


def main():
    try:
        with engine.begin() as conn:
            for sid, (name, note) in SOURCES.items():
                before = row(conn, sid)
                assert before is not None, f"S{sid} missing"
                ov = [dict(r._mapping) for r in conn.execute(
                    text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": sid})]
                receipt["before"][f"S{sid}"] = {"source": before, "override": ov}
                print(f"S{sid}", before, ov, flush=True)
                assert normalize_name(before["display_name"]) == normalize_name(name), before["display_name"]
                assert before["is_oa"] == "False", f"S{sid} already is_oa {before['is_oa']}: stop and look"
                assert not ov, f"S{sid} override already exists: {ov}"
                assert before["apc_usd"] is None, f"S{sid} has an APC: {before['apc_usd']}"

                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                                  "VALUES (:s, TRUE, NULL, :n)"), {"s": sid, "n": note})
                log(f"INSERT source_oa_override S{sid}: curated_is_oa TRUE, no flip year")
                conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = TRUE, high_oa_rate_start_year = NULL, "
                                  "updated_date = now() WHERE id = :id"), {"id": sid})
                conn.execute(text("""
                    UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                                OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                                       updated_date = now()
                    WHERE id = :id"""), {"id": sid})
                log(f"S{sid}: is_oa_high_oa_rate TRUE (all years), is_oa recomputed")
                after = row(conn, sid)
                receipt["after"][f"S{sid}"] = after
                assert after["is_oa"] == "True" and after["is_oa_high_oa_rate"] == "True", after
                assert after["high_oa_rate_start_year"] is None, after
            print(json.dumps(receipt["after"], indent=1, ensure_ascii=False, default=str), flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
