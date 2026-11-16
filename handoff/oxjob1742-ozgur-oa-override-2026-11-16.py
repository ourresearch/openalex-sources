"""Registry edit for Zendesk ticket 26253 (oxjob #1742, support factory off-desk lane, 2026-11-16 CT):
Özgür Yayınları eBooks (S4306477381, ebook platform, 7,673 works) reads hybrid though the press is fully open access
from 2022 and charges authors a publication fee (so gold, not diamond).

  S4306477381  source_oa_override curated_is_oa = TRUE, curated_flip_year = 2021  -> is_oa_high_oa_rate TRUE from 2022
               apc_prices [{"price": 2400, "currency": "TRY"}], apc_usd 48   (chapter fee; e-book TRY 7,500 ~ USD 149)

Evidence (oxjob #1742 work/oa_verification_2026-11-16.md): the press's home page "Open access 100% ... All works free
under a CC BY 4.0 license"; policies page "Our Gold Open Access model"; fees page: e-book TRY 7,500, chapter TRY 2,400
(paid by author/editor/funder after acceptance); Crossref: 7,645 of 7,670 prefix-10.58830 DOIs carry CC BY 4.0 with the
licence start = publication date; 40/40 random DOI pages + the ticket's 6 say CC BY 4.0 with a free PDF; Wayback
2023-03-08: the Aug 2022 book pub10 free to download under CC BY-NC 4.0 (OA from 2022). The 19 pre-2022 works on the
source are 1979-2007 MAG print records with no DOI: the flip year keeps them closed.
Policy choice: follows RULINGS § 3 (#1239): editors' fully-OA claims are accepted after we verify them, keeping the OA-from date.
Without an APC walden would make these works diamond (ZEROIFNULL(apc_usd) = 0 AND source_is_oa -> 1); the fee makes them gold.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Revert: DELETE FROM source_oa_override WHERE source_id = 4306477381 AND note LIKE 'ZD 26253 #1742%';
UPDATE sources SET apc_prices = NULL, apc_usd = NULL, is_oa_high_oa_rate = FALSE, high_oa_rate_start_year = NULL,
is_oa = FALSE WHERE id = 4306477381;  (= the receipt's `before`)
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
SID = 4306477381
NAME = "Özgür Yayınları eBooks"
NOTE = ("ZD 26253 #1742: Özgür Yayınları (Crossref prefix 10.58830) fully OA from 2022, CC BY 4.0 (CC BY-NC before "
        "2025-26), author-paid fee TRY 2,400/chapter, 7,500/e-book; verified on its site, Crossref and Wayback 2026-11-16")
APC_PRICES = [{"price": 2400, "currency": "TRY"}]
APC_USD = 48  # TRY 2,400 at the ECB rate of 2026-11-13 (USD 47.65)
COLS = ("display_name", "type", "publisher", "publisher_id", "is_oa", "is_oa_high_oa_rate", "high_oa_rate_start_year",
        "is_in_doaj", "is_in_scielo", "is_fully_open_in_jstage", "apc_usd", "apc_prices")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "ticket": 26253, "oxjob": 1742,
           "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": SID}).mappings().fetchone()
    return {k: (None if r[k] is None else (r[k] if k == "apc_prices" else str(r[k]))) for k in COLS} if r else None


def main():
    try:
        with engine.begin() as conn:
            before = row(conn)
            assert before is not None, "source missing"
            receipt["before"]["source"] = before
            ov = [dict(r._mapping) for r in conn.execute(
                text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": SID})]
            receipt["before"]["override"] = ov
            assert normalize_name(before["display_name"]) == normalize_name(NAME), before["display_name"]
            assert before["is_oa"] == "False", f"already is_oa {before['is_oa']}: stop and look"
            assert not ov, f"override already exists: {ov}"
            assert before["apc_usd"] is None and before["apc_prices"] in (None, [], "[]"), f"APC already set: {before}"

            conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                              "VALUES (:s, TRUE, 2021, :n)"), {"s": SID, "n": NOTE})
            log("INSERT source_oa_override S4306477381: curated_is_oa TRUE, curated_flip_year 2021 (OA from 2022)")
            conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = TRUE, high_oa_rate_start_year = 2022, "
                              "apc_prices = CAST(:p AS JSONB), apc_usd = :u, updated_date = now() WHERE id = :id"),
                         {"p": json.dumps(APC_PRICES), "u": APC_USD, "id": SID})
            conn.execute(text("""
                UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                            OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                                   updated_date = now()
                WHERE id = :id"""), {"id": SID})
            log("S4306477381: is_oa_high_oa_rate TRUE from 2022, apc_prices TRY 2400, apc_usd 48, is_oa recomputed")
            after = row(conn)
            receipt["after"]["source"] = after
            assert after["is_oa"] == "True" and after["is_oa_high_oa_rate"] == "True" and after["high_oa_rate_start_year"] == "2022", after
            print(json.dumps(after, indent=1, ensure_ascii=False, default=str), flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
