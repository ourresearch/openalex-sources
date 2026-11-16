"""Ticket 23006 (#693, support factory #2254): two journals OICC Press took over from Springer on 1 Jan 2025 still name
Springer Nature as host and Springer's pages as homepage. Witness (16 Nov 2026): DOAJ's journal records (updated
2026-09-17) give publisher 'OICC Press', country GB, homepages https://oiccpress.com/jnsc/ and https://oiccpress.com/ijrowa;
DataCite's records for their 2025-2026 DOIs (10.57647/*) name OICC Press (ROR 01j628m19); the requester (OICC Press)
cites the ISSN International Centre's journal-transfer registry (transfer date 2025-01-01). OICC Press's other journals
in the registry are hosted by institution I4394709092 'OICC Press (United Kingdom)' with no publisher_id, so these two
follow the same shape.

  S2764590048 Journal of Nanostructure in Chemistry (2008-9244 / 2193-8865)
  S2738418679 International Journal of Recycling of Organic Waste in Agriculture (2195-3228 / 2251-7715)
Each: publisher_id 4310319965 (Springer Nature) -> NULL, institution_id NULL -> 4394709092, homepage_url -> DOAJ's,
country_code/country -> GB/United Kingdom. Only if the current values are the expected ones.

Run on desk outside the End 2 End window; dry run by default, ONE transaction:
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob693-oicc-transfers-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob693-oicc-transfers-2026-11-16.py --execute
Revert: put back the receipt's `before` values (publisher_id 4310319965, institution_id NULL, homepage_url, country_code, country).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402

EXECUTE = "--execute" in sys.argv
SPRINGER, OICC = 4310319965, 4394709092
EDITS = [
    dict(sid=2764590048, expect="journal of nanostructure in chemistry", homepage="https://oiccpress.com/jnsc/"),
    dict(sid=2738418679, expect="international journal of recycling of organic waste in agriculture",
         homepage="https://oiccpress.com/ijrowa"),
]
RECEIPT_PATH = os.path.join(HERE, "oxjob693-oicc-transfers-2026-11-16-receipt.json")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 693, "ticket": 23006,
           "approved": "support factory #2254 (Jason 2026-11-15 yes); standing authority", "before": {}, "actions": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def main():
    try:
        with engine.begin() as conn:
            for e in EDITS:
                r = conn.execute(text("SELECT id, display_name, publisher_id, institution_id, homepage_url, country_code, country "
                                      "FROM sources WHERE id = :id"), {"id": e["sid"]}).mappings().fetchone()
                assert r is not None, f"S{e['sid']} missing"
                receipt["before"][str(e["sid"])] = {k: (str(v) if v is not None else None) for k, v in r.items()}
                assert (r["display_name"] or "").strip().lower() == e["expect"], r["display_name"]
                if r["publisher_id"] == SPRINGER and r["institution_id"] is None:
                    conn.execute(text("UPDATE sources SET publisher_id = NULL, institution_id = :i, homepage_url = :h, "
                                      "country_code = 'GB', country = 'United Kingdom', updated_date = now() WHERE id = :id"),
                                 {"i": OICC, "h": e["homepage"], "id": e["sid"]})
                    log(f"S{e['sid']}: publisher_id {SPRINGER} -> NULL, institution_id NULL -> {OICC}, "
                        f"homepage {r['homepage_url']!r} -> {e['homepage']!r}, country {r['country_code']!r}/{r['country']!r} -> 'GB'/'United Kingdom' (ZD 23006)")
                else:
                    log(f"S{e['sid']}: publisher_id={r['publisher_id']} institution_id={r['institution_id']}: left (ZD 23006)")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        json.dump(receipt, open(RECEIPT_PATH, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", RECEIPT_PATH)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
