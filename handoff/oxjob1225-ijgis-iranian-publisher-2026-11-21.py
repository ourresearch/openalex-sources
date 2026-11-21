"""Iranian Journal of Remote Sensing & GIS (S4210176169): publisher Shahid Beheshti University, not 'Armenian Green
Publishing Co.' (oxjob #1225, ZD 7482). Support factory #2254, written off desk 2026-11-21 (lane #2263); run on desk from
the openalex-sources checkout.

Why: ZD 7482's fix moves 303 MAG copies of نشریه سنجش از دور و GIS ایران (gisj.sbu.ac.ir) off IJGIS S111872859 onto
S4210176169 'Journal of Remote Sensing and Gis' (ISSN 2008-5966 / 2588-6185, alternate title 'Iranian journal of remote
sensing & GIS'; curation batches 3-4). The journal's own homepage (https://gisj.sbu.ac.ir/, fetched 21 Nov 2026) gives both
ISSNs and is Shahid Beheshti University's journal site; the registry names publisher P4327876842 'Armenian Green
Publishing Co.' and has no homepage. P4310319054 = Shahid Beheshti University (ROR 0091vmj44).
(P4327876842 is the publisher of 124 sources, all Iranian: a class, logged in #1225 for its own job; not touched here.)

What it does, in ONE transaction (dry run by default, rolled back):
  1. S4210176169 publisher_id 4327876842 -> 4310319054, only if the current value is 4327876842.
  2. homepage_url -> https://gisj.sbu.ac.ir/, only if it is empty.
Run on desk:
    cp <this file> ~/ox/openalex-sources/handoff/oxjob1225-ijgis-iranian-publisher-2026-11-21.py && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-ijgis-iranian-publisher-2026-11-21.py
    ... --execute
Revert: ... --revert [--execute]  (publisher_id back to 4327876842 and homepage_url back to empty, only while they hold
this script's values). Check after the sources sync: https://api.openalex.org/sources/S4210176169 host_organization_name
'Shahid Beheshti University'.
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
REVERT = "--revert" in sys.argv
JOB = 1225
TICKETS = [7482]
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit)"
SOURCE = 4210176169
OLD_PUBLISHER = 4327876842
NEW_PUBLISHER = 4310319054
HOMEPAGE = "https://gisj.sbu.ac.ir/"
RECEIPT_PATH = os.path.join(HERE, "oxjob1225-ijgis-iranian-publisher-2026-11-21-receipt.json")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "mode": "revert" if REVERT else "apply",
           "job": JOB, "tickets": TICKETS, "approved_by": APPROVED, "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn):
    r = conn.execute(text("SELECT id, display_name, issns, publisher_id, institution_id, homepage_url, type "
                          "FROM sources WHERE id = :id"), {"id": SOURCE}).mappings().one()
    return {k: (v if isinstance(v, (list, dict, bool, int, type(None))) else str(v)) for k, v in dict(r).items()}


def apply(conn):
    b = row(conn)
    receipt["before"]["source"] = b
    assert b["type"] == "journal", b
    assert "2008-5966" in json.dumps(b["issns"]), f"S{SOURCE} no longer holds ISSN 2008-5966: look first"
    # P4310319054 = 'Shahid Beheshti University' (ROR 0091vmj44), checked on api.openalex.org/publishers 21 Nov 2026
    # (the registry holds no publishers table).
    n = conn.execute(text("UPDATE sources SET publisher_id = :new, updated_date = now() WHERE id = :id AND publisher_id = :old"),
                     {"new": NEW_PUBLISHER, "id": SOURCE, "old": OLD_PUBLISHER}).rowcount
    log(f"S{SOURCE} publisher_id {OLD_PUBLISHER} -> {NEW_PUBLISHER}" if n else
        f"S{SOURCE} publisher_id is {b['publisher_id']}, expected {OLD_PUBLISHER}: left")
    receipt["publisher_changed"] = bool(n)
    h = conn.execute(text("UPDATE sources SET homepage_url = :h, updated_date = now() WHERE id = :id "
                          "AND (homepage_url IS NULL OR homepage_url = '')"), {"h": HOMEPAGE, "id": SOURCE}).rowcount
    log(f"S{SOURCE} homepage_url -> {HOMEPAGE}" if h else f"S{SOURCE} homepage_url kept: {b['homepage_url']}")
    receipt["homepage_changed"] = bool(h)
    receipt["after"]["source"] = row(conn)


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    if r.get("publisher_changed"):
        n = conn.execute(text("UPDATE sources SET publisher_id = :old WHERE id = :id AND publisher_id = :new"),
                         {"old": OLD_PUBLISHER, "id": SOURCE, "new": NEW_PUBLISHER}).rowcount
        log(f"S{SOURCE} publisher_id back to {OLD_PUBLISHER} ({n} row)")
    if r.get("homepage_changed"):
        n = conn.execute(text("UPDATE sources SET homepage_url = NULL WHERE id = :id AND homepage_url = :h"),
                         {"id": SOURCE, "h": HOMEPAGE}).rowcount
        log(f"S{SOURCE} homepage_url back to empty ({n} row)")


def main():
    try:
        with engine.begin() as conn:
            revert(conn) if REVERT else apply(conn)
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        path = RECEIPT_PATH.replace("-receipt.json", "-revert-receipt.json") if REVERT else RECEIPT_PATH
        json.dump(receipt, open(path, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", path)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
