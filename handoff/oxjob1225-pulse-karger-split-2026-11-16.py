"""Split Karger's 'Pulse' off the BanglaJOL 'Pulse' source (oxjob #1225, ZD 24762, premium).
Support factory #2254, written off desk 2026-11-16 (lane #2263); run on desk from the openalex-sources checkout.

Why: S4210174413 'Pulse' (BanglaJOL, ISSN-L 2074-1855, homepage banglajol.info, in DOAJ) also owns the two ISSNs of
Karger's unrelated journal 'Pulse' (print 2235-8676, electronic 2235-8668; Crossref journals/2235-8668 names
S. Karger AG, 236 DOIs). Walden binds Crossref works by ISSN, so the 241 Karger works (DOI 10.1159) sit on the
BanglaJOL record and read diamond through its DOAJ flag. Live 2026-11-16 (#1225 work/recheck_2026-11-16.json):
works?filter=primary_location.source.id:S4210174413,doi_starts_with:10.1159 = 241 (all diamond), 10.3329 = 207.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Snapshot S4210174413 and its source_issn rows; guards: it is 'Pulse', type journal, owns both Karger ISSNs,
     its issn_l is not one of them; no other source is already Karger's Pulse (publisher_id 4310317820 + name Pulse).
  2. Mint 'Pulse' (type journal, publisher 'S. Karger AG', publisher_id 4310317820 = P4310317820 Karger Publishers,
     homepage https://karger.com/pls, CH / Switzerland, is_oa false: no OA signal; recompute leaves it).
  3. Move the two source_issn rows (2235-8676, 2235-8668) to the new source (guarded UPDATE), issn_l 2235-8676,
     re-sync is_issn_l flags and the issns array on both sources, recompute listed_in.
Side effect: two live sources will be named 'Pulse', so the unique-name fallback in CreateLocationsWithSources stops
binding name-only locations to either (ISSN binding is unaffected). Check after the night whether the 10 Wiley
chapters 10.1002/9781119200956.ch* that sit on S4210174413 today (wrongly) came through that tier.
Effect after the next sources build + End 2 End: the 241 Karger works bind to the new source by ISSN and stop
reading diamond through BanglaJOL's DOAJ flag; the BanglaJOL works stay. Check:
  https://api.openalex.org/works?filter=primary_location.source.id:S4210174413,doi_starts_with:10.1159  (expect 0)
  https://api.openalex.org/sources/issn:2235-8668  (expect the new source)

Run on desk:
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-pulse-karger-split-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-pulse-karger-split-2026-11-16.py --execute
Revert: ... --revert [--execute]  (moves the two ISSN rows back to 4210174413, re-syncs, deletes the minted source).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import recompute_listed_in, refresh_issns_column, sync_issn_l_flags  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB, TICKETS = 1225, [24762]
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit)"
OLD = 4210174413
KARGER_ISSNS = ["2235-8676", "2235-8668"]
NEW = dict(display_name="Pulse", type="journal", publisher="S. Karger AG", publisher_id=4310317820,
           homepage_url="https://karger.com/pls", country_code="CH", country="Switzerland", issn_l="2235-8676")
RECEIPT_PATH = os.path.join(HERE, "oxjob1225-pulse-karger-split-2026-11-16-receipt.json")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "mode": "revert" if REVERT else "apply",
           "job": JOB, "tickets": TICKETS, "approved_by": APPROVED, "before": {}, "actions": [], "after": {}, "revert": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def rows(conn, sql, **kw):
    return [{k: (v if isinstance(v, (list, dict, bool, int, type(None))) else str(v)) for k, v in dict(r).items()}
            for r in conn.execute(text(sql), kw).mappings()]


def apply(conn):
    o = rows(conn, "SELECT * FROM sources WHERE id = :id", id=OLD)
    assert len(o) == 1, f"S{OLD} not found"
    o = o[0]
    receipt["before"]["old"] = o
    receipt["before"]["old_issns"] = rows(conn, "SELECT * FROM source_issn WHERE source_id = :id ORDER BY issn", id=OLD)
    assert (o["display_name"] or "").strip().lower() == "pulse" and o["type"] == "journal", o
    owned = {r["issn"] for r in receipt["before"]["old_issns"]}
    assert set(KARGER_ISSNS) <= owned, f"Karger ISSNs not both on S{OLD} (owned {owned}): already split? nothing written"
    assert o["issn_l"] not in KARGER_ISSNS, f"issn_l {o['issn_l']} is a Karger ISSN: look first"
    dup = rows(conn, "SELECT id, display_name, publisher_id FROM sources WHERE lower(display_name) = 'pulse' "
                     "AND publisher_id = :p AND id <> :id", p=NEW["publisher_id"], id=OLD)
    assert not dup, f"a Karger Pulse source already exists: {dup}"
    new_id = conn.execute(text(
        "INSERT INTO sources (display_name, type, publisher, publisher_id, homepage_url, country_code, country, is_oa) "
        "VALUES (:display_name, :type, :publisher, :publisher_id, :homepage_url, :country_code, :country, FALSE) "
        "RETURNING id"), NEW).scalar()
    log(f"minted source {new_id} 'Pulse' (journal, S. Karger AG / P4310317820, CH)")
    moved = rows(conn, "UPDATE source_issn SET source_id = :new, is_issn_l = FALSE "
                       "WHERE source_id = :old AND issn = ANY(:issns) RETURNING source_id, issn",
                 new=new_id, old=OLD, issns=KARGER_ISSNS)
    assert len(moved) == 2, f"guard: expected 2 ISSN rows moved, got {moved}"
    log(f"source_issn {KARGER_ISSNS}: {OLD} -> {new_id}")
    conn.execute(text("UPDATE sources SET issn_l = :l, updated_date = now() WHERE id = :id"), {"l": NEW["issn_l"], "id": new_id})
    for sid in (new_id, OLD):
        sync_issn_l_flags(conn, sid)
        refresh_issns_column(conn, sid)
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": OLD})
    n = recompute_listed_in(conn)
    log(f"recompute_listed_in: {n}")
    receipt["after"]["old"] = rows(conn, "SELECT id, display_name, issn_l, issns FROM sources WHERE id = :id", id=OLD)
    receipt["after"]["new"] = rows(conn, "SELECT * FROM sources WHERE id = :id", id=new_id)
    receipt["new_source_id"] = new_id
    receipt["revert"].append(f"UPDATE source_issn SET source_id = {OLD}, is_issn_l = FALSE WHERE source_id = {new_id} "
                             f"AND issn IN ('2235-8676', '2235-8668'); then sync_issn_l_flags + refresh_issns_column on "
                             f"{OLD}; DELETE FROM sources WHERE id = {new_id};")


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    new_id = int(r["new_source_id"])
    back = rows(conn, "UPDATE source_issn SET source_id = :old, is_issn_l = FALSE WHERE source_id = :new "
                      "AND issn = ANY(:issns) RETURNING issn", old=OLD, new=new_id, issns=KARGER_ISSNS)
    assert len(back) == 2, back
    sync_issn_l_flags(conn, OLD)
    refresh_issns_column(conn, OLD)
    left = conn.execute(text("SELECT count(*) FROM source_issn WHERE source_id = :id"), {"id": new_id}).scalar()
    assert left == 0, left
    n = conn.execute(text("DELETE FROM sources WHERE id = :id AND display_name = 'Pulse'"), {"id": new_id}).rowcount
    assert n == 1, n
    recompute_listed_in(conn)
    log(f"ISSNs back on {OLD}; deleted {new_id}")


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
