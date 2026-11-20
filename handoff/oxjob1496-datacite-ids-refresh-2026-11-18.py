"""Refresh the derived sources.datacite_ids column after two merges that moved a DataCite client link but not the
column walden reads (oxjob #1496, support factory #2254; written off desk 2026-11-18 by the #1496 prep lane, #2263).

Why: merge() in handoff/oxjob1496-ceur-merge-host-2026-11-17.py (and oxjob629/oxjob1580's, the template) runs
`UPDATE source_datacite_id SET source_id = winner` but never refreshes `sources.datacite_ids` (JSONB), which is the only
DataCite field Sources_Snapshot copies and CreateLocationsWithSources explodes. So the moved clients map to no source:
  - zqfs.ceurws -> 4210204615 CEUR Workshop Proceedings (merge 17 Nov 21:01 CT): live 18 Nov, 412 of 446 10.83095 works
    have no primary source.
  - tib.ubtum -> 4306400453 mediaTUM (#629 merge 15 Nov): live 18 Nov, 442 of 585 10.14459 works have no primary
    source, 92 on mediaTUM.
openalex.sources.sources (05:00 CT 18 Nov snapshot) shows datacite_ids [] for both.

What it does (ONE transaction, dry run by default):
  1. Guard: source_datacite_id rows for the two winners are exactly zqfs.ceurws -> 4210204615 and
     tib.ubtum -> 4306400453; both sources exist and still read datacite_ids [] (else it stops: someone fixed it).
  2. The same UPDATE as jobs/sync_datacite_clients.py _refresh_datacite_jsonb, for those two ids; expect 2 rows,
     after ["zqfs.ceurws"] and ["tib.ubtum"].
  3. Read-only sweep: every source whose datacite_ids differs from its source_datacite_id rows goes into the receipt
     ("drift"). With --fix-drift the same refresh runs for them too (stops if more than 50).

Run on desk from the openalex-sources checkout:
    cp <this file> handoff/ && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1496-datacite-ids-refresh-2026-11-18.py [--execute] [--fix-drift]
Receipt JSON prints after the RECEIPT marker: save it as handoff/oxjob1496-datacite-ids-refresh-2026-11-18-receipt.json
and in #1496 work/. Before 21:30 CT 18 Nov to ride night 2026-11-19. Re-stamps the source of ~412 + ~631 (staged count)
works; no work created or removed. Register in change-throughput-coordinator/changes.yaml.
Revert: UPDATE sources SET datacite_ids = '[]'::jsonb, datacite_id = <receipt before.datacite_id> WHERE id IN
(4210204615, 4306400453) (and the same from the receipt for any --fix-drift ids).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
FIX_DRIFT = "--fix-drift" in sys.argv
JOB = 1496
EXPECT_LINKS = {4210204615: ["zqfs.ceurws"], 4306400453: ["tib.ubtum"]}
EXPECT_NAMES = {4210204615: "CEUR Workshop Proceedings", 4306400453: "mediaTUM (Technical University of Munich)"}
MAX_DRIFT = 50

REFRESH = ("UPDATE sources s SET datacite_ids = COALESCE((SELECT jsonb_agg(datacite_id ORDER BY datacite_id) "
           "FROM source_datacite_id WHERE source_id = s.id), '[]'::jsonb), "
           "datacite_id = COALESCE(s.datacite_id, (SELECT MIN(datacite_id) FROM source_datacite_id "
           "WHERE source_id = s.id)), updated_date = now() WHERE s.id = ANY(:ids)")
DRIFT = ("SELECT s.id, s.display_name, s.datacite_id, s.datacite_ids, l.agg FROM sources s JOIN "
         "(SELECT source_id, jsonb_agg(datacite_id ORDER BY datacite_id) AS agg FROM source_datacite_id GROUP BY 1) l "
         "ON l.source_id = s.id WHERE s.datacite_ids IS DISTINCT FROM l.agg ORDER BY s.id")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "fix_drift": FIX_DRIFT, "job": JOB,
           "before": {}, "drift": [], "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def state(conn, ids):
    out = {}
    for r in conn.execute(text("SELECT id, display_name, datacite_id, datacite_ids FROM sources WHERE id = ANY(:i) "
                               "ORDER BY id"), {"i": list(ids)}).mappings():
        links = [x[0] for x in conn.execute(text("SELECT datacite_id FROM source_datacite_id WHERE source_id = :i "
                                                 "ORDER BY datacite_id"), {"i": r["id"]})]
        out[str(r["id"])] = {"display_name": r["display_name"], "datacite_id": r["datacite_id"],
                             "datacite_ids": r["datacite_ids"], "_links": links}
    return out


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            ids = list(EXPECT_LINKS)
            receipt["before"] = state(conn, ids)
            for sid, links in EXPECT_LINKS.items():
                b = receipt["before"].get(str(sid))
                assert b is not None, f"source {sid} missing"
                assert b["display_name"] == EXPECT_NAMES[sid], (sid, b["display_name"])
                assert b["_links"] == links, (sid, b["_links"])
                assert list(b["datacite_ids"] or []) == [], f"{sid} datacite_ids already {b['datacite_ids']}: stop"
            drift = [dict(r) for r in conn.execute(text(DRIFT)).mappings()]
            receipt["drift"] = [{k: (v if isinstance(v, (list, dict, type(None))) else str(v)) for k, v in d.items()}
                                for d in drift]
            others = [d["id"] for d in drift if d["id"] not in EXPECT_LINKS]
            log(f"drift sweep: {len(drift)} sources differ ({len(others)} besides the two): {others[:20]}")
            todo = ids
            if FIX_DRIFT and others:
                assert len(others) <= MAX_DRIFT, f"{len(others)} drifted sources > {MAX_DRIFT}: look first"
                receipt["before"].update(state(conn, others))
                todo = ids + others
            n = conn.execute(text(REFRESH), {"ids": todo}).rowcount
            assert n == len(todo), (n, todo)
            log(f"REFRESHED datacite_ids on {n} sources: {todo}")
            receipt["after"] = state(conn, todo)
            for sid, links in EXPECT_LINKS.items():
                assert list(receipt["after"][str(sid)]["datacite_ids"]) == links, receipt["after"][str(sid)]
            left = [r[0] for r in conn.execute(text(DRIFT))]
            log(f"drift after: {left}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
