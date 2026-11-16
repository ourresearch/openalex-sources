"""Wiley's journal 'View' gets its name back and loses the Surrey and Bolzano repository feeds (oxjob #1225, ZD 8769,
premium). Support factory #2254, written off desk 2026-11-16 (lane #2263); run on desk from the openalex-sources checkout.

Why: S4210197018 is View (Wiley, ISSN 2688-268X / 2688-3988; Crossref journals/2688-268X: 'View', 636 DOIs), but the
registry row carries the name 'Open Research (University of Surrey)' and homepage openresearch.surrey.ac.uk, and the
Esploro OAI-PMH feeds of the University of Surrey (openresearch.surrey.ac.uk) and the Free University of Bozen-Bolzano
(bia.unibz.it) are bound to it. walden binds every repo record to its endpoint's source, so the two universities'
deposits are credited to View and read OA through View's DOAJ flag. Live 2026-11-16 (#1225 work/view_2026-11-16.json):
54,256 works have a location on S4210197018, 9,181 list it as primary, only 649 of those are View DOIs (10.1002/viw2);
of 1,200 sampled works the bound copies sit on bia.unibz.it (344), openresearch.surrey.ac.uk (292) and the publisher /
Web of Science pages Esploro records link to (the rest).
Both universities already have repository sources: S7407059866 'University of Surrey Open Research repository'
(homepage openresearch.surrey.ac.uk/esploro/) and S7407050986 'Libera Università di Bolzano'.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Snapshot S4210197018 and every oai_pmh_endpoint row bound to it (printed: read them in the dry run).
  2. Classify each endpoint by its pmh_url / pmh_set / name text: Surrey ('surrey', '44sur') -> S7407059866;
     Bolzano ('unibz', 'bolzano', 'bozen', '39ubz') -> S7407050986. Guard: any endpoint not flagged is_journal_host
     that matches neither STOPS the run (look at it, extend the patterns or leave it); is_journal_host endpoints stay.
     Guard: at least one endpoint moves; both targets exist and are repositories.
  3. Re-bind the matched endpoints (guarded UPDATE ... WHERE source_id = the journal).
  4. Rename S4210197018 to 'View' (display_name_before_override = the old name, override_timestamp now()) and set
     homepage_url https://onlinelibrary.wiley.com/journal/2688268x. Nothing else on the journal changes (ISSNs, OA,
     DOAJ, publisher stay).
Effect after the next sources build + End 2 End: Surrey and Bolzano deposits move to their repositories; View keeps
its own 649+ DOIs (gold/diamond). Check:
  https://api.openalex.org/sources/S4210197018 (display_name View)
  https://api.openalex.org/works?filter=primary_location.source.id:S4210197018&group_by=open_access.oa_status
  (expect ~650 works, no green/closed bulk)

Run on desk:
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-view-esploro-rebind-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-view-esploro-rebind-2026-11-16.py --execute
Revert: ... --revert [--execute] (endpoints back to the journal, name/homepage back to the receipt's before values).
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
JOB, TICKETS = 1225, [8769]
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit)"
JOURNAL = 4210197018
JOURNAL_ISSN = "2688-268X"
NEW_NAME = "View"
NEW_HOMEPAGE = "https://onlinelibrary.wiley.com/journal/2688268x"
TARGETS = {
    7407059866: ("surrey", ["surrey", "44sur"]),
    7407050986: ("bolzano", ["unibz", "bolzano", "bozen", "39ubz"]),
}
RECEIPT_PATH = os.path.join(HERE, "oxjob1225-view-esploro-rebind-2026-11-16-receipt.json")
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


def is_host(v):
    return v in (True, "True", "true", "t")


def target_of(e):
    blob = " ".join(str(e.get(k) or "") for k in ("pmh_url", "pmh_set", "name", "repository_name", "id")).lower()
    hits = [sid for sid, (_, pats) in TARGETS.items() if any(p in blob for p in pats)]
    return hits[0] if len(hits) == 1 else (None if not hits else "ambiguous")


def apply(conn):
    j = rows(conn, "SELECT * FROM sources WHERE id = :id", id=JOURNAL)
    assert len(j) == 1, f"S{JOURNAL} not found"
    j = j[0]
    receipt["before"]["journal"] = j
    assert j["type"] == "journal", j["type"]
    issns = {r["issn"] for r in rows(conn, "SELECT issn FROM source_issn WHERE source_id = :id", id=JOURNAL)}
    assert JOURNAL_ISSN in issns, f"S{JOURNAL} no longer owns {JOURNAL_ISSN}: look first"
    for sid in TARGETS:
        t = rows(conn, "SELECT id, display_name, type, homepage_url FROM sources WHERE id = :id", id=sid)
        assert len(t) == 1 and t[0]["type"] == "repository", t  # post-D1: a merged-away source has no row
        receipt["before"][f"target_{sid}"] = t[0]
    eps = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE source_id = :id ORDER BY id", id=JOURNAL)
    receipt["before"]["endpoints_on_journal"] = eps
    plan, unknown = {}, []
    for e in eps:
        t = target_of(e)
        print(f"  endpoint {e['id']} url={e.get('pmh_url')} set={e.get('pmh_set')} host_flag={e.get('is_journal_host')} "
              f"status={e.get('status')} -> {t}")
        if is_host(e.get("is_journal_host")):
            continue
        if t in (None, "ambiguous"):
            unknown.append(e["id"])
        else:
            plan.setdefault(t, []).append(e["id"])
    assert not unknown, f"endpoints on View that match neither university: {unknown}. Look at them; nothing written"
    assert plan, "no Surrey/Bolzano endpoint bound to View any more: already fixed? nothing written"
    for sid, ids in plan.items():
        moved = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :new WHERE id = ANY(:ids) AND source_id = :old "
                           "RETURNING id, pmh_url, pmh_set", new=sid, ids=ids, old=JOURNAL)
        assert len(moved) == len(ids), f"guard: {len(moved)} of {len(ids)} moved"
        for m in moved:
            log(f"endpoint {m['id']} ({m['pmh_url']} set={m['pmh_set']}): source {JOURNAL} -> {sid} ({TARGETS[sid][0]})")
    conn.execute(text("UPDATE sources SET display_name = :n, display_name_before_override = :o, override_timestamp = now(), "
                      "homepage_url = :h, updated_date = now() WHERE id = :id"),
                 {"n": NEW_NAME, "o": j["display_name"], "h": NEW_HOMEPAGE, "id": JOURNAL})
    log(f"S{JOURNAL}: display_name '{j['display_name']}' -> '{NEW_NAME}', homepage '{j.get('homepage_url')}' -> '{NEW_HOMEPAGE}'")
    receipt["plan"] = {str(k): v for k, v in plan.items()}
    receipt["after"]["journal"] = rows(conn, "SELECT id, display_name, homepage_url, display_name_before_override "
                                             "FROM sources WHERE id = :id", id=JOURNAL)
    receipt["after"]["endpoints_on_journal"] = rows(conn, "SELECT id, pmh_url, source_id FROM oai_pmh_endpoint "
                                                          "WHERE source_id = :id", id=JOURNAL)
    for sid, ids in plan.items():
        receipt["revert"].append(f"UPDATE oai_pmh_endpoint SET source_id = {JOURNAL} WHERE id IN "
                                 f"({', '.join(repr(i) for i in ids)}) AND source_id = {sid};")
    receipt["revert"].append(f"UPDATE sources SET display_name = <before.journal.display_name>, homepage_url = "
                             f"<before.journal.homepage_url>, display_name_before_override = <before value>, "
                             f"override_timestamp = <before value> WHERE id = {JOURNAL};")


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    for sid, ids in r["plan"].items():
        back = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :j WHERE id = ANY(:ids) AND source_id = :s "
                          "RETURNING id", j=JOURNAL, ids=ids, s=int(sid))
        assert len(back) == len(ids), back
        log(f"endpoints {ids}: {sid} -> {JOURNAL}")
    b = r["before"]["journal"]
    conn.execute(text("UPDATE sources SET display_name = :n, homepage_url = :h, display_name_before_override = :o, "
                      "override_timestamp = :t, updated_date = now() WHERE id = :id"),
                 {"n": b["display_name"], "h": b.get("homepage_url"), "o": b.get("display_name_before_override"),
                  "t": None if b.get("override_timestamp") in (None, "None") else b["override_timestamp"], "id": JOURNAL})
    log(f"S{JOURNAL} name/homepage back to '{b['display_name']}' / '{b.get('homepage_url')}'")


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
