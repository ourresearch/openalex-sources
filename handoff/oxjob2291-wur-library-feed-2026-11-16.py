"""Wageningen's publications repository feed gets its own repository source (oxjob #2291, ZD 26261; ZD 22707 item 1).
Written by the #2291 session on desk 2026-11-16, adapted from oxjob627-glasgow-enlighten-2026-11-16.py.

Why: Wageningen University & Research's publications repository (OAI-PMH http://library.wur.nl/oai, Identify
repositoryName 'Wageningen University & Research Publications', landing pages research.wur.nl) has no source record.
Its one registered endpoint, 0a26303df1172c2974a, is bound to S4210201231 'Socio-Environmental Systems Modeling'
(SESMO), a small journal WUR hosts on its own OJS at library.wur.nl/ojs: the shared host is how the binding happened.
walden binds every repository record to its endpoint's source (CreateLocationsWithSources: endpoint_to_source), so
every WUR deposit (reports, theses, chapters, Pure news clippings) is credited to SESMO, and because SESMO carries
is_oa_high_oa_rate, CreateWorksBase serves every WUR copy as open (composite_is_oa = is_oa_raw OR source_is_oa).
Night 11-16 table (#2291 README): 276,824 visible + 101,141 xpac works hold a location from the feed, 134,797 visible
with it as primary; SESMO's own articles are 73 Crossref DOIs (10.18174/sesmo), which keep their Crossref location.
WUR asked for exactly this in ZD 26261 ("make sure these records are assigned to the right venue (=our repo)").

What it does, in ONE transaction (dry run by default, rolled back):
  1. Snapshot SESMO's row, every oai_pmh_endpoint on SESMO, every endpoint whose pmh_url mentions wur.nl, and any
     source that already looks like WUR's publications repository.
  2. Guards: S4210201231 is still 'Socio-Environmental Systems Modeling' of type journal; endpoint 0a26303df1172c2974a
     has pmh_url http://library.wur.nl/oai, no set, source_id 4210201231, is_journal_host false.
  3. Mint (or reuse, if one exists) the repository source 'Wageningen University & Research Publications' (the name
     the feed's own Identify gives): type repository, institution 913481162 (Wageningen University & Research, ROR
     04qw24q55), homepage https://research.wur.nl/, country NL, is_oa false (a repository: each record's own OA flag
     decides each copy). Yoda@WUR S7407051423 is WUR's data repository, a different thing: never reused.
  4. Re-bind only endpoint 0a26303df1172c2974a (guarded UPDATE ... WHERE source_id = SESMO). Any other endpoint on
     SESMO (its own OJS feed, if one is ever registered) stays.

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2291-wur-library-feed-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2291-wur-library-feed-2026-11-16.py --execute
The receipt is printed as JSON after the RECEIPT marker and, with --execute, written next to this file as
handoff/oxjob2291-wur-library-feed-2026-11-16-receipt.json; commit both.

Revert (also printed in the receipt):
    ... run_registry.py handoff/oxjob2291-wur-library-feed-2026-11-16.py --revert [--execute]
  = UPDATE oai_pmh_endpoint SET source_id = 4210201231 WHERE id = '0a26303df1172c2974a' AND source_id = <new id>;
    DELETE FROM sources WHERE id = <new id> (only if the script minted it and nothing else is bound to it).
Live after the next sources build + End 2 End (the night after the write).
"""
import json
import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB = 2291
TICKETS = [26261, 22707]
APPROVED = "oxjob #2291 under Jason's standing authority (data-bugs charter rule 3: reversible registry edit, bar 40/40)"
JOURNAL = 4210201231
JOURNAL_NAME = "socio-environmental systems modeling"
ENDPOINT = "0a26303df1172c2974a"
PMH_URL = "http://library.wur.nl/oai"
WUR = 913481162
NEW_NAME = "Wageningen University & Research Publications"
NEW_HOMEPAGE = "https://research.wur.nl/"
RECEIPT_PATH = os.path.join(HERE, "oxjob2291-wur-library-feed-2026-11-16-receipt.json")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "tickets": TICKETS, "approved_by": APPROVED,
           "before": {}, "actions": [], "after": {}, "revert": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def jsonable(row):
    return {k: (None if v is None else v if isinstance(v, (list, dict, bool, int)) else str(v))
            for k, v in dict(row).items()}


def host_of(url):
    h = (urlparse((url or "").strip()).hostname or "").lower()
    return h[4:] if h.startswith("www.") else h


def rows(conn, sql, **kw):
    return [jsonable(r) for r in conn.execute(text(sql), kw).mappings()]


def apply(conn):
    jr = rows(conn, "SELECT * FROM sources WHERE id = :id", id=JOURNAL)
    assert len(jr) == 1, f"journal S{JOURNAL} not found (merged?)"
    j = jr[0]
    receipt["before"]["journal"] = j
    assert (j["display_name"] or "").strip().lower() == JOURNAL_NAME, j["display_name"]
    assert j["type"] == "journal", j["type"]

    on_journal = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE source_id = :id ORDER BY id", id=JOURNAL)
    receipt["before"]["endpoints_on_journal"] = on_journal
    for e in on_journal:
        print(f"  on journal: endpoint {e['id']} url={e.get('pmh_url')} set={e.get('pmh_set')} "
              f"status={e.get('status')} is_journal_host={e.get('is_journal_host')}")
    ep = [e for e in on_journal if e["id"] == ENDPOINT]
    assert ep, f"endpoint {ENDPOINT} is not bound to S{JOURNAL} any more: already fixed? nothing written"
    e = ep[0]
    assert (e.get("pmh_url") or "").strip().rstrip("/") == PMH_URL, e.get("pmh_url")
    assert not e.get("pmh_set"), f"endpoint has a set ({e.get('pmh_set')}): stop, look"
    assert e.get("is_journal_host") in (False, None, "False"), f"{ENDPOINT} is flagged is_journal_host: stop, look"
    others = [x for x in on_journal if x["id"] != ENDPOINT]
    log(f"journal keeps {len(others)} endpoint(s): {[(x['id'], x.get('pmh_url')) for x in others]}")

    anywhere = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE pmh_url ILIKE '%wur.nl%' ORDER BY id")
    receipt["before"]["endpoints_on_wur_hosts"] = anywhere
    elsewhere = [x for x in anywhere if x["id"] != ENDPOINT]
    if elsewhere:
        log(f"note: other wur.nl endpoints (untouched): "
            f"{[(x['id'], x['source_id'], x.get('pmh_url'), x.get('pmh_set')) for x in elsewhere]}")

    existing = rows(conn, "SELECT * FROM sources WHERE lower(display_name) = lower(:n) "
                          "OR (type = 'repository' AND (homepage_url ILIKE '%research.wur.nl%' "
                          "OR homepage_url ILIKE '%library.wur.nl%' OR homepage_url ILIKE '%edepot.wur.nl%')) ORDER BY id",
                    n=NEW_NAME)
    receipt["before"]["existing_wur_publication_sources"] = existing
    minted = False
    if existing:
        assert len(existing) == 1, f"more than one candidate source, look first: {[x['id'] for x in existing]}"
        new = existing[0]
        assert new["type"] == "repository" and int(new["institution_id"] or 0) == WUR, new
        log(f"reusing existing source {new['id']} '{new['display_name']}'")
    else:
        new = jsonable(conn.execute(text(
            "INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, is_oa) "
            "VALUES (:n, 'repository', :inst, :h, 'NL', false) "
            "RETURNING id, display_name, type, institution_id, homepage_url, country_code, is_oa"),
            {"n": NEW_NAME, "inst": WUR, "h": NEW_HOMEPAGE}).mappings().one())
        minted = True
        log(f"minted source {new['id']} '{NEW_NAME}' (repository, institution {WUR}, NL)")
    new_id = int(new["id"])

    moved = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :new "
                       "WHERE id = :ep AND source_id = :old AND is_journal_host = false "
                       "RETURNING id, pmh_url, pmh_set, source_id, status", new=new_id, ep=ENDPOINT, old=JOURNAL)
    assert len(moved) == 1, f"guard: expected 1 row re-bound, got {len(moved)}; nothing written"
    for m in moved:
        log(f"endpoint {m['id']} ({m['pmh_url']} set={m['pmh_set']}): source {JOURNAL} -> {new_id}")

    receipt["after"]["new_source"] = rows(conn, "SELECT * FROM sources WHERE id = :id", id=new_id)
    receipt["after"]["endpoints_on_journal"] = rows(conn, "SELECT id, pmh_url, pmh_set, source_id FROM oai_pmh_endpoint "
                                                          "WHERE source_id = :id ORDER BY id", id=JOURNAL)
    receipt["after"]["endpoints_on_new"] = rows(conn, "SELECT id, pmh_url, pmh_set, source_id FROM oai_pmh_endpoint "
                                                      "WHERE source_id = :id ORDER BY id", id=new_id)
    assert not [x for x in receipt["after"]["endpoints_on_journal"] if x["id"] == ENDPOINT]
    receipt["new_source_id"] = new_id
    receipt["minted"] = minted
    receipt["endpoint_ids"] = [ENDPOINT]
    receipt["revert"].append(f"UPDATE oai_pmh_endpoint SET source_id = {JOURNAL} WHERE id = '{ENDPOINT}' "
                             f"AND source_id = {new_id};")
    if minted:
        receipt["revert"].append(f"DELETE FROM sources WHERE id = {new_id};  -- only once nothing is bound to it")


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    new_id, ids = int(r["new_source_id"]), list(r["endpoint_ids"])
    receipt["before"]["applied_receipt"] = {"run_at": r["run_at"], "new_source_id": new_id, "endpoint_ids": ids}
    back = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :j WHERE id = ANY(:ids) AND source_id = :new "
                      "RETURNING id, source_id", j=JOURNAL, ids=ids, new=new_id)
    assert len(back) == len(ids), f"guard: expected {len(ids)} rows back on the journal, got {len(back)}"
    log(f"endpoints {ids}: source {new_id} -> {JOURNAL}")
    if r.get("minted"):
        left = conn.execute(text("SELECT count(*) FROM oai_pmh_endpoint WHERE source_id = :id"), {"id": new_id}).scalar()
        if left == 0:
            n = conn.execute(text("DELETE FROM sources WHERE id = :id AND display_name = :n"),
                             {"id": new_id, "n": NEW_NAME}).rowcount
            assert n == 1, n
            log(f"deleted minted source {new_id}")
        else:
            log(f"kept source {new_id}: {left} other endpoint(s) bound to it")


def main():
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn)
            else:
                apply(conn)
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
