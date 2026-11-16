"""Glasgow's Enlighten Publications feed gets its own repository source (oxjob #627, ZD 24629, 24641).
Support factory #2254, written off desk 2026-11-16 (lane for #627, #2263); run on desk from the openalex-sources checkout.

Why: the University of Glasgow's main repository, Enlighten Publications (eprints.gla.ac.uk), has no source record.
Its OAI-PMH feed is bound to S4210235606 "ENLIGHTEN (Jurnal Bimbingan dan Konseling Islam)", an Indonesian journal
(IAIN Langsa, ISSN 2622-8912) that shares only the word "Enlighten" (the 83.13 endpoint backfill copied a legacy
name-matched binding with no host check). walden binds every repo record to its endpoint's source
(CreateLocationsWithSources: endpoint_to_source), so every Glasgow deposit is credited to the journal, and because the
journal is in DOAJ, CreateWorksBase also makes each Glasgow copy OA with the journal's CC BY-SA licence.
Live 2026-11-16 (receipts in #627 work/): 19,410 works list the journal as primary source and 138,696 have a location
on it; the journal's own DOIs (10.32505/enlighten) are 119. 21,377 works read green only through a Glasgow copy on
the journal (19,303 of them with best_oa licence cc-by-sa). Glasgow's theses repository is a separate, correct source
(S4306401724, eprints/theses.gla.ac.uk feeds); it is not touched.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Snapshot the journal row, every oai_pmh_endpoint row on the journal, every endpoint on host eprints.gla.ac.uk,
     and any source that already looks like Glasgow's Enlighten Publications.
  2. Guards: the journal is still S4210235606 'ENLIGHTEN (Jurnal Bimbingan ...)' of type journal; at least one
     endpoint on the journal has host exactly eprints.gla.ac.uk; none of those is flagged is_journal_host.
  3. Mint (or reuse, if one exists) the repository source 'Enlighten: Publications (University of Glasgow)':
     type repository, institution 7882870 (University of Glasgow, ROR 00vtgdb53), homepage https://eprints.gla.ac.uk/,
     country GB, is_oa false (a repository; Glasgow's record-level OA flags and fetched PDFs decide each copy).
  4. Re-bind only those Glasgow endpoints from the journal to it (guarded UPDATE ... WHERE source_id = journal).
     Any other endpoint on the journal (its own OJS feed, if registered) stays.

Run on desk:
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob627-glasgow-enlighten-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob627-glasgow-enlighten-2026-11-16.py --execute
The receipt is printed as JSON after the RECEIPT marker and, with --execute, written next to this file as
handoff/oxjob627-glasgow-enlighten-2026-11-16-receipt.json; commit both.

Revert (also printed in the receipt):
    ... run_registry.py handoff/oxjob627-glasgow-enlighten-2026-11-16.py --revert [--execute]
  = UPDATE oai_pmh_endpoint SET source_id = 4210235606 WHERE id IN (<receipt endpoint ids>) AND source_id = <new id>;
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
JOB = 627
TICKETS = [24629, 24641]
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit)"
JOURNAL = 4210235606
JOURNAL_NAME_PREFIX = "enlighten (jurnal bimbingan"
HOST = "eprints.gla.ac.uk"
EXPECTED_ENDPOINT = "9404d12c0875d18ff0e"  # Rohan's 2026-07-20 read; informational, the host decides
GLASGOW = 7882870
NEW_NAME = "Enlighten: Publications (University of Glasgow)"
NEW_HOMEPAGE = "https://eprints.gla.ac.uk/"
RECEIPT_PATH = os.path.join(HERE, "oxjob627-glasgow-enlighten-2026-11-16-receipt.json")

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
    assert (j["display_name"] or "").strip().lower().startswith(JOURNAL_NAME_PREFIX), j["display_name"]
    assert j["type"] == "journal", j["type"]

    on_journal = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE source_id = :id ORDER BY id", id=JOURNAL)
    receipt["before"]["endpoints_on_journal"] = on_journal
    for e in on_journal:
        print(f"  on journal: endpoint {e['id']} host={host_of(e.get('pmh_url'))} set={e.get('pmh_set')} "
              f"status={e.get('status')} is_journal_host={e.get('is_journal_host')}")
    glasgow = [e for e in on_journal if host_of(e.get("pmh_url")) == HOST]
    assert glasgow, f"no {HOST} endpoint is bound to S{JOURNAL} any more: already fixed? nothing written"
    for e in glasgow:
        assert e.get("is_journal_host") in (False, None, "False"), f"{e['id']} is flagged is_journal_host: stop, look"
    if EXPECTED_ENDPOINT not in {e["id"] for e in glasgow}:
        log(f"note: expected endpoint {EXPECTED_ENDPOINT} not among {[e['id'] for e in glasgow]} (host decides)")
    others = [e for e in on_journal if e not in glasgow]
    log(f"journal keeps {len(others)} endpoint(s): {[(e['id'], host_of(e.get('pmh_url'))) for e in others]}")

    anywhere = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE pmh_url ILIKE :p ORDER BY id", p=f"%{HOST}%")
    receipt["before"]["endpoints_on_host"] = anywhere
    elsewhere = [e for e in anywhere if int(e["source_id"]) != JOURNAL and host_of(e.get("pmh_url")) == HOST]
    if elsewhere:
        log(f"note: {HOST} endpoints already on other sources (untouched): "
            f"{[(e['id'], e['source_id'], e.get('pmh_set')) for e in elsewhere]}")

    existing = rows(conn, "SELECT * FROM sources WHERE lower(display_name) = lower(:n) "
                          "OR (type = 'repository' AND homepage_url ILIKE :h) ORDER BY id",
                    n=NEW_NAME, h=f"%{HOST}%")
    receipt["before"]["existing_glasgow_sources"] = existing
    minted = False
    if existing:
        assert len(existing) == 1, f"more than one candidate source, look first: {[x['id'] for x in existing]}"
        new = existing[0]
        assert new["type"] == "repository" and int(new["institution_id"] or 0) == GLASGOW, new
        log(f"reusing existing source {new['id']} '{new['display_name']}'")
    else:
        new = jsonable(conn.execute(text(
            "INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, is_oa) "
            "VALUES (:n, 'repository', :inst, :h, 'GB', false) "
            "RETURNING id, display_name, type, institution_id, homepage_url, country_code, is_oa"),
            {"n": NEW_NAME, "inst": GLASGOW, "h": NEW_HOMEPAGE}).mappings().one())
        minted = True
        log(f"minted source {new['id']} '{NEW_NAME}' (repository, institution {GLASGOW}, GB)")
    new_id = int(new["id"])

    ids = [e["id"] for e in glasgow]
    moved = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :new "
                       "WHERE id = ANY(:ids) AND source_id = :old AND is_journal_host = false "
                       "RETURNING id, pmh_url, pmh_set, source_id, status", new=new_id, ids=ids, old=JOURNAL)
    assert len(moved) == len(ids), f"guard: expected {len(ids)} rows re-bound, got {len(moved)}; nothing written"
    for m in moved:
        log(f"endpoint {m['id']} ({m['pmh_url']} set={m['pmh_set']}): source {JOURNAL} -> {new_id}")

    receipt["after"]["new_source"] = rows(conn, "SELECT * FROM sources WHERE id = :id", id=new_id)
    receipt["after"]["endpoints_on_journal"] = rows(conn, "SELECT id, pmh_url, pmh_set, source_id FROM oai_pmh_endpoint "
                                                          "WHERE source_id = :id ORDER BY id", id=JOURNAL)
    receipt["after"]["endpoints_on_new"] = rows(conn, "SELECT id, pmh_url, pmh_set, source_id FROM oai_pmh_endpoint "
                                                      "WHERE source_id = :id ORDER BY id", id=new_id)
    assert not [e for e in receipt["after"]["endpoints_on_journal"] if host_of(e.get("pmh_url")) == HOST]
    receipt["new_source_id"] = new_id
    receipt["minted"] = minted
    receipt["endpoint_ids"] = ids
    receipt["revert"].append(f"UPDATE oai_pmh_endpoint SET source_id = {JOURNAL} WHERE id IN "
                             f"({', '.join(repr(i) for i in ids)}) AND source_id = {new_id};")
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
