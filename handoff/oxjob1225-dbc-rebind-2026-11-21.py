"""Dolnośląska Biblioteka Cyfrowa (dbc.wroc.pl) gets its own repository source and leaves the journal
Prace Naukowe Uniwersytetu Ekonomicznego we Wrocławiu (oxjob #1225, ZD 22707: works hosted at dbc.wroc.pl are filed under this journal though most are not published in it). Support factory #2254, written off desk 2026-11-21 (lane #2263), cloned from this job's WUR/SESMO script
(executed 16 Nov); run on desk from the openalex-sources checkout. DBC step 2: work/dbc/README.md § Step 2.

Why: S4210167977 'Prace Naukowe Uniwersytetu Ekonomicznego we Wrocławiu' (ISSN 1899-3192 / 2392-0041, DOAJ from 2017)
has the whole Lower Silesian Digital Library feed bound to it (endpoint 966fcd91a2df3d327eb, 85,147 records): PN's own
articles (24,341 records in DBC's PrNaukUE set), ~25 other journals' articles, old prints, newspapers, books, standards and
theses. Its homepage_url is DBC's (http://www.dbc.wroc.pl/dlibra), not the journal's (DOAJ: https://journals.ue.wroc.pl/pn).
DBC's own pages: "Serwis tworzony jest przez Politechnikę Wrocławską, koordynatora Konsorcjum Dolnośląskiej Biblioteki
Cyfrowej" (fetched 21 Nov 2026): run by Wrocław University of Science and Technology (I11923345, ROR 008fyn775).
OpenAlex has no source for DBC.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Snapshot the journal, every oai_pmh_endpoint row on it, every endpoint on host dbc.wroc.pl, any existing DBC source.
  2. Guards: the journal is still S4210167977 of type journal; endpoint 966fcd91a2df3d327eb is on it with host
     dbc.wroc.pl and is not is_journal_host.
  3. Mint (or reuse) 'Dolnośląska Biblioteka Cyfrowa (Politechnika Wrocławska)': type repository, institution 11923345,
     homepage https://dbc.wroc.pl/, PL, is_oa false.
  4. Re-bind only the dbc.wroc.pl endpoints from the journal to it (guarded UPDATE ... WHERE source_id = journal).
  5. The journal's homepage_url -> https://journals.ue.wroc.pl/pn, only while it still points at dbc.wroc.pl.
THEN, same session, the rows that bind PN's own records and the other journals' records back (they need the new id):
  INSERT INTO openalex.sources.article_page_journal_sources (provenance, native_id, source_id, replaces_source_id,
    platform, issn, journal_title, fetched_at, oxjob)
  SELECT provenance, native_id, source_id, <new_source_id from the receipt>, platform, issn, journal_title, fetched_at, oxjob
  FROM openalex.oxlane.oxjob1225_dbc_step2_rows;   -- 42,751 rows: PrNaukUE 24,341 -> S4210167977, 18,410 -> 25 journals
Effect after the next sources build + End 2 End (with step 1's rows or without): 65,879 works re-stamped (40,613 visible);
24,062 visible works leave S4210167977, 16,243 of them as primary; 14,459 visible get their DBC copy on the new repository
source; 595 visible + 839 xpac works (2017+ theses, books, articles open today only through PN's DOAJ flag) read closed:
policy choice follows RULINGS § 3 "A repository record alone never makes a work OA" (#1165), as the WUR move did.
Size: pre-clear the night (walden preclear_e2e.py --by jason, add 79,100 = 65,879 x 1.2; less step 1's 18,100 if step 1
already rode).

Run on desk:
    cp <this file> ~/ox/openalex-sources/handoff/oxjob1225-dbc-rebind-2026-11-21.py && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-dbc-rebind-2026-11-21.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-dbc-rebind-2026-11-21.py --execute
Revert: ... --revert [--execute]
  = UPDATE oai_pmh_endpoint SET source_id = 4210167977 WHERE id IN (<receipt endpoint ids>) AND source_id = <new id>;
    homepage_url back to the receipt's before value; DELETE FROM sources WHERE id = <new id> (only if minted and unbound);
    and DELETE FROM openalex.sources.article_page_journal_sources WHERE platform = 'dbc' AND oxjob = 1225 AND
    replaces_source_id = <new id>.
Checks after the night: https://api.openalex.org/works?filter=primary_location.source.id:S4210167977 (expect ~24,900
visible, was 41,162 on 21 Nov; ~35,900 if only step 1 had ridden);
https://api.openalex.org/works?filter=locations.source.id:<new id> (expect ~14,500 visible).
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
JOB = 1225
TICKETS = [22707]
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit)"
JOURNAL = 4210167977
JOURNAL_NAME_PREFIX = "prace naukowe uniwersytetu ekonomicznego we wroc"
HOST = "dbc.wroc.pl"
EXPECTED_ENDPOINT = "966fcd91a2df3d327eb"
INST = 11923345  # Wrocław University of Science and Technology (DBC consortium coordinator)
NEW_NAME = "Dolnośląska Biblioteka Cyfrowa (Politechnika Wrocławska)"
NEW_HOMEPAGE = "https://dbc.wroc.pl/"
JOURNAL_HOMEPAGE = "https://journals.ue.wroc.pl/pn"  # DOAJ record of 2392-0041
RECEIPT_PATH = os.path.join(HERE, "oxjob1225-dbc-rebind-2026-11-21-receipt.json")

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
    dbc = [e for e in on_journal if host_of(e.get("pmh_url")) == HOST]
    assert dbc, f"no {HOST} endpoint is bound to S{JOURNAL} any more: already fixed? nothing written"
    assert EXPECTED_ENDPOINT in [str(e["id"]) for e in dbc], f"endpoint {EXPECTED_ENDPOINT} not among {[e['id'] for e in dbc]}: look first"
    for e in dbc:
        assert e.get("is_journal_host") in (False, None, "False"), f"{e['id']} is flagged is_journal_host: stop, look"
    others = [e for e in on_journal if e not in dbc]
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
    receipt["before"]["existing_dbc_sources"] = existing
    minted = False
    if existing:
        assert len(existing) == 1, f"more than one candidate source, look first: {[x['id'] for x in existing]}"
        new = existing[0]
        assert new["type"] == "repository" and int(new["institution_id"] or 0) == INST, new
        log(f"reusing existing source {new['id']} '{new['display_name']}'")
    else:
        new = jsonable(conn.execute(text(
            "INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, is_oa) "
            "VALUES (:n, 'repository', :inst, :h, 'PL', false) "
            "RETURNING id, display_name, type, institution_id, homepage_url, country_code, is_oa"),
            {"n": NEW_NAME, "inst": INST, "h": NEW_HOMEPAGE}).mappings().one())
        minted = True
        log(f"minted source {new['id']} '{NEW_NAME}' (repository, institution {INST}, PL)")
    new_id = int(new["id"])

    ids = [e["id"] for e in dbc]
    moved = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :new "
                       "WHERE id = ANY(:ids) AND source_id = :old AND coalesce(is_journal_host, false) = false "
                       "RETURNING id, pmh_url, pmh_set, source_id, status", new=new_id, ids=ids, old=JOURNAL)
    assert len(moved) == len(ids), f"guard: expected {len(ids)} rows re-bound, got {len(moved)}; nothing written"
    for m in moved:
        log(f"endpoint {m['id']} ({m['pmh_url']} set={m['pmh_set']}): source {JOURNAL} -> {new_id}")

    hp = rows(conn, "UPDATE sources SET homepage_url = :h WHERE id = :id AND homepage_url ILIKE :old "
                    "RETURNING id, homepage_url", h=JOURNAL_HOMEPAGE, id=JOURNAL, old=f"%{HOST}%")
    receipt["journal_homepage_before"] = j.get("homepage_url")
    receipt["journal_homepage_changed"] = bool(hp)
    log(f"journal homepage_url {j.get('homepage_url')!r} -> {JOURNAL_HOMEPAGE!r}" if hp else
        f"journal homepage_url left as {j.get('homepage_url')!r} (not a {HOST} URL)")
    if hp:
        receipt["revert"].append(f"UPDATE sources SET homepage_url = {j.get('homepage_url')!r} WHERE id = {JOURNAL};")

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
    if r.get("journal_homepage_changed"):
        n = conn.execute(text("UPDATE sources SET homepage_url = :h WHERE id = :id AND homepage_url = :cur"),
                         {"h": r.get("journal_homepage_before"), "id": JOURNAL, "cur": JOURNAL_HOMEPAGE}).rowcount
        log(f"journal homepage_url back to {r.get('journal_homepage_before')!r} ({n} row)")
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
