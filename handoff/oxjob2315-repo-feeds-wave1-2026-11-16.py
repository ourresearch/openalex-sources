"""Whole university repository feeds filed under one journal, wave 1: eight feeds go to their repository (oxjob #2315).
Written by the #2315 session on desk 2026-11-16, adapted from oxjob2291-wur-library-feed-2026-11-16.py.

Why: each endpoint below harvests a whole repository (no set, is_journal_host false) but is bound in this registry to
ONE small journal the institution hosts or digitized (The Mathematics Enthusiast, Journal of Bioresource Management,
Artium Quaestiones, ...). walden binds every repository record to its endpoint's source (CreateLocationsWithSources:
endpoint_to_source), so theses, faculty papers, digitized books and minutes are credited to that journal, and where the
journal is OA-flagged CreateWorksBase (composite_is_oa = is_oa_raw OR source_is_oa) serves every record green.
Bar (#1857, bar_draw.sql seed 18571117): 35/37 records drawn from these feeds are not the bound journal's articles.
Most of these repositories are ALREADY a source, bound through a second registration of the same OAI server (the
'twin' below, e.g. Wayne's cgi/oai2.cgi vs do/oai/): each feed moves to that same source. Two have none and get one.
Night 11-16 table (#2315 work/flip_size.tsv): 91,871 visible + 69,911 xpac works hold a location from these eight feeds.
URI's feed (31a74f9a3f9166bd178), also in #2315's scope, was already moved to Digital Commons - URI by oxjob #952 at
09:15 CT 16 Nov (its receipt work/registry_fixes_952_receipt.json); this script's guard refused it, so it is left out.

The journal's own series inside a feed: UMT 696 TME works (all keep another TME location), Wright 165 (1 does not),
UNED 395 (1 does not); #2315 work/loss.tsv. The rest have none.
OA: after the move a copy is open only on its own record's flag. #2315 work/flip_size.tsv: 2,293 visible works go
green -> closed; at most 1,041 of them carry a free file (Digital Commons records that list one, every other flip
counted as one); wave 1 keeps only feeds where that upper bound is under 3% of the feed's moved works (README § Wave 1).

What it does, in ONE transaction (dry run by default, rolled back):
  1. For each feed: snapshot the endpoint row and its journal's row. Guards: endpoint still bound to that journal (by
     id and name), pmh_url unchanged, no set, is_journal_host false.
  2. Target: an existing repository source (guard: exists, type repository, display_name as expected), or mint one
     (type repository, the name the feed's own OAI Identify gives, institution, homepage, country, is_oa false) after
     checking no repository source already carries that name or homepage host.
  3. Re-bind the endpoint (guarded UPDATE ... WHERE id = :ep AND source_id = :journal AND is_journal_host = false).
     Nothing else on the journals changes; their own Crossref/DOAJ locations stay.

Run on desk from the openalex-sources checkout or worktree:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2315-repo-feeds-wave1-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2315-repo-feeds-wave1-2026-11-16.py --execute
Revert: ... --revert [--execute]: every moved endpoint back to its journal (guarded on the new source), minted sources
deleted when nothing else is bound to them. Live after the next sources build + End 2 End (night 2026-11-17).
"""
import json
import os
import sys
import unicodedata
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB = 2315
TICKETS = [22707]
APPROVED = ("oxjob #2315 under Jason's standing authority (data-bugs charter rule 3: reversible registry edit; "
            "bar 35/37 records not the journal's; OA flip upper bound 1,041 works, under 3% per feed)")
RECEIPT_PATH = os.path.join(HERE, "oxjob2315-repo-feeds-wave1-2026-11-16-receipt.json")

# (endpoint, pmh_url, journal id, journal name, target) ; target = ('reuse', id, expected name) or ('mint', spec)
FEEDS = [
    ("bb7834da33ebecae57b", "http://scholarworks.umt.edu/do/oai/", 190740074, "The Mathematics Enthusiast",
     ("mint", {"name": "ScholarWorks at University of Montana", "inst": 6750721, "home": "https://scholarworks.umt.edu/",
               "cc": "US", "host": "scholarworks.umt.edu"})),
    ("f5a7329ded80260825f", "http://scholarship.law.nd.edu/do/oai/", 57128295, "Journal of legislation",
     ("mint", {"name": "NDLScholarship", "inst": 107639228, "home": "https://scholarship.law.nd.edu/",
               "cc": "US", "host": "scholarship.law.nd.edu"})),
    ("5aa6e8e31ca9b94882b", "http://corescholar.libraries.wright.edu/do/oai/", 2737205702, "Journal of Bioresource Management",
     ("reuse", 4306400146, "CORE Scholar (Wright State University)")),
    ("c920e07860926a2b374", "http://e-spacio.uned.es:8080/oaiprovider", 2737644977,
     "Espacio Tiempo y Forma Serie I Prehistoria y Arqueología", ("reuse", 7407055166, "UNED repository")),
    ("9ad82ff853b7b7a18b9", "http://digi.ub.uni-heidelberg.de/cgi-bin/digioai.cgi", 4210195739, "Artium Quaestiones",
     ("reuse", 7407064328, "University of Heidelberg, GERMANY, Diglit")),
    ("dc6dd39505cdbee5d8e", "http://ibdigital.uib.es/greenstone/cgi-bin/oaiserver.cgi", 4306531072, "Studia lulliana",
     ("reuse", 4306400044, "Biblioteca Digital de les Illes Balears (Universitat de les Illes Balears)")),
    ("0641949b0adf90d972c", "http://bibliotecavirtual.aragon.es/i18n/oai/oai_bibliotecavirtual.aragon.es.cmd", 4306526100,
     "Revista aragonesa de administración pública/Revista aragonesa de administración pública",
     ("reuse", 4306400211, "La Biblioteca Virtual de Aragón (Gobierno de Aragon)")),
    ("4347954ecce59f18a24", "http://bdigital.uncu.edu.ar/OAI/index.php", 109997589,
     "Revista de la Facultad de Ciencias Agrarias UNCuyo",
     ("reuse", 4377196747, "Open Access Institutional Repository (National University of Cuyo)")),
]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "tickets": TICKETS, "approved_by": APPROVED,
           "before": {}, "actions": [], "after": {}, "moves": [], "revert": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def jsonable(row):
    return {k: (None if v is None else v if isinstance(v, (list, dict, bool, int)) else str(v))
            for k, v in dict(row).items()}


def rows(conn, sql, **kw):
    return [jsonable(r) for r in conn.execute(text(sql), kw).mappings()]


def norm(s):
    return " ".join(unicodedata.normalize("NFC", s or "").strip().lower().split())


def apply(conn):
    for ep, url, jid, jname, target in FEEDS:
        jr = rows(conn, "SELECT * FROM sources WHERE id = :id", id=jid)
        assert len(jr) == 1, f"journal S{jid} not found (merged?)"
        j = jr[0]
        assert norm(j["display_name"]) == norm(jname), (jid, j["display_name"])
        er = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE id = :id", id=ep)
        assert len(er) == 1, f"endpoint {ep} not found"
        e = er[0]
        receipt["before"][ep] = {"endpoint": e, "journal": j}
        assert int(e["source_id"]) == jid, f"{ep} is bound to {e['source_id']}, not S{jid}: already moved? nothing written"
        assert (e.get("pmh_url") or "").strip() == url, (ep, e.get("pmh_url"))
        assert not e.get("pmh_set"), f"{ep} has a set ({e.get('pmh_set')}): stop, look"
        assert e.get("is_journal_host") in (False, None, "False"), f"{ep} is flagged is_journal_host: stop, look"

        minted = False
        if target[0] == "reuse":
            tid, tname = target[1], target[2]
            tr = rows(conn, "SELECT * FROM sources WHERE id = :id", id=tid)
            assert len(tr) == 1 and tr[0]["type"] == "repository", (tid, tr)
            assert norm(tr[0]["display_name"]) == norm(tname), (tid, tr[0]["display_name"])
            new_id = tid
            log(f"{ep}: reuse S{tid} '{tr[0]['display_name']}'")
        else:
            spec = target[1]
            existing = rows(conn, "SELECT id, display_name, type, homepage_url FROM sources WHERE type = 'repository' AND "
                                  "(lower(display_name) = lower(:n) OR homepage_url ILIKE :h)",
                            n=spec["name"], h=f"%{spec['host']}%")
            receipt["before"][ep]["existing_same_name_or_host"] = existing
            if existing:
                assert len(existing) == 1, f"several candidates for {spec['name']}, look first: {existing}"
                new_id = int(existing[0]["id"])
                log(f"{ep}: reuse existing S{new_id} '{existing[0]['display_name']}' (found by name/host)")
            else:
                new = jsonable(conn.execute(text(
                    "INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, is_oa) "
                    "VALUES (:n, 'repository', :inst, :h, :cc, false) "
                    "RETURNING id, display_name, type, institution_id, homepage_url, country_code, is_oa"),
                    {"n": spec["name"], "inst": spec["inst"], "h": spec["home"], "cc": spec["cc"]}).mappings().one())
                new_id = int(new["id"])
                minted = True
                log(f"{ep}: minted S{new_id} '{spec['name']}' (repository, institution I{spec['inst']}, {spec['cc']})")

        moved = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :new "
                           "WHERE id = :ep AND source_id = :old AND is_journal_host = false "
                           "RETURNING id, pmh_url, pmh_set, source_id, status", new=new_id, ep=ep, old=jid)
        assert len(moved) == 1, f"guard: expected 1 row re-bound for {ep}, got {len(moved)}; nothing written"
        log(f"{ep} ({url}): source S{jid} '{jname}' -> S{new_id}")
        receipt["moves"].append({"endpoint": ep, "journal": jid, "new_source": new_id, "minted": minted,
                                 "minted_name": target[1]["name"] if minted else None})
        receipt["revert"].append(f"UPDATE oai_pmh_endpoint SET source_id = {jid} WHERE id = '{ep}' AND source_id = {new_id};")
        if minted:
            receipt["revert"].append(f"DELETE FROM sources WHERE id = {new_id};  -- only once nothing is bound to it")

    ids = [m["endpoint"] for m in receipt["moves"]]
    receipt["after"]["endpoints"] = rows(conn, "SELECT e.id, e.pmh_url, e.source_id, s.display_name, s.type "
                                               "FROM oai_pmh_endpoint e JOIN sources s ON s.id = e.source_id "
                                               "WHERE e.id = ANY(:ids) ORDER BY e.id", ids=ids)
    receipt["after"]["minted"] = rows(conn, "SELECT * FROM sources WHERE id = ANY(:ids)",
                                      ids=[m["new_source"] for m in receipt["moves"] if m["minted"]])
    assert len(receipt["moves"]) == len(FEEDS)


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    receipt["before"]["applied_receipt"] = {"run_at": r["run_at"], "moves": r["moves"]}
    for m in r["moves"]:
        back = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :j WHERE id = :ep AND source_id = :new "
                          "RETURNING id, source_id", j=m["journal"], ep=m["endpoint"], new=m["new_source"])
        assert len(back) == 1, f"guard: {m['endpoint']} is not on S{m['new_source']} any more"
        log(f"{m['endpoint']}: S{m['new_source']} -> S{m['journal']}")
    for m in r["moves"]:
        if not m["minted"]:
            continue
        left = conn.execute(text("SELECT count(*) FROM oai_pmh_endpoint WHERE source_id = :id"),
                            {"id": m["new_source"]}).scalar()
        if left == 0:
            n = conn.execute(text("DELETE FROM sources WHERE id = :id AND display_name = :n"),
                             {"id": m["new_source"], "n": m["minted_name"]}).rowcount
            assert n == 1, n
            log(f"deleted minted source S{m['new_source']}")
        else:
            log(f"kept S{m['new_source']}: {left} other endpoint(s) bound to it")


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
    print(json.dumps(receipt, ensure_ascii=False, default=str)[:3000])


if __name__ == "__main__":
    main()
