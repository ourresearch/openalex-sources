"""Whole university repository feeds filed under one journal, wave 2: the feeds held on 16 Nov go to their repository
(oxjob #2315). Written by the #2315 session on desk 2026-11-18 from wave 1's script (oxjob2315-repo-feeds-wave1-2026-11-16.py).

Why: each endpoint below harvests a whole repository (no set, is_journal_host false) but is bound in this registry to
ONE journal (Työväentutkimus Vuosikirja for HELDA, Civil War Book Review for LSU, JICRCR for UCF STARS, ...). walden
binds every repository record to its endpoint's source (CreateLocationsWithSources: endpoint_to_source), so theses,
faculty papers, photos and minutes are credited to that journal, and where the journal is OA-flagged CreateWorksBase
(composite_is_oa = is_oa_raw OR source_is_oa) serves every record green. Bar (#1857, seed 18571117): 35/37 records
drawn from these feeds are not the bound journal's articles.

Why these were held on 16 Nov, and what changed: moving them flips ~123K visible works green -> closed, and on a random
28 flips 15 carried a free file in the repository that walden never fetches. Since 18 Nov, #2336's table
openalex.repo.repo_free_file_opened opens a repository record whose own file was fetched and is the work's own text
(walden fbb0f3bc, CreateSuperLocations). #2315 crawled the files of every flip record (set '2315_wave2_flip': the OAI
record's own file links, and for HELDA and RIULL, whose oai_dc lists no file, the ORIGINAL bitstreams their 'ore' format
lists) and added them to that table the same night, so a flip with a free file of its own stays open. SHIP below is the
set where the move makes OA clearly more right than wrong (bar at SHIP).

Targets: an existing repository source where the same OAI server already has one (HELDA, ECU, EIU, GVSU, FaHCE's
Memoria Académica, ITESO's CCDoc = Centro CONEICC de Documentación), else a mint named after the server's own OAI
Identify repositoryName with its institution: STARS (University of Central Florida) (the registry's bare 'STARS'
S7407060556 is a different series, so the name carries the institution), LSU Scholarly Repository, RIULL, Jefferson
Digital Commons (both Jefferson endpoints; the cgi/oai2.cgi one sat on 'Oxford Academix (Oxford)'), SaberULA.

What it does, in ONE transaction (dry run by default, rolled back):
  1. For each feed: snapshot the endpoint row and its journal's row. Guards: endpoint still bound to that journal (by
     id and name), pmh_url unchanged, no set, is_journal_host false.
  2. Target: an existing repository source (guard: exists, type repository, display_name as expected), or mint one
     (type repository, name, institution, homepage, country, is_oa false) after checking no repository source already
     carries that name or homepage host.
  3. Re-bind the endpoint (guarded UPDATE ... WHERE id = :ep AND source_id = :journal AND is_journal_host = false).
     Nothing else on the journals changes; their own Crossref/DOAJ locations stay.

Run on desk from the openalex-sources checkout or worktree:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2315-repo-feeds-wave2-2026-11-18.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2315-repo-feeds-wave2-2026-11-18.py --execute
Revert: ... --revert [--execute]: every moved endpoint back to its journal (guarded on the new source), minted sources
deleted when nothing else is bound to them. Live after the next sources build + End 2 End.
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
APPROVED = ("oxjob #2315 under standing authority (data-bugs charter rule 3: reversible registry edit; bar 35/37 "
            "records not the journal's; flips with a free file of their own stay open via repo_free_file_opened)")
RECEIPT_PATH = os.path.join(HERE, "oxjob2315-repo-feeds-wave2-2026-11-18-receipt.json")

# (endpoint, pmh_url, journal id, journal name, target) ; target = ('reuse', id, expected name) or ('mint', spec)
FEEDS_ALL = {
    "helda": ("6920a6236b522aa5226", "https://helda.helsinki.fi/server/oai/request", 4210213322, "Työväentutkimus Vuosikirja",
              ("reuse", 4406922691, "Helda - University of Helsinki Institutional Repository")),
    "ucf": ("6d5a9a54c46b085b851", "http://stars.library.ucf.edu/do/oai/", 4210172555,
            "Journal of International Crisis and Risk Communication Research",
            ("mint", {"name": "STARS (University of Central Florida)", "inst": 106165777,
                      "home": "https://stars.library.ucf.edu/", "cc": "US", "host": "stars.library.ucf.edu"})),
    "lsu": ("zswiv3zmeguzpgpkrl9m", "https://digitalcommons.lsu.edu/do/oai/", 4210169993, "Civil War Book Review",
            ("mint", {"name": "LSU Scholarly Repository (Louisiana State University)", "inst": 121820613,
                      "home": "https://repository.lsu.edu/", "cc": "US", "host": "repository.lsu.edu"})),
    "ecu": ("90855027ee01bac858d", "http://ro.ecu.edu.au/do/oai/", 2765015692, "Australasian Journal of Paramedicine",
            ("reuse", 4306400464, "Research Online (Edith Cowan University)")),
    "eiu": ("5812db7c9e39b46ce34", "http://thekeep.eiu.edu/cgi/oai2.cgi", 2738785026,
            "Journal of Collective Bargaining in the Academy", ("reuse", 4377196467, "The Keep (Eastern Illinois University)")),
    "fahce": ("4fe1e927e9d2eb06baf", "http://www.memoria.fahce.unlp.edu.ar/oaiserver.cgi", 4306521650, "Orientación y Sociedad",
              ("reuse", 4306401611, "Memoria Académica (Facultad de Humanidades y Ciencias de la Educación, "
                                    "Universidad Nacional de La Plata)")),
    "riull": ("088ddcc582d3e9ae4ba", "https://riull.ull.es/oai/request", 4210224415,
              "Scientia Insularum Revista de Ciencias Naturales en islas",
              ("mint", {"name": "RIULL (Universidad de La Laguna)", "inst": 158438070, "home": "https://riull.ull.es/",
                        "cc": "ES", "host": "riull.ull.es"})),
    "jefferson": ("9ee8fd05fec9ff060f5", "http://jdc.jefferson.edu/do/oai/", 4210178181, "The Medicine Forum",
                  ("mint", {"name": "Jefferson Digital Commons (Thomas Jefferson University)", "inst": 149251103,
                            "home": "https://jdc.jefferson.edu/", "cc": "US", "host": "jdc.jefferson.edu"})),
    "jefferson_twin": ("e769ffaefc08299dec0", "http://jdc.jefferson.edu/cgi/oai2.cgi", 4306400608, "Oxford Academix (Oxford)",
                       ("mint", {"name": "Jefferson Digital Commons (Thomas Jefferson University)", "inst": 149251103,
                                 "home": "https://jdc.jefferson.edu/", "cc": "US", "host": "jdc.jefferson.edu"})),
    "gvsu": ("01f00eab890a3a033f8", "http://scholarworks.gvsu.edu/cgi/oai2.cgi", 2764467594, "Language arts journal of Michigan",
             ("reuse", 4377196483, "ScholarWorks - GVSU (Grand Valley State University)")),
    "saberula": ("b16b67fc6a581dab145", "http://saber.ula.ve/dspace-oai/request", 4306500644, "Actualidad Contable FACES",
                 ("mint", {"name": "SaberULA (Universidad de Los Andes)", "inst": 203303854, "home": "http://www.saber.ula.ve/",
                           "cc": "VE", "host": "saber.ula.ve"})),
    "iteso": ("7531e14c91b05a4d6e3", "http://ccdoc.iteso.mx/acervo/oai.aspx", 2736507771, "Acervo",
              ("reuse", 7407064228, "Centro CONEICC de Documentación sobre Comunicación en México")),
}
# Which feeds ship (#2315 README Log 18 Nov 13:40 CT, bar 2): per feed, works the move rightly closes (record lists no
# file, or its file is restricted / dead) must be >= 2x the works that may close wrongly (a served PDF the free-file rule
# does not keep, a free image or video, a link still unanswered). Held: Jefferson (both endpoints; 171 vs 1,727: posters
# and photos), GVSU (245 vs 460), EIU (69 vs 340: homecoming photos). #2315 work/wave2/residual.sql.
SHIP = ["helda", "ucf", "lsu", "ecu", "fahce", "riull", "saberula", "iteso"]
FEEDS = [FEEDS_ALL[k] for k in SHIP]

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
