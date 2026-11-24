"""Repository feeds filed under a source that names someone else (oxjob #2591). Written by the #2591 session on desk
2026-11-24 from #2315's wave 2 script (oxjob2315-repo-feeds-wave2-2026-11-18.py).

Why: walden binds every repository record to its endpoint's source (CreateLocationsWithSources: endpoint_to_source).
Twelve whole-repository OAI feeds below are bound to a source minted in the 2022-10-17 / 2023-05-22 bulk mints and named
from one sample record's publisher or journal field, or from the homepage's error-page title. So Politecnico di
Milano's IRIS (143K visible works) is credited to 'Virtual Community of Pathological Anatomy (University of Castilla La
Mancha)', Palermo's IRIS to 'Nova Science Publishers', UMass Chan's eScholarship to 'JAMA Network (American Medical
Association)', Riga Technical University's ORTUS to the journal 'Science', IISc's ePrints to 'NOT FOUND REPOSITORY'.
Found by #1857 wave 26 (copy-binding finding 1).

Bar (#2591 work/): every location these feeds put on a work, by the record's own OAI namespace and landing host
(identity_all.tsv): 95-100% per feed are the target repository's own records, except the CERN feed (below); fresh random
40 visible works per kind (seed 25911124, bar_verdicts.tsv): 40/40 'another institution' and 40/40 'error-page /
one-programme name' are the target repository's records, 0/80 the bound source's. Institution ids checked live (none
merged). No source below has is_oa true, so no work's is_oa changes through a source flag; ORTUS moves from a journal to a
repository source, so its ~2K open works may read green instead of bronze / hybrid.
Mixed-feed exception, CERN: the endpoint ilcdoc.linearcollider.org carries 79% cds.cern.ch records + 2.3% INSPIRE-id
records landing on CDS (81.3% CDS) and 18.6% ILC agenda (Indico) records. By visible works it is 39,397 CDS vs 355 agenda
(the agenda records are almost all xpac). Rebinding makes the feed 81% right instead of 19% (99% instead of 1% by visible
works); the agenda records move from 'International Linear Collider' to CDS. Accepted as partial progress (data-bugs
charter rule 1); a record-level split is a later step.

What it does, in ONE transaction (dry run by default, rolled back):
  REBIND (the repository already has its own source, bound through a second registration of the same OAI server):
    RUJ, CERN CDS (the 'International Linear Collider' feed ilcdoc.linearcollider.org serves CDS records; 99% of its
    visible works are CDS records, the ~355 ILC agenda ones go with them), eScholarship@UMassChan, IRIS UniPA, PuSH.
  MINT + REBIND (no source of its own; the bound source keeps other content): ORTUS (Riga Technical University), off
    the journal Science; the Argentine Ministry of Education's repository, off S4306400132 (that source also holds
    Universidad Central de Chile's DataCite DOIs, so it is not renamed).
  RENAME in place (the source holds nothing but this feed: no ISSN, no DataCite client, only this feed's endpoints, a
    handful of MAG copies of the same records): IRIS Politecnico di Milano, DigitalCommons@UMaine, Leuphana's research
    portal, ePrints@IISc, ESPOL's DSpace. Name, institution (host), homepage, country and publisher text are set.
  FILL (only if empty): host institution and country of two rebind targets, eScholarship@UMassChan and PuSH.
Held, not in this script: Taipei Medical U / Nanhua (one feed mixing two universities' records under placeholder ids),
Dove Medical Press and IntechOpen (publisher feeds; #2572's route).

Run on desk from the openalex-sources checkout or worktree:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2591-feeds-on-wrong-sources-2026-11-24.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2591-feeds-on-wrong-sources-2026-11-24.py --execute
Revert: ... --revert [--execute]: every rebound endpoint back to its old source (guarded on the new one and its URL),
every renamed source back to its before-row (only if all five written columns still hold what was written), every fill
back to NULL (only if it still holds the written value); any conflict aborts the whole revert. Minted sources are kept
(0 works once their endpoint moves back; an id the API has published is never deleted); add --delete-minted to delete
them before the night publishes. Live after the next Sources build + End 2 End.
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
DELETE_MINTED = "--delete-minted" in sys.argv
JOB = 2591
TICKETS = [22707]
APPROVED = ("oxjob #2591 under standing authority (data-bugs charter rule 3: reversible registry edit; bar 80/80 fresh "
            "sample; by OAI namespace 95-100% of each feed's records are the target repository's, CERN feed 81.3% "
            "(99% of visible works) as a recorded mixed-feed exception)")
RECEIPT_PATH = os.path.join(HERE, "oxjob2591-feeds-on-wrong-sources-2026-11-24-receipt.json")

# key: (endpoint, pmh_url, old source id, old source name, target id, target name); target host checked in TARGET_INST
REBIND = {
    "ruj": ("a97c624b20b4aa90dbe", "http://ruj.uj.edu.pl/oai/request", 4306400316,
            "Homo Politicus (Academy of Humanities and Economics in Lodz)",
            4306401249, "Jagiellonian University Repository (Jagiellonian University)"),
    "cds": ("b5c715cc5aa3bf447d2", "http://ilcdoc.linearcollider.org/oai2d", 4306400735, "International Linear Collider",
            4306402195, "CERN Document Server (European Organization for Nuclear Research)"),
    "umasschan": ("3d1ce8e3e96ff1a0f7c", "http://escholarship.umassmed.edu/cgi/oai2.cgi", 4306400883,
                  "The Journal of the American Medical Association (JAMA) Network (American Medical Association)",
                  7407064370, "eScholarship@UMassChan"),
    "unipa": ("7250bb1d842c3888d8e", "http://iris.unipa.it/oai/request", 4306401065,
              "Nova Science Publishers (Nova Science Publishers, Inc.)", 4377196289, "IRIS UniPA (University of Palermo)"),
    "push": ("aeed578ca97a5c9e54a", "http://push-zb.helmholtz-muenchen.de/oai2/oai2.php", 4377196115, "Site cant be reached",
             7407055352, "PuSH - Publication Server of Helmholtz Zentrum München"),
}
# expected host institution of each rebind target now (None = empty; FILL sets it)
TARGET_INST = {4306401249: 126596746, 4306402195: 67311998, 7407064370: None, 4377196289: 900890020, 7407055352: None}
# key: (endpoint, pmh_url, old source id, old source name, spec); spec["hosts"] = every host alias checked for a duplicate
MINT = {
    "ortus": ("785a8aa99551edbaf62", "http://ortus.rtu.lv/science/oai/openaire", 3880285, "Science",
              {"name": "ORTUS (Riga Technical University)", "inst": 201787326, "home": "https://ortus.rtu.lv/science/",
               "cc": "LV", "hosts": ["ortus.rtu.lv", "science.rtu.lv"], "publisher": "Riga Technical University"}),
    "ar_educacion": ("5da46a7173087821adf", "http://repositorio.educacion.gov.ar:8080/oai/request", 4306400132,
                     "Repositorio Institucional",
                     {"name": "Repositorio Institucional del Ministerio de Educación (Argentina)", "inst": 4210092196,
                      "home": "http://repositorio.educacion.gov.ar/", "cc": "AR", "hosts": ["educacion.gov.ar"],
                      "publisher": "Ministerio de Educación (Argentina)"}),
}
# key: (source id, old name, {endpoint: pmh_url} that must be exactly the ones bound to it, new values)
RENAME = {
    "polimi": (4306400312, "Virtual Community of Pathological Anatomy (University of Castilla La Mancha)",
               {"34d723dfd8819a9ccbb": "http://re.public.polimi.it/oai/request"},
               {"display_name": "IRIS Politecnico di Milano", "institution_id": 93860229,
                "homepage_url": "https://re.public.polimi.it/", "country_code": "IT", "publisher": "Politecnico di Milano"}),
    "umaine": (4306402196, "DigitalCommons (California Polytechnic State University)",
               {"22f16ad5a9cb2c02031": "http://digitalcommons.library.umaine.edu/do/oai/"},
               {"display_name": "DigitalCommons@UMaine (University of Maine)", "institution_id": 7947594,
                "homepage_url": "https://digitalcommons.library.umaine.edu/", "country_code": "US",
                "publisher": "University of Maine"}),
    "leuphana": (4306400489, "Multilingual Matters (Channel View Publications)",
                 {"e61a715fff48472d41f": "http://pure.leuphana.de/ws/oai"},
                 {"display_name": "Leuphana Research Portal (Leuphana University of Lüneburg)", "institution_id": 113456305,
                  "homepage_url": "https://fis.leuphana.de/", "country_code": "DE",
                  "publisher": "Leuphana University of Lüneburg"}),
    "iisc": (4377196309, "NOT FOUND REPOSITORY (Indian Institute of Science Bangalore)",
             {"38b6899f00d0ca86f03": "http://eprints.iisc.ac.in/cgi/oai2", "942100ea666ae8c24b8": "http://eprints.iisc.ernet.in/perl/oai2",
              "e89577042ed9e995139": "http://eprints.iisc.ernet.in/cgi/oai2"},
             {"display_name": "ePrints@IISc (Indian Institute of Science Bangalore)", "institution_id": 59270414,
              "homepage_url": "https://eprints.iisc.ac.in/", "country_code": "IN",
              "publisher": "Indian Institute of Science Bangalore"}),
    "espol": (4306401060, "Maestría en Control de Operaciones y Gestión Logística (Escuela Superior Politécnica del Litoral)",
              {"3cac3b142cf30d8dbc1": "http://dspace.espol.edu.ec/oai/request"},
              {"display_name": "Repositorio DSpace ESPOL (Escuela Superior Politécnica del Litoral)", "institution_id": 2135383,
               "homepage_url": "http://www.dspace.espol.edu.ec/", "country_code": "EC",
               "publisher": "Escuela Superior Politécnica del Litoral"}),
}
# key: (source id, expected name, column values to set only where the column is NULL)
FILL = {
    "umasschan": (7407064370, "eScholarship@UMassChan",
                  {"institution_id": 166722992, "country_code": "US",
                   "publisher": "University of Massachusetts Chan Medical School"}),
    "push": (7407055352, "PuSH - Publication Server of Helmholtz Zentrum München", {"institution_id": 3018134672}),
}
# alternate titles a renamed source may keep (IISc's are its own repository's names); any other title stops the run
KEEP_ALT = {4377196309: {"ePrints-IISc. (Indian Institute of Science Bangalore)", "ePrints@IISc (Indian Institute of Science)"}}
RENAME_COLS = ["display_name", "institution_id", "homepage_url", "country_code", "publisher"]
SRC_COLS = ("id, display_name, type, issn_l, issns, datacite_ids, publisher, publisher_id, institution_id, homepage_url, "
            "country_code, is_oa, alternate_titles, updated_date")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "tickets": TICKETS, "approved_by": APPROVED,
           "before": {}, "actions": [], "after": {}, "moves": [], "renames": [], "fills": [], "revert": []}


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


def source(conn, sid, name=None, typ=None):
    r = rows(conn, f"SELECT {SRC_COLS} FROM sources WHERE id = :id FOR UPDATE", id=sid)
    assert len(r) == 1, f"source S{sid} not found (merged?)"
    if name is not None:
        assert norm(r[0]["display_name"]) == norm(name), (sid, r[0]["display_name"])
    if typ is not None:
        assert r[0]["type"] == typ, (sid, r[0]["type"])
    return r[0]


def endpoint(conn, ep, url, old):
    er = rows(conn, "SELECT id, pmh_url, pmh_set, source_id, status, is_journal_host FROM oai_pmh_endpoint WHERE id = :id "
                    "FOR UPDATE", id=ep)
    assert len(er) == 1, f"endpoint {ep} not found"
    e = er[0]
    assert int(e["source_id"]) == old, f"{ep} is bound to {e['source_id']}, not S{old}: already moved? nothing written"
    assert e.get("pmh_url") == url, (ep, e.get("pmh_url"))
    assert not e.get("pmh_set"), f"{ep} has a set ({e.get('pmh_set')}): stop, look"
    assert e.get("is_journal_host") in (False, None), f"{ep} is flagged is_journal_host: stop, look"
    return e


def rebind(conn, ep, url, old, new):
    moved = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :new WHERE id = :ep AND source_id = :old "
                       "AND pmh_url = :url AND pmh_set IS NULL AND coalesce(is_journal_host, false) = false "
                       "RETURNING id, pmh_url, pmh_set, source_id, status", new=new, ep=ep, old=old, url=url)
    assert len(moved) == 1, f"guard: expected 1 row re-bound for {ep}, got {len(moved)}; nothing written"
    receipt["revert"].append(f"UPDATE oai_pmh_endpoint SET source_id = {old} WHERE id = '{ep}' AND source_id = {new};")


def apply(conn):
    for key, (ep, url, old, oname, tid, tname) in REBIND.items():
        o = source(conn, old, oname)
        t = source(conn, tid, tname, "repository")
        assert t["institution_id"] == TARGET_INST[tid], (tid, "host institution changed", t["institution_id"])
        e = endpoint(conn, ep, url, old)
        receipt["before"][key] = {"endpoint": e, "old_source": o, "target": t}
        rebind(conn, ep, url, old, tid)
        log(f"{key}: {ep} ({url}) S{old} '{oname}' -> S{tid} '{t['display_name']}'")
        receipt["moves"].append({"key": key, "endpoint": ep, "pmh_url": url, "old_source": old, "new_source": tid,
                                 "minted": False})

    for key, (ep, url, old, oname, spec) in MINT.items():
        o = source(conn, old, oname)
        e = endpoint(conn, ep, url, old)
        existing = rows(conn, "SELECT id, display_name, type, homepage_url FROM sources WHERE lower(display_name) = lower(:n) "
                              "OR (type = 'repository' AND (homepage_url ~* :h OR alternate_titles::text ~* :h))",
                        n=spec["name"], h="(" + "|".join(x.replace(".", "\\.") for x in spec["hosts"]) + ")")
        receipt["before"][key] = {"endpoint": e, "old_source": o, "existing_same_name_or_host": existing}
        assert not existing, f"a source already carries {spec['name']} / {spec['hosts']}: look first: {existing}"
        new = jsonable(conn.execute(text(
            "INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, publisher, is_oa, "
            "created_date, updated_date) VALUES (:n, 'repository', :inst, :h, :cc, :p, false, now(), now()) "
            "RETURNING id, display_name, type, institution_id, homepage_url, country_code, publisher, is_oa"),
            {"n": spec["name"], "inst": spec["inst"], "h": spec["home"], "cc": spec["cc"], "p": spec["publisher"]}
        ).mappings().one())
        nid = int(new["id"])
        rebind(conn, ep, url, old, nid)
        receipt["revert"].append(f"DELETE FROM sources WHERE id = {nid};  -- only once nothing is bound to it")
        log(f"{key}: minted S{nid} '{spec['name']}' (repository, I{spec['inst']}, {spec['cc']}); "
            f"{ep} ({url}) S{old} '{oname}' -> S{nid}")
        receipt["moves"].append({"key": key, "endpoint": ep, "pmh_url": url, "old_source": old, "new_source": nid,
                                 "minted": True, "minted_name": spec["name"]})

    for key, (sid, oname, eps, vals) in RENAME.items():
        s = source(conn, sid, oname, "repository")
        assert not s["issn_l"] and not s["issns"], (sid, "has an ISSN: stop, look")
        assert not s["datacite_ids"], (sid, "has a DataCite client: stop, look")
        assert s["publisher_id"] is None, (sid, "linked to a publisher record: stop, look")
        alt = set(s["alternate_titles"] or [])
        assert alt <= KEEP_ALT.get(sid, set()), (sid, "alternate titles of another entity", alt)
        bound = sorted(r["id"] for r in rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE source_id = :id", id=sid))
        assert bound == sorted(eps), (sid, "bound endpoints changed", bound)
        for ep, url in eps.items():
            endpoint(conn, ep, url, sid)
        receipt["before"][key] = {"source": s, "endpoints": bound}
        sets = ", ".join(f"{c} = :{c}" for c in RENAME_COLS)
        done = rows(conn, f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id AND display_name = :old "
                          f"AND type = 'repository' RETURNING {SRC_COLS}", id=sid, old=s["display_name"], **vals)
        assert len(done) == 1, f"guard: rename of S{sid} hit {len(done)} rows"
        log(f"{key}: S{sid} renamed '{oname}' -> '{vals['display_name']}' (I{vals['institution_id']}, {vals['country_code']})")
        receipt["renames"].append({"key": key, "source": sid, "before": {c: s[c] for c in RENAME_COLS}, "after": vals})

    for key, (sid, name, vals) in FILL.items():
        s = source(conn, sid, name, "repository")
        receipt["before"][f"fill_{key}"] = s
        written = {}
        for col, v in vals.items():
            assert s[col] is None or s[col] == v, (sid, col, "holds another value", s[col])
            n = conn.execute(text(f"UPDATE sources SET {col} = :v, updated_date = now() WHERE id = :id AND {col} IS NULL"),
                             {"v": v, "id": sid}).rowcount
            if n == 1:
                written[col] = v
        log(f"fill {key}: S{sid} {written or 'nothing (already set)'}")
        receipt["fills"].append({"key": key, "source": sid, "written": written})

    ids = [m["endpoint"] for m in receipt["moves"]] + [e for v in RENAME.values() for e in v[2]]  # v[2] is a dict
    receipt["after"]["endpoints"] = rows(conn, "SELECT e.id, e.pmh_url, e.source_id, s.display_name, s.type, s.institution_id "
                                               "FROM oai_pmh_endpoint e JOIN sources s ON s.id = e.source_id "
                                               "WHERE e.id = ANY(:ids) ORDER BY e.id", ids=ids)
    touched = ([m["new_source"] for m in receipt["moves"]] + [v[0] for v in RENAME.values()] + [v[0] for v in FILL.values()])
    receipt["after"]["sources"] = rows(conn, f"SELECT {SRC_COLS} FROM sources WHERE id = ANY(:ids) ORDER BY id", ids=touched)
    assert len(receipt["moves"]) == len(REBIND) + len(MINT) and len(receipt["renames"]) == len(RENAME)


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    receipt["before"]["applied_receipt"] = {"run_at": r["run_at"], "moves": r["moves"], "renames": r["renames"],
                                            "fills": r["fills"]}
    for m in r["moves"]:
        back = rows(conn, "UPDATE oai_pmh_endpoint SET source_id = :o WHERE id = :ep AND source_id = :new AND pmh_url = :url "
                          "RETURNING id, source_id", o=m["old_source"], ep=m["endpoint"], new=m["new_source"], url=m["pmh_url"])
        assert len(back) == 1, f"guard: {m['endpoint']} is not on S{m['new_source']} any more"
        log(f"{m['endpoint']}: S{m['new_source']} -> S{m['old_source']}")
    for m in r["moves"]:
        if not m["minted"]:
            continue
        if not DELETE_MINTED:
            log(f"kept minted S{m['new_source']} '{m['minted_name']}' (pass --delete-minted to delete it before it is published)")
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
    for rn in r["renames"]:
        sets = ", ".join(f"{c} = :{c}" for c in RENAME_COLS)
        b = {c: (int(v) if c == "institution_id" and v is not None else v) for c, v in rn["before"].items()}
        guard = " AND ".join(f"{c} IS NOT DISTINCT FROM :cur_{c}" for c in RENAME_COLS)
        n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id AND {guard}"),
                         {"id": rn["source"], **b, **{f"cur_{c}": v for c, v in rn["after"].items()}}).rowcount
        assert n == 1, f"guard: S{rn['source']} changed since it was written: {rn['after']}"
        log(f"S{rn['source']}: back to '{b['display_name']}'")
    for f in r["fills"]:
        for col, v in f["written"].items():
            n = conn.execute(text(f"UPDATE sources SET {col} = NULL, updated_date = now() WHERE id = :id AND {col} = :v"),
                             {"id": f["source"], "v": v}).rowcount
            assert n == 1, f"guard: S{f['source']}.{col} no longer {v}"
            log(f"S{f['source']}: {col} {v} -> NULL")


def main():
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn)
            else:
                apply(conn)
            if not EXECUTE:
                raise DryRun()
            # written before the commit, so the before-rows survive a crash between commit and the final receipt
            pend = RECEIPT_PATH.replace(".json", ("-revert" if REVERT else "") + "-pending.json")
            assert REVERT or not os.path.exists(RECEIPT_PATH), "an executed receipt exists already: refusing to run twice"
            json.dump(receipt, open(pend, "w"), ensure_ascii=False, indent=1, default=str)
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
