"""oxjob #954 (support factory #2254), desk apply lane 17 Nov 2026: the repository and conference steps of the #954 job's
work/desk_steps_2026-11-17.md § 2-3 (prepared off-desk by #2263), plus 7233's S:A feed. ONE transaction, dry run by default.
Repositories go in as repositories (type 'repository', is_journal_host false, green copies; Casey 2026-10-25, #1822).
Every feed answered OAI Identify from desk at 12:10 CT 17 Nov (UCU also to the harvester's own User-Agent).

  ZD 26112  mint 'Uganda Christian University Digital Institutional Repository' (I92808645, UG) + its DSpace feed
  ZD 3973   RECERCAT S4306402147: endpoint 4b73a6a8562e503572f http://oai.recercat.cat/request (failing; watermark
            2021-06-27) -> https://recercat.cat/oai/request (Identify 'RECERCAT'; 459,998 records on 16 Nov)
  ZD 23262  Lyrasis: mint 'Scholarly Commons @Augusta University' (I25041050, US) and 'Arizona Christian University
            Repository' (I2799487676, US), each + feed; Newman Kansas feed (repository.newmanu.edu) on S4306402633, whose
            host is already I112679378 Newman University (Kansas), + homepage/country; its old endpoint
            mddgz73rxbq6lmeheetj harvests Birmingham Newman's CREST feed (UK; no answer from desk 17 Nov, last harvest
            2024-11), so it moves to a new source 'Newman University Repository (Birmingham Newman University)'
            (I210300861, GB). Retiring it would need Kyle (SOP § 4); on the right source it harms nothing.
  ZD 1055   Rosario S4306401708: endpoint e32bf056ca6323edcac /oai/request (TLS drops) -> DSpace 7 baseURL
            https://repository.urosario.edu.co/server/oai/request (both answer 200 today; Identify's baseURL is the latter)
  ZD 7233   S:A (Supplements:Articles, 256 records) on S2738442270 Archeologia e Calcolatori: add_journal's overlap guard
            held it because 'S:A' and the registered 'J:A' row have different key families, but in OJS 'J:A' and 'S:A'
            are disjoint sets (ListSets: Journal:Article vs Supplements:Articles) and both rows feed the same source.
  ZD 21915  mint conference source 'Partner conferences of the International Scientific Journal Research Focus'
            (the series, #1528 POLICY), publisher 'Ilm-fan va innovatsiyalar akademiyasi LLC', UZ. No feed: walden's
            name-fallback tier binds the 2,143 source-less Crossref locations whose container name equals it (no source
            holds that normalized name today). is_oa stays derived (not set by hand).
Revert: the receipt lists every id and before row. Delete each minted source's oai_pmh_endpoint and sources rows;
UPDATE the two repointed endpoints' pmh_url back (receipt 'before'); move mddgz73rxbq6lmeheetj back to 4306402633;
S4306402633 homepage_url/country_code back to NULL; delete the S:A endpoint row.
"""
import json
import os
import secrets
import string
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "minted": [], "endpoints": [], "updates": []}


def eid():
    return "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(20))


def mint(c, name, typ, inst=None, home=None, cc=None, publisher=None, ticket=None):
    assert not c.execute(text("SELECT 1 FROM sources WHERE lower(display_name) = lower(:n)"), {"n": name}).fetchone(), name
    sid = c.execute(text("INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, publisher, is_oa) "
                         "VALUES (:n, :t, :i, :h, :cc, :p, false) RETURNING id"),
                    {"n": name, "t": typ, "i": inst, "h": home, "cc": cc, "p": publisher}).scalar()
    R["minted"].append({"source_id": sid, "name": name, "type": typ, "institution_id": inst, "ticket": ticket})
    print(f"MINTED {sid} {name!r} ({typ}, I{inst}, {cc})")
    return sid


def endpoint(c, url, sid, journal_host=False, pmh_set=None, ticket=None):
    host = url.split("/")[2]
    clash = c.execute(text("SELECT id, pmh_url, pmh_set FROM oai_pmh_endpoint WHERE pmh_url ILIKE :h AND coalesce(pmh_set,'') = coalesce(:s,'')"),
                      {"h": f"%{host}%", "s": pmh_set}).fetchall()
    assert not clash, (url, pmh_set, clash)
    i = eid()
    c.execute(text("INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
                   "source_id, is_journal_host, green_scrape, rand) "
                   "VALUES (:id, :u, :s, 'oai_dc', true, true, 'active', :sid, :jh, true, random())"),
              {"id": i, "u": url, "s": pmh_set, "sid": sid, "jh": journal_host})
    R["endpoints"].append({"endpoint_id": i, "pmh_url": url, "pmh_set": pmh_set, "source_id": sid, "ticket": ticket})
    print(f"REGISTERED endpoint {i} {url} [{pmh_set or '-'}] -> {sid}")
    return i


def repoint(c, endpoint_id, old, new, ticket):
    n = c.execute(text("UPDATE oai_pmh_endpoint SET pmh_url = :new WHERE id = :id AND pmh_url = :old"),
                  {"new": new, "id": endpoint_id, "old": old}).rowcount
    assert n == 1, (endpoint_id, old)
    R["updates"].append({"endpoint_id": endpoint_id, "field": "pmh_url", "before": old, "after": new, "ticket": ticket})
    print(f"REPOINTED {endpoint_id}: {old} -> {new}")


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        # 26112 Uganda Christian University
        s = mint(c, "Uganda Christian University Digital Institutional Repository", "repository", 92808645,
                 "https://ucudir.ucu.ac.ug", "UG", ticket=26112)
        endpoint(c, "https://ucudir.ucu.ac.ug/server/oai/request", s, ticket=26112)
        # 3973 RECERCAT
        repoint(c, "4b73a6a8562e503572f", "http://oai.recercat.cat/request", "https://recercat.cat/oai/request", 3973)
        # 23262 Lyrasis
        s = mint(c, "Scholarly Commons @Augusta University", "repository", 25041050, "https://scholarlycommons.augusta.edu", "US", ticket=23262)
        endpoint(c, "https://scholarlycommons.augusta.edu/server/oai/request", s, ticket=23262)
        s = mint(c, "Arizona Christian University Repository", "repository", 2799487676, "https://arizonachristian.dspacedirect.org", "US", ticket=23262)
        endpoint(c, "https://arizonachristian.dspacedirect.org/server/oai/request", s, ticket=23262)
        before = dict(c.execute(text("SELECT institution_id, homepage_url, country_code FROM sources WHERE id = 4306402633")).mappings().one())
        assert before["institution_id"] == 112679378, before
        c.execute(text("UPDATE sources SET homepage_url = 'https://repository.newmanu.edu', country_code = 'US', updated_date = now() WHERE id = 4306402633"))
        R["updates"].append({"source_id": 4306402633, "before": before, "after": {"homepage_url": "https://repository.newmanu.edu", "country_code": "US"}, "ticket": 23262})
        bham = mint(c, "Newman University Repository (Birmingham Newman University)", "repository", 210300861,
                    "https://newman.collections.crest.ac.uk", "GB", publisher="Newman University", ticket=23262)
        n = c.execute(text("UPDATE oai_pmh_endpoint SET source_id = :b WHERE id = 'mddgz73rxbq6lmeheetj' AND source_id = 4306402633"), {"b": bham}).rowcount
        assert n == 1
        R["updates"].append({"endpoint_id": "mddgz73rxbq6lmeheetj", "field": "source_id", "before": 4306402633, "after": bham, "ticket": 23262})
        print(f"MOVED endpoint mddgz73rxbq6lmeheetj 4306402633 -> {bham}")
        endpoint(c, "https://repository.newmanu.edu/server/oai/request", 4306402633, ticket=23262)
        # 1055 Rosario
        repoint(c, "e32bf056ca6323edcac", "https://repository.urosario.edu.co/oai/request",
                "https://repository.urosario.edu.co/server/oai/request", 1055)
        # 7233 Archeologia e Calcolatori Supplements:Articles
        endpoint(c, "https://www.archcalc.cnr.it/oai", 2738442270, journal_host=True, pmh_set="S:A", ticket=7233)
        # 21915 conference series
        mint(c, "Partner conferences of the International Scientific Journal Research Focus", "conference", None,
             "https://refocus-conferences.uz/index.php/PCISJRF", "UZ", publisher="Ilm-fan va innovatsiyalar akademiyasi LLC", ticket=21915)
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str))
