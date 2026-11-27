"""oxjob #954 (support factory #2254), desk wake 954-1127-1300, 27 Nov 2026: AVESIS wave 3 for a repository add request (the
AVESIS research-information systems, submitted through the retired /repositories/add form).
ONE transaction, dry run by default (rolled back); --execute commits.

Rule: D4, 15 Nov 2026 (#2254): harvest university and national research-information systems (CRIS) such as Turkey's
AVESIS; DOI-less, metadata-only records are the point. Same code as wave 2 (handoff/oxjob954-avesis-wave2-2026-11-26.py,
run 26 Nov, openalex-sources e898587; outside-reviewed): one repository source per university, name '<OpenAlex
institution name> - AVESIS', institution = that university, homepage https://avesis.<x>.edu.tr, country TR, is_oa copied
from the template source; feed https://avesis.<x>.edu.tr/api/oai2 with endpoint settings copied at run time from the live
Bursa Uludag AVESIS endpoint fabemua4ms3k2icp35uy.

Wave 3 = the 8 smallest feeds left (59K-107K records each, 616,708 records: ListIdentifiers completeListSize, probed from
desk 27 Nov 13:25 CT, one request at a time; all answer Identify in 1-3 s and a first page in 3-6 s). The 9 left after this
wave (114K-292K records, ~1.56M) are later waves. Every institution id was read on the API 27 Nov (all TR, none merged);
no source for any of these 8 hosts exists (API sources?search=avesis: 33 sources, none of these; the script re-checks).

Why it is safe to go on: wave 2's first harvests (27 Nov daily run) walked 7 of 9 feeds whole (36,528-54,167 records each,
all in repo_items by 13:28 UTC); one stopped on a bad record (mcbu, 32,800 of 57,538; checkpoint moved past it, window
harvest packaged) and one is behind a Cloudflare block of the whole university site (medipol, 403 on every path).

Size and night: each feed's first harvest runs with the daily harvest (~10:00 UTC) after this commits; records harvested
before the night's start ride that night's End 2 End as new works / new locations: about 617K stamps if every first
harvest completes (pre-clear --add 741000 for that night; an unused pre-clear is harmless). Known AVESIS failure modes
on a first harvest (job #954 PLAN, 22-27 Nov): one bad record stops the walk ('resumptionToken invalid or expired' within
seconds of a brisk page, at the same record every day) and the checkpoint stays NULL; a slow feed can time out; a
university firewall can refuse the harvester. All are handled by the morning-after check in job #954's desk steps
(bisect / checkpoint past the bad day + window harvests).

Run on desk from the openalex-sources checkout (lanes_registry login, rows only):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-wave3-2026-11-27.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-wave3-2026-11-27.py --execute
Revert (from the receipt), by state:
  - before the next daily harvest starts (~10:00 UTC; it loads the active endpoint list at its start): DELETE FROM
    oai_pmh_endpoint WHERE id IN (<receipt endpoints>); DELETE FROM sources WHERE id IN (<receipt minted>).
  - after a harvest has started: keep the rows; UPDATE oai_pmh_endpoint SET status = 'inactive' for the receipt endpoints,
    and add their records to openalex.works.location_denylist by endpoint (hides those locations; works whose only
    location is denied drop out with it).
"""
import json
import os
import secrets
import string
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
JOB = 954
TICKET = 22329
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "ticket": TICKET, "wave": 3,
     "template": None, "minted": [], "endpoints": [], "skipped": []}

TEMPLATE_ENDPOINT = "fabemua4ms3k2icp35uy"   # Bursa Uludag University - AVESIS, live since Feb 2026
TEMPLATE_SOURCE = 7407055210

# (slug, OpenAlex institution id, institution display name on the API 27 Nov, Identify repositoryName, completeListSize 27 Nov)
WAVE3 = [
    ("trakya", 12387023, "Trakya University", "AVESIS - Trakya Üniversitesi", 59592),
    ("sdu", 175124709, "Süleyman Demirel Üniversitesi", "AVESIS - Süleyman Demirel Üniversitesi", 63504),
    ("anadolu", 133743585, "Anadolu University", "AVESIS - Anadolu Üniversitesi", 63756),
    ("inonu", 20126385, "Inonu University", "AVESIS - İnönü Üniversitesi", 67427),
    ("cumhuriyet", 12700597, "Sivas Cumhuriyet Üniversitesi", "AVESIS - Sivas Cumhuriyet Üniversitesi", 69055),
    ("ogu", 51209816, "Eskişehir Osmangazi University", "AVESIS - Eskisehir Osmangazi University", 85002),
    ("ktu", 162720556, "Karadeniz Technical University", "AVESIS - Karadeniz Teknik Üniversitesi", 101139),
    ("omu", 227306068, "Ondokuz Mayıs University", "AVESIS - Ondokuz Mayıs Üniversitesi", 107233),
]


def eid():
    return "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(20))


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        t = c.execute(text("SELECT pmh_url, pmh_set, metadata_prefix, green_scrape, is_journal_host, in_walden, "
                           "ready_to_run, status, source_id FROM oai_pmh_endpoint WHERE id = :id"),
                      {"id": TEMPLATE_ENDPOINT}).mappings().one()
        assert t["source_id"] == TEMPLATE_SOURCE and "avesis.uludag.edu.tr" in (t["pmh_url"] or ""), dict(t)
        assert t["status"] == "active" and not t["is_journal_host"], dict(t)
        # the reviewed feed contract: oai_dc, no set, scheduled, in Walden (fail before any insert if the template drifted)
        assert t["metadata_prefix"] == "oai_dc" and t["pmh_set"] is None and t["ready_to_run"] and t["in_walden"], dict(t)
        R["template"] = {k: (None if v is None else str(v)) for k, v in t.items()}
        print(f"TEMPLATE {TEMPLATE_ENDPOINT}: {R['template']}")
        ts = c.execute(text("SELECT type, is_oa FROM sources WHERE id = :id"), {"id": TEMPLATE_SOURCE}).mappings().one()
        assert ts["type"] == "repository", dict(ts)

        for slug, inst, inst_name, repo_name, n in WAVE3:
            host = f"avesis.{slug}.edu.tr"
            url = f"https://{host}/api/oai2"
            name = f"{inst_name} - AVESIS"
            clash_ep = c.execute(text("SELECT id, source_id FROM oai_pmh_endpoint WHERE pmh_url ILIKE :h"),
                                 {"h": f"%{host}%"}).fetchall()
            clash_src = c.execute(text("SELECT id, display_name FROM sources WHERE lower(display_name) = lower(:n) "
                                       "OR homepage_url ILIKE :h"), {"n": name, "h": f"%{host}%"}).fetchall()
            if clash_ep or clash_src:
                R["skipped"].append({"slug": slug, "endpoints": [list(map(str, r)) for r in clash_ep],
                                     "sources": [list(map(str, r)) for r in clash_src]})
                print(f"SKIPPED {slug}: already there (endpoints {clash_ep}, sources {clash_src})")
                continue
            sid = c.execute(text("INSERT INTO sources (display_name, type, institution_id, homepage_url, country_code, is_oa) "
                                 "VALUES (:n, 'repository', :i, :h, 'TR', :oa) RETURNING id"),
                            {"n": name, "i": inst, "h": f"https://{host}", "oa": bool(ts["is_oa"])}).scalar()
            R["minted"].append({"source_id": sid, "name": name, "institution_id": inst, "slug": slug,
                                "repositoryName": repo_name, "feed_records": n})
            print(f"MINTED {sid} {name!r} (repository, I{inst}, TR)")
            e = eid()
            c.execute(text("INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, "
                           "status, source_id, is_journal_host, green_scrape, rand) "
                           "VALUES (:id, :u, :s, :mp, :rr, :iw, 'active', :sid, false, :gs, random())"),
                      {"id": e, "u": url, "s": t["pmh_set"], "mp": t["metadata_prefix"], "rr": t["ready_to_run"],
                       "iw": t["in_walden"], "sid": sid, "gs": t["green_scrape"]})
            R["endpoints"].append({"endpoint_id": e, "pmh_url": url, "pmh_set": t["pmh_set"], "source_id": sid, "slug": slug})
            print(f"REGISTERED endpoint {e} {url} [{t['pmh_set'] or '-'}] -> {sid}")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
