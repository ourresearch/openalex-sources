"""oxjob #954 (support factory #2254), off-desk lane #2263, 26 Nov 2026: AVESIS wave 2 for a repository add request (the
AVESIS research-information systems, submitted through the retired /repositories/add form).
ONE transaction, dry run by default (rolled back); --execute commits.

Rule: D4, 15 Nov 2026 (#2254): harvest university and national research-information systems (CRIS) such as Turkey's
AVESIS; DOI-less, metadata-only records are the point. Same shape as wave 1 (handoff/oxjob954-avesis-wave1-2026-11-21.py,
run 21 Nov, openalex-sources 2a0f6f0): one repository source per university, name '<OpenAlex institution name> - AVESIS',
institution = that university, homepage https://avesis.<x>.edu.tr, country TR, is_oa copied from the template source;
feed https://avesis.<x>.edu.tr/api/oai2 with endpoint settings copied at run time from the live Bursa Uludag AVESIS
endpoint fabemua4ms3k2icp35uy. Wave 1's host-institution step (B) is not repeated: all 26 AVESIS sources have one.

Wave 2 = the 9 smallest feeds left (36K-58K records each, 405,183 records: ListIdentifiers completeListSize, probed from
the cloud 26 Nov 05:52-05:56 CT, receipt job #954 work/avesis_wave2_probe_2026-11-26.json). The 17 left after this wave (63K-292K
records, 2.18M) are later waves. Every institution id was read on the API 26 Nov (all TR, none merged); no source or
endpoint for any of these 9 hosts exists (API sources?search=avesis: 26 sources, none of these; the script re-checks).

Why it is safe to go on: after waves 1 and A4, all 26 AVESIS sources hold 76-95% of their feed records as works on the
API (median ~92%; job #954 work/avesis_night1126_check_2026-11-26.json); non-works 0.7-1.2% on the big feeds.

Size and night: each feed's first harvest runs with the daily harvest (~10:00 UTC) after this commits; records harvested
before the night's start ride that night's End 2 End as new works / new locations: about 405K stamps if every first
harvest completes (pre-clear --add 487000 for that night; an unused pre-clear is harmless). Known AVESIS failure modes
on a first harvest (job #954 PLAN, 22-25 Nov): one bad record per feed stops the walk ('resumptionToken invalid or
expired' at the same record every day) and the checkpoint stays NULL; a slow feed can time out. Both are handled by the
morning-after check in job #954's desk steps (restore / bisect / checkpoint past the bad day + window harvests).

Run on desk from the openalex-sources checkout (lanes_registry login, rows only):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-wave2-2026-11-26.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-wave2-2026-11-26.py --execute
Revert (from the receipt): DELETE FROM oai_pmh_endpoint WHERE id IN (<receipt endpoints>); DELETE FROM sources WHERE id IN
(<receipt minted>) (both before any harvested record is published; after, also location_denylist by endpoint).
Tested 26 Nov off desk on SQLite through a small sqlalchemy stand-in (ILIKE read as LIKE): dry run rolled back, --execute
minted 8 + skipped a planted clash, a re-run skipped all 9 (job #954 work/oxjob954-avesis-wave2-2026-11-26.py.md).
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
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "ticket": TICKET, "wave": 2,
     "template": None, "minted": [], "endpoints": [], "skipped": []}

TEMPLATE_ENDPOINT = "fabemua4ms3k2icp35uy"   # Bursa Uludag University - AVESIS, live since Feb 2026
TEMPLATE_SOURCE = 7407055210

# (slug, OpenAlex institution id, institution display name on the API 26 Nov, Identify repositoryName, completeListSize 26 Nov)
WAVE2 = [
    ("ebyu", 176037994, "Erzincan Binali Yıldırım University", "AVESIS - Erzincan Binali Yıldırım Üniversitesi", 36524),
    ("bozok", 2799303410, "Yozgat Bozok Üniversitesi", "AVESIS - Yozgat Bozok Üniversitesi", 39409),
    ("acibadem", 322491407, "Acıbadem University", "AVESIS - Acıbadem Mehmet Ali Aydınlar Üniversitesi", 39474),
    ("medipol", 3125470973, "Istanbul Medipol University", "AVESIS - İstanbul Medipol Üniversitesi", 41530),
    ("mku", 46000314, "Mustafa Kemal University", "AVESIS - Hatay Mustafa Kemal Üniversitesi", 43253),
    ("hacibayram", 3129222432, "Ankara Hacı Bayram Veli University", "AVESIS - Ankara Hacı Bayram Veli Üniversitesi", 45550),
    ("bogazici", 4405392, "Boğaziçi University", "AVESIS - Boğaziçi Üniversitesi", 47757),
    ("erdogan", 161597582, "Recep Tayyip Erdoğan University", "AVESIS - Recep Tayyip Erdoğan Üniversitesi", 54148),
    ("mcbu", 16748637, "Manisa Celal Bayar University", "AVESIS - Manisa Celal Bayar Üniversitesi", 57538),
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
        R["template"] = {k: (None if v is None else str(v)) for k, v in t.items()}
        print(f"TEMPLATE {TEMPLATE_ENDPOINT}: {R['template']}")
        ts = c.execute(text("SELECT type, is_oa FROM sources WHERE id = :id"), {"id": TEMPLATE_SOURCE}).mappings().one()
        assert ts["type"] == "repository", dict(ts)

        for slug, inst, inst_name, repo_name, n in WAVE2:
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
