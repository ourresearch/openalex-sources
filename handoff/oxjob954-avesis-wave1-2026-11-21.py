"""oxjob #954 (support factory #2254), off-desk lane #2263, 21 Nov 2026: AVESIS wave 1 for ZD 22329 (Burak: the
client universities' AVESIS research-information systems, submitted through the retired /repositories/add form).
ONE transaction, dry run by default (rolled back); --execute commits.

Rule: D4, 15 Nov 2026 (#2254): harvest university and national research-information systems (CRIS) such as Turkey's
AVESIS; DOI-less, metadata-only records are the point. Shape: the 11 AVESIS feeds already live (one repository source per
university, homepage https://avesis.<x>.edu.tr, feed https://avesis.<x>.edu.tr/api/oai2), so this script copies the
endpoint settings (metadata_prefix, pmh_set, green_scrape, is_journal_host, in_walden, ready_to_run) from the live
Bursa Uludag endpoint fabemua4ms3k2icp35uy at run time instead of guessing them.

Evidence (job #954 work/avesis_batch_2026-11-21.json + .csv): 52 of the 224 .edu.tr domains of OpenAlex's Turkish
education institutions answer https://avesis.<x>.edu.tr/api/oai2?verb=Identify as 'AVESIS - <university>' (probed from
the cloud 21 Nov 11:30 CT); 11 are registered; 41 are new with 2,830,197 records (ListIdentifiers completeListSize; the
'openaire' set is the whole feed). D4 guards already done for the platform (job #954 Log 15-16 Nov): non-works ('other')
0.7-1.2% on the five biggest live feeds; 37 of 38 sampled DOI-less Erciyes works have no DOI twin.

  A. Wave 1 = the 15 smallest new feeds (each <= 30K records; 249,982 records in all): mint one repository source per
     university (name '<OpenAlex institution name> - AVESIS', institution = that university, homepage, country TR,
     is_oa false like the live 11) and register its feed.
  B. Host institution on the 11 live AVESIS sources (all have institution_id NULL today): set it where still NULL.
     Country TR where NULL.
The other 26 new feeds (37K-292K records each, 2.58M in all) are later waves: same script shape, next lanes.

Size and night: a feed's first harvest runs with the daily harvest (~10:00 UTC) after this commits; records harvested
before ~23:00 UTC ride that night's End 2 End as new works / new locations: about 250K stamps if every first harvest
completes (pre-clear: --add 300000 for that night). Partial first harvests are a known AVESIS pattern (Erciyes holds
6,998 of 112,390 records, Ataturk 9,668 of 151,927), so the real number is likely lower.

Run on desk from the openalex-sources checkout (lanes_registry login, rows only):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-wave1-2026-11-21.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-wave1-2026-11-21.py --execute
Revert (from the receipt): DELETE FROM oai_pmh_endpoint WHERE id IN (<receipt endpoints>); DELETE FROM sources WHERE id IN
(<receipt minted>) (both before any harvested record is published; after, also location_denylist by endpoint);
UPDATE sources SET institution_id = NULL[, country_code = NULL] WHERE id IN (<receipt host_set ids>) per its 'before'.
Tested 21 Nov off desk against a local Postgres 16 built from openalex-sources migrations 001-048 (two empty-DB
prechecks in 034 and 042 relaxed) and seeded with the template endpoint and the 11 live sources.
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
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "ticket": TICKET,
     "template": None, "minted": [], "endpoints": [], "host_set": [], "skipped": []}

TEMPLATE_ENDPOINT = "fabemua4ms3k2icp35uy"   # Bursa Uludag University - AVESIS, live since Feb 2026
TEMPLATE_SOURCE = 7407055210

# (slug, OpenAlex institution id, institution display name, Identify repositoryName, feed records 21 Nov)
WAVE1 = [
    ("pa", 181033969, "Police Academy", "AVESIS - Polis Akademisi", 2579),
    ("agu", 95293974, "Abdullah Gül University", "AVESIS - Abdullah Gül Üniversitesi", 6492),
    ("hakkari", 160534799, "Hakkari University", "AVESIS - Hakkari Üniversitesi", 7598),
    ("tedu", 4210115299, "TED University", "AVESIS - TED Üniversitesi", 8931),
    ("kent", 4210109286, "Istanbul Kent University", "AVESIS - İstanbul Kent Üniversitesi", 9182),
    ("iku", 197185219, "Istanbul Kültür University", "AVESIS - İstanbul Kültür Üniversitesi", 14753),
    ("ticaret", 53210213, "Istanbul Commerce University", "AVESIS - İstanbul Ticaret University", 16870),
    ("maltepe", 44457188, "Maltepe University", "AVESIS - Maltepe Üniversitesi", 17203),
    ("lokmanhekim", 4210139404, "Lokman Hekim Üniversitesi", "AVESIS - Lokman Hekim Üniversitesi", 18488),
    ("klu", 264262938, "Kırklareli University", "AVESIS - Kırklareli Üniversitesi", 21097),
    ("ksbu", 4387156457, "Kutahya Saglik Bilimleri Universitesi", "AVESIS - Kütahya Sağlık Bilimleri Üniversitesi", 21259),
    ("isparta", 4210118295, "Isparta University of Applied Sciences", "AVESIS - Isparta Uygulamalı Bilimler Üniversitesi", 24421),
    ("gelisim", 4210091267, "İstanbul Gelişim Üniversitesi", "AVESIS - İstanbul Gelişim Üniversitesi", 25730),
    ("bilecik", 169255954, "Bilecik Şeyh Edebali Üniversitesi", "AVESIS - Bilecik Şeyh Edebali Üniversitesi", 26792),
    ("usak", 204585392, "Usak University", "AVESIS - Uşak Üniversitesi", 28587),
]

# (live AVESIS source id, institution id, slug) : host institution for the 11 already registered
LIVE = [
    (7407055159, 64134780, "atauni"), (7407055213, 4210150808, "aybu"), (7407055198, 115953327, "comu"),
    (7407055139, 87673952, "erciyes"), (7407055142, 94294566, "gsu"), (7407055337, 4387154747, "kayseri"),
    (7407055291, 51826884, "kocaeli"), (7407055210, 131835042, "uludag"), (7407055138, 311431067, "yeniyuzyil"),
    (7407055331, 4101805, "yildiz"), (7407055328, 118567650, "yyu"),
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

        # A. wave 1
        for slug, inst, inst_name, repo_name, n in WAVE1:
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

        # B. host institution on the live 11
        for sid, inst, slug in LIVE:
            row = c.execute(text("SELECT display_name, type, homepage_url, institution_id, country_code FROM sources "
                                 "WHERE id = :id"), {"id": sid}).mappings().one()
            assert row["type"] == "repository" and f"avesis.{slug}.edu.tr" in (row["homepage_url"] or ""), (sid, dict(row))
            if row["institution_id"] is not None and row["country_code"] is not None:
                R["skipped"].append({"source_id": sid, "why": "institution and country already set",
                                     "institution_id": row["institution_id"]})
                print(f"KEPT {sid} {row['display_name']!r}: institution I{row['institution_id']}")
                continue
            new_inst = row["institution_id"] if row["institution_id"] is not None else inst
            new_cc = row["country_code"] or "TR"
            n = c.execute(text("UPDATE sources SET institution_id = :i, country_code = :cc, updated_date = now() "
                               "WHERE id = :id"), {"i": new_inst, "cc": new_cc, "id": sid}).rowcount
            assert n == 1
            R["host_set"].append({"source_id": sid, "before": {"institution_id": row["institution_id"],
                                                               "country_code": row["country_code"]},
                                  "after": {"institution_id": new_inst, "country_code": new_cc}})
            print(f"HOST {sid} {row['display_name']!r}: institution {row['institution_id']} -> {new_inst}, "
                  f"country {row['country_code']} -> {new_cc}")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
