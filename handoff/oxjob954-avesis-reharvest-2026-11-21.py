"""oxjob #954 (support factory #2254), off-desk lane #2263, 21 Nov 2026: full re-harvest of the 11 live AVESIS feeds
(ZD 22329). ONE transaction, dry run by default (rolled back); --execute commits.

Why: the 11 AVESIS endpoints hold 419,151 of their feeds' 849,642 records (openalex.repo.repo_works distinct native ids vs
ListIdentifiers completeListSize, 21 Nov). Nine got their rows from the 27-28 Feb bulk load and since then only records
whose OAI datestamp is newer: e.g. Erciyes holds 6,998 but its feed has 109,683 records datestamped <= 2026-02-27 and only
2,508 after; Ataturk 9,668 held vs 148,034 / 3,611; Galatasaray 834 vs 7,979; Yeni Yuzyil 1,293 vs 13,331. Yildiz,
registered 3 Oct, got 40,160 on its first harvest (the 6 h first-harvest cap, openalex-ingest b6e551b) and only newer
records since; its feed has 103,296 records <= 2026-02-27. So the harvest checkpoint sits past records it never fetched.

What it does: for each endpoint, guarded on id + source + AVESIS host, snapshots the harvest-state columns and sets
most_recent_date_harvested = NULL, which openalex-ingest treats as a first harvest with no start date (b6e551b, 28 Sep:
"no start date on a first harvest"). DESK: confirm that reading in openalex-ingest repositories.py before --execute; if the
checkpoint lives elsewhere, adapt RESET below. Big feeds (Ataturk 152K, Uludag 112K, Erciyes 112K, Yildiz 106K) may hit the
6 h first-harvest cap again: check repo_works after the first run and repeat the reset for any that stopped short (records
re-sent are de-duplicated by native id; only the missing ones become new works or locations).
DESK CORRECTION (apply lane, 23 Nov 2026): do NOT repeat the reset for a feed that stopped short. A first harvest that hits
the 6 h cap or dies (AVESIS 'resumptionToken invalid or expired') writes no checkpoint (openalex-ingest b6e551b), so it
stays NULL and restarts from record 1 every day, re-walking the same records and taking no new ones: Yildiz has done so
daily (40,400 records, then the token error), and the new Lokman Hekim and Bilecik feeds die at 5,000 and 4,800 each day.
The records a failed walk reached still land (saved in batches). So the morning after the first re-harvest, restore the
receipt's 'before' on every endpoint whose last_health_status is not 'success' (or whose checkpoint is still NULL): its
daily incremental harvest resumes and the walked records stay.

Size: up to ~430K records not held today (feed minus held), about 70% DOI-less, so up to ~430K stamps on the night after the
re-harvest: pre-clear --add 520000 for that night. Run it on a different night from AVESIS wave 1
(oxjob954-avesis-wave1-2026-11-21.py) so each night's change is readable on its own.

Run on desk from the openalex-sources checkout (lanes_registry login, rows only):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-reharvest-2026-11-21.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-reharvest-2026-11-21.py --execute
Revert (before the harvest runs): restore most_recent_date_harvested from the receipt's 'before' per endpoint. After the
harvest, the records are real AVESIS records of a registered feed; drop them only through location_denylist by endpoint.
Tested 21 Nov off desk against a local Postgres 16 built from openalex-sources migrations 001-048.
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 954, "ticket": 22329, "reset": []}

# (endpoint id, source id, AVESIS host, records held 21 Nov, feed records 21 Nov)
LIVE = [
    ("89pasehbwubh8ncng4v7", 7407055159, "avesis.atauni.edu.tr", 9668, 151927),
    ("xnhvykpnk7yvv9tak7ts", 7407055139, "avesis.erciyes.edu.tr", 6998, 112390),
    ("dhqdpdbrf3b8hkfjyy8h", 7407055331, "avesis.yildiz.edu.tr", 40160, 106286),
    ("ggd2pfohu3nnucvfffpu", 7407055142, "avesis.gsu.edu.tr", 834, 8157),
    ("bxthzyj4syvrpt2utd9v", 7407055138, "avesis.yeniyuzyil.edu.tr", 1293, 13748),
    ("dkhlx35pjqrfgg9fl6kh", 7407055337, "avesis.kayseri.edu.tr", 2336, 11062),
    ("ufdgwawy6y2iajbrpzas", 7407055291, "avesis.kocaeli.edu.tr", 66002, 90102),
    ("nbmzr26f46psyfd68eks", 7407055328, "avesis.yyu.edu.tr", 62357, 79373),
    ("fabemua4ms3k2icp35uy", 7407055210, "avesis.uludag.edu.tr", 92194, 111952),
    ("nalmjmsi8wbh7jzbt3dw", 7407055198, "avesis.comu.edu.tr", 80029, 94926),
    ("5m743ffwstayw9tuvr9c", 7407055213, "avesis.aybu.edu.tr", 57280, 69719),
]
STATE = ["pmh_url", "source_id", "status", "most_recent_date_harvested", "last_harvest_started", "last_harvest_finished",
         "earliest_timestamp", "last_record_count", "last_health_status", "error"]


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        for eid, sid, host, held, feed in LIVE:
            row = c.execute(text(f"SELECT {', '.join(STATE)} FROM oai_pmh_endpoint WHERE id = :id"), {"id": eid}).mappings().one()
            assert row["source_id"] == sid and host in (row["pmh_url"] or "") and row["status"] == "active", (eid, dict(row))
            before = {k: (None if v is None else str(v)) for k, v in row.items()}
            n = c.execute(text("UPDATE oai_pmh_endpoint SET most_recent_date_harvested = NULL WHERE id = :id AND source_id = :sid"),
                          {"id": eid, "sid": sid}).rowcount
            assert n == 1, eid
            R["reset"].append({"endpoint_id": eid, "source_id": sid, "host": host, "held_21nov": held, "feed_21nov": feed,
                               "before": before})
            print(f"RESET {eid} ({host}, S{sid}): most_recent_date_harvested {before['most_recent_date_harvested']} -> NULL "
                  f"(held {held:,} of {feed:,})")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
