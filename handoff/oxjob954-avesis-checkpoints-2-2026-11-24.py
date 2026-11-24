"""oxjob #954 (support factory #2254), desk wake 954-1124-1300, 24 Nov 2026: the checkpoint-past-the-bad-record fix
(handoff/oxjob954-avesis-checkpoints-2026-11-24.py, 5e7e21b, outside-reviewed) for two more AVESIS feeds whose A4 first
harvest died on one bad record (ZD 22329). ONE transaction, dry run by default (rolled back); --execute commits.

Found from desk 24 Nov 13:15-13:30 CT, one request at a time (OAI ListIdentifiers, crafted stateless token at the last
walked GUID from openalex.repo.repo_items; #954 work/avesis_bisect_2026-11-24.json):
  Ataturk 89pasehbwubh8ncng4v7: the page after 6e652a17-... fails (badResumptionToken, 3 s); bisect: bad record at
      2025-08-01T00:00:00Z. Feed 151,964 = until 2025-07-31: 142,811 + 1 Aug 2025: ~562 + from 2025-08-02: 8,591 (both walk).
  AYBU 5m743ffwstayw9tuvr9c: the page after 985173d5-... fails (2 s); bisect: 2023-06-01T00:00:00Z.
      Feed 69,804 = until 2023-05-31: 55,623 + 1 Jun 2023: ~370 + from 2023-06-02: 13,811 (both walk).
  Both windows walked end to end from desk (ListIdentifiers, 13:17-13:22 CT): 8,591 of 8,591 and 13,811 of 13,811, no error.
  (YYU and COMU answer the page after their last walked GUID with 200 records: their 24 Nov timeouts were transient.)

What it does: for each endpoint, guarded on id + source + AVESIS host + status active + checkpoint equal to the value
the restore (handoff/oxjob954-avesis-restore-2026-11-24.py, 6c4b6d2) put back, or NULL (anything else: SKIP and read why),
sets most_recent_date_harvested to bad day + 2. The daily run sends from = checkpoint.date() - 1 day (openalex-ingest
repositories.py), so it walks from the day after the bad day to yesterday: Ataturk 8,591, AYBU 13,811 records, once.
The records before the bad day need a Heroku window harvest (NEEDS HEROKU, #954 work/desk_steps_2026-11-24.md § 2).

Run on desk from the openalex-sources checkout (lanes_registry login, rows only), before the next daily run (~10:00 UTC):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-checkpoints-2-2026-11-24.py [--execute]
Revert (before the daily run): set most_recent_date_harvested back to the receipt's 'before' per endpoint (2026-11-22 /
2026-11-21). After it, the records are real AVESIS records of a registered feed; drop them only through location_denylist.
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 954, "ticket": 22329, "set": [], "skipped": []}

# (endpoint id, source id, AVESIS host, bad record datestamp, checkpoint the restore put back, new checkpoint = bad day + 2)
MOVE = [
    ("89pasehbwubh8ncng4v7", 7407055159, "avesis.atauni.edu.tr", "2025-08-01T00:00:00Z", "2026-11-22 00:00:00", "2025-08-03 00:00:00"),
    ("5m743ffwstayw9tuvr9c", 7407055213, "avesis.aybu.edu.tr", "2023-06-01T00:00:00Z", "2026-11-21 00:00:00", "2023-06-03 00:00:00"),
]
STATE = ["pmh_url", "source_id", "status", "most_recent_date_harvested", "last_harvest_started", "last_harvest_finished",
         "last_record_count", "last_health_status", "error"]


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        for eid, sid, host, bad, restored, ckpt in MOVE:
            row = c.execute(text(f"SELECT {', '.join(STATE)} FROM oai_pmh_endpoint WHERE id = :id"), {"id": eid}).mappings().one()
            assert str(row["source_id"]) == str(sid) and host in (row["pmh_url"] or "") and row["status"] == "active", (eid, dict(row))
            before = {k: (None if v is None else str(v)) for k, v in row.items()}
            cur = row["most_recent_date_harvested"]
            if cur is not None and str(cur)[:19] != restored:
                R["skipped"].append({"endpoint_id": eid, "host": host, "why": f"checkpoint is neither NULL nor {restored}", "before": before})
                print(f"SKIP {eid} ({host}): most_recent_date_harvested = {before['most_recent_date_harvested']}")
                continue
            n = c.execute(text("UPDATE oai_pmh_endpoint SET most_recent_date_harvested = :ts WHERE id = :id AND source_id = :sid "
                               "AND (most_recent_date_harvested IS NULL OR most_recent_date_harvested = :restored)"),
                          {"ts": ckpt, "id": eid, "sid": sid, "restored": restored}).rowcount
            assert n == 1, eid
            R["set"].append({"endpoint_id": eid, "source_id": sid, "host": host, "bad_record_datestamp": bad, "checkpoint": ckpt,
                             "before": before})
            print(f"SET {eid} ({host}, S{sid}): most_recent_date_harvested {before['most_recent_date_harvested']} -> {ckpt} (bad record {bad})")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
