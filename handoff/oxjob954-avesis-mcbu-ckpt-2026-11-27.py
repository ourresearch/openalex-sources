"""oxjob #954 (support factory #2254), desk wake 954-1127-1300, 27 Nov 2026: move one AVESIS wave 2 feed past its bad
record, the fix of handoff/oxjob954-avesis-checkpoints-2-2026-11-24.py (outside-reviewed 24 Nov) for Manisa Celal Bayar
University - AVESIS. ONE transaction, dry run by default (rolled back); --execute commits.

Its first harvest (27 Nov daily run) stopped at 32,763 walked of 57,538 on 'resumptionToken invalid or expired' 5 s after
a brisk page; the checkpoint stayed NULL, so every daily run would walk the same 32,800 and stop again. Found from desk
27 Nov, one request at a time (OAI ListIdentifiers, crafted stateless token at the last walked GUID from
openalex.repo.repo_items, 913224bd-...): that page fails (badResumptionToken, 1.6 s); bisect over from/until windows at
that offset: bad record at 2026-03-01T00:00:00Z. Walked end to end from desk (ListIdentifiers): from 2026-03-02: 1,527 of
1,527, no error; 2026-03-01T00:00:01..23:59:59: no records; 1900-01-01..2026-02-28: 55,824 of 55,824, no error
(280 pages); 2026-02-28T00:00:01..23:59:59: no records (so a window run's until=2026-02-28T00:00:00Z misses nothing).
Feed 57,544 = 55,824 + 1,527 + 193 stamped 2026-03-01T00:00:00Z (the bad day; ~110 of them, GUIDs below 913224bd,
were walked on 27 Nov; the rest stay out until AVESIS fixes the record or the harvester can split around it).

What it does: guarded on id + source + AVESIS host + status active + checkpoint still NULL (anything else: SKIP and read
why), sets most_recent_date_harvested to bad day + 2 = 2026-03-03. The daily run sends from = checkpoint.date() - 1 day
(openalex-ingest repositories.py), so it walks 2026-03-02 to yesterday once (1,527 records), then runs incrementally.
The records before the bad day need a Heroku window harvest (NEEDS HEROKU, job #954 work/desk_steps_2026-11-27.md).

Run on desk from the openalex-sources checkout (lanes_registry login, rows only), before the next daily run (~10:00 UTC):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-mcbu-ckpt-2026-11-27.py [--execute]
Revert (before the daily run): set most_recent_date_harvested back to NULL, and drop the packaged window step. After it, the records are real AVESIS records
of a registered feed; drop them only through location_denylist.
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 954, "set": [], "skipped": []}

EID, SID, HOST = "s6o25feo5psyjjjw87pm", 7407155038, "avesis.mcbu.edu.tr"
BAD = "2026-03-01T00:00:00Z"
NEW = "2026-03-03 00:00:00"
STATE = ["pmh_url", "source_id", "status", "most_recent_date_harvested", "last_harvest_started", "last_harvest_finished",
         "last_record_count", "last_health_status", "error"]


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        row = c.execute(text(f"SELECT {', '.join(STATE)} FROM oai_pmh_endpoint WHERE id = :id"), {"id": EID}).mappings().one()
        assert str(row["source_id"]) == str(SID) and HOST in (row["pmh_url"] or "") and row["status"] == "active", dict(row)
        before = {k: (None if v is None else str(v)) for k, v in row.items()}
        if row["most_recent_date_harvested"] is not None:
            R["skipped"].append({"endpoint_id": EID, "host": HOST, "why": "checkpoint is not NULL", "before": before})
            print(f"SKIP {EID} ({HOST}): most_recent_date_harvested = {before['most_recent_date_harvested']}")
        else:
            n = c.execute(text("UPDATE oai_pmh_endpoint SET most_recent_date_harvested = :ts WHERE id = :id AND source_id = :sid "
                               "AND status = 'active' AND most_recent_date_harvested IS NULL"), {"ts": NEW, "id": EID, "sid": SID}).rowcount
            assert n == 1, EID
            R["set"].append({"endpoint_id": EID, "source_id": SID, "host": HOST, "bad_record_datestamp": BAD, "checkpoint": NEW,
                             "before": before})
            print(f"SET {EID} ({HOST}, S{SID}): most_recent_date_harvested NULL -> {NEW} (bad record {BAD})")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
