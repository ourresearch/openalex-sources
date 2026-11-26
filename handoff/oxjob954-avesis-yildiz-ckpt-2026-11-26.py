"""oxjob #954 (support factory #2254), off-desk lane #2263, 26 Nov 2026: move Yildiz AVESIS's harvest checkpoint forward
so its daily run stops re-walking 38K slow records and failing (ZD 22329). ONE transaction, dry run by default; --execute
commits.

Why: Yildiz dhqdpdbrf3b8hkfjyy8h (S7407055331) has sat at most_recent_date_harvested 2019-09-03 since 24 Nov (set on
purpose past its bad record 2019-09-01T00, openalex-sources 5e7e21b). The daily run sends from = checkpoint - 1 day =
2019-09-02, i.e. ~38,234 records at 33-35 s a page (vs the harvester's 60 s read timeout); it has not finished since:
connection_error on 25 Nov (3 retries at 25,800), and the 25 Nov window run.1679 (2019-09-02..2026-11-24) stopped at
37,800 when the server expired its token after 2 h 6 min. A run that doesn't finish keeps the start checkpoint, so every
day repeats the same walk. Meanwhile the records are in: openalex.repo.repo_items holds 104,856 distinct Yildiz
identifiers of the feed's 106,392 (37,884 datestamped 2019-09-02 or later vs 38,234 served; 26 Nov, job #954
work/yildiz_tail_2026-11-26.json), and the API shows 96,614 works with a Yildiz location (26 Nov, after night 11-26).
With the checkpoint at 2026-11-25 the daily run asks for from=2026-11-24 (a few pages) and advances normally again.
The 386 records of 2019-09-02..2025 not held yet are left to the optional year-window step (#954 desk_steps_2026-11-26 s3).

Guards: endpoint id + source + AVESIS host + active; checkpoint still exactly 2019-09-03 (anything else: a run finished
since, or someone moved it: SKIP and read why); no harvest running (last_harvest_started later than
last_harvest_finished: SKIP, run again after it ends, because a failing run restores its start value over ours).
Receipt keeps 'before'.

Run on desk from the openalex-sources checkout (lanes_registry login, rows only), after 26 Nov's daily Yildiz run ends:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-yildiz-ckpt-2026-11-26.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-yildiz-ckpt-2026-11-26.py --execute
Size: no new records by itself (it shortens the daily walk); no pre-clear.
Revert: UPDATE oai_pmh_endpoint SET most_recent_date_harvested = '2019-09-03 00:00:00' WHERE id = 'dhqdpdbrf3b8hkfjyy8h'.
Tested 26 Nov off desk on SQLite through a small sqlalchemy stand-in: dry run rolls back; --execute sets it; a re-run
SKIPs; a running harvest SKIPs; a checkpoint already moved SKIPs.
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 954, "ticket": 22329, "set": [], "skipped": []}

EID, SID, HOST = "dhqdpdbrf3b8hkfjyy8h", 7407055331, "avesis.yildiz.edu.tr"
EXPECT = "2019-09-03"
NEW = "2026-11-25 00:00:00"
STATE = ["pmh_url", "source_id", "status", "most_recent_date_harvested", "last_harvest_started", "last_harvest_finished",
         "last_record_count", "last_health_status", "error"]


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        row = c.execute(text(f"SELECT {', '.join(STATE)} FROM oai_pmh_endpoint WHERE id = :id"), {"id": EID}).mappings().one()
        assert str(row["source_id"]) == str(SID) and HOST in (row["pmh_url"] or "") and row["status"] == "active", dict(row)
        before = {k: (None if v is None else str(v)) for k, v in row.items()}
        cur = before["most_recent_date_harvested"]
        started, finished = before["last_harvest_started"], before["last_harvest_finished"]
        if cur is None or cur[:10] != EXPECT:
            R["skipped"].append({"endpoint_id": EID, "why": f"checkpoint is {cur}, not {EXPECT}: a run finished or it was moved", "before": before})
            print(f"SKIP {EID}: most_recent_date_harvested = {cur} (expected {EXPECT}); read last_* to see why")
        elif started and (not finished or started > finished):
            R["skipped"].append({"endpoint_id": EID, "why": "harvest running (started after last finish)", "before": before})
            print(f"SKIP {EID}: harvest running (started {started}, finished {finished}); run again after it ends")
        else:
            n = c.execute(text("UPDATE oai_pmh_endpoint SET most_recent_date_harvested = :ts "
                               "WHERE id = :id AND source_id = :sid AND most_recent_date_harvested = :cur"),
                          {"ts": NEW, "id": EID, "sid": SID, "cur": row["most_recent_date_harvested"]}).rowcount
            assert n == 1, n
            R["set"].append({"endpoint_id": EID, "source_id": SID, "checkpoint": NEW, "before": before})
            print(f"SET {EID} ({HOST}, S{SID}): most_recent_date_harvested {cur} -> {NEW} "
                  f"(last run: {before['last_health_status']}, {before['last_record_count']} records, finished {finished})")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
