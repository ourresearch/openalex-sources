"""oxjob #954 (support factory #2254), off-desk lane #2263, 24 Nov 2026: move the harvest checkpoint of the AVESIS feeds
whose whole-feed harvest dies on one bad record past that record's datestamp (ZD 22329). ONE transaction, dry run by
default (rolled back); --execute commits.

Why: AVESIS resumption tokens are stateless keyset tokens ('metadataPrefix=oai_dc[&from&until]&offset=<last GUID>',
200 a page, GUID order). The server fails every page that would hold one bad record (reported as badResumptionToken),
so a first harvest (NULL checkpoint, no 'from') breaks at the same record every day and re-walks the same records the
next day. A from/until window that leaves out the bad record's datestamp walks to its end (probes from the cloud,
24 Nov: #954 work/avesis_break_probe_2026-11-24.json). The daily run sends from=most_recent_date_harvested and
until=yesterday (ojs-journal-ingest RUNBOOK; the hal_tei pattern 'most_recent_date_harvested = backfill cut day, so the
daily run carries it from there'), so a checkpoint the day after the bad bucket gets every later record.
  Lokman Hekim kiee85fvdy9q5yzl4vpe: bad bucket 2005-07-01T00; from=2005-07-02 walked 16,025 of 16,025 (no error).
  Bilecik 5bvm5qty945osy03t1vr: bad bucket 2026-02-28T00; until=2026-02-27 walked 25,668 of 25,668 and
  from=2026-03-01 walked 1,086 of 1,086 (no error).
  Yildiz dhqdpdbrf3b8hkfjyy8h: bad record datestamped 2019-09-01T00:00:00 (sequential bisect at the failing offset);
  ~38K records from 2019-09-02, ~14.6K held; not walked end to end (33 s a page): another bad record later in GUID
  order would stop it again, and the walked records still land.
  The records datestamped before the bad bucket (Lokman Hekim ~1,800 not held, Bilecik ~21,000, Yildiz ~42,000) need a
  Heroku window harvest (--start-date ... --end-date <bucket day - 1>), NEEDS HEROKU; then re-run this script's
  --execute --reapply after that run (a window harvest can move the checkpoint back before the bad bucket, and the next
  daily run would then hit it again; --reapply moves a checkpoint that sits before the target forward to it).

What it does: for each endpoint, guarded on id + source + AVESIS host + status active + checkpoint still NULL (a NULL
means the first harvest keeps failing; anything else means a harvest succeeded since 24 Nov: SKIP and read why),
sets most_recent_date_harvested to two days after the bad bucket (see the desk correction below). Receipt keeps 'before'.

Desk correction, 24 Nov 12:00 CT (apply lane): the daily run does NOT send from=<checkpoint>. openalex-ingest main
(repositories.py, per-endpoint dates) sends first_date = most_recent_date_harvested.date() - timedelta(days=1), so the
packaged checkpoints (bad day + 1) would have sent from=<bad day> and hit the bad record again. The checkpoints are now
bad day + 2 (Lokman Hekim 2005-07-03, Bilecik 2026-03-02, Yildiz 2019-09-03), so the daily run sends exactly the from
the probes walked clean (2005-07-02, 2026-03-01, 2019-09-02).

Run on desk from the openalex-sources checkout (lanes_registry login, rows only), before 21:30 CT so the next daily run
(~10:00 UTC) harvests it and the night after rides it:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-checkpoints-2026-11-24.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-checkpoints-2026-11-24.py --execute
Revert (before the daily run): set most_recent_date_harvested back to the receipt's 'before' (NULL) per endpoint.
After the harvest, the records are real AVESIS records of a registered feed; drop them only through location_denylist.
Tested 24 Nov off desk on SQLite with the oai_pmh_endpoint columns it touches (dry run, execute, re-run skips,
--reapply moves a checkpoint set back to 2026-02-27 and leaves a later one alone).
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
REAPPLY = "--reapply" in sys.argv  # after a Heroku window harvest: also move a checkpoint that sits BEFORE the target
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 954, "ticket": 22329, "set": [], "skipped": []}

# (endpoint id, source id, AVESIS host, bad bucket, new checkpoint = TWO days after it: the daily run sends
#  from = checkpoint.date() - 1 day, so this makes from = the day after the bad bucket)
MOVE = [
    ("kiee85fvdy9q5yzl4vpe", 7407153843, "avesis.lokmanhekim.edu.tr", "2005-07-01T00", "2005-07-03 00:00:00"),
    ("5bvm5qty945osy03t1vr", 7407153848, "avesis.bilecik.edu.tr", "2026-02-28T00", "2026-03-02 00:00:00"),
    ("dhqdpdbrf3b8hkfjyy8h", 7407055331, "avesis.yildiz.edu.tr", "2019-09-01T00:00:00", "2019-09-03 00:00:00"),
]
STATE = ["pmh_url", "source_id", "status", "most_recent_date_harvested", "last_harvest_started", "last_harvest_finished",
         "last_record_count", "last_health_status", "error"]


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        for eid, sid, host, bucket, ckpt in MOVE:
            row = c.execute(text(f"SELECT {', '.join(STATE)} FROM oai_pmh_endpoint WHERE id = :id"), {"id": eid}).mappings().one()
            assert str(row["source_id"]) == str(sid) and host in (row["pmh_url"] or "") and row["status"] == "active", (eid, dict(row))
            before = {k: (None if v is None else str(v)) for k, v in row.items()}
            cur = row["most_recent_date_harvested"]
            behind = cur is not None and REAPPLY and str(cur)[:19] < ckpt
            if cur is not None and not behind:
                R["skipped"].append({"endpoint_id": eid, "why": "checkpoint not NULL (and not behind the target with --reapply)", "before": before})
                print(f"SKIP {eid} ({host}): most_recent_date_harvested = {before['most_recent_date_harvested']}")
                continue
            n = c.execute(text("UPDATE oai_pmh_endpoint SET most_recent_date_harvested = :ts "
                               "WHERE id = :id AND source_id = :sid AND (most_recent_date_harvested IS NULL OR :reapply)"),
                          {"ts": ckpt, "id": eid, "sid": sid, "reapply": bool(behind)}).rowcount
            assert n == 1, eid
            R["set"].append({"endpoint_id": eid, "source_id": sid, "host": host, "bad_bucket": bucket, "checkpoint": ckpt,
                             "before": before})
            print(f"SET {eid} ({host}, S{sid}): most_recent_date_harvested {before['most_recent_date_harvested']} -> {ckpt} (bad bucket {bucket})")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
