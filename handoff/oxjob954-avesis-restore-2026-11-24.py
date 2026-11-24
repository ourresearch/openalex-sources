"""oxjob #954 (support factory #2254), desk wake 954-1124-1300, 24 Nov 2026: restore the harvest checkpoint of the A4
AVESIS feeds whose first harvest (24 Nov ~10:00 UTC daily run) did not finish (ZD 22329). ONE transaction, dry run by
default (rolled back); --execute commits.

Why: A4 (handoff/oxjob954-avesis-reharvest-2026-11-21.py, 23 Nov) set most_recent_date_harvested to NULL on 11 AVESIS
feeds so the daily run walked each whole feed. A first harvest that fails keeps NULL, so the next daily run restarts from
record 1 and re-walks the same records. The records it walked already landed, so the fix is to put back the checkpoint
the feed had before A4 (the A4 receipt's 'before'); the daily run then goes on harvesting incrementally from there.

Registry read 24 Nov 13:10 CT (SELECT only): 6 of 11 finished ('success', checkpoint 2026-11-23); Yildiz has its #954
checkpoint 2019-09-03 on purpose (5e7e21b; not touched here). The other 4 are NULL:
  token  = 'resumptionToken invalid or expired' within ~5 s of a brisk page: AVESIS's one-bad-record failure, the same
           record every walk (#954 work/avesis_break_probe_2026-11-24.json), so a retry gets no further. Restored now.
      Ataturk 89pasehbwubh8ncng4v7: walked 66,200 of 151,927 (12:37 UTC); AYBU 5m743ffwstayw9tuvr9c: 41,600 of 69,719.
  timeout = 'Read timed out (60 s)' after retries, at 14:57-15:19 UTC, in the same 80 s as Yildiz's timeout and AYBU's
           failure (Yildiz had walked past that point on 23 Nov): a slowdown, not a bad record. Left NULL for ONE more
           daily run (25 Nov), restored by --timeouts on 25 Nov if it failed again.
      YYU (Van) nbmzr26f46psyfd68eks: walked 45,000 of 79,373; COMU nalmjmsi8wbh7jzbt3dw: 32,800 of 94,926.

What it does: for each endpoint, guarded on id + source + AVESIS host + status active + checkpoint still NULL (anything
else means a harvest finished or someone wrote it since: SKIP), sets most_recent_date_harvested to the A4 receipt's
'before'. Receipt keeps the state it replaced.

Run on desk from the openalex-sources checkout (lanes_registry login, rows only), before the next daily run (~10:00 UTC):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-restore-2026-11-24.py              # dry run
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob954-avesis-restore-2026-11-24.py --execute    # token pair
    ... --timeouts --execute   # 25 Nov: YYU + COMU, if their retry failed (still NULL); a finished one is skipped
Revert: set most_recent_date_harvested back to NULL on the endpoints in the receipt's 'set' (the daily run then re-walks
the whole feed and dies at the same place).
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

EXECUTE = "--execute" in sys.argv
TIMEOUTS = "--timeouts" in sys.argv
engine = create_engine(os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1), future=True)
R = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "timeouts": TIMEOUTS, "job": 954, "ticket": 22329,
     "set": [], "skipped": []}

# (endpoint id, source id, AVESIS host, checkpoint before A4 = A4 receipt 'before', failure class on 24 Nov)
TOKEN = [
    ("89pasehbwubh8ncng4v7", 7407055159, "avesis.atauni.edu.tr", "2026-11-22 00:00:00", "token: walked 66,200 of 151,927"),
    ("5m743ffwstayw9tuvr9c", 7407055213, "avesis.aybu.edu.tr", "2026-11-21 00:00:00", "token: walked 41,600 of 69,719"),
]
TIMEOUT = [
    ("nbmzr26f46psyfd68eks", 7407055328, "avesis.yyu.edu.tr", "2026-11-20 00:00:00", "timeout: walked 45,000 of 79,373 on 24 Nov"),
    ("nalmjmsi8wbh7jzbt3dw", 7407055198, "avesis.comu.edu.tr", "2026-11-20 00:00:00", "timeout: walked 32,800 of 94,926 on 24 Nov"),
]
STATE = ["pmh_url", "source_id", "status", "most_recent_date_harvested", "last_harvest_started", "last_harvest_finished",
         "last_record_count", "last_health_status", "error"]


class DryRun(Exception):
    pass


try:
    with engine.begin() as c:
        for eid, sid, host, ckpt, why in (TIMEOUT if TIMEOUTS else TOKEN):
            row = c.execute(text(f"SELECT {', '.join(STATE)} FROM oai_pmh_endpoint WHERE id = :id"), {"id": eid}).mappings().one()
            assert str(row["source_id"]) == str(sid) and host in (row["pmh_url"] or "") and row["status"] == "active", (eid, dict(row))
            before = {k: (None if v is None else str(v)) for k, v in row.items()}
            if row["most_recent_date_harvested"] is not None:
                R["skipped"].append({"endpoint_id": eid, "host": host, "why": "checkpoint not NULL (a harvest finished or it was written since)",
                                     "before": before})
                print(f"SKIP {eid} ({host}): most_recent_date_harvested = {before['most_recent_date_harvested']}, "
                      f"{before['last_health_status']}, {before['last_record_count']} records")
                continue
            n = c.execute(text("UPDATE oai_pmh_endpoint SET most_recent_date_harvested = :ts "
                               "WHERE id = :id AND source_id = :sid AND most_recent_date_harvested IS NULL"),
                          {"ts": ckpt, "id": eid, "sid": sid}).rowcount
            assert n == 1, eid
            R["set"].append({"endpoint_id": eid, "source_id": sid, "host": host, "checkpoint": ckpt, "why": why, "before": before})
            print(f"SET {eid} ({host}, S{sid}): most_recent_date_harvested NULL -> {ckpt} ({why}; "
                  f"now {before['last_health_status']}, {before['last_record_count']} records)")
        if not EXECUTE:
            raise DryRun()
    print("COMMITTED")
except DryRun:
    print("DRY RUN - rolled back")
print("RECEIPT")
print(json.dumps(R, default=str, ensure_ascii=False))
