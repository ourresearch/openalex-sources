"""Harvest IRUA (University of Antwerp) through its open-access set (oxjob #227, ZD 27248).
Support factory #2254, off-desk lane, 2026-11-17. Run on desk from the openalex-sources checkout.

Why: IRUA's OAI server (https://repository.uantwerpen.be/oai/abua/server.phtml, endpoint mmv3envg3kaaztya9tmo,
source S4306401849) answers a list request WITHOUT a set with a broken list: it repeats records and leaves most out.
Measured live 17 Nov 2026 12:20-12:40 CT (receipt work/irua_oai_list_check_2026-11-17.json):
  - 13 Jan 2026, no set: 4,125 rows, 2,157 distinct, without c:irua:188765 or c:irua:170009 (GetRecord stamps both
    2026-01-13). Same day, set=set:irua:openaccess: 15,998 rows, 15,998 distinct, both present.
  - 10 Nov 2026: no set 20 rows / 13 distinct; open-access set 31; only 6 of those 31 are in the no-set list.
  - Full open-access set from 2002-12-31: 45,773 records in 46 pages (68 s), no repeats, complete. Our harvest holds
    16,019 of them (repo_items, all endpoints' history); 29,754 (65%) were never harvested.
  - Full no-set list from 2002-12-31: past 700 pages / 700,700 rows (IRUA ids only reach ~228K) when stopped.
The endpoint has pmh_set NULL, so the daily harvester, and the full re-harvest packaged on 16 Nov, read the broken list.
Setting pmh_set = 'set:irua:openaccess' makes every harvest read the clean list. Records outside that set (10,032
closed or metadata-only records harvested so far) stay as they are in repo_works; they just stop getting updates.

What it does, in ONE transaction (dry run by default): one UPDATE of oai_pmh_endpoint.pmh_set on mmv3envg3kaaztya9tmo,
with guards (row is active, on S4306401849, at repository.uantwerpen.be/oai/abua, pmh_set NULL; no other active
endpoint already on (pmh_url, 'set:irua:openaccess'), which the partial unique index endpoint_active_url_set_key would
refuse anyway). No source, route or Walden change: walden maps endpoint_id -> source_id (openalex.sources.
endpoint_to_source), which the set doesn't touch; S3 files stay under the same endpoint prefix.

Run:
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file>
    ... same + --execute
Receipt: printed as JSON after the RECEIPT marker; keep it in the job's work/ dir.
Revert: UPDATE oai_pmh_endpoint SET pmh_set = NULL WHERE id = 'mmv3envg3kaaztya9tmo' AND pmh_set = 'set:irua:openaccess';
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
JOB = 227
TICKETS = [27248]
EP, SOURCE, NEW_SET = "mmv3envg3kaaztya9tmo", 4306401849, "set:irua:openaccess"
URL_PART = "repository.uantwerpen.be/oai/abua"

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "tickets": TICKETS,
           "before": None, "after": None, "actions": []}


def jsonable(row):
    return {k: (None if v is None else v if isinstance(v, (list, dict, bool)) else str(v))
            for k, v in dict(row).items() if k != "sample_pmh_record"}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            row = conn.execute(text("SELECT * FROM oai_pmh_endpoint WHERE id = :id"), {"id": EP}).mappings().fetchone()
            assert row is not None, f"endpoint {EP} missing"
            before = receipt["before"] = jsonable(row)
            print(f"  before: {EP} source={before['source_id']} url={before['pmh_url']} set={before['pmh_set']} "
                  f"status={before['status']} ready_to_run={before['ready_to_run']} "
                  f"most_recent_date_harvested={before.get('most_recent_date_harvested')}")
            assert before["status"] == "active", before["status"]
            assert before["source_id"] == str(SOURCE), before["source_id"]
            assert URL_PART in (before["pmh_url"] or ""), before["pmh_url"]
            if before["pmh_set"] == NEW_SET:
                log(f"already set: {EP} pmh_set = {NEW_SET}; nothing to do")
                raise DryRun()
            assert before["pmh_set"] is None, f"unexpected pmh_set {before['pmh_set']!r}: stop and look"
            clash = conn.execute(text("SELECT id, status FROM oai_pmh_endpoint WHERE pmh_url = :u AND pmh_set = :s "
                                      "AND status = 'active'"), {"u": row["pmh_url"], "s": NEW_SET}).fetchall()
            assert not clash, f"another active endpoint already on this url+set: {clash}"
            n = conn.execute(text("UPDATE oai_pmh_endpoint SET pmh_set = :s WHERE id = :id AND pmh_set IS NULL "
                                  "AND status = 'active'"), {"s": NEW_SET, "id": EP}).rowcount
            assert n == 1, n
            log(f"UPDATED {EP}: pmh_set NULL -> {NEW_SET}")
            after = receipt["after"] = jsonable(conn.execute(
                text("SELECT * FROM oai_pmh_endpoint WHERE id = :id"), {"id": EP}).mappings().fetchone())
            assert after["pmh_set"] == NEW_SET and after["status"] == "active" and after["source_id"] == str(SOURCE)
            print(f"  after: {EP} set={after['pmh_set']} status={after['status']} source={after['source_id']}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back" if not EXECUTE else "no change")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
