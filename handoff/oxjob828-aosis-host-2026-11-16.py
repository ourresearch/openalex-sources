"""ZD 23415 (oxjob #828, support factory #2254), 2026-11-16: Advances in Corporate Governance (S7407057810,
ISSN 3078-2252 / 3135-4602) has no host. Crossref's journal record (api.crossref.org/journals/3078-2252) names AOSIS as
publisher, and all 23 of its works carry AOSIS DOIs (10.4102). Host -> P4310315322 AOSIS (the existing publisher entity).

Run on desk, NOT while the End 2 End is running (05:00 UTC until it publishes; the registry is read live by federation):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob828-aosis-host-2026-11-16.py [--execute]
Revert: UPDATE sources SET publisher_id = <before.publisher_id>, publisher = <before.publisher> WHERE id = 7407057810
(before-row printed in the receipt).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
SID = 7407057810
P_AOSIS = 4310315322

with engine.begin() as conn:
    row = conn.execute(text("SELECT id, display_name, publisher_id, publisher, institution_id, issns FROM sources "
                            "WHERE id = :id"), {"id": SID}).mappings().fetchone()
    assert row is not None and row["display_name"] == "Advances in Corporate Governance", row
    before = {k: (None if v is None else str(v)) for k, v in row.items()}
    print("BEFORE", json.dumps(before))
    assert row["publisher_id"] is None, "host already set; nothing to do"
    if EXECUTE:
        n = conn.execute(text("UPDATE sources SET publisher_id = :p, publisher = :pn, updated_date = now() WHERE id = :id"),
                         {"p": P_AOSIS, "pn": "AOSIS", "id": SID}).rowcount
        assert n == 1
        after = dict(conn.execute(text("SELECT id, display_name, publisher_id, publisher FROM sources WHERE id = :id"),
                                  {"id": SID}).mappings().fetchone())
        print("RECEIPT", json.dumps({"job": 828, "ticket": 23415, "executed_at": datetime.now(timezone.utc).isoformat(),
                                     "before": before, "after": {k: str(v) for k, v in after.items()}}))
    else:
        print("dry run: would set publisher_id", P_AOSIS, "publisher 'AOSIS'")
