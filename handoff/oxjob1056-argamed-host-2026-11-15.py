"""ZD 4166 (oxjob #1056, support factory #2254), 2026-11-15: ArGaMed (S4389157735, ISSN 2695-3951) has no host.
Its Crossref registrant is the Centro Ramon Pineiro para a Investigacion en Humanidades (member 30639), a Xunta de
Galicia research centre, and the requester names the Xunta de Galicia as publisher. Host -> P4310321796 Xunta de
Galicia (the existing publisher entity).

Run on desk (dry run by default):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1056-argamed-host-2026-11-15.py [--execute]
Revert: UPDATE sources SET publisher_id = <before.publisher_id>, publisher = <before.publisher> WHERE id = 4389157735
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
SID = 4389157735
P_XUNTA = 4310321796

with engine.begin() as conn:
    row = conn.execute(text("SELECT id, display_name, publisher_id, publisher, institution_id, issns FROM sources "
                            "WHERE id = :id"), {"id": SID}).mappings().fetchone()
    assert row is not None and row["display_name"] == "ArGaMed", row
    before = {k: (None if v is None else str(v)) for k, v in row.items()}
    print("BEFORE", json.dumps(before))
    assert row["publisher_id"] is None, "host already set; nothing to do"
    if EXECUTE:
        n = conn.execute(text("UPDATE sources SET publisher_id = :p, publisher = :pn, updated_date = now() WHERE id = :id"),
                         {"p": P_XUNTA, "pn": "Xunta de Galicia", "id": SID}).rowcount
        assert n == 1
        after = dict(conn.execute(text("SELECT id, display_name, publisher_id, publisher FROM sources WHERE id = :id"),
                                  {"id": SID}).mappings().fetchone())
        print("RECEIPT", json.dumps({"job": 1056, "ticket": 4166, "executed_at": datetime.now(timezone.utc).isoformat(),
                                     "before": before, "after": {k: str(v) for k, v in after.items()}}))
    else:
        print("dry run: would set publisher_id", P_XUNTA, "publisher 'Xunta de Galicia'")
