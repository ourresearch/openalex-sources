"""ZD 25822 (oxjob #1705, support factory #2254), 2026-11-17: Eleusis (S7407122610, eISSN 2953-5360) was minted by the
registry's weekly DOAJ mint (jobs.doaj --mint, 16 Nov 05:45 UTC) with no host and no homepage. The editor's ticket and
DOAJ (journal 9a3144b2161940ee91a52ac69f91ee38: publisher 'Universidad Nacional de Quilmes', ref.journal
https://revistaeleusis.web.unq.edu.ar/) give both. Host -> P4310318141 'National University of Quilmes' (the existing
publisher entity; it already hosts the sister UNQ journal S7407067077 Revista Intercambios).

Run on desk from the openalex-sources checkout, NOT while the End 2 End is running (05:00 UTC until it publishes):
    cp <this file> handoff/ && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py \
        handoff/oxjob1705-eleusis-host-2026-11-17.py [--execute]
Revert: UPDATE sources SET publisher_id = NULL, homepage_url = NULL WHERE id = 7407122610 (before-row printed in the
receipt).
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
SID = 7407122610
P_UNQ = 4310318141
HOMEPAGE = "https://revistaeleusis.web.unq.edu.ar/"
COLS = "id, display_name, type, issn_l, publisher_id, publisher, institution_id, homepage_url"

with engine.begin() as conn:
    row = conn.execute(text(f"SELECT {COLS} FROM sources WHERE id = :id"), {"id": SID}).mappings().fetchone()
    assert row is not None and row["issn_l"] == "2953-5360" and row["display_name"].startswith("Eleusis"), row
    before = {k: (None if v is None else str(v)) for k, v in row.items()}
    print("BEFORE", json.dumps(before))
    # merge_into_id was dropped by migration 021 (merged losers are deleted), so "row is not None" is the merge guard
    assert row["publisher_id"] is None and row["institution_id"] is None, "host already set; nothing to do"
    assert row["homepage_url"] is None, "homepage already set; look again"
    if EXECUTE:
        n = conn.execute(text("UPDATE sources SET publisher_id = :p, homepage_url = :u, updated_date = now() "
                              "WHERE id = :id AND publisher_id IS NULL AND homepage_url IS NULL"),
                         {"p": P_UNQ, "u": HOMEPAGE, "id": SID}).rowcount
        assert n == 1, n
        after = dict(conn.execute(text(f"SELECT {COLS} FROM sources WHERE id = :id"), {"id": SID}).mappings().fetchone())
        print("RECEIPT", json.dumps({"job": 1705, "ticket": 25822, "executed_at": datetime.now(timezone.utc).isoformat(),
                                     "before": before, "after": {k: (None if v is None else str(v))
                                                                 for k, v in after.items()}}))
    else:
        print("dry run: would set publisher_id", P_UNQ, "and homepage_url", HOMEPAGE)
