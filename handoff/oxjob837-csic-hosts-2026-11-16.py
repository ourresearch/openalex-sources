"""ZD 3994 (oxjob #837 -> #1157, support factory #2254, off-desk lane), 2026-11-16: three Editorial CSIC journals lack
the publisher the other 36 sources on CSIC's Crossref prefix 10.3989 carry (P4310321139 Spanish National Research
Council, ROR 02gfc7t72; receipts: #837 work/csic_sources_publisher_2026-11-16.json):

    S5406931731 Anuario Musical (0211-3538 / 1988-4125, anuariomusical.revistas.csic.es)   publisher_id NULL -> 4310321139
    S5407050314 Revista de Estudios Utopicos REUTOPIA (3101-0180, reutopia.revistas.csic.es) publisher_id NULL -> 4310321139
    S958541350  Hispania (0018-2141 / 1988-8368, hispania.revistas.csic.es)                 publisher_id 4362726015 -> 4310321139
                (P4362726015 is a second CSIC publisher record with the same ROR; Hispania is its only source)

The requester (Editorial CSIC) asked for exactly this on Al-Qantara (done) and for Anuario Musical's merge (done; the
merged record has no publisher). Same shape as handoff/oxjob1056-argamed-host-2026-11-15.py.

Run on desk (dry run by default), from ~/ox/openalex-sources:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob837-csic-hosts-2026-11-16.py [--execute]
Revert: UPDATE sources SET publisher_id = <before.publisher_id>, publisher = <before.publisher> WHERE id = <id>
(before-rows printed in the receipt).
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
P_CSIC = 4310321139
# publisher text: what 36 of the 41 sources already on P4310321139 carry (the rest: INIA 2, IGME 1, Springer 1, NULL 1; desk check 16 Nov 10:15 CT);
# keeps the two that already say it unchanged and fills Hispania's NULL.
P_CSIC_NAME = "Editorial CSIC"
# id -> (expected display_name prefix, expected publisher_id before)
TARGETS = {
    5406931731: ("Anuario Musical", None),
    5407050314: ("Revista de Estudios Ut", None),
    958541350: ("Hispania", 4362726015),
}

with engine.begin() as conn:
    befores = []
    for sid, (name, pub_before) in TARGETS.items():
        row = conn.execute(text("SELECT id, display_name, publisher_id, publisher, issns FROM sources WHERE id = :id"),
                           {"id": sid}).mappings().fetchone()
        assert row is not None and (row["display_name"] or "").startswith(name), (sid, row)
        assert row["publisher_id"] == pub_before, f"{sid}: publisher_id is {row['publisher_id']}, expected {pub_before}"
        before = {k: (None if v is None else str(v)) for k, v in row.items()}
        print("BEFORE", json.dumps(before))
        befores.append(before)
    if EXECUTE:
        afters = []
        for sid in TARGETS:
            n = conn.execute(text("UPDATE sources SET publisher_id = :p, publisher = :pn, updated_date = now() "
                                  "WHERE id = :id"), {"p": P_CSIC, "pn": P_CSIC_NAME, "id": sid}).rowcount
            assert n == 1, (sid, n)
            afters.append({k: str(v) for k, v in conn.execute(text(
                "SELECT id, display_name, publisher_id, publisher FROM sources WHERE id = :id"),
                {"id": sid}).mappings().fetchone().items()})
        print("RECEIPT", json.dumps({"job": 837, "ticket": 3994, "executed_at": datetime.now(timezone.utc).isoformat(),
                                     "before": befores, "after": afters}))
    else:
        print("dry run: would set publisher_id", P_CSIC, repr(P_CSIC_NAME), "on", sorted(TARGETS))
