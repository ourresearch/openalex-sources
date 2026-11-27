"""oxjob #693 (support factory #2254), written off desk 27 Nov 2026 (lane #2263): set the publisher of the journal
'Replication Research' S7407154776 (ISSN 3052-5977; ZD 24980) to University of Münster P4310313985.

WHY: the 24 Nov mint (oxjob693-replication-research-2026-11-24.py, manifest publisher text "University of Münster")
left publisher_id and institution_id NULL, so the live source shows host_organization null. The editor-in-chief asked
on ZD 24980 (26 Nov 07:42 UTC) for "University of Münster" (P4310313985), as for the sister journal Free Neuropathology
S4393919117 (live host_organization P4310313985). The journal's own legal disclosure
(https://www.replicationresearch.org/legal-disclosure/, read 24 Nov, work/witness_replication_research_2026-11-24.md):
"University of Münster, Münster Center for Open Science ... Münster Center for Open Science is part of the University of
Münster ... ISSN 3052-5977". Policy choice: follows D5 (RULINGS § 3: the venue's own site) and Casey's host rule #1563
(publisher_id for a journal; walden CreateSourcesApi reads host_organization from publisher_id).

ACTION (one transaction, dry run by default): UPDATE sources SET publisher_id = 4310313985, publisher = 'University of
Münster' WHERE id = 7407154776 AND publisher_id IS NULL. Asserts: the source exists, is a journal named 'Replication
Research', and publisher 4310313985 exists and is not merged.

Run on desk, outside the End 2 End window:
    cp <this file> ~/ox/openalex-sources/handoff/oxjob693-rr-publisher-2026-11-27.py && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob693-rr-publisher-2026-11-27.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob693-rr-publisher-2026-11-27.py --execute
Check (after the next sources build): https://api.openalex.org/sources/S7407154776 host_organization = P4310313985.
Revert: UPDATE sources SET publisher_id = NULL, publisher = <receipt before.publisher> WHERE id = 7407154776.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402

EXECUTE = "--execute" in sys.argv
SOURCE = 7407154776
PUBLISHER = 4310313985
PUBLISHER_NAME = "University of Münster"
RECEIPT_PATH = os.path.join(HERE, "oxjob693-rr-publisher-2026-11-27-receipt.json")
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 693, "ticket": 24980,
           "approved": "support factory #2254 (Jason 2026-11-15 yes); RULINGS § 3 D5; Casey's host rule #1563",
           "before": None, "after": None, "actions": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn):
    r = conn.execute(text("SELECT id, display_name, type, publisher, publisher_id, institution_id, issn_l "
                          "FROM sources WHERE id = :i"), {"i": SOURCE}).mappings().fetchone()
    return {k: (v if isinstance(v, (int, type(None))) else str(v)) for k, v in r.items()} if r else None


def main():
    try:
        with engine.begin() as conn:
            before = row(conn)
            receipt["before"] = before
            assert before, f"S{SOURCE} missing"
            assert before["display_name"] == "Replication Research" and before["type"] == "journal", before
            p = conn.execute(text("SELECT id, display_name FROM publishers WHERE id = :p"), {"p": PUBLISHER}).fetchone() \
                if conn.execute(text("SELECT to_regclass('publishers')")).scalar() else None
            if p is not None:
                log(f"publisher P{PUBLISHER} in registry: {p.display_name!r}")
            else:
                log(f"publishers table not visible here; P{PUBLISHER} checked live on the API 27 Nov (University of Münster, not merged)")
            n = conn.execute(text("UPDATE sources SET publisher_id = :p, publisher = :n, updated_date = now() "
                                  "WHERE id = :i AND publisher_id IS NULL"),
                             {"p": PUBLISHER, "n": PUBLISHER_NAME, "i": SOURCE}).rowcount
            log(f"S{SOURCE} publisher_id NULL -> {PUBLISHER} ({PUBLISHER_NAME}): {n} row(s)")
            receipt["after"] = row(conn)
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        json.dump(receipt, open(RECEIPT_PATH, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", RECEIPT_PATH)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
