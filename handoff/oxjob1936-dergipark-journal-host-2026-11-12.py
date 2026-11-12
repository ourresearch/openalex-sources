"""DergiPark's feed is a journal host (oxjob #1936, 2026-11-12): one guarded flag, with a receipt.

DergiPark (dergipark.org.tr) is the platform Turkish journals publish on: every article page there is the journal's own
site. Its one OAI feed (endpoint c9b39f920453a26ec1c) is bound to the portal record S4306401840, a repository. From the
night of 12 Nov walden's CreateLocationsWithSources re-binds each DergiPark record to its own journal by the journal slug
and year (openalex.sources.dergipark_journal_sources). This flag makes CreateWorksBase read those re-bound locations as
the publisher's copy (host_type publisher, publishedVersion). Its gate also needs a journal-typed source, so records that
stay on the portal record (a repository) are untouched. The 042 comment says "one journal per endpoint"; here the journal
is decided per record, which is what that rule protects.

    python handoff/oxjob1936-dergipark-journal-host-2026-11-12.py              # dry run, rolled back
    python handoff/oxjob1936-dergipark-journal-host-2026-11-12.py --execute    # write + receipt
    python handoff/oxjob1936-dergipark-journal-host-2026-11-12.py --revert [--execute]
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
ENDPOINT, SOURCE = "c9b39f920453a26ec1c", 4306401840
OLD, NEW = (True, False) if REVERT else (False, True)


class DryRun(Exception):
    pass


receipt = {"oxjob": 1936, "endpoint_id": ENDPOINT, "mode": "revert" if REVERT else "apply"}
try:
    with engine.begin() as conn:
        rows = conn.execute(text(
            "UPDATE oai_pmh_endpoint SET is_journal_host = :new "
            "WHERE id = :id AND source_id = :sid AND is_journal_host = :old "
            "RETURNING id, pmh_url, source_id, is_journal_host"),
            {"new": NEW, "old": OLD, "id": ENDPOINT, "sid": SOURCE}).fetchall()
        receipt["changed"] = [dict(r._mapping) for r in rows]
        print(f"rows changed: {len(rows)} (expect 1): {receipt['changed']}", flush=True)
        if len(rows) != 1:
            raise SystemExit("guard failed: the endpoint is not in the expected state; nothing written")
        if not EXECUTE:
            raise DryRun()
except DryRun:
    print("dry run: rolled back")
else:
    receipt["executed_at"] = datetime.now(timezone.utc).isoformat()
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        f"oxjob1936-dergipark-journal-host-2026-11-12{'-revert' if REVERT else ''}-receipt.json")
    json.dump(receipt, open(path, "w"), indent=1)
    print("receipt:", path)
