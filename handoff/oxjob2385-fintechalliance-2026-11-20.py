"""Point two journals at their publisher, FinTechAlliance (oxjob #2385, ruling D12, 2026-11-20).

Publisher P4404685465 FinTechAlliance (Kyiv, UA) was created on 20 Nov under ruling D12: Crossref member 34490
'FinTechAlliance' owns prefix 10.55643 and deposits both journals' DOIs, and both journals' own sites name
TOV "FinTechAlliance" as publisher (fkd.net.ua, ser.net.ua).
  4210175534 Financial and credit activity problems of theory and practice: publisher_id 4366153121 -> 4404685465
             (the old record is the journal's former publisher)
  4387279944 Socio-economic relations in the digital society: no host -> 4404685465

WORKS RE-STAMP: ~2,950 works take the new host on the End 2 End after the write.

Guard per row, re-checked inside the UPDATE: publisher_id still what it was and institution_id NULL.
    python handoff/oxjob2385-fintechalliance-2026-11-20.py              # dry run, rolled back
    python handoff/oxjob2385-fintechalliance-2026-11-20.py --execute    # write + print the receipt
    python handoff/oxjob2385-fintechalliance-2026-11-20.py --revert <receipt.json> [--execute]
Revert puts publisher_id back only on rows still holding what this run wrote.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

NEW = 4404685465
ROWS = [(4210175534, 4366153121), (4387279944, None)]  # (source id, publisher_id before)
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None


class DryRun(Exception):
    pass


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT,
               "changed": {}, "skipped": {}}
    try:
        with engine.begin() as conn:
            if REVERT:
                prior = json.load(open(REVERT, encoding="utf-8"))
                for sid, (frm, to) in prior["changed"].items():
                    n = conn.execute(text("UPDATE sources SET publisher_id = :frm, updated_date = now() "
                                          "WHERE id = :id AND publisher_id = :to"),
                                     {"id": int(sid), "frm": frm, "to": to}).rowcount
                    (receipt["changed"] if n else receipt["skipped"])[sid] = [to, frm] if n else "edited since"
            else:
                for sid, frm in ROWS:
                    n = conn.execute(text("UPDATE sources SET publisher_id = :new, updated_date = now() "
                                          "WHERE id = :id AND publisher_id IS NOT DISTINCT FROM :frm "
                                          "AND institution_id IS NULL"),
                                     {"id": sid, "frm": frm, "new": NEW}).rowcount
                    if n:
                        receipt["changed"][str(sid)] = [frm, NEW]
                    else:
                        receipt["skipped"][str(sid)] = "missing, or publisher/institution changed since"
            for r in conn.execute(text("SELECT id, publisher_id, institution_id FROM sources WHERE id = ANY(:ids)"),
                                  {"ids": [s for s, _ in ROWS]}):
                print(f"{r.id}: publisher_id {r.publisher_id}, institution_id {r.institution_id}", flush=True)
            print(f"changed {len(receipt['changed'])}, skipped {len(receipt['skipped'])}", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
