"""Repoint 81 sources whose host organization is a merged or missing entity (oxjob #1880).

Their publisher_id / institution_id is an id the API 404s on: 13 journals on publishers merged in 2023 (Springer
Gabler, Springer Berlin Heidelberg, Springer US, Wiley-VCH), 48 journals on publisher ids that exist in no publisher
table, 20 repositories on merged institutions (13 of them merged into 'Deleted Institution', e.g. Dialnet ->
Universidad Internacional de La Rioja). The source page's host link 404s and
filter=primary_location.source.host_organization:I3032752892 still returns Dialnet's 1.73M works.

handoff/oxjob1880-dead-hosts.csv (built by oxjob #1880 work/host_decisions.py, every target checked live on the API
26 Oct) gives per source the old and new publisher_id / institution_id and the rule:
  merge_map (19)   follow OpenAlex's own merge_into_id to the live entity
  named_host (39)  the live entity the source names (display name, publisher string, homepage / landing-page domain)
  sibling (1)      unnamed JSCE journal sharing the ghost id with six JSCE journals
  none (22)        nothing live to point at: the dead id is cleared (no host rather than a 404 link)

WORKS RE-STAMP: every work with a location in these sources gets a new embedded host (3,378,320 on 26 Oct, Dialnet
2.76M). It is a rider on the End 2 End that follows the write (oxjobs changes.yaml id 1880-dead-hosts): run --execute
only on the evening of the night it is registered for, before 00:00 CT.

Guard per row: the source exists and still has exactly the old publisher_id AND institution_id (never overwrites a
later edit). Revert: the receipt lists every (source_id, old publisher_id, old institution_id); set them back.

One transaction, dry run by default, receipt after RECEIPT.
    python handoff/oxjob1880-dead-hosts.py [--execute]
"""
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "oxjob1880-dead-hosts.csv")
EXECUTE = "--execute" in sys.argv
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "changed": {}, "skipped": {}}


class DryRun(Exception):
    pass


def to_int(x):
    return int(x) if x not in ("", None) else None


def main():
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    assert len(rows) == 81, len(rows)
    try:
        with engine.begin() as conn:
            cur = {r.id: (r.publisher_id, r.institution_id) for r in conn.execute(
                text("SELECT id, publisher_id, institution_id FROM sources WHERE id = ANY(:i)"),
                {"i": [int(r["source_id"]) for r in rows]})}
            rules = Counter()
            for r in rows:
                sid = int(r["source_id"])
                old = (to_int(r["old_publisher_id"]), to_int(r["old_institution_id"]))
                new = (to_int(r["new_publisher_id"]), to_int(r["new_institution_id"]))
                now = cur.get(sid)
                if now != old:
                    receipt["skipped"][str(sid)] = f"now {now}, expected {old}"
                    continue
                k = conn.execute(text(
                    "UPDATE sources SET publisher_id = :np, institution_id = :ni, updated_date = now() "
                    "WHERE id = :s AND publisher_id IS NOT DISTINCT FROM :op AND institution_id IS NOT DISTINCT FROM :oi"),
                    {"np": new[0], "ni": new[1], "op": old[0], "oi": old[1], "s": sid}).rowcount
                assert k == 1, sid
                receipt["changed"][str(sid)] = {"old": list(old), "new": list(new), "rule": r["rule"]}
                rules[r["rule"]] += 1
            print(f"changed {len(receipt['changed'])} of {len(rows)} {dict(rules)}; skipped {len(receipt['skipped'])} "
                  f"{receipt['skipped']}", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
