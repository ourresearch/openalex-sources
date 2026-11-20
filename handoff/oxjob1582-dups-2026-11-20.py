"""Re-point journals whose publisher record was a duplicate (oxjob #1582, ruling D12, 2026-11-20).

On 19 Nov oxjob #1582 created 2,795 publishers under ruling D12 and pointed OJS journals at them. A second duplicate
check on 20 Nov (the same method as oxjob #2385's: local fuzzy candidates over every existing publisher and
institution, then two independent model checks that had to agree at high confidence) found 9 of them already existed:
7 as institutions, 2 as publishers. The publisher records are merged (publishers) or removed (institutions) in
openalex.publishers.publishers by the oxjob; this script moves each registry row that points at one of those 9 records
to the record that already existed.

handoff/oxjob1582-dups-2026-11-20.csv: source_id, from_publisher_id (the duplicate), target_kind
('publisher' | 'institution'), target_id.

WORKS RE-STAMP: the works of these journals take the surviving host. Rider on the End 2 End that follows the write
(changes.yaml id 1582-ojs-hosts-w2); run --execute before 21:30 CT of the night it is registered for.

Guard per row, re-checked inside the UPDATE: publisher_id still equals the duplicate and institution_id is NULL.
    python handoff/oxjob1582-dups-2026-11-20.py              # dry run, rolled back
    python handoff/oxjob1582-dups-2026-11-20.py --execute    # write + print the receipt
    python handoff/oxjob1582-dups-2026-11-20.py --revert <receipt.json> [--execute]
Revert puts publisher_id back to the duplicate (and institution_id back to NULL) only on rows still holding what this
run wrote.
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "oxjob1582-dups-2026-11-20.csv")
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
COUNTS = ("SELECT COUNT(*) FILTER (WHERE publisher_id IS NULL AND institution_id IS NULL), "
          "COUNT(*) FILTER (WHERE publisher_id IS NOT NULL), COUNT(*) FILTER (WHERE institution_id IS NOT NULL) FROM sources")


class DryRun(Exception):
    pass


def repoint(conn, receipt):
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    assert len({r["source_id"] for r in rows}) == len(rows), "duplicate source ids in the manifest"
    assert {r["target_kind"] for r in rows} <= {"publisher", "institution"}, "bad target_kind"
    receipt["changed"], receipt["skipped"] = {}, {}
    for kind in ("publisher", "institution"):
        part = [r for r in rows if r["target_kind"] == kind]
        if not part:
            continue
        sets = "publisher_id = v.tid" if kind == "publisher" else "publisher_id = NULL, institution_id = v.tid"
        done = {r.id for r in conn.execute(text(f"""
            UPDATE sources s SET {sets}, updated_date = now()
            FROM unnest(CAST(:ids AS bigint[]), CAST(:froms AS bigint[]), CAST(:tids AS bigint[])) AS v(id, from_id, tid)
            WHERE s.id = v.id AND s.publisher_id = v.from_id AND s.institution_id IS NULL
            RETURNING s.id
        """), {"ids": [int(r["source_id"]) for r in part], "froms": [int(r["from_publisher_id"]) for r in part],
               "tids": [int(r["target_id"]) for r in part]})}
        for r in part:
            if int(r["source_id"]) in done:
                receipt["changed"][r["source_id"]] = [kind, int(r["from_publisher_id"]), int(r["target_id"])]
            else:
                receipt["skipped"][r["source_id"]] = "missing, or no longer on the duplicate publisher"
        print(f"{kind}: re-pointed {len(done)} of {len(part)}", flush=True)
    print(f"skipped {len(receipt['skipped'])}", flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["skipped"], receipt["reverted"] = {}, 0
    for kind, col in (("publisher", "publisher_id"), ("institution", "institution_id")):
        wrote = {int(sid): (frm, tid) for sid, (k, frm, tid) in prior["changed"].items() if k == kind}
        if not wrote:
            continue
        done = {r.id for r in conn.execute(text(f"""
            UPDATE sources s SET publisher_id = v.from_id, institution_id = NULL, updated_date = now()
            FROM unnest(CAST(:ids AS bigint[]), CAST(:froms AS bigint[]), CAST(:tids AS bigint[])) AS v(id, from_id, tid)
            WHERE s.id = v.id AND s.{col} = v.tid
            RETURNING s.id
        """), {"ids": list(wrote), "froms": [f for f, _ in wrote.values()], "tids": [t for _, t in wrote.values()]})}
        receipt["skipped"].update({str(sid): f"{col} no longer {t}" for sid, (_, t) in wrote.items() if sid not in done})
        receipt["reverted"] += len(done)
    print(f"reverted {receipt['reverted']}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT}
    try:
        with engine.begin() as conn:
            receipt["before"] = list(conn.execute(text(COUNTS)).fetchone())
            (revert if REVERT else repoint)(conn, receipt)
            receipt["after"] = list(conn.execute(text(COUNTS)).fetchone())
            print(f"no host {receipt['before'][0]} -> {receipt['after'][0]}; publisher_id {receipt['before'][1]} -> "
                  f"{receipt['after'][1]}; institution_id {receipt['before'][2]} -> {receipt['after'][2]}", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
