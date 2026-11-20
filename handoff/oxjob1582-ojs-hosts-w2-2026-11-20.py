"""Host organization for OJS journals that had none, wave 2 (oxjob #1582, 2026-11-20).

After wave 1 (handoff/oxjob1582-ojs-hosts-2026-11-19.py, 13,960 journals), 16,033 OJS journals still had no host. For
those whose own site names a publisher that wave 1 found no record for, #1582 looked again with local fuzzy candidates
(every word order, legal suffix and of/for variant of every live publisher and institution, including the ~7,400
publishers created 18-19 Nov) and asked the same matching question; this script points each matched journal at the
existing record: a publisher, or an institution (written as the institution's ROR-matched publisher record when one
exists, else as the institution: the ojs-journals charter's 6 Oct 2026 host rule). No publishers are created.

handoff/oxjob1582-ojs-hosts-w2-2026-11-20.csv (built by oxjob #1582 work/route_w2.py) lists per source: target_kind
('publisher' | 'institution'), target_id, target_name, and the registry publisher text it was built from. Bar on a
pre-registered draw: see the oxjob's README.

WORKS RE-STAMP: every work with a location in these sources gets the host embedded. It is a rider on the End 2 End that
follows the write (changes.yaml id 1582-ojs-hosts-w2): run --execute only after the night before has published and before
21:30 CT of the night it is registered for.

Guard per row, re-checked inside the UPDATE: publisher_id AND institution_id are both NULL (never overwrites), and the
registry's publisher text still equals the text the manifest was built from.

    python handoff/oxjob1582-ojs-hosts-w2-2026-11-20.py              # dry run, rolled back
    python handoff/oxjob1582-ojs-hosts-w2-2026-11-20.py --execute    # write + print the receipt
    python handoff/oxjob1582-ojs-hosts-w2-2026-11-20.py --revert <receipt.json> [--execute]

Revert sets the column back to NULL on each receipt row only if it still carries the id this run wrote.
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
CSV = sys.argv[sys.argv.index("--manifest") + 1] if "--manifest" in sys.argv else os.path.join(HERE, "oxjob1582-ojs-hosts-w2-2026-11-20.csv")
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
COUNTS = ("SELECT COUNT(*) FILTER (WHERE publisher_id IS NULL AND institution_id IS NULL), "
          "COUNT(*) FILTER (WHERE publisher_id IS NOT NULL), COUNT(*) FILTER (WHERE institution_id IS NOT NULL) FROM sources")
COLUMN = {"publisher": "publisher_id", "institution": "institution_id"}


class DryRun(Exception):
    pass


def fill(conn, receipt):
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    ids = [int(r["source_id"]) for r in rows]
    assert len(set(ids)) == len(ids), "duplicate source ids in the manifest"
    assert {r["target_kind"] for r in rows} <= set(COLUMN), "bad target_kind"
    receipt["changed"], receipt["skipped"] = {}, {}
    for kind, col in COLUMN.items():
        part = [r for r in rows if r["target_kind"] == kind]
        if not part:
            continue
        done = {r.id for r in conn.execute(text(f"""
            UPDATE sources s SET {col} = v.tid, updated_date = now()
            FROM unnest(CAST(:ids AS bigint[]), CAST(:tids AS bigint[]), CAST(:texts AS text[])) AS v(id, tid, expected)
            WHERE s.id = v.id AND s.publisher_id IS NULL AND s.institution_id IS NULL
              AND btrim(coalesce(s.publisher, '')) = v.expected
            RETURNING s.id
        """), {"ids": [int(r["source_id"]) for r in part], "tids": [int(r["target_id"]) for r in part],
               "texts": [(r["expected_publisher_text"] or "").strip() for r in part]})}
        for r in part:
            if int(r["source_id"]) in done:
                receipt["changed"][r["source_id"]] = [col, None, int(r["target_id"])]
            else:
                receipt["skipped"][r["source_id"]] = "missing, already hosted, or publisher text changed"
        print(f"{col}: filled {len(done)} of {len(part)}", flush=True)
    print(f"skipped {len(receipt['skipped'])}", flush=True)
    print("top targets:", Counter(r["target_name"] for r in rows if r["source_id"] in receipt["changed"]).most_common(8), flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["skipped"], receipt["reverted"] = {}, 0
    for col in COLUMN.values():
        wrote = {int(sid): tid for sid, (c, _, tid) in prior["changed"].items() if c == col}
        if not wrote:
            continue
        done = {r.id for r in conn.execute(text(f"""
            UPDATE sources s SET {col} = NULL, updated_date = now()
            FROM unnest(CAST(:ids AS bigint[]), CAST(:tids AS bigint[])) AS v(id, tid)
            WHERE s.id = v.id AND s.{col} = v.tid
            RETURNING s.id
        """), {"ids": list(wrote), "tids": list(wrote.values())})}
        receipt["skipped"].update({str(sid): f"{col} no longer {tid}" for sid, tid in wrote.items() if sid not in done})
        receipt["reverted"] += len(done)
    print(f"reverted {receipt['reverted']}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT}
    try:
        with engine.begin() as conn:
            receipt["before"] = list(conn.execute(text(COUNTS)).fetchone())
            (revert if REVERT else fill)(conn, receipt)
            receipt["after"] = list(conn.execute(text(COUNTS)).fetchone())
            print(f"sources with no host {receipt['before'][0]} -> {receipt['after'][0]}; with a publisher_id "
                  f"{receipt['before'][1]} -> {receipt['after'][1]}; with an institution_id {receipt['before'][2]} -> "
                  f"{receipt['after'][2]}", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
