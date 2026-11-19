"""Publisher for journals whose DOI prefix other sources already host (oxjob #2408), plus wrong hosts on those prefixes.

Journals with no host organization whose own Crossref deposits name one publisher, while other OpenAlex sources on the
same DOI prefix already carry a host. A prefix alone is not proof of the publisher (shared, transferred and agent
prefixes), so each journal's target comes from what Crossref names for that journal (its journal record, else its DOI
deposits): the existing OpenAlex record that is that organisation or its group's record, the publisher a platform
journal's own site names, or a publisher created under ruling D12 (oxjob #2385's rule: Crossref membership plus the
journal's own site). Sources on the same prefixes whose host is a clearly different organisation from the one their own
Crossref deposits name are repointed. Bars per tier on pre-registered draws: see the oxjob.

The CSV (built by oxjob #2408 work/build_manifest.py) has per source: action ('fill' | 'repoint'), target_kind
('publisher' | 'institution'), target_id, expected_publisher_text (the registry's publisher text it was built from), and
for repoint rows old_column / old_id.

WORKS RE-STAMP: every work with a location in these sources gets the host embedded; a rider on the End 2 End that follows
the write (changes.yaml id 2408-hosted-prefix-publishers). Run --execute only after the night before has published and
before 21:30 CT of the night it rides.

Guards, re-checked inside each UPDATE:
  fill:    publisher_id AND institution_id are both NULL, and the registry publisher text still equals the expected text.
  repoint: the old column still holds old_id, and the other host column is NULL.

    python handoff/<this>.py --manifest <csv>                   # dry run, rolled back
    python handoff/<this>.py --manifest <csv> --execute         # write + print the receipt
    python handoff/<this>.py --revert <receipt.json> [--execute]

Revert puts each receipt row back (fill -> NULL; repoint -> old id) only if it still carries the id this run wrote.
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

CSV = sys.argv[sys.argv.index("--manifest") + 1] if "--manifest" in sys.argv else None
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
COUNTS = ("SELECT COUNT(*) FILTER (WHERE publisher_id IS NULL AND institution_id IS NULL), "
          "COUNT(*) FILTER (WHERE publisher_id IS NOT NULL), COUNT(*) FILTER (WHERE institution_id IS NOT NULL) FROM sources")
COLUMN = {"publisher": "publisher_id", "institution": "institution_id"}
OTHER = {"publisher_id": "institution_id", "institution_id": "publisher_id"}


class DryRun(Exception):
    pass


def fill(conn, receipt):
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    ids = [int(r["source_id"]) for r in rows]
    assert len(set(ids)) == len(ids), "duplicate source ids in the manifest"
    assert {r["target_kind"] for r in rows} <= set(COLUMN), "bad target_kind"
    assert {r["action"] for r in rows} <= {"fill", "repoint"}, "bad action"
    receipt["changed"], receipt["skipped"] = {}, {}
    for kind, col in COLUMN.items():
        part = [r for r in rows if r["target_kind"] == kind and r["action"] == "fill"]
        if part:
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
            print(f"fill {col}: {len(done)} of {len(part)}", flush=True)
        for old_col in COLUMN.values():
            part = [r for r in rows if r["target_kind"] == kind and r["action"] == "repoint" and r["old_column"] == old_col]
            if not part:
                continue
            clear = f", {old_col} = NULL" if old_col != col else ""
            done = {r.id for r in conn.execute(text(f"""
                UPDATE sources s SET {col} = v.tid{clear}, updated_date = now()
                FROM unnest(CAST(:ids AS bigint[]), CAST(:tids AS bigint[]), CAST(:olds AS bigint[])) AS v(id, tid, old)
                WHERE s.id = v.id AND s.{old_col} = v.old AND s.{OTHER[old_col]} IS NULL
                RETURNING s.id
            """), {"ids": [int(r["source_id"]) for r in part], "tids": [int(r["target_id"]) for r in part],
                   "olds": [int(r["old_id"]) for r in part]})}
            for r in part:
                if int(r["source_id"]) in done:
                    receipt["changed"][r["source_id"]] = [col, [old_col, int(r["old_id"])], int(r["target_id"])]
                else:
                    receipt["skipped"][r["source_id"]] = "missing or host changed since the manifest"
            print(f"repoint {old_col} -> {col}: {len(done)} of {len(part)}", flush=True)
    print(f"skipped {len(receipt['skipped'])}", flush=True)
    print("top targets:", Counter(r["target_name"] for r in rows if r["source_id"] in receipt["changed"]).most_common(8), flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["skipped"], receipt["reverted"] = {}, 0
    for sid, (col, old, tid) in prior["changed"].items():
        if old is None:
            sql, p = f"UPDATE sources SET {col} = NULL, updated_date = now() WHERE id = :id AND {col} = :tid RETURNING id", {}
        else:
            old_col, old_id = old
            sets = f"{old_col} = :old" + (f", {col} = NULL" if old_col != col else "")
            sql, p = f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id AND {col} = :tid RETURNING id", {"old": old_id}
        if conn.execute(text(sql), {"id": int(sid), "tid": tid, **p}).fetchone():
            receipt["reverted"] += 1
        else:
            receipt["skipped"][sid] = f"{col} no longer {tid}"
    print(f"reverted {receipt['reverted']}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def main():
    assert CSV or REVERT, "--manifest <csv> or --revert <receipt>"
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "manifest": CSV, "revert_of": REVERT}
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
