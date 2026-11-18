"""Publisher for journals whose publisher had no OpenAlex record (oxjob #2385, ruling D12, 2026-11-18).

16,325 journals had no host organization although their own Crossref deposits name one publisher, because that
publisher had no OpenAlex record. Ruling D12 (Jason, 17 Nov 2026): create a publisher ROR doesn't list when its Crossref
membership (it deposits the journal's DOIs) and the journal's own site (naming it as publisher) both confirm it.
#2385 created those publishers in openalex.publishers.publishers (receipt in the oxjob); this script points each
witnessed journal at its publisher, or at the existing institution / publisher the Crossref member already is.

handoff/oxjob2385-d12-hosts-2026-11-18.csv (built by oxjob #2385 work/build_manifest.py) lists per source:
target_kind ('publisher' | 'institution'), target_id, and the registry publisher text it was built from. A source is
in it only when its Crossref member deposits >= 90% of its Crossref records (>= 5, >= half its works), its DOI prefix
carries no hosted source, and its own site names the member as publisher (code match, or an Opus judge at high/medium
confidence). Bar on a pre-registered draw: see the oxjob's README.

WORKS RE-STAMP: every work with a location in these sources gets the host embedded. It is a rider on the End 2 End that
follows the write (changes.yaml id 2385-d12-publishers): run --execute only after the night before has published and
before 21:30 CT of the night it is registered for.

Guard per row, re-checked inside the UPDATE: publisher_id AND institution_id are both NULL (never overwrites), and the
registry's publisher text still equals the text the manifest was built from.

    python handoff/oxjob2385-d12-hosts-2026-11-18.py              # dry run, rolled back
    python handoff/oxjob2385-d12-hosts-2026-11-18.py --execute    # write + print the receipt
    python handoff/oxjob2385-d12-hosts-2026-11-18.py --revert <receipt.json> [--execute]

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
CSV = sys.argv[sys.argv.index("--manifest") + 1] if "--manifest" in sys.argv else os.path.join(HERE, "oxjob2385-d12-hosts-2026-11-18.csv")
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
