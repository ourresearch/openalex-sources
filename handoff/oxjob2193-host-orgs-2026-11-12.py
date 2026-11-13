"""Publisher for sources that have none though Crossref's own deposits name one (oxjob #2193, wave 1, 2026-11-12).

`sources.publisher_id` is NULL on thousands of journals, ebook platforms and book series whose publisher is known:
The BMJ (S4393911389, 389K works), Advanced Materials (S99352657), 143 Wiley journals, 126 Hans Publishers journals.
The source page shows no host organization and publisher filters miss their works (BMJ's publisher view lacked 41%).

handoff/oxjob2193-host-orgs-2026-11-12.csv (built by oxjob #2193 from openalex_dev.default.oxjob2193_manifest_w1)
lists per source the publisher to set. A source is in it only when:
  - the publisher string on its Crossref records (>= 90% one string, >= 5 records, >= half its works) equals the
    display name of exactly one live OpenAlex publisher, and
  - every other rule that fires for it (DOI-prefix owner, registry publisher text, hosted sources with the same
    Crossref string) names the same publisher, and its DOI prefix is not shared between hosts.
OJS (#1582) and Diamond Discovery Hub (#1848) sources and DOI-agent prefixes (BHL, OSTI, TechKnowledge, EKB) are out.
Bar on a fresh draw of 100 (seed 219301): 98 agree with Crossref's journal record / prefix member.

WORKS RE-STAMP: every work with a location in these sources gets the host embedded (about 1.54M works). It is a rider
on the End 2 End that follows the write (oxjobs changes.yaml id 2193-sources-publisher-w1): run --execute only on the
evening of the night it is registered for, before 23:00 CT.

Guard per row, re-checked inside the UPDATE: publisher_id AND institution_id are both NULL (never overwrites), and
the registry's publisher text still equals the text the manifest was built from.

    python handoff/oxjob2193-host-orgs-2026-11-12.py              # dry run, rolled back
    python handoff/oxjob2193-host-orgs-2026-11-12.py --execute    # write + print the receipt
    python handoff/oxjob2193-host-orgs-2026-11-12.py --revert handoff/oxjob2193-host-orgs-2026-11-12-receipt.json [--execute]

Wave 2 (same night, 1,408 sources, ~378K works; rider 2193-sources-publisher-w2): the DOI-prefix owner and the
hosted sources with the same Crossref publisher string agree on one publisher, and that string shares a distinctive
word or acronym with the publisher's name. Bar 98 of 100 (seed 219302). Add
`--manifest handoff/oxjob2193-host-orgs-w2-2026-11-12.csv`; its receipt is ...-w2-2026-11-12-receipt.json.

Revert sets publisher_id back to NULL on each receipt row only if it still carries the id this run wrote.
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
# --manifest <csv> picks another wave's list (wave 2: oxjob2193-host-orgs-w2-2026-11-12.csv); default is wave 1
CSV = sys.argv[sys.argv.index("--manifest") + 1] if "--manifest" in sys.argv else os.path.join(HERE, "oxjob2193-host-orgs-2026-11-12.csv")
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
COUNTS = ("SELECT COUNT(*) FILTER (WHERE publisher_id IS NULL AND institution_id IS NULL), "
          "COUNT(*) FILTER (WHERE publisher_id IS NOT NULL) FROM sources")


class DryRun(Exception):
    pass


def fill(conn, receipt):
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    ids = [int(r["source_id"]) for r in rows]
    assert len(set(ids)) == len(ids), "duplicate source ids in the manifest"
    done = {r.id for r in conn.execute(text("""
        UPDATE sources s SET publisher_id = v.tid, updated_date = now()
        FROM unnest(CAST(:ids AS bigint[]), CAST(:tids AS bigint[]), CAST(:texts AS text[])) AS v(id, tid, expected)
        WHERE s.id = v.id AND s.publisher_id IS NULL AND s.institution_id IS NULL
          AND btrim(coalesce(s.publisher, '')) = v.expected
        RETURNING s.id
    """), {"ids": ids, "tids": [int(r["target_publisher_id"]) for r in rows],
           "texts": [(r["expected_publisher_text"] or "").strip() for r in rows]})}
    receipt["changed"] = {r["source_id"]: [None, int(r["target_publisher_id"])] for r in rows if int(r["source_id"]) in done}
    receipt["skipped"] = {r["source_id"]: "missing, already hosted, or publisher text changed"
                          for r in rows if int(r["source_id"]) not in done}
    print(f"filled {len(done)} of {len(rows)}; skipped {len(receipt['skipped'])}", flush=True)
    print("top publishers:", Counter(r["target_name"] for r in rows if int(r["source_id"]) in done).most_common(8), flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    wrote = {int(sid): tid for sid, (_, tid) in prior["changed"].items()}
    done = {r.id for r in conn.execute(text("""
        UPDATE sources s SET publisher_id = NULL, updated_date = now()
        FROM unnest(CAST(:ids AS bigint[]), CAST(:tids AS bigint[])) AS v(id, tid)
        WHERE s.id = v.id AND s.publisher_id = v.tid
        RETURNING s.id
    """), {"ids": list(wrote), "tids": list(wrote.values())})}
    receipt["skipped"] = {str(sid): f"publisher_id no longer {tid}" for sid, tid in wrote.items() if sid not in done}
    receipt["reverted"] = len(done)
    print(f"reverted {len(done)} of {len(wrote)}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT}
    try:
        with engine.begin() as conn:
            receipt["before"] = list(conn.execute(text(COUNTS)).fetchone())
            (revert if REVERT else fill)(conn, receipt)
            receipt["after"] = list(conn.execute(text(COUNTS)).fetchone())
            print(f"sources with no host {receipt['before'][0]} -> {receipt['after'][0]}; "
                  f"sources with a publisher_id {receipt['before'][1]} -> {receipt['after'][1]}", flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
