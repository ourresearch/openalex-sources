"""Point each source's issn_l at the ISSN-L the ISSN International Centre assigns (oxjob #2154).

The ISSN IC designates one linking ISSN (ISSN-L) per continuing resource; `jobs/issn_to_issnl`
reloads its table. `sources_lib` reads that table only when a source is minted or merged, and
`insert_issns` never overwrites a stored issn_l, so legacy rows kept a first-ISSN guess and later
table changes never reached the registry: 5,402 sources (2.53M works) on 2026-11-11. Example:
Journal of Materials Chemistry A stored 2050-7488 while the ISSN IC gives 2050-7496, so
`/sources?filter=issn_l:2050-7496` found nothing.

A source is in scope when its own ISSNs map to exactly ONE ISSN-L in the table and its issn_l is
not that ISSN-L. Two actions:

  repoint  the ISSN-L is already one of the source's ISSNs: set issn_l to it. Moves no work
           (walden matches works to sources by the whole ISSN list, never by issn_l), so it is
           safe after every table reload; jobs/issn_to_issnl runs it.
  adopt    no source owns the ISSN-L: add it to the source's ISSNs, then repoint. Adding an ISSN
           can pull works that carry it onto the source (walden's Crossref and DataCite ISSN
           tiers), so only ids listed in --adopt-ids, vetted against those tiers, are adopted.

Never written: a source whose ISSNs map to two or more ISSN-Ls (one record holding two journals,
#1726), an ISSN-L another source owns (a merge question), an ISSN-L two sources would both adopt,
and ids in KEEP (an issn_l chosen on purpose; none today).

  python -m jobs.reconcile_issn_l                                 # dry run: counts by class
  python -m jobs.reconcile_issn_l --report FILE.tsv               # every candidate with its class
  python -m jobs.reconcile_issn_l --apply --receipt FILE.csv [--adopt-ids FILE] [--max-changes N]
  python -m jobs.reconcile_issn_l --revert FILE.csv [--apply]     # put every receipt row back
"""
import argparse
import csv
import sys
from collections import Counter

from sqlalchemy import text

from sources_lib import refresh_issns_column

KEEP = frozenset()  # source ids whose issn_l is deliberately not the table's (none as of 2026-11-11)
DEFAULT_MAX_CHANGES = 500  # guard for the scheduled run after a table reload

CANDIDATES_SQL = """
WITH s AS (
  SELECT s.id, s.issn_l,
         (SELECT array_agg(si.issn ORDER BY si.issn) FROM source_issn si WHERE si.source_id = s.id) AS own,
         (SELECT array_agg(DISTINCT m.issn_l ORDER BY m.issn_l) FROM source_issn si
            JOIN issn_to_issnl m ON m.issn = si.issn WHERE si.source_id = s.id) AS tl
  FROM sources s WHERE s.issn_l IS NOT NULL
)
SELECT s.id, s.issn_l, s.own, s.tl,
       (SELECT o.source_id FROM source_issn o
         WHERE cardinality(s.tl) = 1 AND o.issn = s.tl[1] AND o.source_id <> s.id) AS true_l_owner
FROM s
WHERE s.tl IS NOT NULL AND NOT (s.issn_l = ANY(s.tl))
ORDER BY s.id
"""

WRITE_ACTIONS = ("repoint", "adopt")


def classify(source_id, issn_l, own, tl, true_l_owner, adopt_ids=frozenset(), shared=frozenset()):
    """(class, new issn_l or None) for one source. Pure: no database."""
    if source_id in KEEP:
        return "keep", None
    if len(tl) != 1:
        return "multi_family", None
    new = tl[0]
    if new == issn_l:
        return "agrees", None
    if new in own:
        return "repoint", new
    if true_l_owner is not None:
        return "owned_elsewhere", new
    if new in shared:
        return "shared_missing", new
    if source_id in adopt_ids:
        return "adopt", new
    return "missing", new


def plan(conn, adopt_ids=frozenset()):
    rows = conn.execute(text(CANDIDATES_SQL)).fetchall()
    missing = Counter(r.tl[0] for r in rows
                      if len(r.tl) == 1 and r.tl[0] not in r.own and r.true_l_owner is None)
    shared = {l for l, n in missing.items() if n > 1}
    out = []
    for r in rows:
        cls, new = classify(r.id, r.issn_l, r.own, r.tl, r.true_l_owner, adopt_ids, shared)
        out.append({"source_id": r.id, "class": cls, "old_issn_l": r.issn_l, "new_issn_l": new,
                    "own": ",".join(r.own), "table_issn_l": ",".join(r.tl),
                    "true_l_owner": r.true_l_owner})
    return out


def write_one(conn, sid, old, new, action):
    """Repoint (after adopting, if asked). Guarded on the old value: False if the row moved meanwhile."""
    if action == "adopt":
        added = conn.execute(text(
            "INSERT INTO source_issn (source_id, issn, is_issn_l) VALUES (:sid, :l, FALSE) "
            "ON CONFLICT (issn) DO NOTHING"), {"sid": sid, "l": new}).rowcount
        if added != 1:
            return False
    n = conn.execute(text(
        "UPDATE sources SET issn_l = :new, updated_date = now() WHERE id = :sid AND issn_l = :old "
        "AND EXISTS (SELECT 1 FROM source_issn WHERE source_id = :sid AND issn = :new)"),
        {"sid": sid, "old": old, "new": new}).rowcount
    if n != 1:
        if action == "adopt":
            conn.execute(text("DELETE FROM source_issn WHERE source_id = :sid AND issn = :l"),
                         {"sid": sid, "l": new})
        return False
    conn.execute(text(
        "UPDATE source_issn SET is_issn_l = (issn = :new) WHERE source_id = :sid "
        "AND is_issn_l IS DISTINCT FROM (issn = :new)"), {"sid": sid, "new": new})
    refresh_issns_column(conn, sid)
    return True


def revert_one(conn, sid, old, new, action):
    """Undo one receipt row: issn_l back to old; an adopted ISSN is removed again."""
    n = conn.execute(text(
        "UPDATE sources SET issn_l = :old, updated_date = now() WHERE id = :sid AND issn_l = :new "
        "AND EXISTS (SELECT 1 FROM source_issn WHERE source_id = :sid AND issn = :old)"),
        {"sid": sid, "old": old, "new": new}).rowcount
    if n != 1:
        return False
    conn.execute(text(
        "UPDATE source_issn SET is_issn_l = (issn = :old) WHERE source_id = :sid "
        "AND is_issn_l IS DISTINCT FROM (issn = :old)"), {"sid": sid, "old": old})
    if action == "adopt":
        conn.execute(text("DELETE FROM source_issn WHERE source_id = :sid AND issn = :new"),
                     {"sid": sid, "new": new})
    refresh_issns_column(conn, sid)
    return True


def run(engine, apply=False, adopt_ids=frozenset(), max_changes=DEFAULT_MAX_CHANGES,
        receipt=None, report=None, log=print):
    """Plan, and with apply=True write every repoint/adopt in ONE transaction. Returns the counts."""
    with engine.begin() as conn:
        rows = plan(conn, adopt_ids)
        counts = Counter(r["class"] for r in rows)
        todo = [r for r in rows if r["class"] in WRITE_ACTIONS]
        log(f"reconcile_issn_l: {len(rows):,} sources disagree with the ISSN-L table; "
            + ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))
        if report:
            with open(report, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["source_id"], delimiter="\t")
                w.writeheader()
                w.writerows(rows)
        if not apply:
            log(f"dry run: would write {len(todo):,}")
            return counts
        if len(todo) > max_changes:
            raise SystemExit(f"reconcile_issn_l: {len(todo):,} changes > --max-changes {max_changes:,}; "
                             "nothing written (a table reload this large needs a look first)")
        written = 0
        for r in todo:
            r["written"] = write_one(conn, r["source_id"], r["old_issn_l"], r["new_issn_l"], r["class"])
            written += r["written"]
            if r["written"]:
                log(f"  {r['class']} S{r['source_id']} {r['old_issn_l']} -> {r['new_issn_l']}")
        if receipt:
            with open(receipt, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["source_id", "old_issn_l", "new_issn_l", "action", "written"])
                for r in todo:
                    w.writerow([r["source_id"], r["old_issn_l"], r["new_issn_l"], r["class"], r["written"]])
        log(f"reconcile_issn_l: wrote {written:,} of {len(todo):,} (DONE)")
        counts["written"] = written
        return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--adopt-ids", help="file of source ids (one per line) allowed to adopt a missing ISSN-L")
    ap.add_argument("--max-changes", type=int, default=DEFAULT_MAX_CHANGES)
    ap.add_argument("--receipt", help="CSV of every planned write and whether it was written")
    ap.add_argument("--report", help="TSV of every disagreeing source with its class")
    ap.add_argument("--revert", help="a receipt CSV to undo")
    args = ap.parse_args()

    from db import engine  # here, so tests can import this module without a database

    if args.revert:
        rows = [r for r in csv.DictReader(open(args.revert)) if r["written"] == "True"]
        with engine.begin() as conn:
            if not args.apply:
                print(f"dry run: would revert {len(rows):,}")
                return
            done = sum(revert_one(conn, int(r["source_id"]), r["old_issn_l"], r["new_issn_l"], r["action"])
                       for r in rows)
            print(f"reverted {done:,} of {len(rows):,} (DONE)")
        return

    adopt_ids = frozenset()
    if args.adopt_ids:
        adopt_ids = frozenset(int(x) for x in open(args.adopt_ids).read().split() if x.strip())
    run(engine, apply=args.apply, adopt_ids=adopt_ids, max_changes=args.max_changes,
        receipt=args.receipt, report=args.report)


if __name__ == "__main__":
    sys.exit(main())
