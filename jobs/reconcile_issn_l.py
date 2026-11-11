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
#1726), an ISSN-L another source owns (a merge question), and an ISSN-L two sources would both adopt.

  python -m jobs.reconcile_issn_l                                 # dry run: counts by class
  python -m jobs.reconcile_issn_l --report FILE.tsv               # every candidate with its class
  python -m jobs.reconcile_issn_l --apply --receipt FILE.csv [--adopt-ids FILE] [--max-changes N]
  python -m jobs.reconcile_issn_l --revert FILE.csv [--apply]     # put every receipt row back
"""
import argparse
import csv
from collections import Counter

from sqlalchemy import text

from sources_lib import sync_issn_l_flags

DEFAULT_MAX_CHANGES = 500  # guard for the scheduled run after a table reload

# MATERIALIZED: tl is read four times below; without it Postgres may re-run the subquery each time
CANDIDATES_SQL = """
WITH s AS MATERIALIZED (
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


def classify(source_id, issn_l, own, tl, true_l_owner, adopt_ids=frozenset()):
    """(class, new issn_l or None) for one disagreeing source. Pure: no database."""
    if len(tl) != 1:
        return "multi_family", None
    new = tl[0]
    if new in own:
        return "repoint", new
    if true_l_owner is not None:
        return "owned_elsewhere", new
    return ("adopt" if source_id in adopt_ids else "missing"), new


def plan(conn, adopt_ids=frozenset()):
    out = []
    for r in conn.execute(text(CANDIDATES_SQL)):
        cls, new = classify(r.id, r.issn_l, r.own, r.tl, r.true_l_owner, adopt_ids)
        out.append({"source_id": r.id, "class": cls, "old_issn_l": r.issn_l, "new_issn_l": new,
                    "own": ",".join(r.own), "table_issn_l": ",".join(r.tl),
                    "true_l_owner": r.true_l_owner})
    # an unowned ISSN-L two sources would both adopt is a merge question, not an adoption
    unowned = Counter(r["new_issn_l"] for r in out if r["class"] in ("adopt", "missing"))
    for r in out:
        if r["class"] in ("adopt", "missing") and unowned[r["new_issn_l"]] > 1:
            r["class"] = "shared_missing"
    return out


def _set_issn_l(conn, sid, frm, to):
    """issn_l frm -> to, only if it is still frm and the source owns to. True if written."""
    return conn.execute(text(
        "UPDATE sources SET issn_l = :to, updated_date = now() WHERE id = :sid AND issn_l = :frm "
        "AND EXISTS (SELECT 1 FROM source_issn WHERE source_id = :sid AND issn = :to)"),
        {"sid": sid, "frm": frm, "to": to}).rowcount == 1


def _drop_issn(conn, sid, issn):
    conn.execute(text("DELETE FROM source_issn WHERE source_id = :sid AND issn = :l"), {"sid": sid, "l": issn})


def write_one(conn, sid, old, new, action):
    """Repoint (after adopting, if asked). False, with nothing left behind, if the row moved meanwhile."""
    if action == "adopt" and conn.execute(text(
            "INSERT INTO source_issn (source_id, issn, is_issn_l) VALUES (:sid, :l, FALSE) "
            "ON CONFLICT (issn) DO NOTHING"), {"sid": sid, "l": new}).rowcount != 1:
        return False
    if not _set_issn_l(conn, sid, old, new):
        if action == "adopt":
            _drop_issn(conn, sid, new)
        return False
    sync_issn_l_flags(conn, sid)
    return True


def revert_one(conn, sid, old, new, action):
    """Undo one receipt row: issn_l back to old; an adopted ISSN is removed again."""
    if not _set_issn_l(conn, sid, new, old):
        return False
    if action == "adopt":
        _drop_issn(conn, sid, new)
    sync_issn_l_flags(conn, sid)
    return True


def run(engine, apply=False, adopt_ids=frozenset(), max_changes=DEFAULT_MAX_CHANGES,
        receipt=None, report=None, log=print):
    """Plan, and with apply=True write every repoint/adopt in ONE transaction. Returns the number written."""
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
            return 0
        if len(todo) > max_changes:
            raise SystemExit(f"reconcile_issn_l: {len(todo):,} changes > --max-changes {max_changes:,}; "
                             "nothing written (a table reload this large needs a look first)")
        for r in todo:
            r["written"] = write_one(conn, r["source_id"], r["old_issn_l"], r["new_issn_l"], r["class"])
            if r["written"]:
                log(f"  {r['class']} S{r['source_id']} {r['old_issn_l']} -> {r['new_issn_l']}")
        if receipt:
            with open(receipt, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["source_id", "old_issn_l", "new_issn_l", "action", "written"])
                for r in todo:
                    w.writerow([r["source_id"], r["old_issn_l"], r["new_issn_l"], r["class"], r["written"]])
        written = sum(r["written"] for r in todo)
        log(f"reconcile_issn_l: wrote {written:,} of {len(todo):,} (DONE)")
        return written


def revert(engine, receipt, apply=False, log=print):
    rows = [r for r in csv.DictReader(open(receipt)) if r["written"] == "True"]
    if not apply:
        log(f"dry run: would revert {len(rows):,}")
        return
    with engine.begin() as conn:
        done = sum(revert_one(conn, int(r["source_id"]), r["old_issn_l"], r["new_issn_l"], r["action"])
                   for r in rows)
    log(f"reverted {done:,} of {len(rows):,} (DONE)")


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
        revert(engine, args.revert, apply=args.apply)
        return
    adopt_ids = frozenset(int(x) for x in open(args.adopt_ids).read().split()) if args.adopt_ids else frozenset()
    run(engine, apply=args.apply, adopt_ids=adopt_ids, max_changes=args.max_changes,
        receipt=args.receipt, report=args.report)


if __name__ == "__main__":
    main()
