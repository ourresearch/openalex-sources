"""Delete 28 placeholder "sources" from the registry: oxjob #654, Zendesk ticket 2147.

A placeholder source is a registry record whose whole name is a missing-value marker that some upstream record printed
where a venue name should be: 'unknown', 'N/A', 'None', 'NULL eBooks', 'Unknown host publication' (Pure's default),
'Default journal' (Pure's default), 'UNSPECIFIED (2004)' (EPrints' default), '[Publisher not identified] eBooks',
'[S.l. : s.n.] eBooks' ... It is not a venue. Walden's name tiers faithfully put every location whose upstream venue
string is that marker onto it, so unrelated works from many real venues share one fake "journal". Deleting the record
makes those locations fall through to the later tiers (URL / DOI-prefix / registrant rules) or to no source, which is
the truth: the upstream record didn't say where the work was published.

The list (ids, names, the live API state and a work sample for each, checked 2026-11-15 CT) is in oxjob #654
work/placeholder_sources_delete_list.csv and work/placeholder_sources_before_2026-11-15.json. Every record: no ISSN,
no host organization, no homepage; every sampled work comes from a different, unrelated venue.
Ticket 2147's own four: S4306533916 'unknown', S4306533918 'Unknown host publication', S4306489518 'Sample Publisher
eBooks', S4306463679 'NULL eBooks'. Not here on purpose: 'Missing Journal Name 00769576: CNPO' (S4306520025; looks like
the real serial ISSN 0076-9576: rename/merge, not delete), repository-typed records, and records with an ISSN.

Guards (per record; a record that fails one is SKIPPED and listed, the rest go ahead):
  * the live display_name must still equal the expected one (normalized),
  * no ISSN (issn_l NULL, no source_issn rows),
  * no row in ANY other registry table that points at it (discovered from information_schema: every public table
    with a source_id / loser_id / winner_id / merge_into_id column), except the derived stats tables
    source_works_count and source_publication_years, whose rows are deleted with the record (as merges do).
    So a record bound to an endpoint, a DataCite client, an OA override, a merge, a beacon mint ... is left alone.

Run on the dyno (dry run by default; everything in ONE transaction):
    heroku run -a openalex-sources --no-tty "python handoff/oxjob654-placeholder-sources-2026-11-16.py"
    heroku run -a openalex-sources --no-tty "python handoff/oxjob654-placeholder-sources-2026-11-16.py --execute"
The receipt (every deleted row as JSON, plus its stats rows) is printed after the RECEIPT marker, because the dyno's
disk is thrown away; save it to oxjob #654 work/placeholder_sources_receipt_2026-11-16.json.

Revert (re-inserts every deleted row with its original id, exactly as it was; the next night re-matches its locations):
    heroku run -a openalex-sources --no-tty "python handoff/oxjob654-placeholder-sources-2026-11-16.py --revert -" \
        < placeholder_sources_receipt_2026-11-16.json            # add --execute to commit

Downstream: walden CreateSources rebuilds openalex.sources.sources from this table on the next End 2 End, so the
locations re-match that night; sync_sources (Sync Sources to Elasticsearch) deletes the 28 stale ES docs, after which
/sources/<id> answers 404 and filter=locations.source.id:<id> counts 0.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
TICKET = 2147
JOB = 654
APPROVED = "support factory lane (#2254) under Jason's standing authority, 2026-11-15; reversible from the receipt"

# id -> expected display_name (live API, 2026-11-15 CT)
EXPECT = {
    # ticket 2147's own four
    4306533916: "unknown",
    4306533918: "Unknown host publication",
    4306489518: "Sample Publisher eBooks",
    4306463679: "NULL eBooks",
    # same recipe: the whole name is a missing-value marker
    4306482006: "Unknown eBooks",
    4306482007: "Unknown Penerbit eBooks",
    4306533917: "Unknown conference",
    4306476955: "Not Available eBooks",
    4306520425: "N/A",
    4306520424: "n/a",
    4306514527: "ISSN: N/A",
    4306521037: "(No Title)",
    4306520933: "None",
    2764385371: "No source information available",
    4306482013: "Unspecified eBooks",
    4306533935: "UNSPECIFIED (2000)",
    4306533936: "UNSPECIFIED (2002)",
    4306533937: "UNSPECIFIED (2003)",
    4306533938: "UNSPECIFIED (2004)",
    4306533939: "UNSPECIFIED (2005)",
    4306533940: "UNSPECIFIED (2006)",
    4306533941: "UNSPECIFIED (2008)",
    4306463818: "[Publisher not identified] eBooks",
    4306479788: "[S.l. : s.n.] eBooks",
    4306478438: "Publisher information missing eBooks",
    4306521746: "Other Information: No journal information given for this preprint",
    4306533943: "Untitled Event",
    4306507633: "Default journal",
}
DROP_WITH = ("source_works_count", "source_publication_years")  # derived stats rows, deleted with the record
REF_COLUMNS = ("source_id", "loser_id", "winner_id", "merge_into_id")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "ticket": TICKET,
           "approved": APPROVED, "mode": "revert" if REVERT else "delete",
           "before": {}, "dropped_with": {}, "skipped": {}, "actions": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


class DryRun(Exception):
    pass


def ref_columns(conn):
    """Every (table, column) in public that can point at a source id, other than sources.id itself."""
    rows = conn.execute(text(
        "SELECT c.table_name, c.column_name FROM information_schema.columns c "
        "JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
        "WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE' AND c.column_name = ANY(:cols) "
        "ORDER BY 1, 2"), {"cols": list(REF_COLUMNS)}).fetchall()
    return [(r[0], r[1]) for r in rows if not (r[0] == "sources" and r[1] == "id")]


def delete_all():
    with engine.begin() as conn:
        refs = ref_columns(conn)
        log(f"tables that can point at a source: {refs}")
        for sid, name in EXPECT.items():
            row = conn.execute(text("SELECT to_jsonb(s) AS j FROM sources s WHERE id = :id"), {"id": sid}).fetchone()
            if row is None:
                receipt["skipped"][str(sid)] = "not in the registry (already gone?)"
                log(f"SKIP {sid}: not in the registry")
                continue
            j = row.j
            if normalize_name(j.get("display_name") or "") != normalize_name(name):
                receipt["skipped"][str(sid)] = f"name is now {j.get('display_name')!r}, expected {name!r}"
                log(f"SKIP {sid}: name changed to {j.get('display_name')!r}")
                continue
            if j.get("issn_l") or (j.get("issns") not in (None, [], "[]")):
                receipt["skipped"][str(sid)] = f"has ISSN {j.get('issn_l')} {j.get('issns')}"
                log(f"SKIP {sid}: has an ISSN")
                continue
            blockers = {}
            for t, c in refs:
                if t == "sources" and c == "merge_into_id":
                    n = conn.execute(text("SELECT count(*) FROM sources WHERE merge_into_id = :id"), {"id": sid}).scalar()
                elif t in DROP_WITH:
                    continue
                else:
                    n = conn.execute(text(f'SELECT count(*) FROM "{t}" WHERE "{c}" = :id'), {"id": sid}).scalar()
                if n:
                    blockers[f"{t}.{c}"] = n
            if blockers:
                receipt["skipped"][str(sid)] = {"referenced_by": blockers}
                log(f"SKIP {sid} {name!r}: referenced by {blockers}")
                continue
            receipt["before"][str(sid)] = j
            dropped = {}
            for t in DROP_WITH:
                rows = [r.j for r in conn.execute(text(f'SELECT to_jsonb(x) AS j FROM "{t}" x WHERE source_id = :id'),
                                                    {"id": sid})]
                if rows:
                    conn.execute(text(f'DELETE FROM "{t}" WHERE source_id = :id'), {"id": sid})
                    dropped[t] = rows
            receipt["dropped_with"][str(sid)] = dropped
            n = conn.execute(text("DELETE FROM sources WHERE id = :id"), {"id": sid}).rowcount
            assert n == 1, (sid, n)
            log(f"DELETED {sid} {name!r} (type {j.get('type')}; stats rows {({k: len(v) for k, v in dropped.items()})})")
        left = conn.execute(text("SELECT id FROM sources WHERE id = ANY(:i)"), {"i": list(EXPECT)}).fetchall()
        log(f"deleted {len(receipt['before'])} of {len(EXPECT)}; skipped {len(receipt['skipped'])}; "
            f"still in the registry: {sorted(r[0] for r in left)}")
        if not EXECUTE:
            raise DryRun()


def revert_all(path):
    old = json.load(sys.stdin if path == "-" else open(path))
    with engine.begin() as conn:
        for sid, j in old["before"].items():
            if conn.execute(text("SELECT 1 FROM sources WHERE id = :id"), {"id": int(sid)}).fetchone():
                log(f"SKIP revert {sid}: already present")
                continue
            conn.execute(text("INSERT INTO sources OVERRIDING SYSTEM VALUE "
                              "SELECT * FROM jsonb_populate_record(NULL::sources, CAST(:j AS JSONB))"),
                         {"j": json.dumps(j)})
            for t, rows in (old.get("dropped_with", {}).get(sid) or {}).items():
                for r in rows:
                    conn.execute(text(f'INSERT INTO "{t}" SELECT * FROM jsonb_populate_record(NULL::"{t}", CAST(:j AS JSONB))'),
                                 {"j": json.dumps(r)})
            log(f"RESTORED {sid} {j.get('display_name')!r}")
        if not EXECUTE:
            raise DryRun()


def main():
    try:
        if REVERT:
            revert_all(sys.argv[sys.argv.index("--revert") + 1])
        else:
            delete_all()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back (add --execute to commit)")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
