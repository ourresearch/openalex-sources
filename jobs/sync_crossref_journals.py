"""Stage 2 of the Crossref journals job: read the `crossref_journal` staging table
and add-or-update journal sources via the upsert primitive.

  python -m jobs.sync_crossref_journals [--dry-run] [--limit N] [--batch N]

Matching is done in memory off a single preloaded ISSN index (like guts'
add_missing_journals): the fully-matched majority ('unchanged') is skipped without
touching the DB, so we only write actual mints/enrichments. --dry-run reports the
classification with no writes and no id minting.
"""
import argparse
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text

from db import engine
from sources_lib import normalize_issns, upsert_journal_by_issn


def _load_issn_index(conn):
    """issn -> source_id, and source_id -> set(issns), from one scan."""
    issn_to_sid, sid_to_issns = {}, defaultdict(set)
    for sid, issn in conn.execute(text("SELECT source_id, issn FROM source_issn")):
        issn_to_sid[issn] = sid
        sid_to_issns[sid].add(issn)
    return issn_to_sid, sid_to_issns


def _classify(issns, issn_to_sid, sid_to_issns):
    matched = {issn_to_sid[i] for i in issns if i in issn_to_sid}
    if not matched:
        return "added"
    if len(matched) > 1:
        return "conflict"
    sid = next(iter(matched))
    return "updated" if set(issns) - sid_to_issns[sid] else "unchanged"


def run(dry_run=False, limit=None, batch=500):
    sql = "SELECT issns, title, publisher FROM crossref_journal ORDER BY id"
    if limit:
        sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        staged = conn.execute(text(sql)).fetchall()
        issn_to_sid, sid_to_issns = _load_issn_index(conn)
    print(f"{len(staged)} staged journals; {len(issn_to_sid)} known ISSNs; dry_run={dry_run}")

    counts = Counter()
    conn = engine.connect()
    trans = conn.begin()
    written = 0
    try:
        for row in staged:
            issns = normalize_issns(list(row.issns or []))
            if not issns:
                counts["skipped_no_issn"] += 1
                continue
            kind = _classify(issns, issn_to_sid, sid_to_issns)
            counts[kind] += 1
            if kind == "unchanged" or dry_run:
                continue
            # actual write for added / updated / conflict
            outcome, sid = upsert_journal_by_issn(
                conn, issns=issns, display_name=row.title,
                publisher=row.publisher, source_feed="crossref",
            )
            if sid:  # keep the in-memory index current within this run
                for i in issns:
                    issn_to_sid[i] = sid
                    sid_to_issns[sid].add(i)
            written += 1
            if written % batch == 0:
                trans.commit()
                trans = conn.begin()
        trans.rollback() if dry_run else trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    print("outcome summary:", dict(counts))
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch", type=int, default=500)
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, batch=args.batch)


if __name__ == "__main__":
    main()
