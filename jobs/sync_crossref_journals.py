"""Stage 2 of the Crossref journals job: read the `crossref_journal` staging table
and add-or-update journal sources via the shared match cascade (sources_lib).

  python -m jobs.sync_crossref_journals [--dry-run] [--limit N] [--batch N]

Matching is done in memory off the preloaded MatchContext (like guts'
add_missing_journals): the fully-matched majority ('unchanged') is skipped without
touching the DB, so we only write actual mints/enrichments. --dry-run reports the
classification with no writes and no id minting. Crossref rows never name-link:
the context is built without the name index, so no-match rows mint directly.
"""
import argparse
from collections import Counter

from sqlalchemy import text

from db import engine
from sources_lib import (
    MatchContext,
    enrich_journal,
    match_source,
    mint_source,
    normalize_issns,
    park_multi_match,
    recompute_is_oa,
)


def run(dry_run=False, limit=None, batch=500):
    sql = "SELECT issns, title, publisher FROM crossref_journal ORDER BY id"
    if limit:
        sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        staged = conn.execute(text(sql)).fetchall()
        ctx = MatchContext(conn)
    print(f"{len(staged)} staged journals; {len(ctx.issn_to_sid)} known ISSNs; dry_run={dry_run}")

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
            kind = ctx.classify(issns)
            counts[kind] += 1
            if kind == "unchanged" or dry_run:
                continue
            kind, val = match_source(conn, ctx, issns, row.title, "crossref")
            if kind == "issn":
                enrich_journal(conn, ctx, val, issns, display_name=row.title,
                               publisher=row.publisher)
            elif kind == "issn_multi":
                park_multi_match(conn, "crossref", issns, val, row.title)
            else:  # 'none' -- no name fallback for crossref
                mint_source(conn, ctx, row.title, issns=issns, publisher=row.publisher)
            written += 1
            if written % batch == 0:
                trans.commit()
                trans = conn.begin()
        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    if not dry_run:
        apply_scielo_flag()
    print("outcome summary:", dict(counts))
    return counts


def apply_scielo_flag():
    """SciELO membership is derived from the Crossref publisher prefix (walden
    parity); flag any matched source not yet flagged, then re-derive is_oa."""
    with engine.begin() as conn:
        n = conn.execute(text("""
            UPDATE sources s SET is_in_scielo = TRUE, updated_date = now()
            WHERE s.merge_into_id IS NULL
              AND s.is_in_scielo IS DISTINCT FROM TRUE
              AND EXISTS (
                SELECT 1 FROM crossref_journal d
                CROSS JOIN LATERAL unnest(d.issns) AS di(issn)
                JOIN source_issn si ON si.issn = UPPER(di.issn)
                WHERE si.source_id = s.id AND LOWER(d.publisher) LIKE 'scielo%')
        """)).rowcount
        oa = recompute_is_oa(conn)
        print(f"scielo flag: {n} sources newly flagged; is_oa recomputed on {oa}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch", type=int, default=500)
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, batch=args.batch)


if __name__ == "__main__":
    main()
