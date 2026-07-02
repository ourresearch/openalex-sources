"""Drain the multi_match conflict queue: auto-merge high-confidence duplicates.

  python -m jobs.resolve_conflicts [--dry-run] [--limit N]

Each unresolved multi_match issue names >=2 sources sharing the incoming ISSNs.
Auto-merge policy (everything else is marked needs_review for a human):
  - exactly 2 matched sources, neither already merged
  - normalized display_names are EQUAL (diacritic/case/punctuation-insensitive)
  - types compatible (equal, or one NULL)
  - loser has no curator override (merge_source guards this too)
Winner = more works (source_works_count snapshot; missing counts as 0),
tie broken by lower (older) id. All queue rows carrying the same id-set are
resolved together.
"""
import argparse
import os
import sys
import unicodedata
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text

from db import engine
from sources_lib import merge_source


def normalize_name(name):
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.casefold().replace("&", " and ")
    s = "".join(c if c.isalnum() else " " for c in s)
    s = " ".join(s.split())
    if s.startswith("the "):
        s = s[4:]
    return s


def classify(pair, sources, works, merged_this_run):
    """Return (verdict, winner, loser). verdict in {auto, needs_review:<why>}."""
    a, b = pair
    sa, sb = sources.get(a), sources.get(b)
    if not sa or not sb:
        return "needs_review:missing_source", None, None
    if sa.merge_into_id or sb.merge_into_id or a in merged_this_run or b in merged_this_run:
        return "needs_review:chain", None, None
    if normalize_name(sa.display_name) != normalize_name(sb.display_name):
        return "needs_review:name_mismatch", None, None
    if sa.type and sb.type and sa.type != sb.type:
        return "needs_review:type_mismatch", None, None
    if sa.override_timestamp or sb.override_timestamp:
        return "needs_review:override", None, None
    winner, loser = sorted(pair, key=lambda i: (-works.get(i, 0), i))
    return "auto", winner, loser


def run(dry_run=False, limit=None, batch=200):
    with engine.connect() as conn:
        issues = conn.execute(text(
            "SELECT id, matched_source_ids, detail FROM source_ingest_issue "
            "WHERE issue_type = 'multi_match' AND resolved_at IS NULL ORDER BY id"
        )).fetchall()
        all_ids = sorted({i for r in issues for i in r.matched_source_ids})
        sources = {
            r.id: r
            for r in conn.execute(
                text(
                    "SELECT id, display_name, type, merge_into_id, override_timestamp "
                    "FROM sources WHERE id = ANY(:ids)"
                ),
                {"ids": all_ids},
            )
        }
        works = dict(conn.execute(
            text("SELECT source_id, works_count FROM source_works_count "
                 "WHERE source_id = ANY(:ids)"), {"ids": all_ids}
        ).fetchall())
    print(f"{len(issues)} unresolved issues, {len(all_ids)} distinct sources, "
          f"{len(works)} with works counts; dry_run={dry_run}", flush=True)

    # group issue rows by their id-set so each pair is decided once
    by_set = {}
    for r in issues:
        by_set.setdefault(tuple(sorted(r.matched_source_ids)), []).append(r)
    sets = list(by_set.items())
    if limit:
        sets = sets[: int(limit)]

    counts = Counter()
    merged_this_run = set()
    conn = engine.connect()
    trans = conn.begin()
    done = 0
    try:
        for id_set, rows in sets:
            if len(id_set) != 2:
                verdict, winner, loser = f"needs_review:{len(id_set)}_way", None, None
            else:
                verdict, winner, loser = classify(id_set, sources, works, merged_this_run)

            if verdict == "auto" and not dry_run:
                outcome = merge_source(
                    conn, loser, winner, rule="auto_name_match", source_feed="crossref",
                    detail={
                        "issue_ids": [r.id for r in rows],
                        "loser_name": sources[loser].display_name,
                        "winner_name": sources[winner].display_name,
                        "works": {str(i): works.get(i, 0) for i in id_set},
                    },
                )
                if outcome == "merged":
                    merged_this_run.add(loser)
                    resolution = f"auto_merged:{loser}->{winner}"
                else:
                    resolution = f"needs_review:{outcome}"
            elif verdict == "auto":
                resolution = f"would_merge:{loser}->{winner}"
            else:
                resolution = verdict
            counts[resolution.split(":")[0]] += 1

            if not dry_run:
                conn.execute(
                    text("UPDATE source_ingest_issue SET resolved_at = now(), "
                         "resolution = :res WHERE id = ANY(:ids)"),
                    {"res": resolution, "ids": [r.id for r in rows]},
                )
            done += 1
            if done % batch == 0:
                if not dry_run:
                    trans.commit()
                    trans = conn.begin()
                print(f"  {done}/{len(sets)} sets processed", flush=True)
        trans.rollback() if dry_run else trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    print("resolution summary:", dict(counts), flush=True)
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
