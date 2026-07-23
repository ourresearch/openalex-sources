"""Backfill: preserve merged losers' names as winner alternate_titles.

Casey ruling 2026-07-22: loser display names + alternate titles that differ from
the winner's name should live in the winner's alternate_titles jsonb array, so
merges (especially cross-language ones) don't shed the venue's other identities.

Walks source_merge history; for each merge, unions loser display_name +
alternate_titles into the winner's alternate_titles, deduped case-insensitively,
skipping anything normalize_name-equal to the winner's display_name. Idempotent:
re-running adds nothing new. Default DRY RUN; --execute to write.

  python -m jobs.backfill_alt_titles [--execute] [--batch-like PATTERN]
"""
import argparse
import json

from sqlalchemy import text

from db import engine
from sources_lib import normalize_name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="write (default: dry run)")
    ap.add_argument("--batch-like", default=None,
                    help="restrict to source_merge rows whose detail->>'batch' LIKE this")
    args = ap.parse_args()

    where = "WHERE sm.detail->>'batch' LIKE :b" if args.batch_like else ""
    with engine.connect() as conn:
        merges = conn.execute(text(f"""
            SELECT sm.loser_id, sm.winner_id,
                   l.display_name AS l_name, l.alternate_titles AS l_alt,
                   w.display_name AS w_name, w.alternate_titles AS w_alt
            FROM source_merge sm
            JOIN sources l ON l.id = sm.loser_id
            JOIN sources w ON w.id = sm.winner_id
            {where}
        """), {"b": args.batch_like} if args.batch_like else {}).fetchall()

    updated, added_total = 0, 0
    for m in merges:
        candidates = [m.l_name or ""] + list(m.l_alt or [])
        existing = list(m.w_alt or [])
        have = {t.strip().lower() for t in existing + [m.w_name or ""]}
        w_norm = normalize_name(m.w_name or "")
        new = []
        for t in candidates:
            t = (t or "").strip()
            if not t or t.lower() in have or normalize_name(t) == w_norm:
                continue
            new.append(t)
            have.add(t.lower())
        if not new:
            continue
        updated += 1
        added_total += len(new)
        if args.execute:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE sources SET alternate_titles = CAST(:a AS JSONB), "
                    "updated_date = now() WHERE id = :id"
                ), {"a": json.dumps(existing + new), "id": m.winner_id})
        else:
            print(f"WOULD ADD to {m.winner_id} ({m.w_name!r}): {new}")

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"\n{mode} — {len(merges)} merges scanned: "
          f"{updated} winners gain titles, {added_total} titles total")


if __name__ == "__main__":
    main()
