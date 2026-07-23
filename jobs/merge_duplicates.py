"""Execute a reviewed batch of duplicate-source merges from a manifest CSV.

  python -m jobs.merge_duplicates path/to/manifest.csv [--execute] [--limit N] [--batch LABEL]

Manifest columns: winner_id, loser_id, subclass (free-form evidence tag), origin.
Produced by the oxjob #629 dedup scan; every pair was verified against the live
registry at scan time, but this job RE-CHECKS every guard at execution moment:

  - both sources exist and neither is already merged
  - loser has no curator override (merge_source guards this too)
  - normalized display_names are still EQUAL (a rename since scan = abort pair)
  - types still compatible (equal, or one NULL)
  - winner still has >= works than loser (source_works_count snapshot;
    missing counts as 0) — else the pair is skipped as winner_stale, never swapped
    silently

Default is a DRY RUN that prints what would happen and writes nothing; pass
--execute to merge for real. (Deliberately inverted from resolve_conflicts'
--dry-run default: this job takes a human-reviewed list and can touch hundreds
of rows in one invocation.) Each merge commits in its own transaction, so an
interrupted run leaves a consistent prefix; re-running is idempotent because
already_merged pairs are skipped.
"""
import argparse
import csv
import os
from collections import Counter

from sqlalchemy import text

from db import engine
from sources_lib import merge_source, normalize_name


def load_manifest(path):
    with open(path, newline="") as f:
        return [
            (int(r["winner_id"]), int(r["loser_id"]), r.get("subclass", ""))
            for r in csv.DictReader(f)
        ]


def check_pair(conn, winner_id, loser_id):
    """Re-run every guard now. Returns 'ok' or a refusal reason."""
    rows = {
        r.id: r
        for r in conn.execute(
            text(
                "SELECT id, display_name, type, merge_into_id, override_timestamp "
                "FROM sources WHERE id IN (:w, :l)"
            ),
            {"w": winner_id, "l": loser_id},
        )
    }
    if winner_id == loser_id:
        return "self_pair"
    w, l = rows.get(winner_id), rows.get(loser_id)
    if not w or not l:
        return "missing_source"
    if w.merge_into_id is not None or l.merge_into_id is not None:
        return "already_merged"
    if l.override_timestamp is not None:
        return "loser_overridden"
    if normalize_name(w.display_name) != normalize_name(l.display_name):
        return "name_diverged"
    if w.type and l.type and w.type != l.type:
        return "type_conflict"
    works = {
        r.source_id: r.works_count
        for r in conn.execute(
            text(
                "SELECT DISTINCT ON (source_id) source_id, works_count "
                "FROM source_works_count WHERE source_id IN (:w, :l) "
                "ORDER BY source_id, as_of DESC"
            ),
            {"w": winner_id, "l": loser_id},
        )
    }
    if works.get(loser_id, 0) > works.get(winner_id, 0):
        return "winner_stale"
    return "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("manifest")
    ap.add_argument("--execute", action="store_true", help="actually merge (default: dry run)")
    ap.add_argument("--limit", type=int, help="stop after N pairs (canary)")
    ap.add_argument("--batch", default="A", help="batch label recorded in source_merge.detail")
    ap.add_argument(
        "--skip-log",
        default=None,
        help="CSV to append skipped/refused pairs to (default: <manifest>_skips.csv); "
        "feeds the WEIRD-CASES intake queue in oxjob #629",
    )
    args = ap.parse_args()

    pairs = load_manifest(args.manifest)
    if args.limit:
        pairs = pairs[: args.limit]

    skip_log = args.skip_log or args.manifest.replace(".csv", "_skips.csv")
    skip_rows = []

    outcomes = Counter()
    for winner_id, loser_id, subclass in pairs:
        with engine.begin() as conn:
            verdict = check_pair(conn, winner_id, loser_id)
            if verdict != "ok":
                outcomes[f"skip:{verdict}"] += 1
                skip_rows.append((winner_id, loser_id, subclass, verdict, args.batch))
                print(f"SKIP  {loser_id} -> {winner_id}  {verdict}")
                continue
            if not args.execute:
                outcomes["would_merge"] += 1
                continue
            result = merge_source(
                conn,
                loser_id,
                winner_id,
                rule=f"batch_dedup:{subclass}",
                source_feed="oxjob629",
                detail={"batch": args.batch, "subclass": subclass},
            )
            outcomes[result] += 1
            if result != "merged":
                skip_rows.append((winner_id, loser_id, subclass, f"refused:{result}", args.batch))
                print(f"REFUSED  {loser_id} -> {winner_id}  {result}")

    if skip_rows:
        new_file = not os.path.exists(skip_log)
        with open(skip_log, "a", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["winner_id", "loser_id", "subclass", "reason", "batch"])
            w.writerows(skip_rows)
        print(f"skip log: {len(skip_rows)} rows appended to {skip_log}")

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"\n{mode} — {len(pairs)} pairs: {dict(outcomes)}")


if __name__ == "__main__":
    main()
