"""Backfill: assign host organizations to sources that name one but were never linked.

Publisher-cleanup Batch 0 (Casey directive 2026-08-11: "fix missing publisher links").
Consumes a reviewed manifest CSV built from the Databricks worklist
(openalex_dev.rohan_lab.pubclean_batch0_manifest): unambiguous name matches only,
single entity kind, >=3 tokens, aggregators excluded, >=1M-works rows hand-reviewed.

Manifest columns: source_id, target_kind ('publisher'|'institution'), target_id,
expected_publisher_str.

Per-row guards (any failure -> skip + counted, never a write):
  - source exists and is live (merged sources are skipped)
  - BOTH publisher_id and institution_id are currently NULL (never overwrites,
    which also makes re-runs idempotent: a second pass skips everything it set)
  - the registry's current free-text `publisher` equals expected_publisher_str
    (drift guard: the manifest was built from a snapshot; if the registry row
    changed since, a human should look again rather than the job writing)

Default DRY RUN; --execute to write. --limit N for the canary rung.

  python -m jobs.backfill_host_orgs --manifest batch0.csv [--limit 10] [--execute]
"""
import argparse
import csv

from sqlalchemy import text

from db import engine

VALID_KINDS = {"publisher", "institution"}


def load_manifest(path, limit=None):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            kind = row["target_kind"].strip()
            if kind not in VALID_KINDS:
                raise ValueError(f"bad target_kind {kind!r} on source {row['source_id']}")
            rows.append({
                "source_id": int(row["source_id"]),
                "kind": kind,
                "target_id": int(row["target_id"]),
                "expected": (row["expected_publisher_str"] or "").strip(),
            })
            if limit and len(rows) >= limit:
                break
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="reviewed manifest CSV")
    ap.add_argument("--execute", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=None, help="only process first N rows (canary)")
    args = ap.parse_args()

    manifest = load_manifest(args.manifest, args.limit)
    counts = {"linked": 0, "already_linked": 0, "missing_source": 0,
              "merged_source": 0, "text_drift": 0}
    drift_examples = []

    with engine.connect() as conn:
        current = {
            r.id: r for r in conn.execute(text(
                "SELECT id, publisher_id, institution_id, publisher "
                "FROM sources WHERE id = ANY(:ids)"
            ), {"ids": [m["source_id"] for m in manifest]}).fetchall()
        }

    to_write = []
    for m in manifest:
        row = current.get(m["source_id"])
        if row is None:
            counts["missing_source"] += 1
            continue
        # merge_into_id dropped from the registry 2026-08-14 (D1: merged rows now deleted
        # outright), so missing_source covers the merged case.
        if row.publisher_id is not None or row.institution_id is not None:
            counts["already_linked"] += 1
            continue
        if (row.publisher or "").strip() != m["expected"]:
            counts["text_drift"] += 1
            if len(drift_examples) < 10:
                drift_examples.append((m["source_id"], row.publisher, m["expected"]))
            continue

        counts["linked"] += 1
        column = "publisher_id" if m["kind"] == "publisher" else "institution_id"
        if args.execute:
            to_write.append((column, m["target_id"], m["source_id"], m["expected"]))
        else:
            print(f"WOULD SET {column}={m['target_id']} on source {m['source_id']} "
                  f"(publisher text {m['expected']!r})")

    # One statement per chunk: each network round-trip to Heroku costs ~0.1-0.8s,
    # so per-row statements can't finish inside a 2-minute shell timeout. A single
    # VALUES-join UPDATE per 500 rows keeps the whole batch to a handful of
    # round-trips. Every guard is re-checked server-side inside the statement, so
    # a row that changed since the pre-pass is skipped, never overwritten.
    CHUNK = 500
    done = 0
    by_column = {}
    for column, tid, sid, expected in to_write:
        by_column.setdefault(column, []).append((sid, tid, expected))
    for column, rows in by_column.items():
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i:i + CHUNK]
            values_sql = ", ".join(
                f"({sid}, {tid}, :e{j})" for j, (sid, tid, _) in enumerate(chunk)
            )
            params = {f"e{j}": expected for j, (_, _, expected) in enumerate(chunk)}
            with engine.begin() as conn:
                result = conn.execute(text(
                    f"UPDATE sources s SET {column} = v.tid, updated_date = now() "
                    f"FROM (VALUES {values_sql}) AS v(sid, tid, expected) "
                    "WHERE s.id = v.sid "
                    "  AND s.publisher_id IS NULL AND s.institution_id IS NULL "
                    "  AND btrim(coalesce(s.publisher, '')) = v.expected"
                ), params)
            done += result.rowcount
            if result.rowcount != len(chunk):
                print(f"NOTE: chunk wrote {result.rowcount}/{len(chunk)} — "
                      f"remainder skipped by server-side guards (re-run to see which)",
                      flush=True)
            print(f"committed {done} rows ({column})", flush=True)

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"\n{mode} — {len(manifest)} manifest rows: {counts}")
    for sid, got, expected in drift_examples:
        print(f"  drift: source {sid} publisher is {got!r}, manifest expected {expected!r}")


if __name__ == "__main__":
    main()
