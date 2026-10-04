"""One-off: load ojs_beacon_mint from the committed mint receipts (oxjob #1539).

Reads every data/ojs_beacon/receipt-*.csv that is not a dry run, takes rows whose
outcome starts with "mint" and carries a source_id, and inserts them (ON CONFLICT DO
NOTHING). The edition is the Beacon edition the candidates were cut from.

Also loads the pilot mints (#805/#1417, 2026-09-28..10-03) that predate the receipts,
with receipt = 'pilot' (coordinator #1417, 2026-10-04).

  python -m scripts.backfill_ojs_beacon_mint [--dry-run] [--edition 2026-07-18]
"""
import argparse
import csv
import glob
from pathlib import Path

from sqlalchemy import text

from db import engine

ROOT = Path(__file__).resolve().parent.parent

PILOT_MINTS = [
    7407065356, 7407065357, 7407065358, 7407065367, 7407065383, 7407065384, 7407065401,
    7407065410, 7407065412, 7407065420, 7407068431, 7407068439, 7407068451, 7407068460,
    7407068474, 7407068476, 7407068477, 7407068484, 7407068488, 7407068498, 7407068513,
    7407068514,
]


def rows_from_receipts(edition):
    out = {}
    for p in sorted(glob.glob(str(ROOT / "data" / "ojs_beacon" / "receipt-*.csv"))):
        if "-dry" in p:
            continue
        name = Path(p).name
        with open(p, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["outcome"].startswith("mint") and r["source_id"]:
                    out.setdefault(int(r["source_id"]), (edition, name))
    for sid in PILOT_MINTS:
        out.setdefault(sid, (edition, "pilot"))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--edition", default="2026-07-18")
    a = ap.parse_args()
    rows = rows_from_receipts(a.edition)
    print(f"{len(rows)} minted source ids in receipts", flush=True)
    with engine.begin() as conn:
        present = {r[0] for r in conn.execute(
            text("SELECT id FROM sources WHERE id = ANY(:ids)"), {"ids": list(rows)})}
        missing = sorted(set(rows) - present)
        if missing:
            print(f"not in sources (skipped): {len(missing)} e.g. {missing[:5]}")
        if a.dry_run:
            print("dry run; nothing written")
            return
        n = 0
        for sid in sorted(present):
            edition, receipt = rows[sid]
            n += conn.execute(text(
                "INSERT INTO ojs_beacon_mint (source_id, edition, receipt) "
                "VALUES (:s, :e, :r) ON CONFLICT (source_id) DO NOTHING"),
                {"s": sid, "e": edition, "r": receipt}).rowcount
        total = conn.execute(text("SELECT COUNT(*) FROM ojs_beacon_mint")).scalar()
    print(f"inserted {n}; ojs_beacon_mint now {total} rows", flush=True)


if __name__ == "__main__":
    main()
