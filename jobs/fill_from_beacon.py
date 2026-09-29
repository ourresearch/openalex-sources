"""Fill missing OJS journal homepage / country from the PKP Beacon (oxjob #1425).

FILL-ONLY: a column is written only where the registry value is NULL, so curated
and feed values are never overwritten. `country` (the name) is written together with
a filled `country_code`, and only when the source has no name yet. The name comes from
the CSV, not from the registry: the registry's most common name per code is polluted
by MARC codes stored as ISO (CI -> "Croatia", IO -> "Indonesia"), so the builder picks
the most common registry name that resolves to the same ISO code. `updated_date` moves only for rows that fill.
Neither column is in the works location struct, so no works are re-stamped.

The input is a reviewed CSV (built and verified outside this repo, see the oxjob):
  source_id, homepage_url, country_code, country, evidence
Blank means "nothing to fill". Homepages in the file were fetched live and
verified to be the journal's OJS page; country codes are ISO 3166-1 alpha-2.

  python -m jobs.fill_from_beacon --csv data/beacon_fill/<date>.csv            # dry run
  python -m jobs.fill_from_beacon --csv ... --apply --receipt fills.csv        # write
"""
import argparse
import csv
import re
import sys

from sqlalchemy import text

ISO2 = re.compile(r"^[A-Z]{2}$")

FILL_SQL = text(
    "UPDATE sources SET "
    "  homepage_url = COALESCE(homepage_url, :hp), "
    "  country = CASE WHEN country_code IS NULL AND :cc IS NOT NULL "
    "                 THEN COALESCE(country, :cn) ELSE country END, "
    "  country_code = COALESCE(country_code, :cc), "
    "  updated_date = now() "
    "WHERE id = :id "
    "  AND ((homepage_url IS NULL AND :hp IS NOT NULL) "
    "       OR (country_code IS NULL AND :cc IS NOT NULL)) "
    "RETURNING id"
)


def load_rows(path):
    """Parse and validate the fill CSV; refuse the whole file on any bad row."""
    rows, errors = [], []
    with open(path, newline="") as f:
        for n, r in enumerate(csv.DictReader(f), start=2):
            try:
                sid = int(r["source_id"])
            except (KeyError, ValueError):
                errors.append(f"line {n}: bad source_id {r.get('source_id')!r}")
                continue
            hp = (r.get("homepage_url") or "").strip() or None
            cc = (r.get("country_code") or "").strip().upper() or None
            cn = (r.get("country") or "").strip() or None
            if hp and not hp.startswith(("http://", "https://")):
                errors.append(f"line {n}: homepage_url not http(s): {hp!r}")
            if cc and not ISO2.match(cc):
                errors.append(f"line {n}: country_code not ISO2: {cc!r}")
            if cc and not cn:
                errors.append(f"line {n}: country_code {cc} without a country name")
            if hp or cc:
                rows.append({"id": sid, "hp": hp, "cc": cc, "cn": cn if cc else None})
    ids = [r["id"] for r in rows]
    if len(ids) != len(set(ids)):
        errors.append("duplicate source_id rows")
    return rows, errors


def plan(conn, rows):
    """Current values for the targeted sources; what would fill and what is skipped."""
    cur = {r.id: r for r in conn.execute(text(
        "SELECT id, homepage_url, country_code, country FROM sources WHERE id = ANY(:ids)"
    ), {"ids": [r["id"] for r in rows]})}
    out = {"missing_source": 0, "hp_fill": 0, "hp_keep": 0, "cc_fill": 0, "cc_keep": 0}
    for r in rows:
        c = cur.get(r["id"])
        if c is None:
            out["missing_source"] += 1
            continue
        if r["hp"]:
            out["hp_fill" if c.homepage_url is None else "hp_keep"] += 1
        if r["cc"]:
            out["cc_fill" if c.country_code is None else "cc_keep"] += 1
    return cur, out


def run(csv_path, apply=False, receipt=None, batch=500):
    from db import engine  # deferred: importing this module must not need DATABASE_URL

    rows, errors = load_rows(csv_path)
    if errors:
        for e in errors[:20]:
            print("refused:", e, file=sys.stderr)
        raise SystemExit(f"{len(errors)} invalid rows; nothing written")

    with engine.connect() as conn:
        before, counts = plan(conn, rows)
    print(f"{len(rows)} rows; would fill homepage {counts['hp_fill']} (keep {counts['hp_keep']}), "
          f"country {counts['cc_fill']} (keep {counts['cc_keep']}); "
          f"{counts['missing_source']} source ids not in registry", flush=True)
    if not apply:
        print("dry run; pass --apply to write")
        return counts

    filled = []
    with engine.connect() as conn:
        for i in range(0, len(rows), batch):
            with conn.begin():
                for r in rows[i:i + batch]:
                    if conn.execute(FILL_SQL, r).scalar() is not None:
                        filled.append(r["id"])
            print(f"  {min(i + batch, len(rows))}/{len(rows)} processed, {len(filled)} filled", flush=True)

    if receipt:
        with engine.connect() as conn:
            after = {r.id: r for r in conn.execute(text(
                "SELECT id, homepage_url, country_code, country FROM sources WHERE id = ANY(:ids)"
            ), {"ids": filled})}
        with open(receipt, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["source_id", "homepage_before", "homepage_after",
                        "country_code_before", "country_code_after", "country_before", "country_after"])
            for sid in filled:
                b, a = before[sid], after[sid]
                w.writerow([sid, b.homepage_url, a.homepage_url, b.country_code, a.country_code,
                            b.country, a.country])
    print(f"filled {len(filled)} sources")
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--receipt", help="write a before/after CSV of filled rows (with --apply)")
    ap.add_argument("--batch", type=int, default=500)
    a = ap.parse_args()
    run(a.csv, apply=a.apply, receipt=a.receipt, batch=a.batch)


if __name__ == "__main__":
    main()
