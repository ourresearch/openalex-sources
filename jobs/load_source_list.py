"""Load (or reload) one external source list into source_list_member and
recompute sources.listed_in (oxjob #1205).

This is a BY-HAND job, on purpose: there is no scheduled refresh. When the
maintainers publish a new edition (typically it arrives by email or a support
ticket), convert it to the CSV shape below, drop it in data/source_lists/,
and run this once. If the list goes stale, it goes stale.

CSV columns (header required):
  name               journal title (informational)
  issns              one or more ISSNs, ';'-separated, NNNN-NNNX
  active             true/false — false rows are kept for history, never members
  withdrawn_date     YYYY-MM-DD or blank
  withdrawal_reason  free text or blank

The list id must already exist in source_list (see migration 038). The load is
a full replace for that list (DELETE + INSERT, one transaction), then
sources_lib.recompute_listed_in() rewrites sources.listed_in for every source
whose value changed (all lists, not just this one). Idempotent: re-running on
the same file changes nothing.

  python -m jobs.load_source_list --list cdd-cnu-sante \
      --csv data/source_lists/cdd-cnu-sante-2026-07-01.csv --version 2026-07-01 [--dry-run]
"""
import argparse
import csv
import re
from datetime import date

from sqlalchemy import text

from db import engine
from sources_lib import normalize_issns, recompute_listed_in

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dX]$")


def read_rows(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f), start=2):
            issns = [x for x in normalize_issns(re.split(r"[;,\s]+", r.get("issns") or "")) if x]
            bad = [x for x in issns if not ISSN_RE.match(x)]
            if bad:
                print(f"  line {i}: skipping malformed ISSN(s) {bad} for {r.get('name')!r}")
                issns = [x for x in issns if ISSN_RE.match(x)]
            if not issns:
                print(f"  line {i}: no ISSN, cannot match: {r.get('name')!r}")
                continue
            active = (r.get("active") or "true").strip().lower() in ("true", "1", "yes", "y")
            wd = (r.get("withdrawn_date") or "").strip() or None
            rows.append({
                "name": (r.get("name") or "").strip() or None,
                "issns": issns,
                "active": active,
                "withdrawn_date": date.fromisoformat(wd) if wd else None,
                "withdrawal_reason": (r.get("withdrawal_reason") or "").strip() or None,
            })
    return rows


def run(list_id, csv_path, version, dry_run=False):
    rows = read_rows(csv_path)
    n_active = sum(1 for r in rows if r["active"])
    print(f"{csv_path}: {len(rows)} rows ({n_active} active), list={list_id}, version={version}")

    conn = engine.connect()
    trans = conn.begin()
    try:
        if not conn.execute(text("SELECT 1 FROM source_list WHERE id = :id"), {"id": list_id}).first():
            raise SystemExit(f"unknown list id {list_id!r}: add it to source_list first (see migration 038)")

        before = conn.execute(text("SELECT COUNT(*) FROM source_list_member WHERE list_id = :id"),
                              {"id": list_id}).scalar()
        conn.execute(text("DELETE FROM source_list_member WHERE list_id = :id"), {"id": list_id})

        members, seen = [], {}
        for r in rows:
            for issn in r["issns"]:
                # same ISSN on two rows (renamed journal + successor): keep the active one
                prev = seen.get(issn)
                if prev is not None and (members[prev]["active"] or not r["active"]):
                    continue
                rec = {"list_id": list_id, "issn": issn, "name": r["name"], "active": r["active"],
                       "withdrawn_date": r["withdrawn_date"], "withdrawal_reason": r["withdrawal_reason"]}
                if prev is not None:
                    members[prev] = rec
                else:
                    seen[issn] = len(members)
                    members.append(rec)
        conn.execute(text("""
            INSERT INTO source_list_member (list_id, issn, name, active, withdrawn_date, withdrawal_reason)
            VALUES (:list_id, :issn, :name, :active, :withdrawn_date, :withdrawal_reason)
        """), members)
        conn.execute(text("UPDATE source_list SET list_version = :v, loaded_at = now() WHERE id = :id"),
                     {"v": version, "id": list_id})

        matched = conn.execute(text("""
            SELECT COUNT(DISTINCT si.source_id)
            FROM source_list_member m JOIN source_issn si ON si.issn = m.issn
            WHERE m.list_id = :id AND m.active
        """), {"id": list_id}).scalar()
        unmatched = conn.execute(text("""
            SELECT m.name, m.issn FROM source_list_member m
            LEFT JOIN source_issn si ON si.issn = m.issn
            WHERE m.list_id = :id AND m.active AND si.issn IS NULL
            ORDER BY m.name
        """), {"id": list_id}).fetchall()
        changed = recompute_listed_in(conn)
        listed = conn.execute(text("SELECT COUNT(*) FROM sources WHERE :id = ANY(listed_in)"),
                              {"id": list_id}).scalar()

        print(f"member rows: {before} -> {len(members)} ({sum(m['active'] for m in members)} active ISSNs)")
        print(f"active ISSNs matching a registry source: {matched} distinct sources")
        print(f"active ISSNs matching nothing: {len(unmatched)}")
        for name, issn in unmatched[:40]:
            print(f"  unmatched {issn}  {name}")
        print(f"sources.listed_in rows changed (all lists): {changed}")
        print(f"sources now listed in {list_id}: {listed}")

        if dry_run:
            trans.rollback()
            print("DRY RUN — rolled back")
        else:
            trans.commit()
            print("committed")
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", required=True, help="source_list.id, e.g. cdd-cnu-sante")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--version", required=True, help="edition date YYYY-MM-DD, recorded on source_list")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    run(a.list, a.csv, date.fromisoformat(a.version), dry_run=a.dry_run)


if __name__ == "__main__":
    main()
