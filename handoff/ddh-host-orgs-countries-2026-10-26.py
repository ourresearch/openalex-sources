"""Host organizations, then countries, for Diamond Discovery Hub sources (oxjob #1848; Kyle's 'ddh source list' session,
2026-10-26: match host orgs first, then fill countries).

1. Host orgs: handoff/ddh-host-orgs-2026-10-26.csv (709 listed_in:ddh sources with no host org). DDH's publisher name
   matched to an existing publisher / institution entity the #1563 way (normalized exact / segment / contained name match,
   country guard, per-string judge, ROR-twin rule: publisher_id where the institution has a ROR publisher twin), plus
   reviewed parent-organization aliases (university presses and units -> their university). No entities minted.
   Guards per row: source exists, 'ddh' in listed_in, publisher_id AND institution_id both NULL (never overwrites).
2. Countries: handoff/ddh-country-2026-10-26.csv (518 listed_in:ddh sources with no country_code). Host org's single
   country (rule=host; only if the source's host after step 1 is that host), else DDH's country (rule=ddh). Fill-only:
   country_code IS NULL; `country` takes the registry's own spelling for the code (CSV name as fallback) when empty.
   Works don't embed a source's country; host-org links re-stamp the journals' works at the next End 2 End.

One transaction, dry run by default, receipt after RECEIPT.
    heroku run -a openalex-sources --no-tty "python handoff/ddh-host-orgs-countries-2026-10-26.py [--execute]"
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

HERE = os.path.dirname(os.path.abspath(__file__))
EXECUTE = "--execute" in sys.argv
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "host": {}, "country": {}, "skipped": {}}


def log(msg):
    print(msg, flush=True)
    receipt.setdefault("actions", []).append(msg)


class DryRun(Exception):
    pass


def current(conn, ids):
    return {r.id: r for r in conn.execute(text(
        "SELECT id, publisher_id, institution_id, country_code, country, listed_in FROM sources WHERE id = ANY(:i)"),
        {"i": list(ids)})}


def hosts(conn):
    rows = list(csv.DictReader(open(os.path.join(HERE, "ddh-host-orgs-2026-10-26.csv"), encoding="utf-8")))
    cur = current(conn, [int(r["source_id"]) for r in rows])
    n = {"publisher_id": 0, "institution_id": 0}
    for r in rows:
        sid, tid = int(r["source_id"]), int(r["target_id"])
        col = "publisher_id" if r["target_kind"] == "publisher" else "institution_id"
        c = cur.get(sid)
        why = ("gone" if c is None else "not ddh" if "ddh" not in (c.listed_in or [])
               else "already linked" if (c.publisher_id or c.institution_id) else None)
        if why:
            receipt["skipped"][f"host {sid}"] = why
            continue
        k = conn.execute(text(f"UPDATE sources SET {col} = :t, updated_date = now() WHERE id = :s "
                              "AND publisher_id IS NULL AND institution_id IS NULL"), {"t": tid, "s": sid}).rowcount
        assert k == 1, sid
        receipt["host"][str(sid)] = [col, tid]
        n[col] += 1
    log(f"host orgs: {sum(n.values())} of {len(rows)} linked ({n['publisher_id']} publisher_id, "
        f"{n['institution_id']} institution_id); skipped {sum(1 for k in receipt['skipped'] if k.startswith('host'))}")


def countries(conn):
    rows = list(csv.DictReader(open(os.path.join(HERE, "ddh-country-2026-10-26.csv"), encoding="utf-8")))
    cur = current(conn, [int(r["source_id"]) for r in rows])
    names, n = {}, {"host": 0, "ddh": 0}
    for r in rows:
        sid, code = int(r["source_id"]), r["country_code"]
        c = cur.get(sid)
        why = None
        if c is None:
            why = "gone"
        elif "ddh" not in (c.listed_in or []):
            why = "not ddh"
        elif c.country_code is not None:
            why = f"already {c.country_code}"
        elif r["rule"] == "host":
            h = f"P{c.publisher_id}" if c.publisher_id else f"I{c.institution_id}" if c.institution_id else None
            if h != r["host_id"]:
                why = f"host is {h}, expected {r['host_id']}"
        if why:
            receipt["skipped"][f"country {sid}"] = why
            continue
        if code not in names:
            names[code] = conn.execute(text(
                "SELECT country FROM sources WHERE country_code = :c AND country IS NOT NULL "
                "GROUP BY country ORDER BY COUNT(*) DESC LIMIT 1"), {"c": code}).scalar() or r["country"]
        k = conn.execute(text("UPDATE sources SET country_code = :c, country = COALESCE(country, :n), updated_date = now() "
                              "WHERE id = :s AND country_code IS NULL"), {"c": code, "n": names[code], "s": sid}).rowcount
        assert k == 1, sid
        receipt["country"][str(sid)] = [c.country_code, code, r["rule"]]
        n[r["rule"]] += 1
    log(f"countries: {sum(n.values())} of {len(rows)} filled ({n['host']} from host org, {n['ddh']} from DDH); "
        f"skipped {sum(1 for k in receipt['skipped'] if k.startswith('country'))}; spellings {names}")


def main():
    try:
        with engine.begin() as conn:
            before = conn.execute(text(
                "SELECT COUNT(*) FILTER (WHERE publisher_id IS NULL AND institution_id IS NULL), "
                "COUNT(*) FILTER (WHERE country_code IS NULL), COUNT(*) FROM sources WHERE 'ddh' = ANY(listed_in)")).fetchone()
            log(f"before: ddh sources {before[2]}, no host org {before[0]}, no country {before[1]}")
            hosts(conn)
            countries(conn)
            after = conn.execute(text(
                "SELECT COUNT(*) FILTER (WHERE publisher_id IS NULL AND institution_id IS NULL), "
                "COUNT(*) FILTER (WHERE country_code IS NULL) FROM sources WHERE 'ddh' = ANY(listed_in)")).fetchone()
            log(f"after: no host org {after[0]}, no country {after[1]}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
