"""DOAJ journal feed: download the public metadata CSV (doaj.org/csv, ~23K
journals, no credentials), stage it, and apply DOAJ status to sources:

  is_in_doaj / is_in_doaj_start_year / doaj_license, plus delistings (flag off
  when a journal leaves DOAJ); is_oa is re-derived by sources_lib.recompute_is_oa.

It also fills a NULL sources.country_code from DOAJ's 'Country of publisher'
(oxjob #2185; fill_country). A country the registry already holds is never
overwritten, whatever DOAJ says.

The CSV lags DOAJ's live index by a month or more for newly created records
(oxjob #548 C5: 8 confirmed journals admitted 2026-06-08..07-03 absent from the
2026-07-07 CSV; API total 23,155 vs CSV 23,041), so treating it as the full
universe falsely delists recently (re)admitted journals. fetch() therefore
supplements the staging with a search-API sweep of records created in the last
SUPPLEMENT_WINDOW_DAYS. Both the CSV floor check and any API failure abort the
run BEFORE staging is replaced — a partial universe must never reach apply().

With --mint, DOAJ also ADDS journals the registry lacks (DOAJ is human-vetted,
and we already harvest DOAJ articles into the works pipeline — their locations
need these sources to match against) via the shared match cascade (sources_lib):
ISSN match -> already known; unique guarded name match -> attach the DOAJ ISSNs
to that source; ambiguous or guard-refused -> conflict row; no match -> mint.

  python -m jobs.doaj [--dry-run] [--skip-fetch] [--mint]
"""
import argparse
import csv
import io
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from sqlalchemy import text

from db import engine
from sources_lib import (
    MatchContext,
    country_iso,
    insert_issns,
    match_source,
    mint_source,
    normalize_issns,
    park_multi_match,
    recompute_is_oa,
    recompute_listed_in,
    resolve_issn_l,
)

DOAJ_CSV_URL = "https://doaj.org/csv"
DOAJ_API_SEARCH = "https://doaj.org/api/search/journals/"
SUPPLEMENT_WINDOW_DAYS = 365  # observed CSV lag is ~1 month; a year is cheap (~50 pages)
MIN_CSV_ROWS = 20000  # truncated CSV would mass-delist; abort instead

LICENSE_MAP = {
    "CC BY": "cc-by",
    "CC BY-NC": "cc-by-nc",
    "CC BY-NC-ND": "cc-by-nc-nd",
    "CC BY-NC-SA": "cc-by-nc-sa",
    "CC BY-SA": "cc-by-sa",
    "CC BY-ND": "cc-by-nd",
    "Public domain": "public-domain",
}


def supplement_from_api(staged_issns):
    """Journals created in the last SUPPLEMENT_WINDOW_DAYS that the CSV lacks.

    Raises on any API failure — apply() must never run against a universe
    that is missing the recent-admissions window.
    """
    # The search API rejects paging past 1,000 results, so walk the window in
    # date chunks and split any chunk that would overflow the cap.
    since = (datetime.now(timezone.utc) - timedelta(days=SUPPLEMENT_WINDOW_DAYS)).date()
    until = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    rows = []
    spans = [(since, until)]
    while spans:
        a, b = spans.pop()
        query = quote(f"created_date:[{a} TO {b}]", safe="")
        page = 1
        while True:
            r = requests.get(f"{DOAJ_API_SEARCH}{query}",
                             params={"page": page, "pageSize": 100}, timeout=60)
            r.raise_for_status()
            data = r.json()
            total = data.get("total", 0)
            if total > 1000:
                if (b - a).days <= 1:
                    raise RuntimeError(f"DOAJ span {a}..{b} exceeds the API paging cap")
                mid = a + (b - a) / 2
                spans.extend([(a, mid), (mid, b)])
                break
            for rec in data.get("results", []):
                bj = rec.get("bibjson", {})
                issns = [v.strip().upper() for v in (bj.get("pissn"), bj.get("eissn"))
                         if v and v.strip()]
                if not issns or any(i in staged_issns for i in issns):
                    continue
                licenses = [l.get("type") for l in (bj.get("license") or [])]
                publisher = bj.get("publisher") or {}
                rows.append({
                    "issns": issns,
                    "title": (bj.get("title") or "").strip() or None,
                    "publisher": (publisher.get("name") or "").strip() or None,
                    "license": next((LICENSE_MAP[t] for t in licenses if t in LICENSE_MAP), None),
                    "oa_start_year": bj.get("oa_start") if isinstance(bj.get("oa_start"), int) else None,
                    "country": (publisher.get("country") or "").strip() or None,
                })
            # DOAJ silently caps pageSize (50 since 2026-10-30), so page by what it returns
            if page * data["pageSize"] >= total:
                break
            page += 1
    # adjacent spans share a boundary day; drop any duplicate journals
    seen, deduped = set(), []
    for row in rows:
        key = tuple(sorted(row["issns"]))
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def fetch():
    r = requests.get(DOAJ_CSV_URL, timeout=300)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    rows = []
    for rec in reader:
        issns = [
            v.strip().upper()
            for v in (rec.get("Journal ISSN (print version)"),
                      rec.get("Journal EISSN (online version)"))
            if v and v.strip()
        ]
        if not issns:
            continue
        year = (rec.get("When did the journal start to publish all content using an open license?") or "").strip()
        rows.append({
            "issns": issns,
            "title": (rec.get("Journal title") or "").strip() or None,
            "publisher": (rec.get("Publisher") or "").strip() or None,
            "license": LICENSE_MAP.get((rec.get("Journal license") or "").strip()),
            "oa_start_year": int(year) if year.isdigit() else None,
            "country": (rec.get("Country of publisher") or "").strip() or None,
        })
    if len(rows) < MIN_CSV_ROWS:
        raise RuntimeError(f"DOAJ CSV suspiciously small ({len(rows)} rows < {MIN_CSV_ROWS}); "
                           "aborting before staging to avoid a mass delist")
    staged_issns = {i for row in rows for i in row["issns"]}
    api_rows = supplement_from_api(staged_issns)  # raises on failure, before TRUNCATE
    insert = text(
        "INSERT INTO doaj_journal (issns, title, publisher, license, oa_start_year, country) "
        "VALUES (:issns, :title, :publisher, :license, :oa_start_year, :country)"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE doaj_journal"))
        for i in range(0, len(rows), 5000):
            conn.execute(insert, rows[i:i + 5000])
        if api_rows:
            conn.execute(insert, api_rows)
    print(f"staged {len(rows)} DOAJ journals from CSV "
          f"+ {len(api_rows)} recent admissions the CSV lacks (API supplement)", flush=True)


def mint_missing(dry_run=False, batch=200):
    """Add DOAJ journals whose ISSNs match no source (guarded; see module doc)."""
    with engine.connect() as conn:
        staged = conn.execute(text(
            "SELECT issns, title, publisher FROM doaj_journal ORDER BY id")).fetchall()
        ctx = MatchContext(conn, name_link=True)

    counts = Counter()
    conn = engine.connect()
    trans = conn.begin()
    done = 0
    try:
        for row in staged:
            issns = normalize_issns(list(row.issns or []))
            if not issns:
                counts["already_known"] += 1
                continue
            kind, val = match_source(conn, ctx, issns, row.title, "doaj",
                                     publisher=row.publisher, dry_run=dry_run)
            if kind in ("issn", "issn_multi"):
                # any known ISSN means the journal exists; apply() flags it.
                # multi-ISSN merge candidates are Crossref's daily job's beat.
                counts["already_known"] += 1
                continue
            if kind == "name":
                counts["linked_by_name"] += 1
                if not dry_run:
                    issn_l = resolve_issn_l(conn, issns, member_only=True)
                    insert_issns(conn, val, issns, issn_l)
                    ctx.register(val, issns)
            elif kind == "name_parked":
                counts[f"name_link_parked_{val}"] += 1
            elif kind == "name_multi":
                counts["conflict"] += 1
                if not dry_run:
                    park_multi_match(conn, "doaj", issns, val, row.title)
            else:  # 'none'
                counts["added"] += 1
                if not dry_run:
                    mint_source(conn, ctx, row.title, issns=issns, publisher=row.publisher)
            done += 1
            if done % batch == 0 and not dry_run:
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
    print("mint summary:", dict(counts), flush=True)
    return counts


def apply(dry_run=False):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMP TABLE _doaj_flags ON COMMIT DROP AS
            SELECT si.source_id,
                   MIN(d.oa_start_year) AS start_year,
                   MIN(d.license) AS license
            FROM doaj_journal d
            CROSS JOIN LATERAL unnest(d.issns) AS di(issn)
            JOIN source_issn si ON si.issn = di.issn
            GROUP BY si.source_id
        """))
        n_match = conn.execute(text("SELECT COUNT(*) FROM _doaj_flags")).scalar()

        turned_on = conn.execute(text("""
            SELECT COUNT(*) FROM sources s JOIN _doaj_flags f ON f.source_id = s.id
            WHERE s.is_in_doaj IS DISTINCT FROM TRUE
               OR s.is_in_doaj_start_year IS DISTINCT FROM f.start_year
               OR s.doaj_license IS DISTINCT FROM f.license
        """)).scalar()
        turned_off = conn.execute(text("""
            SELECT COUNT(*) FROM sources s
            WHERE s.is_in_doaj = TRUE
              AND NOT EXISTS (SELECT 1 FROM _doaj_flags f WHERE f.source_id = s.id)
        """)).scalar()
        print(f"{n_match} sources matched in DOAJ; {turned_on} to set/update, "
              f"{turned_off} delistings; dry_run={dry_run}", flush=True)
        fill_country(conn, dry_run=dry_run)
        if dry_run:
            return

        conn.execute(text("""
            UPDATE sources s SET
                is_in_doaj = TRUE,
                is_in_doaj_start_year = f.start_year,
                doaj_license = f.license,
                updated_date = now()
            FROM _doaj_flags f
            WHERE f.source_id = s.id
              AND (s.is_in_doaj IS DISTINCT FROM TRUE
                   OR s.is_in_doaj_start_year IS DISTINCT FROM f.start_year
                   OR s.doaj_license IS DISTINCT FROM f.license)
        """))
        # delistings: flag off; is_oa is re-derived below
        conn.execute(text("""
            UPDATE sources s SET
                is_in_doaj = FALSE,
                is_in_doaj_start_year = NULL,
                doaj_license = NULL,
                updated_date = now()
            WHERE s.is_in_doaj = TRUE
              AND NOT EXISTS (SELECT 1 FROM _doaj_flags f WHERE f.source_id = s.id)
        """))
        oa = recompute_is_oa(conn)
        listed = recompute_listed_in(conn)
        print(f"listed_in rows changed: {listed}")
    print(f"applied (DONE); is_oa recomputed on {oa}", flush=True)


def multinational_publishers(conn):
    """DOAJ publisher names (lower-cased) whose journals DOAJ lists under two or more countries.

    For Elsevier, Wiley, Oxford University Press and the like, 'Country of publisher' is the office
    that handles the journal, which is often not the journal's home (oxjob #2185: all 3 misses in
    a 50-journal check against the journals' own sites were journals of such publishers).
    """
    countries = defaultdict(set)
    for publisher, country in conn.execute(text(
            "SELECT DISTINCT publisher, country FROM doaj_journal "
            "WHERE publisher IS NOT NULL AND country IS NOT NULL")):
        iso = country_iso(country)
        if iso:
            countries[publisher.strip().lower()].add(iso[0])
    return {p for p, codes in countries.items() if len(codes) > 1}


def fill_country(conn, dry_run=False):
    """Fill sources.country_code where it is NULL from DOAJ's 'Country of publisher' (oxjob #2185).

    DOAJ's journal record is supplied by the journal, and OpenAlex defines a source's country as
    its home country, typically its publisher's. Only NULLs are filled: a code the registry
    already holds is never touched. A source whose DOAJ records name two different countries, a
    value that is not an ISO 3166-1 country, or a publisher DOAJ lists under several countries
    (multinational_publishers) is left alone. `country` (the name) is filled only
    where it is NULL too. Returns [(source_id, code, name, name_filled)] for the rows filled (or
    that a dry run would fill), so a caller can keep a receipt; revert = set those country_codes
    (and the names this filled) back to NULL.
    """
    rows = conn.execute(text("""
        SELECT s.id, array_agg(DISTINCT d.country) AS countries, s.country IS NULL AS name_null,
               array_agg(DISTINCT lower(btrim(d.publisher))) AS publishers
        FROM doaj_journal d
        CROSS JOIN LATERAL unnest(d.issns) AS di(issn)
        JOIN source_issn si ON si.issn = di.issn
        JOIN sources s ON s.id = si.source_id
        WHERE s.country_code IS NULL AND d.country IS NOT NULL
        GROUP BY s.id ORDER BY s.id
    """)).fetchall()
    multinational = multinational_publishers(conn)
    filled, skipped = [], Counter()
    for sid, countries, name_null, publishers in rows:
        resolved = {country_iso(c) for c in countries}
        if multinational.intersection(publishers):
            skipped["publisher under several countries"] += 1
        elif None in resolved:
            skipped["not an ISO country: " + "; ".join(sorted(countries))] += 1
        elif len(resolved) != 1:
            skipped["two DOAJ countries"] += 1
        else:
            filled.append((sid, *next(iter(resolved)), name_null))
    if filled and not dry_run:
        written = {r.id for r in conn.execute(text("""
            UPDATE sources s SET country_code = v.code, country = COALESCE(s.country, v.name),
                                 updated_date = now()
            FROM unnest(CAST(:ids AS bigint[]), CAST(:codes AS text[]), CAST(:names AS text[]))
                 AS v(id, code, name)
            WHERE s.id = v.id AND s.country_code IS NULL
            RETURNING s.id
        """), {"ids": [f[0] for f in filled], "codes": [f[1] for f in filled],
               "names": [f[2] for f in filled]})}
        if len(written) < len(filled):
            skipped["changed under us"] = len(filled) - len(written)
        filled = [f for f in filled if f[0] in written]
    print(f"country_code filled from DOAJ on {len(filled)} sources (NULL only); "
          f"skipped {dict(skipped)}; dry_run={dry_run}", flush=True)
    return filled


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-fetch", action="store_true", help="apply from existing staging")
    ap.add_argument("--mint", action="store_true", help="add journals the registry lacks")
    args = ap.parse_args()
    if not args.skip_fetch:
        fetch()
    if args.mint:
        mint_missing(dry_run=args.dry_run)  # before apply, so new mints get flagged
    apply(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
