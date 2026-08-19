"""Butler et al. / ScholCommLab historical APC list prices -> per-year APC
on sources (oxjob #571; v1 doi:10.7910/DVN/CR1MMV, v2 doi:10.7910/DVN/AZ985C,
both CC0).

v2 (2019-2025, 14 publishers, 69,856 rows) renamed every column (lowercase),
added issn_l + AUD + type_of_fee/apc_text, and dropped APC_provided/APC_order
(journal-year duplicates are resolved upstream). parse_file() detects the
layout per file, so v1 reruns still work. Bronze schema is unchanged:
type_of_fee/apc_text/collector/comment stay file-only (the raw file is
archived on Dataverse; apc_provided is derived from price presence).

Medallion split (Jason/Casey decision 2026-07-17):
  bronze  butler_apc_journal_year -- raw rows, ALL original currencies +
          collection metadata (the audit trail). Staged TRUNCATE+reload,
          one transaction.
  gold    sources.apc_usd_by_year -- USD-only JSONB dict of OBSERVED years
          only, e.g. {"2019": 1790, ..., "2023": 2390}. NO fill in either
          direction (Casey + Kyle decision 2026-07-21, reversing the
          dense-2000->present shape shipped 07-21 morning: "populate the
          years we have rather than repeat data going backwards" / backfill
          "would be a lot of bad data"). In-window gaps stay absent too.
          Pre-/post-window fallback is CONSUMER-side (phase-2 work-level
          lookup; apc_usd below covers "current").

apply: match each staged journal's ISSNs against source_issn with ISSN-L
expansion (issn_to_issnl), resolve multi-matches (issn_l preference -> active
-> more works, per SCHEMA-DESIGN.md), then write per source:
  apc_usd_by_year  the observed-years dict (gold)
  apc_usd          the MOST RECENT observed year's value (Casey ack
                   2026-07-21 in-meeting; no other job writes apc_usd --
                   verified, the old DOAJ-derived values were frozen).
                   NOTE: 56 explicit-$0 journals set apc_usd = 0, which
                   downstream flags them diamond OA -- intended.

  apc_prices       refreshed on covered sources from the most recent
                   observed year's ORIGINAL-currency prices in bronze
                   (fallback: the dataset USD value). Shape kept EXACTLY
                   [{"price": int, "currency": str}] -- walden parses it
                   with a FIXED ARRAY<STRUCT<price INT, currency STRING>>
                   schema; never change the shape. Refresh added 2026-07-22:
                   the 2022-frozen values visibly contradicted the new
                   apc_usd in API responses.

RECENCY GUARD (added for the v2 rerun): apc_usd/apc_prices claim to describe
TODAY, so they are only written when the dataset observed the journal within
RECENCY_WINDOW years of its own max year. A journal whose observations stop
early (publisher transfer, delisting) keeps whatever the registry currently
says -- which may be post-v1 curation the dataset predates (e.g. the 2026-08
Comptes Rendus fix: their Elsevier rows end 2020, the journals are diamond
now). The history dict is still written for such journals; history is not
a claim about the present.

Rows priced in some currency but with no USD value would need conversion at
today's FX rate (meeting decision); in v1 AND v2 every priced row has a USD
value (v2's 505 USD-less rows are unpriced complex-fee rows, empty in every
currency), so the job counts-and-skips such rows (counter no_usd_needs_fx)
rather than shipping an FX table it can't exercise.

--dry-run is fully read-only and safe BEFORE migration 020: the file is
parsed in memory (bronze is neither required nor written), matching runs
against live registry reads, and the report covers parsed rows, match
counters, dicts built, and sample gold output.

  python -m jobs.butler_apc --file APCdataset-annualAPCs_Published-v1.txt \
      --dataset-version butler_v1 [--dry-run] [--skip-fetch]
  python -m jobs.butler_apc --file scholcommlab_apc_dataset_2019_2025.csv \
      --dataset-version butler_v2 [--dry-run] [--skip-fetch]
"""
import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date

import psycopg2.extras
from sqlalchemy import text

from db import engine
from sources_lib import normalize_issns

CURRENCIES = ("USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD")  # AUD: v2+
MIN_ROWS = 30000       # a truncated download must not mass-wipe the staging
MIN_ROWS_V2 = 60000    # v2 has 69,856 rows; the v1 floor would let a 43%
                       # truncation through and TRUNCATE bronze behind it
RECENCY_WINDOW = 1  # years behind the dataset's max year a journal's latest
                    # observation may lag and still refresh apc_usd/apc_prices
                    # (1 absorbs known collection gaps, e.g. Sage 2025 =
                    # hybrid-only; older = history-only, see module doc)
PROVENANCE_PREFIX = "butler"


def normalize_issn(raw):
    """Uppercase, hyphenate, keep check-digit X. Returns None for non-ISSN
    strings. Bad check digits pass through on purpose: 7 v1 ISSNs are
    publisher typos the registry may carry verbatim."""
    if not raw:
        return None
    s = raw.strip().upper().replace(" ", "")
    if "-" not in s and len(s) == 8:
        s = s[:4] + "-" + s[4:]
    if len(s) != 9 or s[4] != "-":
        return None
    digits = s[:4] + s[5:]
    if not (digits[:7].isdigit() and (digits[7].isdigit() or digits[7] == "X")):
        return None
    return s


def parse_row(row):
    """One annual-file row -> staging dict (None if it has no usable ISSN)."""
    issns = normalize_issns(
        [normalize_issn(row.get("ISSN_1")), normalize_issn(row.get("ISSN_2"))]
    )
    if not issns:
        return None
    prices = []
    price_usd = None
    for cur in CURRENCIES:
        val = (row.get(f"APC_{cur}") or "").strip()
        flag = (row.get(f"APC_{cur}-originalORconverted") or "").strip()
        if not val:
            continue
        if cur == "USD":
            price_usd = float(val)  # original or converted; both usable
        if flag == "original":
            prices.append({"currency": cur, "price": round(float(val)), "original": True})
    order = (row.get("APC_order") or "").strip()
    return {
        "unique_id": int(row["unique_id"]),
        "publisher": (row.get("Publisher") or "").strip() or None,
        "issns": issns,
        "journal": (row.get("Journal") or "").strip() or None,
        "oa_status": (row.get("OA_status") or "").strip() or None,
        "apc_provided": (row.get("APC_provided") or "").strip() or None,
        "apc_order": int(order) if order else None,
        "apc_year": int(row["APC_year"]),
        "apc_date": (row.get("APC_date") or "").strip() or None,
        "prices": json.dumps(prices) if prices else None,
        "price_usd": price_usd,
        "apc_source": (row.get("APC_source") or "").strip() or None,
    }


def parse_row_v2(row):
    """One v2-layout row -> the same staging dict shape as parse_row.
    APC_provided is derived (any price present = yes); APC_order stays NULL
    (v2 resolved mid-year transitions upstream). The file's issn_l column is
    a FALLBACK only, never merged with direct ISSNs: 18 journals carry an
    issn_l that is a DIFFERENT journal's direct ISSN (upstream fuzzy-
    validation errors, e.g. Environmental Epigenetics carrying Current
    Zoology's 1674-5507), which would mis-route their prices -- and all 18
    have direct ISSNs, so the fallback never fires for them. 113 rows have
    ONLY an issn_l; for those it is the one identifier there is. The
    registry's own issn_to_issnl expansion covers linking either way."""
    issns = normalize_issns(
        [normalize_issn(row.get("issn1")), normalize_issn(row.get("issn2"))]
    )
    if not issns:
        issns = normalize_issns([normalize_issn(row.get("issn_l"))])
    if not issns:
        return None
    # v2's original/converted flags are unreliable: tens of thousands of rows
    # flag annual-FX-derived values (cents, varying yearly with the rate) as
    # "original". Two-part verdict per entry, staged alongside the raw flag so
    # bronze keeps the full audit trail and gold filters on the verdict:
    #   1. a currency NAMED by other columns' "converted from X" flags is the
    #      conversion source = genuinely original, cents or not (keeps Year's
    #      Work in English Studies' real GBP 3028.20);
    #   2. otherwise flagged-original counts only if integer-valued -- list
    #      prices are integers; rounding 535.78 AUD into apc_prices would
    #      fabricate one. price_usd is unaffected either way: converted USD
    #      is explicitly usable (2026-07-17 meeting).
    conversion_sources = set()
    for cur in CURRENCIES:
        flag = (row.get(f"apc_{cur.lower()}_originalORconverted") or "").strip()
        m = re.search(r"converted\s*from\s*([A-Za-z]{3})", flag, re.IGNORECASE)
        if m:  # also matches the file's lone "ConvertedFromUSD" variant
            conversion_sources.add(m.group(1).upper())
    prices = []
    price_usd = None
    for cur in CURRENCIES:
        val = (row.get(f"apc_{cur.lower()}") or "").strip()
        flag = (row.get(f"apc_{cur.lower()}_originalORconverted") or "").strip()
        if not val:
            continue
        if cur == "USD":
            price_usd = float(val)
        prices.append({
            "currency": cur, "price": float(val), "flag": flag,
            "original": flag == "original" and (
                cur in conversion_sources or float(val).is_integer()),
        })
    # v2 apc_date carries a timestamp suffix ("2021-06-10 00:00:00 UTC");
    # one row is DD/MM/YYYY, which would abort the staging transaction at
    # the ::date cast (Postgres default datestyle is MDY)
    apc_date = (row.get("apc_date") or "").split(" ")[0].strip() or None
    if apc_date:
        try:
            if "/" in apc_date:  # one row is DD/MM/YYYY
                d, m, y = apc_date.split("/")
                apc_date = f"{y}-{int(m):02d}-{int(d):02d}"
            date.fromisoformat(apc_date)
        except (ValueError, TypeError):
            apc_date = None
    return {
        "unique_id": int(row["unique_id"]),
        "publisher": (row.get("publisher") or "").strip() or None,
        "issns": issns,
        "journal": (row.get("journal") or "").strip() or None,
        "oa_status": (row.get("oa_status") or "").strip() or None,
        "apc_provided": "yes" if (price_usd is not None or prices) else "no",
        "apc_order": None,
        "apc_year": int(row["apc_year"]),
        "apc_date": apc_date,
        "prices": json.dumps(prices) if prices else None,
        "price_usd": price_usd,
        "apc_source": (row.get("apc_source") or "").strip() or None,
    }


def parse_file(path, dataset_version):
    rows, skipped = [], 0
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        header = f.readline()
        f.seek(0)
        # v1 ships tab-delimited .txt; v2's Dataverse zip ships comma CSV
        # (the .tab re-export is tab). Detect both, per file.
        reader = csv.DictReader(f, delimiter="\t" if "\t" in header else ",")
        fields = set(reader.fieldnames or [])
        if "issn1" in fields:
            row_parser = parse_row_v2
        elif "ISSN_1" in fields:
            row_parser = parse_row
        else:
            raise RuntimeError(
                f"unrecognized Butler column layout: {sorted(fields)[:8]}...")
        for raw in reader:
            parsed = row_parser(raw)
            if parsed is None:
                skipped += 1
                continue
            parsed["dataset_version"] = dataset_version
            rows.append(parsed)
    min_rows = MIN_ROWS_V2 if row_parser is parse_row_v2 else MIN_ROWS
    if len(rows) < min_rows:
        raise RuntimeError(f"Butler file suspiciously small ({len(rows)} rows "
                           f"< {min_rows}); aborting before staging")
    print(f"parsed {len(rows)} Butler journal-year rows "
          f"({skipped} skipped: no usable ISSN)", flush=True)
    return rows


def stage(rows, dataset_version):
    # execute_batch, not executemany: one round trip per row is fine on a
    # dyno but ~20 min from a laptop over WAN for 36k rows
    insert = (
        "INSERT INTO butler_apc_journal_year (unique_id, publisher, issns, journal, "
        "oa_status, apc_provided, apc_order, apc_year, apc_date, prices, price_usd, "
        "apc_source, dataset_version) VALUES (%(unique_id)s, %(publisher)s, "
        "%(issns)s, %(journal)s, %(oa_status)s, %(apc_provided)s, %(apc_order)s, "
        "%(apc_year)s, %(apc_date)s::date, %(prices)s::jsonb, %(price_usd)s, "
        "%(apc_source)s, %(dataset_version)s)"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE butler_apc_journal_year"))
        psycopg2.extras.execute_batch(
            conn.connection.cursor(), insert, rows, page_size=500)
    print(f"staged {len(rows)} Butler journal-year rows ({dataset_version})", flush=True)


def load_staged(conn):
    return [dict(r._mapping) for r in conn.execute(text(
        "SELECT unique_id, publisher, issns, journal, apc_year, apc_order, "
        "apc_date, prices, price_usd, apc_provided, dataset_version "
        "FROM butler_apc_journal_year"))]


def match_rows(conn, rows):
    """-> ({source_id: [row dicts]}, multi-match issue list, counters).

    ISSN -> source via source_issn, expanded through issn_to_issnl (both the
    raw ISSNs and their mapped ISSN-Ls), mirroring sources_lib.match_source's
    expansion. Multi-matches resolve to ONE winner: issn_l-owning source, then
    active over merged, then more works / lower id (resolve_conflicts rule).
    """
    issn_to_sid = {}
    for sid, issn in conn.execute(text("SELECT source_id, issn FROM source_issn")):
        issn_to_sid[issn] = sid
    # issn_to_issnl is ~2.6M rows; expand ALL dataset ISSNs in one ANY() query
    # instead of loading the table or querying per journal
    all_issns = sorted({i for r in rows for i in r["issns"]})
    issnl_map = defaultdict(set)
    for issn, issn_l in conn.execute(text(
            "SELECT issn, issn_l FROM issn_to_issnl "
            "WHERE issn = ANY(:i) AND issn_l IS NOT NULL"), {"i": all_issns}):
        issnl_map[issn].add(issn_l)
    meta = {r.id: r for r in conn.execute(text(
        "SELECT s.id, s.issn_l, COALESCE(w.works_count, 0) AS works "
        "FROM sources s LEFT JOIN source_works_count w ON w.source_id = s.id"))}

    by_journal = defaultdict(list)
    for r in rows:
        by_journal[r["unique_id"]].append(r)

    counts = Counter()
    per_source = defaultdict(list)
    issues = []
    for uid, jrows in by_journal.items():
        issns = normalize_issns([i for r in jrows for i in r["issns"]])
        mapped = set().union(*(issnl_map[i] for i in issns)) if issns else set()
        candidates = {issn_to_sid[i] for i in set(issns) | mapped if i in issn_to_sid}
        if not candidates:
            counts["unmatched"] += 1
            continue
        if len(candidates) == 1:
            winner = next(iter(candidates))
            counts["matched"] += 1
        else:
            counts["multi_match"] += 1
            winner = min(candidates, key=lambda sid: (
                0 if meta[sid].issn_l in issns else 1,       # owns a dataset ISSN-L
                -meta[sid].works,                             # more works
                sid,                                          # older id
            ))
            issues.append((issns, sorted(candidates),
                           f"butler unique_id={uid} -> winner {winner}"))
        per_source[winner].extend(jrows)
    return per_source, issues, counts


def _as_date(v):
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(v) if v else None
    except ValueError:
        return None


def build_usd_by_year(rows, counts):
    """Row dicts (possibly from several unique_ids on one source) ->
    ({"<observed year>": usd, ...}, most-recent observed value, apc_prices).

    Observation per year: the row's dataset USD value (original or Butler-
    converted), rounded. Collisions within a year resolve by highest
    apc_order, then -- when order gives no answer and the colliding rows
    name different publishers (v2's publisher-transfer duplicate) -- the
    INCOMING publisher (the one that isn't the previous year's: the transfer
    year runs on the new publisher's list), then latest apc_date. Rows
    without a USD value are not observations. NO fill: observed years only
    (module doc)."""
    by_year = defaultdict(list)
    for r in rows:
        if r["price_usd"] is None:
            if r["prices"]:  # priced in some currency but no USD: needs FX
                counts["no_usd_needs_fx"] += 1
            continue
        by_year[r["apc_year"]].append(r)
    if not by_year:
        return None, None, None
    observed = {}  # year -> (usd, price entries)
    prev_pub = None
    for y in sorted(by_year):
        cands = by_year[y]
        transfer = len({c.get("publisher") for c in cands}) > 1
        best = max(cands, key=lambda r: (
            r["apc_order"] or 1,
            1 if (transfer and prev_pub and r.get("publisher")
                  and r["publisher"] != prev_pub) else 0,
            _as_date(r["apc_date"]) or date.min,
        ))
        raw = best["prices"]
        raw = json.loads(raw) if isinstance(raw, str) else (raw or [])
        observed[y] = (round(best["price_usd"]), raw)
        prev_pub = best.get("publisher") or prev_pub
    latest = observed[max(observed)]
    # apc_prices payload: latest year's original-verdict entries (v1 rows
    # carry only originals; v2 rows carry every cell plus the verdict --
    # module doc), walden shape [{"price", "currency"}]; no originals ->
    # the dataset USD value
    prices = ([{"price": round(p["price"]), "currency": p["currency"]}
               for p in latest[1] if p.get("original")]
              or [{"price": latest[0], "currency": "USD"}])
    return ({str(y): observed[y][0] for y in sorted(observed)},
            latest[0], prices)


def apply(dataset_version, rows=None, dry_run=False):
    update = (
        # apc_prices: same fixed shape walden parses, refreshed content
        # (module doc). apc_usd = most recent observed value (Casey ack).
        "UPDATE sources SET apc_usd_by_year = %(by_year)s::jsonb, "
        "apc_usd = %(usd)s, apc_prices = %(prices)s::jsonb, "
        "updated_date = now() WHERE id = %(id)s"
    )
    update_history_only = (
        # recency guard (module doc): stale journals get history only;
        # apc_usd/apc_prices keep the registry's current state (curation
        # may be newer than the dataset's last sighting of the journal).
        "UPDATE sources SET apc_usd_by_year = %(by_year)s::jsonb, "
        "updated_date = now() WHERE id = %(id)s"
    )
    with engine.begin() as conn:
        if rows is None:
            rows = load_staged(conn)
        dataset_max_year = max(r["apc_year"] for r in rows)
        # a dataset that is itself old (a v1 re-run, or this file years from
        # now) must not stamp its terminal years as current prices: force
        # every write down the history-only path
        dataset_is_stale = dataset_max_year < date.today().year - 1
        if dataset_is_stale:
            print(f"dataset max year {dataset_max_year} is stale vs today: "
                  f"ALL updates will be history-only", flush=True)
        per_source, issues, counts = match_rows(conn, rows)
        print(f"[{dataset_version}] match: {dict(counts)}; "
              f"{len(per_source)} candidate sources; dry_run={dry_run}", flush=True)
        samples = []
        stale_samples = []
        pending = []
        pending_history = []
        for sid, srows in per_source.items():
            by_year, current, prices = build_usd_by_year(srows, counts)
            if not by_year:
                counts["no_priced_rows"] += 1
                continue
            stale = dataset_is_stale or (
                max(int(y) for y in by_year) < dataset_max_year - RECENCY_WINDOW)
            if len({r["unique_id"] for r in srows}) > 1:
                counts["multi_uid_sources"] += 1
            if dry_run:
                journal = next((r["journal"] for r in srows if r["journal"]), None)
                if stale:
                    counts["would_update_history_only"] += 1
                    if len(stale_samples) < 8:
                        stale_samples.append((sid, journal, by_year))
                else:
                    counts["would_update"] += 1
                    if len(samples) < 3 or (journal and "scientific reports" in journal.lower()):
                        samples.append((sid, journal, by_year, current, prices))
            elif stale:
                pending_history.append({"id": sid, "by_year": json.dumps(by_year)})
                counts["updated_history_only"] += 1
            else:
                pending.append({"id": sid, "by_year": json.dumps(by_year),
                                "usd": current, "prices": json.dumps(prices)})
                counts["updated"] += 1
        if pending:
            psycopg2.extras.execute_batch(
                conn.connection.cursor(), update, pending, page_size=500)
        if pending_history:
            psycopg2.extras.execute_batch(
                conn.connection.cursor(), update_history_only, pending_history,
                page_size=500)
        if dry_run:
            for issns, ids, detail in issues[:20]:
                print(f"  multi_match {issns} -> {ids} ({detail})")
            for sid, journal, by_year, current, prices in samples[:8]:
                years = sorted(by_year)
                edges = {y: by_year[y] for y in years[:2] + years[-2:]}
                print(f"  sample source {sid} ({journal}): {len(by_year)} years, "
                      f"edges {edges}, current={current}, apc_prices={prices}")
            for sid, journal, by_year in stale_samples:
                print(f"  history-only source {sid} ({journal}): last observed "
                      f"{max(by_year)} < {dataset_max_year - RECENCY_WINDOW}, "
                      f"apc_usd/apc_prices untouched")
            print(f"dry-run (NO WRITES): {dict(counts)}", flush=True)
            return counts
        # multi-match pairs are logged, NOT parked into source_ingest_issue
        # (pending Casey ack, OPEN-QUESTIONS #7): parking can trigger
        # resolve_conflicts auto-merges, the one hard-to-reverse side effect.
        # To park later: rerun with --skip-fetch after restoring the
        # park_multi_match call, or hand the log lines to the dedup campaign.
        for issns, ids, detail in issues:
            print(f"  multi_match (logged only) {issns} -> {ids} ({detail})")
    print(f"applied (DONE): {dict(counts)}", flush=True)
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="path to the Butler annual APCs tab-delimited file")
    ap.add_argument("--dataset-version", default="butler_v1",
                    help="provenance tag: butler_v1 / butler_v2")
    ap.add_argument("--dry-run", action="store_true",
                    help="read-only: parse + match + build, write nothing")
    ap.add_argument("--skip-fetch", action="store_true", help="apply from existing staging")
    args = ap.parse_args()
    rows = None
    if not args.skip_fetch:
        if not args.file:
            ap.error("--file is required unless --skip-fetch")
        rows = parse_file(args.file, args.dataset_version)
        if not args.dry_run:
            stage(rows, args.dataset_version)
    apply(args.dataset_version, rows=rows, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
