"""Butler et al. historical APC list prices -> per-year APC on sources
(oxjob #571; dataset doi:10.7910/DVN/CR1MMV, CC0).

Medallion split (Jason/Casey decision 2026-07-17):
  bronze  butler_apc_journal_year -- raw rows, ALL original currencies +
          collection metadata (the audit trail). Staged TRUNCATE+reload,
          one transaction.
  gold    sources.apc_usd_by_year -- USD-only JSONB dict {"2000": 3000, ...},
          DENSE from 2000 through the current year, carry-forward filled
          (gaps + post-window = last observed value; pre-window = first
          observed value carried backward; NEVER midpoint interpolation).

Backward-fill to 2000 overstates early years for journals first observed
later (APCs mostly rose; many weren't OA in 2000). Decided in-meeting, but
get explicit Casey ack before anything PUBLIC reads this column
(OPEN-QUESTIONS #2).

apply: match each staged journal's ISSNs against source_issn with ISSN-L
expansion (issn_to_issnl), resolve multi-matches (issn_l preference -> active
-> more works, per SCHEMA-DESIGN.md), then write apc_usd_by_year per source.

Legacy columns are UNTOUCHED, deliberately:
  apc_prices  walden parses it with a FIXED ARRAY<STRUCT<price INT,
              currency STRING>> schema -- never change its shape or
              semantics from this job.
  apc_usd     Butler-vs-DOAJ precedence is DEFERRED to phase 2 (decision
              2026-07-20); the new column does not compete with it yet.

Rows priced in some currency but with no USD value would need conversion at
today's FX rate (meeting decision); in v1 every priced row has a USD value,
so the job counts-and-skips such rows (counter no_usd_needs_fx) rather than
shipping an FX table it can't exercise.

--dry-run is fully read-only and safe BEFORE migration 020: the file is
parsed in memory (bronze is neither required nor written), matching runs
against live registry reads, and the report covers parsed rows, match
counters, dicts built, and sample gold output.

  python -m jobs.butler_apc --file APCdataset-annualAPCs_Published-v1.txt \
      --dataset-version butler_v1 [--dry-run] [--skip-fetch]
"""
import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import date

from sqlalchemy import text

from db import engine
from sources_lib import normalize_issns

CURRENCIES = ("USD", "EUR", "GBP", "JPY", "CHF", "CAD")
MIN_ROWS = 30000  # a truncated download must not mass-wipe the staging
PROVENANCE_PREFIX = "butler"
FILL_START_YEAR = 2000  # gold column covers FILL_START_YEAR..current year, dense


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


def parse_file(path, dataset_version):
    rows, skipped = [], 0
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        for raw in csv.DictReader(f, delimiter="\t"):
            parsed = parse_row(raw)
            if parsed is None:
                skipped += 1
                continue
            parsed["dataset_version"] = dataset_version
            rows.append(parsed)
    if len(rows) < MIN_ROWS:
        raise RuntimeError(f"Butler file suspiciously small ({len(rows)} rows "
                           f"< {MIN_ROWS}); aborting before staging")
    print(f"parsed {len(rows)} Butler journal-year rows "
          f"({skipped} skipped: no usable ISSN)", flush=True)
    return rows


def stage(rows, dataset_version):
    insert = text(
        "INSERT INTO butler_apc_journal_year (unique_id, publisher, issns, journal, "
        "oa_status, apc_provided, apc_order, apc_year, apc_date, prices, price_usd, "
        "apc_source, dataset_version) VALUES (:unique_id, :publisher, :issns, "
        ":journal, :oa_status, :apc_provided, :apc_order, :apc_year, "
        "CAST(:apc_date AS DATE), CAST(:prices AS JSONB), :price_usd, :apc_source, "
        ":dataset_version)"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE butler_apc_journal_year"))
        for i in range(0, len(rows), 5000):
            conn.execute(insert, rows[i:i + 5000])
    print(f"staged {len(rows)} Butler journal-year rows ({dataset_version})", flush=True)


def load_staged(conn):
    return [dict(r._mapping) for r in conn.execute(text(
        "SELECT unique_id, issns, journal, apc_year, apc_order, apc_date, "
        "prices, price_usd, apc_provided, dataset_version "
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
        "SELECT s.id, s.issn_l, s.merge_into_id, COALESCE(w.works_count, 0) AS works "
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
                0 if meta[sid].merge_into_id is None else 1,  # active beats redirect
                -meta[sid].works,                             # more works
                sid,                                          # older id
            ))
            issues.append((issns, sorted(candidates),
                           f"butler unique_id={uid} -> winner {winner}"))
        # follow a merge redirect so the series lands on the surviving source
        if meta[winner].merge_into_id is not None:
            counts["redirected"] += 1
            winner = meta[winner].merge_into_id
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
    """Row dicts (possibly from several unique_ids on one source) -> dense
    {"2000": usd, ..., "<current year>": usd} dict + current-year value.

    Observation per year: the row's dataset USD value (original or Butler-
    converted), rounded. Collisions within a year (publisher-transfer
    duplicates, order-2 transitions) resolve by highest apc_order then latest
    apc_date. Rows with APC_provided != yes have price_usd NULL and are not
    observations. Fill: carry-forward between/after observations, first value
    carried backward before the window. Never interpolates."""
    observed = {}  # year -> (order, date, usd)
    for r in rows:
        if r["price_usd"] is None:
            if r["prices"]:  # priced in some currency but no USD: needs FX
                counts["no_usd_needs_fx"] += 1
            continue
        order = r["apc_order"] or 1
        rdate = _as_date(r["apc_date"]) or date.min
        cur = observed.get(r["apc_year"])
        if cur is None or (order, rdate) > cur[:2]:
            observed[r["apc_year"]] = (order, rdate, round(r["price_usd"]))
    if not observed:
        return None, None
    this_year = date.today().year
    years = sorted(observed)
    filled, last = {}, observed[years[0]][2]  # pre-window = first value backward
    for y in range(FILL_START_YEAR, this_year + 1):
        if y in observed:
            last = observed[y][2]
        filled[str(y)] = last
    return filled, filled[str(this_year)]


def apply(dataset_version, rows=None, dry_run=False):
    update = text(
        # apc_usd and apc_prices deliberately NOT in this statement
        # (precedence deferred / walden fixed-schema contract; module doc).
        # updated_date deliberately NOT bumped either (deviation from repo
        # convention, pending Casey ack): the bump triggers downstream
        # re-sync and can't be un-done, while adding it later is one UPDATE
        # over WHERE apc_usd_by_year IS NOT NULL.
        "UPDATE sources SET apc_usd_by_year = CAST(:by_year AS JSONB) "
        "WHERE id = :id"
    )
    with engine.begin() as conn:
        if rows is None:
            rows = load_staged(conn)
        per_source, issues, counts = match_rows(conn, rows)
        print(f"[{dataset_version}] match: {dict(counts)}; "
              f"{len(per_source)} candidate sources; dry_run={dry_run}", flush=True)
        samples = []
        for sid, srows in per_source.items():
            by_year, current = build_usd_by_year(srows, counts)
            if not by_year:
                counts["no_priced_rows"] += 1
                continue
            if dry_run:
                counts["would_update"] += 1
                journal = next((r["journal"] for r in srows if r["journal"]), None)
                if len(samples) < 3 or (journal and "scientific reports" in journal.lower()):
                    samples.append((sid, journal, by_year, current))
            else:
                conn.execute(update, {"id": sid, "by_year": json.dumps(by_year)})
                counts["updated"] += 1
        if dry_run:
            for issns, ids, detail in issues[:20]:
                print(f"  multi_match {issns} -> {ids} ({detail})")
            for sid, journal, by_year, current in samples[:8]:
                years = sorted(by_year)
                edges = {y: by_year[y] for y in years[:2] + years[-2:]}
                print(f"  sample source {sid} ({journal}): {len(by_year)} years, "
                      f"edges {edges}, current={current}")
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
