"""Butler et al. / ScholCommLab historical APC list prices -> per-year APC
on sources (oxjob #571; v1 doi:10.7910/DVN/CR1MMV, v2 doi:10.7910/DVN/AZ985C,
both CC0).

v2 (2019-2025, 14 publishers, 69,856 rows) renamed every column (lowercase),
added issn_l + AUD + type_of_fee/apc_text, and dropped APC_provided/APC_order
(journal-year duplicates are resolved upstream). parse_file() detects the
layout per file, so v1 reruns still work.  v2 legacy rows have corrupted
original/converted flags, so a v2 parse requires the v1 annual file as a
provenance reference.  v2 remains authoritative for the numeric values.
type_of_fee/apc_text/collector/comment stay file-only (the raw file is archived
on Dataverse; apc_provided is derived from price presence).

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
  gold    sources.apc_prices_by_year -- publisher-original currency prices for
          observed years.  It is additive; apc_usd_by_year remains the USD
          authority and years without trusted original-currency provenance are
          omitted from this map. Exact-zero years use a single USD 0 entry:
          zero means no fee, so choosing among nominal local currencies would
          add no information and could create unstable Work representations.

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
      --dataset-version butler_v2 \
      --v1-reference-file APCdataset-annualAPCs_Published-v1.txt \
      --conversion-rates-file scholcommlab_apc_dataset_2019_2025_conversion_rates.tab \
      [--dry-run] [--skip-fetch]
"""
import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from math import isfinite

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
CURRENCY_ORDER = {currency: position for position, currency in enumerate(CURRENCIES)}
CONVERSION_RELATIVE_TOLERANCE = 0.05
HISTORICAL_PRICE_MAX = Decimal("99999999999999.9999")


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


def normalize_publisher(raw):
    """Comparison-only publisher key for v1/v2 provenance reconciliation."""
    return re.sub(r"[^a-z0-9]", "", (raw or "").lower())


def _float_or_none(raw):
    try:
        return float(raw) if raw not in (None, "") else None
    except (TypeError, ValueError):
        return None


def load_v1_reference(path):
    """Load v1 publisher-original currency provenance for v2 legacy rows.

    v2 rewrote almost every legacy original/converted flag to ``original``.
    The v1 file is therefore used only to identify original currencies; v2
    continues to supply all numeric values.  Two indexes cover stable journal
    ids and the upstream id remaps/publisher transfers present in v2.
    """
    by_key = defaultdict(list)
    by_issn = defaultdict(list)
    row_count = 0
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        header = f.readline()
        f.seek(0)
        reader = csv.DictReader(f, delimiter="\t" if "\t" in header else ",")
        if "ISSN_1" not in set(reader.fieldnames or []):
            raise RuntimeError("--v1-reference-file is not a Butler v1 annual file")
        for reference_id, raw in enumerate(reader, start=1):
            row_count += 1
            issns = normalize_issns([
                normalize_issn(raw.get("ISSN_1")),
                normalize_issn(raw.get("ISSN_2")),
            ])
            original_prices = {}
            for cur in CURRENCIES:
                value = _float_or_none(raw.get(f"APC_{cur}"))
                flag = (raw.get(f"APC_{cur}-originalORconverted") or "").strip()
                if value is not None and flag == "original":
                    original_prices[cur] = value
            ref = {
                "reference_id": reference_id,
                "unique_id": int(raw["unique_id"]),
                "publisher_key": normalize_publisher(raw.get("Publisher")),
                "apc_year": int(raw["APC_year"]),
                "apc_date": (raw.get("APC_date") or "").strip() or None,
                "issns": issns,
                "original_prices": original_prices,
            }
            key = (ref["unique_id"], ref["apc_year"], ref["publisher_key"])
            by_key[key].append(ref)
            for issn in issns:
                by_issn[(ref["apc_year"], ref["publisher_key"], issn)].append(ref)
    if row_count < MIN_ROWS:
        raise RuntimeError(
            f"v1 reference suspiciously small ({row_count} rows < {MIN_ROWS})"
        )
    return {"by_key": by_key, "by_issn": by_issn, "row_count": row_count}


def load_conversion_rates(path):
    """Load the v2 dataset's annual currency conversion matrix."""
    rates = {}
    row_count = 0
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        header = f.readline()
        f.seek(0)
        reader = csv.DictReader(f, delimiter="\t" if "\t" in header else ",")
        fields = set(reader.fieldnames or [])
        required = {"Year", "Source_Currency"} | {f"To_{c}" for c in CURRENCIES}
        if not required <= fields:
            raise RuntimeError("--conversion-rates-file has an unexpected schema")
        for raw in reader:
            row_count += 1
            year = int(raw["Year"])
            source = (raw["Source_Currency"] or "").strip().strip('"').upper()
            if source not in CURRENCY_ORDER:
                raise RuntimeError(f"unexpected conversion source currency {source!r}")
            for target in CURRENCIES:
                value = _float_or_none(raw.get(f"To_{target}"))
                if value is None or value <= 0:
                    raise RuntimeError(
                        f"missing conversion rate year={year} {source}->{target}"
                    )
                rates[(year, source, target)] = value
    if not rates or row_count < len(CURRENCIES):
        raise RuntimeError("conversion-rate file is empty or truncated")
    return rates


def _reference_values_match(ref, v2_row):
    """Whether every v1-original value agrees with the corresponding v2 cell."""
    if not ref["original_prices"]:
        return False
    for currency, old_value in ref["original_prices"].items():
        new_value = _float_or_none(v2_row.get(f"apc_{currency.lower()}"))
        if new_value is None or abs(new_value - old_value) >= 0.005:
            return False
    return True


def resolve_v1_original_currencies(v2_row, reference):
    """Return (currency set, audit basis) for one v2 ``data_version=1`` row.

    Stable ids resolve first.  When v2 remapped a journal id or corrected a
    publisher transfer, direct ISSN + year + publisher finds the corresponding
    v1 record.  Rare same-year duplicates resolve by their original-price
    vector before collection date; if all remaining candidates agree on the
    currency set, that consensus is sufficient.  Anything else fails closed.
    """
    publisher_key = normalize_publisher(v2_row.get("publisher"))
    year = int(v2_row["apc_year"])
    key = (int(v2_row["unique_id"]), year, publisher_key)
    candidates = list(reference["by_key"].get(key, []))
    basis = "v1_reference_key"
    if not candidates:
        direct_issns = normalize_issns([
            normalize_issn(v2_row.get("issn1")),
            normalize_issn(v2_row.get("issn2")),
        ])
        deduped = {}
        for issn in direct_issns:
            for ref in reference["by_issn"].get((year, publisher_key, issn), []):
                deduped[ref["reference_id"]] = ref
        candidates = list(deduped.values())
        basis = "v1_reference_issn"

    if len(candidates) > 1:
        value_matches = [r for r in candidates if _reference_values_match(r, v2_row)]
        if len(value_matches) == 1:
            candidates = value_matches
            basis += "_value_match"
    if len(candidates) > 1:
        raw_date = (v2_row.get("apc_date") or "").split(" ")[0].strip() or None
        date_matches = [r for r in candidates if r["apc_date"] == raw_date]
        if len(date_matches) == 1:
            candidates = date_matches
            basis += "_date_match"
    if len(candidates) > 1:
        currency_sets = {
            tuple(sorted(r["original_prices"], key=CURRENCY_ORDER.get))
            for r in candidates
        }
        if len(currency_sets) == 1:
            return set(next(iter(currency_sets))), basis + "_currency_consensus"

    if len(candidates) != 1:
        raise RuntimeError(
            "could not reconcile v2 legacy original currencies for "
            f"unique_id={v2_row.get('unique_id')} year={v2_row.get('apc_year')} "
            f"publisher={v2_row.get('publisher')!r}; candidates={len(candidates)}"
        )
    return set(candidates[0]["original_prices"]), basis


def infer_corrected_legacy_currencies(v2_row, conversion_rates):
    """Conservatively infer originals added to formerly-unpriced v1 rows.

    For each possible source currency, annual rates identify which other cells
    are compatible with mechanical conversions; unexplained cells plus the
    source are original candidates.  Converted values can themselves act as a
    source because the FX matrix is transitive, so the candidate must also
    equal the independent whole-unit list-price signal used by the shipped
    parser.  On all 35,462 priced v1-overlap rows with known provenance this
    agreement rule accepted 18,009 exact currency sets with zero false-positive
    currencies: 100% membership precision, 45.86% membership recall, and
    50.78% exact-row recall.  Ambiguous rows return no local currencies and
    safely use apc_usd_by_year instead.
    """
    values = {
        currency: _float_or_none(v2_row.get(f"apc_{currency.lower()}"))
        for currency in CURRENCIES
    }
    values = {currency: value for currency, value in values.items() if value is not None}
    if not values or all(value == 0 for value in values.values()):
        return set(), "v2_legacy_zero_or_unpriced"
    if conversion_rates is None:
        raise RuntimeError(
            "corrected v2 legacy prices require --conversion-rates-file"
        )

    year = int(v2_row["apc_year"])
    scored = []
    for source, source_value in values.items():
        explained = set()
        error_sum = 0.0
        for target, target_value in values.items():
            if target == source:
                continue
            rate = conversion_rates.get((year, source, target))
            if rate is None:
                raise RuntimeError(
                    f"missing conversion rate year={year} {source}->{target}"
                )
            expected = source_value * rate
            relative_error = abs(target_value - expected) / max(abs(target_value), 1)
            if relative_error <= CONVERSION_RELATIVE_TOLERANCE:
                explained.add(target)
                error_sum += relative_error
        predicted = {source} | (set(values) - {source} - explained)
        roundness = min(abs(source_value - round(source_value)), 0.499)
        score = (
            len(explained),
            -roundness,
            -len(predicted),
            -CURRENCY_ORDER[source],
            -error_sum,
        )
        scored.append((score, predicted))
    predicted = max(scored, key=lambda item: item[0])[1]
    whole_unit_candidates = {
        currency for currency, value in values.items() if value.is_integer()
    }
    if predicted and predicted == whole_unit_candidates:
        return predicted, "v2_legacy_conversion_inference_confirmed"
    return set(), "v2_legacy_conversion_inference_rejected"


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
            prices.append({
                "currency": cur,
                "price": round(float(val)),
                # Keep the bronze/current-price representation above exactly
                # as shipped, but retain cents for the new historical field.
                "historical_price": float(val),
                "original": True,
                "historical_original": True,
                "historical_original_basis": "v1_flag",
            })
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
        "source_record_id": None,
        "source_data_version": 1,
    }


def parse_row_v2(row, v1_reference=None, conversion_rates=None):
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
    raw_source_data_version = (row.get("data_version") or "").strip()
    if raw_source_data_version not in ("", "1", "2"):
        raise RuntimeError(
            f"unexpected v2 data_version={raw_source_data_version!r} "
            f"for record_id={row.get('record_id')}"
        )
    source_data_version = (
        int(raw_source_data_version) if raw_source_data_version else None
    )
    if source_data_version == 1:
        if v1_reference is None:
            raise RuntimeError(
                "v2 contains legacy data_version=1 rows; "
                "provide --v1-reference-file"
            )
        historical_currencies, legacy_basis = resolve_v1_original_currencies(
            row, v1_reference
        )
        # A small corrected subset was unpriced in v1 but carries positive
        # prices in v2 (including IZA).  There is no v1 original-currency flag
        # to overlay for these rows.  Infer only when the annual conversion
        # matrix and the independent whole-unit signal agree exactly; all
        # other cases intentionally publish USD only.
        if not historical_currencies:
            historical_currencies, legacy_basis = (
                infer_corrected_legacy_currencies(row, conversion_rates)
            )
    else:
        historical_currencies, legacy_basis = set(), None

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
        current_original = flag == "original" and (
            cur in conversion_sources or float(val).is_integer()
        )
        if source_data_version == 1:
            historical_original = cur in historical_currencies
            historical_basis = (
                legacy_basis if historical_original
                else f"{legacy_basis}_not_original"
            )
        elif source_data_version == 2:
            # Direct v2 flags are internally consistent except for one row
            # whose source AUD cell is itself marked "converted from AUD".
            # A currency named as the conversion source is original by
            # construction, even when its own cell is mislabeled.
            historical_original = flag == "original" or cur in conversion_sources
            if flag == "original":
                historical_basis = "v2_flag"
            elif cur in conversion_sources:
                historical_basis = "v2_conversion_source"
            else:
                historical_basis = "v2_converted"
        else:
            # 550 ACS rows have a blank data_version and label every converted
            # currency original.  Keep normalized USD in apc_usd_by_year but
            # do not claim any publisher-original currency without provenance.
            historical_original = False
            historical_basis = "untrusted_missing_data_version"
        prices.append({
            "currency": cur, "price": float(val), "flag": flag,
            # Preserve the shipped heuristic exactly: current apc_prices must
            # not change as a side effect of adding historical currencies.
            "original": current_original,
            "historical_original": historical_original,
            "historical_original_basis": historical_basis,
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
        "source_record_id": (
            int(row["record_id"]) if (row.get("record_id") or "").strip() else None
        ),
        "source_data_version": source_data_version,
    }


def parse_file(
    path,
    dataset_version,
    v1_reference_file=None,
    conversion_rates_file=None,
):
    rows, skipped = [], 0
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        header = f.readline()
        f.seek(0)
        # v1 ships tab-delimited .txt; v2's Dataverse zip ships comma CSV
        # (the .tab re-export is tab). Detect both, per file.
        reader = csv.DictReader(f, delimiter="\t" if "\t" in header else ",")
        fields = set(reader.fieldnames or [])
        if "issn1" in fields:
            if not v1_reference_file:
                raise RuntimeError(
                    "v2 parsing requires --v1-reference-file because its "
                    "legacy original/converted flags are unreliable"
                )
            if not conversion_rates_file:
                raise RuntimeError(
                    "v2 parsing requires --conversion-rates-file to reconcile "
                    "corrected legacy rows"
                )
            v1_reference = load_v1_reference(v1_reference_file)
            conversion_rates = load_conversion_rates(conversion_rates_file)
            row_parser = lambda row: parse_row_v2(
                row, v1_reference, conversion_rates
            )
            is_v2 = True
        elif "ISSN_1" in fields:
            row_parser = parse_row
            is_v2 = False
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
    min_rows = MIN_ROWS_V2 if is_v2 else MIN_ROWS
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
        "apc_source, dataset_version, source_record_id, source_data_version) "
        "VALUES (%(unique_id)s, %(publisher)s, "
        "%(issns)s, %(journal)s, %(oa_status)s, %(apc_provided)s, %(apc_order)s, "
        "%(apc_year)s, %(apc_date)s::date, %(prices)s::jsonb, %(price_usd)s, "
        "%(apc_source)s, %(dataset_version)s, %(source_record_id)s, "
        "%(source_data_version)s)"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE butler_apc_journal_year"))
        psycopg2.extras.execute_batch(
            conn.connection.cursor(), insert, rows, page_size=500)
    print(f"staged {len(rows)} Butler journal-year rows ({dataset_version})", flush=True)


def load_staged(conn):
    return [dict(r._mapping) for r in conn.execute(text(
        "SELECT unique_id, publisher, issns, journal, apc_year, apc_order, "
        "apc_date, prices, price_usd, apc_provided, dataset_version, "
        "source_record_id, source_data_version "
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


def _public_historical_prices(raw_prices):
    """Strip audit fields and return one deterministic entry per currency."""
    by_currency = {}
    for entry in raw_prices:
        if not entry.get("historical_original"):
            continue
        currency = (entry.get("currency") or "").upper()
        # v1's legacy ``price`` field is intentionally rounded because it also
        # feeds the unchanged current apc_prices output.  Its exact source
        # amount lives in historical_price.  v2's price is already exact.
        value = _float_or_none(entry.get("historical_price", entry.get("price")))
        if (
            currency not in CURRENCY_ORDER
            or value is None
            or not isfinite(value)
            or value < 0
        ):
            raise RuntimeError(f"invalid trusted historical APC price: {entry!r}")
        exact_value = Decimal(str(value))
        if exact_value.as_tuple().exponent < -4 or exact_value > HISTORICAL_PRICE_MAX:
            raise RuntimeError(
                "trusted historical APC price does not fit DECIMAL(18,4): "
                f"{entry!r}"
            )
        price = int(value) if value.is_integer() else value
        if currency in by_currency and by_currency[currency] != price:
            raise RuntimeError(
                f"conflicting trusted historical APC prices for {currency}: "
                f"{by_currency[currency]} vs {price}"
            )
        by_currency[currency] = price
    return [
        {"price": by_currency[currency], "currency": currency}
        for currency in CURRENCIES
        if currency in by_currency
    ]


def build_usd_by_year(rows, counts):
    """Row dicts (possibly from several unique_ids on one source) ->
    (USD-by-year, most-recent USD, current apc_prices,
     publisher-original apc_prices_by_year).

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
        return None, None, None, None
    observed = {}  # year -> (usd, raw price entries, historical public prices)
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
        raw_usd = float(best["price_usd"])
        usd = round(raw_usd)
        if raw_usd == 0:
            historical_prices = [{"price": 0, "currency": "USD"}]
        else:
            # a zero local price alongside a positive USD year is a dataset
            # artifact, not a real price -- publishing it would surface
            # {value: 0} Works (Codex review finding 6, 2026-08-26)
            historical_prices = []
            for entry in _public_historical_prices(raw):
                if entry["price"] == 0:
                    counts["zero_local_price_dropped"] = counts.get("zero_local_price_dropped", 0) + 1
                    continue
                historical_prices.append(entry)
        observed[y] = (usd, raw, historical_prices)
        prev_pub = best.get("publisher") or prev_pub
    latest = observed[max(observed)]
    # apc_prices payload: latest year's original-verdict entries (v1 rows
    # carry only originals; v2 rows carry every cell plus the verdict --
    # module doc), walden shape [{"price", "currency"}]; no originals ->
    # the dataset USD value
    prices = ([{"price": round(p["price"]), "currency": p["currency"]}
               for p in latest[1] if p.get("original")]
              or [{"price": latest[0], "currency": "USD"}])
    prices_by_year = {
        str(y): observed[y][2]
        for y in sorted(observed)
        if observed[y][2]
    }
    return (
        {str(y): observed[y][0] for y in sorted(observed)},
        latest[0],
        prices,
        prices_by_year,
    )


def validate_historical_audit(rows):
    """Prevent old staging rows from silently producing an empty new field."""
    for row in rows:
        raw = row.get("prices")
        raw = json.loads(raw) if isinstance(raw, str) else (raw or [])
        if any("historical_original" not in price for price in raw):
            raise RuntimeError(
                "staging predates APC currency reconciliation; reparse with "
                "--file and, for v2, --v1-reference-file plus "
                "--conversion-rates-file before applying"
            )


def apply(dataset_version, rows=None, dry_run=False):
    update = (
        # apc_prices: same fixed shape walden parses, refreshed content
        # (module doc). apc_usd = most recent observed value (Casey ack).
        "UPDATE sources SET apc_usd_by_year = %(by_year)s::jsonb, "
        "apc_prices_by_year = %(prices_by_year)s::jsonb, "
        "apc_usd = %(usd)s, apc_prices = %(prices)s::jsonb, "
        "updated_date = now() WHERE id = %(id)s"
    )
    update_history_only = (
        # recency guard (module doc): stale journals get history only;
        # apc_usd/apc_prices keep the registry's current state (curation
        # may be newer than the dataset's last sighting of the journal).
        "UPDATE sources SET apc_usd_by_year = %(by_year)s::jsonb, "
        "apc_prices_by_year = %(prices_by_year)s::jsonb, "
        "updated_date = now() WHERE id = %(id)s"
    )
    with engine.begin() as conn:
        if rows is None:
            rows = load_staged(conn)
        validate_historical_audit(rows)
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
            by_year, current, prices, prices_by_year = build_usd_by_year(
                srows, counts
            )
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
                        samples.append((
                            sid, journal, by_year, prices_by_year, current, prices
                        ))
            elif stale:
                pending_history.append({
                    "id": sid,
                    "by_year": json.dumps(by_year),
                    "prices_by_year": json.dumps(prices_by_year),
                })
                counts["updated_history_only"] += 1
            else:
                pending.append({"id": sid, "by_year": json.dumps(by_year),
                                "prices_by_year": json.dumps(prices_by_year),
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
            for sid, journal, by_year, prices_by_year, current, prices in samples[:8]:
                years = sorted(by_year)
                edges = {y: by_year[y] for y in years[:2] + years[-2:]}
                print(f"  sample source {sid} ({journal}): {len(by_year)} years, "
                      f"edges {edges}, historical currencies={prices_by_year}, "
                      f"current={current}, apc_prices={prices}")
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
    ap.add_argument(
        "--v1-reference-file",
        help=("v1 annual file used only to recover publisher-original currency "
              "provenance for v2 legacy rows"),
    )
    ap.add_argument(
        "--conversion-rates-file",
        help=("v2 annual conversion-rate matrix used to reconcile corrected "
              "legacy prices that were unpriced in v1"),
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="read-only: parse + match + build, write nothing")
    ap.add_argument("--skip-fetch", action="store_true", help="apply from existing staging")
    args = ap.parse_args()
    rows = None
    if not args.skip_fetch:
        if not args.file:
            ap.error("--file is required unless --skip-fetch")
        rows = parse_file(
            args.file,
            args.dataset_version,
            v1_reference_file=args.v1_reference_file,
            conversion_rates_file=args.conversion_rates_file,
        )
        if not args.dry_run:
            stage(rows, args.dataset_version)
    apply(args.dataset_version, rows=rows, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
